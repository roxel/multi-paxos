import socketserver
from paxos.core import Participant, string_to_address, address_to_node_id, Message, StoreMixin, Node
from paxos.protocol import PaxosHandler
from threading import Timer
import random


class Server(StoreMixin, Participant):
    HEARTBEAT_PERIOD = 5
    HEARTBEAT_TIMEOUT = 2 * HEARTBEAT_PERIOD

    def get_randomized_timeout():
        """
        Wait for an addtional random period in order to
        minimize the risk of selecting multiple leaders
        """
        return Server.HEARTBEAT_TIMEOUT + int(random.random() * Server.HEARTBEAT_PERIOD)

    def __init__(self, address, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.address = address
        self.host, self.port = string_to_address(address)
        self.id = address_to_node_id(self.servers, self.address)
        self.quorum_size = self.initial_participants // 2 + 1
        self.tcp_daemon = None

        self._prop_num_lock = Lock()
        self._last_heartbeat_lock = Lock()
        self._last_value_lock = Lock()
        self._leader_id_lock = Lock()
        self._prepare_responses_lock = Lock()

        self._highest_prop_num = 0
        self._last_heartbeat = 0
        self._last_value = 0
        self._leader_id = None
        self._prepare_responses = []

        self.send_heartbeat_timer = None
        self.heartbeat_timeout_timer = Timer(
            Server.get_randomized_timeout(),
            self.handle_heartbeat_timeout)
        
        self.nodes = {}
        for idx, address in enumerate(self.servers):
            if (idx != self.id):
                self.nodes[idx] = Node(address=address, node_id=idx)

        self.heartbeat_timeout_timer.start()

    def answer_to(self, message, node_id):
        self.nodes[node_id].send_message(message)

    def handle_heartbeat_timeout(self):
        """
        First send a test Prepare with low ballot number
        to check if there is a stable leader 
        """    
        print("Hearbeat timeout!")
        low_prop_num_prepare_msg = Message(
            message_type=Message.MSG_PREPARE,
            sender_id=self.id,
            prop_num=-1
        )
        for node in self.nodes.values():
            node.send_message(low_prop_num_prepare_msg)
        
        """
        Wait a while for NACK messages to come
        and check if there is a stable leader
        """
        Timer(Server.HEARTBEAT_PERIOD, self.handle_low_prop_num).start()
    
    def count_nacks(self):
        with (self._prepare_responses_lock):
            nacks = [res for res in self._prepare_responses if (res.message_type == Message.MSG_PREPARE_NACK)]
            self._prepare_responses = []
        
        top_leader, leader_occurrences, top_heartbeat, heartbeat_occurrences = (None,) * 4
        if (len(nacks) > 0):
            leader_heartbeats = {}
            leader_occurrences = {}
            for nack in nacks:
                leader = nack.leader_id
                heartbeat = nack.last_heartbeat
                if (leader not in leader_occurrences):
                    leader_occurrences[leader] = 1
                else:
                    leader_occurrences[leader] += 1

                if (leader not in leader_heartbeats):
                    leader_heartbeats[leader] = {}
                if (heartbeat not in leader_heartbeats[leader]):
                    leader_heartbeats[leader][heartbeat] = 1
                else:
                    leader_heartbeats[leader][heartbeat] += 1

            top_leader, leader_occurrences = sorted(
                leader_occurrences.items(), key=lambda e: e[1], reverse=True)[0]
            top_heartbeat, heartbeat_occurrences = sorted(
                leader_heartbeats[leader].items(), key=lambda e: e[1], reverse=True)[0]

        return top_leader, leader_occurrences, top_heartbeat, heartbeat_occurrences

    def handle_low_prop_num(self):
        """
        Count reported leader IDs and heartbeat numbers 
        to find out, if there is a stable leader
        prepare_responses = [(leader_id, last_heartbeat)...]
        """

        top_leader, leader_occurrs, top_heartbeat, heartbeat_occurrs = self.count_nacks()

        if (leader_occurrences >= self.quorum_size 
                and heartbeat_occurrences >= self.quorum_size
                and top_heartbeat > self.get_last_heartbeat()):
            """
            Other nodes are connected with a stable leader
            so let's stop the election process and reset 
            the heartbeat timer
            """
            print("A stable leader detected. Stopping election")
            print(leader)
            self.heartbeat_timeout_timer().cancel
            self.heartbeat_timeout_timer(Server.HEARTBEAT_TIMEOUT, self.handle_heartbeat_timeout)
            self.heartbeat_timeout_timer.start()
            return
        else:
            """
            There is no stable leader
            let's send proper Prepare messages
            """
            print("No stable leader detected. Starting election")
            prepare_msg = Message(message_type=Message.MSG_PREPARE,
                sender_id=self.id,
                prop_num=self.next_proposal_num()
            )
            for node in self.nodes.values():
                node.send_message(prepare_msg)
            Timer(Server.HEARTBEAT_TIMEOUT, self.handle_prepare_responses).start()

    def handle_prepare_responses(self):
        """
        Count received Promises and Nacks.
        If a leader is pointed to in at least
        {quorum_size} cases, set a leader
        and reset the heartbeat timer
        """
        pass        


    def next_proposal_num(self):
        return 0
    
    def send_heartbeats(self):
        pass

    """
    Synchronized accessors
    """

    def get_highest_prop_num(self):
        with (self._prop_num_lock):
            return self._highest_prop_num

    def set_highest_prop_num(self, prop_num):
        with (self._prop_num_lock):
            self._highest_prop_num = prop_num

    def get_last_value(self):
        with (self._last_value_lock):
            return self._last_value

    def set_last_value(self, value):
        with (self._last_value_lock):
            self._last_value = value

    def get_leader_id(self):
        with (self._leader_id_lock):
            return self._leader_id

    def set_leader_id(self, leader_id):
        with (self._leader_id_lock):
            self._leader_id = leader_id

    def get_last_heartbeat(self):
        with (self._last_heartbeat_lock):
            return self._last_heartbeat

    def set_last_heartbeat(self, heartbeat):
        with (self._last_heartbeat_lock):
            self._last_heartbeat = heartbeat

    def get_prepare_responses(self):
        with (self._prepare_responses_lock):
            return self._prepare_responses

    def set_prepare_responses(self, prepare_responses):
        with (self._prepare_responses_lock):
            self._prepare_responses = prepare_responses

    def append_prepare_responses(self, res):
        with (self._prepare_responses_lock):
            self._prepare_responses.append(res)

    def clear_prepare_responses(self):
        with (self._prepare_responses_lock):
            self._prepare_responses = []

    """
    """

    def run(self):
        print("Starting server {}".format(self.id))
        self.tcp_daemon = Server.CustomTCPServer((self.host, self.port), Server.TCPHandler, self)
        try:
            self.tcp_server.serve_forever()
        except KeyboardInterrupt:
            print("Terminating server {}".format(self.id))

    def shutdown(self):
        if (self.tcp_daemon):
            self.tcp_daemon.shutdown()
        if (self.heartbeat_timeout_timer and self.heartbeat_timeout_timer.is_alive()):
            self.heartbeat_timeout_timer.cancel()
        if (self.send_heartbeat_timer and self.send_heartbeat_timer.is_alive()):
            self.send_heartbeat_timer.cancel()

    class CustomTCPServer(socketserver.TCPServer):
        def __init__(self, server_address, RequestHandlerClass, paxos_server, bind_and_activate=True):
            self.paxos_server = paxos_server
            socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            print("Received message from %s:%s" % (self.client_address[0], self.client_address[1]))
            self.data = self.request.recv(1024).strip()
            self.request.sendall(b'ok')
            PaxosHandler(Message.unserialize(self.data), self.server.paxos_server).process()