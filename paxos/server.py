import socketserver
from paxos.core import Participant, string_to_address, address_to_node_id, Message, StoreMixin, Node, synchronized
from paxos.protocol import PaxosHandler
from threading import Timer
import random


class Server(StoreMixin, Participant):
    HEARTBEAT_PERIOD = 1
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

        self._prop_num_lock = Lock()
        self._leader_id_lock = Lock()
        self._last_value_lock = Lock()
        self._prepare_responses_lock = Lock()

        self._highest_prop_num = None
        self._leader_id = None
        self._last_value = None
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

    def handle_heartbeat_timeout(self):
        """
        First send a test Prepare with low ballot number
        to check if there is a stable leader 
        """    
        print("Hearbeat timeout has passed!")
        low_ballot_prepare = Message(
            message_type=Message.MSG_PREPARE,
            sender_id=self.id,
            prop_num="(-1,-1)"
        )
        for node in self.nodes:
            node.send_message(low_ballot_prepare)
        
        """
        Wait a while for NACK messages to come
        and check if there is a stable leader
        """
        (Timer(Server.HEARTBEAT_PERIOD, self.handle_low_ballot_check)).start()
        
    def handle_low_ballot_check(self):
        """
        Count reported leader IDs and heartbeat numbers 
        to find out, if there is a stable leader
        """
        pass
    
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

    def get_prepare_responses(self):
        with (self._prepare_responses_lock):
            return self._prepare_responses

    def set_prepare_responses(self, prepare_responses):
        with (self._prepare_responses_lock):
            self._prepare_responses = prepare_responses

    """
    """

    def answer_to(self, message, node_id):
        self.nodes[node_id].send(message)

    def run(self):
        print("Starting server {}".format(self.id))
        server = socketserver.TCPServer((self.host, self.port), Server.TCPHandler(self))
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Terminating server {}".format(self.id))

    class TCPHandler(socketserver.BaseRequestHandler):
        def __init__(self, server):
            self.server = server

        def handle(self):
            print("Received message from %s:%s" % (self.client_address[0], self.client_address[1]))
            self.data = self.request.recv(1024).strip()
            self.request.sendall(b'ok')
            PaxosHandler(Message.unserialize(self.data), self.server).process()
