import random
import time
import socketserver
from threading import Timer, Lock

from paxos.core import Participant, Message, Node
from paxos.helpers import string_to_address, address_to_node_id
from paxos.store import StoreMixin
from paxos.protocol import PaxosHandler, ProposalNumber


class Server(StoreMixin, Participant):
    HEARTBEAT_PERIOD = 0.5
    HEARTBEAT_TIMEOUT = 3 * HEARTBEAT_PERIOD

    def __init__(self, address, redis_host='localhost', redis_port=6379, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.address = address
        self.host, self.port = string_to_address(address)
        self.id = address_to_node_id(self.servers, self.address)
        self.current_node_no = self.initial_participants

        self.redis_host = redis_host
        self.redis_port = redis_port

        self.tcp_daemon = None

        self._init_locks()

        self._own_prop_num = ProposalNumber(self.id, 0)
        self._highest_prepare_msg = Message(message_type=Message.MSG_PREPARE,
                                            sender_id=self.id,
                                            prop_num=self._own_prop_num.as_list(),
                                            key='', value='')
        self._last_heartbeat = 0
        self._leader_id = None
        self._prepare_phase_complete = False

        self.send_heartbeat_timer = None
        self.heartbeat_timeout_timer = None
        self.prepare_timeout_timer = None

        self.nodes = {}
        for idx, address in enumerate(self.servers):
            if idx != self.id:
                self.nodes[idx] = Node(address=address, node_id=idx)

        self.reset_heartbeat_timeout_timer(
            Server.get_randomized_timeout(),
            self.handle_heartbeat_timeout)

    def _init_locks(self):
        self._highest_prepare_msg_lock = Lock()
        self._leader_id_lock = Lock()
        self._last_heartbeat_lock = Lock()
        self._heartbeat_timeout_lock = Lock()
        self._prepare_phase_complete_lock = Lock()
        self._own_prop_num_lock = Lock()

    def get_next_prop_num(self):
        with self._own_prop_num_lock:
            self._own_prop_num = self._own_prop_num.increased()
            return self._own_prop_num

    @property
    def own_prop_num(self):
        with self._own_prop_num_lock:
            return self._own_prop_num

    @own_prop_num.setter
    def own_prop_num(self, prop_num):
        with self._own_prop_num_lock:
            self._own_prop_num = prop_num

    @property
    def highest_prepare_msg(self):
        with self._highest_prepare_msg_lock:
            return self._highest_prepare_msg

    @highest_prepare_msg.setter
    def highest_prepare_msg(self, msg):
        with self._highest_prepare_msg_lock:
            with self._own_prop_num_lock:
                self._highest_prepare_msg = msg
                prop_num = ProposalNumber.from_list(msg.prop_num)
                self._own_prop_num = ProposalNumber(self.id, prop_num.round_no)

    @property
    def leader_id(self):
        with self._leader_id_lock:
            return self._leader_id

    @leader_id.setter
    def leader_id(self, leader_id):
        with self._leader_id_lock:
            self._leader_id = leader_id

    @property
    def last_heartbeat(self):
        with self._last_heartbeat_lock:
            return self._last_heartbeat

    @last_heartbeat.setter
    def last_heartbeat(self, heartbeat):
        with self._last_heartbeat_lock:
            self._last_heartbeat = heartbeat

    @property
    def prepare_phase_complete(self):
        with self._prepare_phase_complete_lock:
            return self._prepare_phase_complete

    @prepare_phase_complete.setter
    def prepare_phase_complete(self, prepare_status):
        with self._prepare_phase_complete_lock:
            self._prepare_phase_complete = prepare_status

    @staticmethod
    def get_randomized_timeout():
        """
        Wait for an additional random period in order to
        minimize the risk of selecting multiple leaders
        """
        return Server.HEARTBEAT_TIMEOUT + int(random.random() * Server.HEARTBEAT_TIMEOUT)

    def handle_heartbeat_timeout(self):
        """
        First send a test Prepare with low proposal number
        to check if there is a stable leader
        """
        print('Hearbeat timeout')
        self.leader_id = None
        low_prop_num_prepare_msg = Message(
            message_type=Message.MSG_PREPARE,
            sender_id=self.id,
            prop_num=ProposalNumber.get_lowest_possible().as_list(),
            key="low-ball",
            value=0
        )

        # Send prepare messages synchronously
        responses = []
        for id, node in self.nodes.items():
            if id != self.leader_id:
                response = Message.unserialize(node.send_immediate(low_prop_num_prepare_msg))
                responses.append(response)
        self.handle_low_prop_num(responses)

    def count_nacks(self, responses):
        nacks = [res for res in responses if (res.message_type == Message.MSG_PREPARE_NACK)]
        top_leader, top_leader_occurrences, top_heartbeat, heartbeat_occurrences = (None,) * 4

        if len(nacks) > 0:
            leader_heartbeats = {}
            leader_occurrences = {}
            for nack in nacks:
                leader = nack.leader_id
                heartbeat = nack.last_heartbeat
                if leader not in leader_occurrences:
                    leader_occurrences[leader] = 1
                else:
                    leader_occurrences[leader] += 1

                if leader not in leader_heartbeats:
                    leader_heartbeats[leader] = {}
                if heartbeat not in leader_heartbeats[leader]:
                    leader_heartbeats[leader][heartbeat] = 1
                else:
                    leader_heartbeats[leader][heartbeat] += 1
            top_leader, top_leader_occurrences = sorted(
                leader_occurrences.items(), key=lambda e: e[1], reverse=True)[0]
            top_heartbeat, heartbeat_occurrences = sorted(
                leader_heartbeats[leader].items(), key=lambda e: e[1], reverse=True)[0]

        return top_leader, top_leader_occurrences, top_heartbeat, heartbeat_occurrences

    def handle_low_prop_num(self, responses):
        """
        Count reported leader IDs and heartbeat numbers
        to find out, if there is a stable leader
        """
        print("[Low-ball Prepare] Counting low-ball responses")
        top_leader, leader_occurrences, top_heartbeat, heartbeat_occurrences = self.count_nacks(responses)
        condition = top_leader is not None \
            and top_leader > self.id \
            and leader_occurrences >= self.quorum_size \
            and heartbeat_occurrences >= self.quorum_size

        if condition:
            """
            Other nodes are connected with a stable leader
            so let's stop the election process and reset
            the heartbeat timer
            """
            print("[Low-ball Prepare] A stable leader detected. Stopping election")
            self.leader_id = top_leader
            self.reset_heartbeat_timeout_timer(
                Server.get_randomized_timeout(),
                self.handle_heartbeat_timeout)
        else:
            """
            There is no stable leader
            Let's assume this node is the
            new leader and send heartbeats to
            others. If there is a node with a
            bigger proposal number, it will
            be set as leader after receiving its
            heartbeat
            """
            self.leader_id = self.id
            self.get_next_prop_num()
            self.send_heartbeats()

    def handle_heartbeat(self, message):
        if message.sender_id > self.id:
            print('[Heartbeat from {}]'.format(message.sender_id))
            if self.send_heartbeat_timer and self.send_heartbeat_timer.is_alive():
                self.send_heartbeat_timer.cancel()
            self.last_heartbeat = message.heartbeat
            self.leader_id = message.sender_id
            self.reset_heartbeat_timeout_timer(
                Server.get_randomized_timeout(),
                self.handle_heartbeat_timeout)

    def next_heartbeat(self):
        return time.time()

    def send_heartbeats(self):
        heartbeat = Message(
            message_type=Message.MSG_HEARTBEAT,
            heartbeat=self.next_heartbeat(),
            sender_id=self.id
        )
        for node in self.nodes.values():
            node.send_immediate(heartbeat)

        self.send_heartbeat_timer = Timer(Server.HEARTBEAT_PERIOD, self.send_heartbeats)
        self.send_heartbeat_timer.start()

    def reset_heartbeat_timeout_timer(self, timeout, job):
        with self._heartbeat_timeout_lock:
            if self.heartbeat_timeout_timer and self.heartbeat_timeout_timer.is_alive():
                self.heartbeat_timeout_timer.cancel()
            self.heartbeat_timeout_timer = Timer(timeout, job)
            self.heartbeat_timeout_timer.start()

    # server methods

    def run(self):
        print("Starting server {}".format(self.id))
        self.tcp_daemon = Server.CustomTCPServer((self.host, self.port), Server.TCPHandler, self)
        try:
            self.tcp_daemon.serve_forever()
        except KeyboardInterrupt:
            print("Terminating server {}".format(self.id))
            self.shutdown()

    def shutdown(self):
        if self.tcp_daemon:
            self.tcp_daemon.shutdown()
        if self.heartbeat_timeout_timer and self.heartbeat_timeout_timer.is_alive():
            self.heartbeat_timeout_timer.cancel()
        if self.send_heartbeat_timer and self.send_heartbeat_timer.is_alive():
            self.send_heartbeat_timer.cancel()

    class CustomTCPServer(socketserver.TCPServer):
        def __init__(self, server_address, RequestHandlerClass, paxos_server, bind_and_activate=True):
            self.paxos_server = paxos_server
            self.allow_reuse_address = True
            socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass,
                                            bind_and_activate=bind_and_activate)

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            self.data = self.request.recv(1024).strip()
            PaxosHandler(Message.unserialize(self.data), self.server.paxos_server, self.request).process()
