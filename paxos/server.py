import random
import time
import socketserver
from threading import Timer, Lock

from paxos.core import Participant, Message, Node, ProposalNumber
from paxos.helpers import string_to_address, address_to_node_id
from paxos.store import StoreMixin
from paxos.protocol import PaxosHandler, ProposalNumber


class Server(StoreMixin, Participant):
    HEARTBEAT_PERIOD = 1
    HEARTBEAT_TIMEOUT = 4 * HEARTBEAT_PERIOD

    def __init__(self, address, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.address = address
        self.host, self.port = string_to_address(address)
        self.id = address_to_node_id(self.servers, self.address)
        self.quorum_size = self.initial_participants // 2 + 1
        self.tcp_daemon = None

        self._prop_num_lock = Lock()
        self._own_prop_num_lock = Lock()
        self._last_heartbeat_lock = Lock()
        self._last_value_lock = Lock()
        self._leader_id_lock = Lock()
        self._prepare_responses_lock = Lock()
        self._heartbeat_timeout_lock = Lock()

        self._highest_prop_num = ProposalNumber(self.id, 0)
        self._own_prop_num = ProposalNumber(self.id, 0)
        self._last_heartbeat = 0
        self._last_value = 0
        self._leader_id = None
        self._prepare_responses = []

        self.send_heartbeat_timer = None
        self.heartbeat_timeout_timer = None

        self.nodes = {}
        for idx, address in enumerate(self.servers):
            if idx != self.id:
                self.nodes[idx] = Node(address=address, node_id=idx)
        self.reset_heartbeat_timeout_timer(
            Server.get_randomized_timeout(),
            self.handle_heartbeat_timeout)

    @staticmethod
    def get_randomized_timeout():
        """
        Wait for an additional random period in order to
        minimize the risk of selecting multiple leaders
        """
        return Server.HEARTBEAT_TIMEOUT + int(random.random() * Server.HEARTBEAT_TIMEOUT)

    def answer_to(self, message, node_id):
        self.nodes[node_id].send_message(message)

    def handle_heartbeat_timeout(self):
        """
        First send a test Prepare with low proposal number
        to check if there is a stable leader
        """
        print('Hearbeat timeout')
        self.set_leader_id(None)
        low_prop_num_prepare_msg = Message(
            message_type=Message.MSG_PREPARE,
            sender_id=self.id,
            prop_num=ProposalNumber.get_lowest_possible().as_tuple()
        )
        # Wait a while for NACK messages to come
        # and check if there is a stable leader
        self.reset_heartbeat_timeout_timer(
            Server.HEARTBEAT_PERIOD,
            self.handle_low_prop_num)
        for id, node in self.nodes.items():
            # If there is no heartbeat signal from
            # the leader node, don't send messages
            # to it
            if id != self.get_leader_id():
                node.send_message(low_prop_num_prepare_msg)

    def count_nacks(self):
        nacks = [res for res in self._prepare_responses if (res.message_type == Message.MSG_PREPARE_NACK)]
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

    def handle_low_prop_num(self):
        """
        Count reported leader IDs and heartbeat numbers
        to find out, if there is a stable leader
        prepare_responses = [(leader_id, last_heartbeat)...]
        """

        print("[Low-ball Prepare] Counting low-ball responses")
        with self._prepare_responses_lock:
            top_leader, leader_occurrences, top_heartbeat, heartbeat_occurrences = self.count_nacks()
            self._prepare_responses = []

        condition = top_leader is not None and leader_occurrences >= self.quorum_size \
            and heartbeat_occurrences >= self.quorum_size
        if condition:
            """
            Other nodes are connected with a stable leader
            so let's stop the election process and reset
            the heartbeat timer
            """
            print("[Low-ball Prepare] A stable leader detected. Stopping election")
            self.set_leader_id(top_leader)
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
            self.send_heartbeats()

    def handle_heartbeat(self, message):
        if message.sender_id > self.id:
            print('[Heartbeat from {}]'.format(message.sender_id))
            if self.send_heartbeat_timer and self.send_heartbeat_timer.is_alive():
                self.send_heartbeat_timer.cancel()
            self.set_last_heartbeat(message.heartbeat)
            self.set_leader_id(message.sender_id)
            self.reset_heartbeat_timeout_timer(
                Server.get_randomized_timeout(),
                self.handle_heartbeat_timeout)

    def next_proposal_num(self):
        with self._own_prop_num_lock:
            with self._prop_num_lock:
                self._own_prop_num = ProposalNumber(self.id, self._own_prop_num.round_no + 1)
                if self._own_prop_num > self._highest_prop_num:
                    self._highest_prop_num = self._own_prop_num
                return self._highest_prop_num

    def next_heartbeat(self):
        return time.time()

    def send_heartbeats(self):
        heartbeat = Message(
            message_type=Message.MSG_HEARTBEAT,
            heartbeat=self.next_heartbeat(),
            sender_id=self.id
        )
        for node in self.nodes.values():
            node.send_message(heartbeat)

        self.send_heartbeat_timer = Timer(Server.HEARTBEAT_PERIOD, self.send_heartbeats)
        self.send_heartbeat_timer.start()

    # Synchronized accessors

    def get_highest_prop_num(self):
        with self._prop_num_lock:
            return self._highest_prop_num

    def set_highest_prop_num(self, prop_num):
        with self._prop_num_lock:
            self._highest_prop_num = prop_num

    def get_own_prop_num(self):
        with self._prop_num_lock:
            return self._own_prop_num

    def set_own_prop_num(self, prop_num):
        with self._prop_num_lock:
            self._own_prop_num = prop_num

    def get_last_value(self):
        with self._last_value_lock:
            return self._last_value

    def set_last_value(self, value):
        with self._last_value_lock:
            self._last_value = value

    def get_leader_id(self):
        with self._leader_id_lock:
            return self._leader_id

    def set_leader_id(self, leader_id):
        with self._leader_id_lock:
            self._leader_id = leader_id

    def get_last_heartbeat(self):
        with self._last_heartbeat_lock:
            return self._last_heartbeat

    def set_last_heartbeat(self, heartbeat):
        with self._last_heartbeat_lock:
            self._last_heartbeat = heartbeat

    def get_prepare_responses(self):
        with self._prepare_responses_lock:
            return self._prepare_responses

    def set_prepare_responses(self, prepare_responses):
        with self._prepare_responses_lock:
            self._prepare_responses = prepare_responses

    def get_prepare_response_with_the_highest_num(self):
        with (self._prepare_responses_lock):
            N = self._prepare_responses[0].prop_num
            response = self._prepare_responses[0]
            for prepare_response in self._prepare_responses:
                if(prepare_response.prop_num > N):
                    N = prepare_response.prop_num
                    response = prepare_response.prop_num
            return response

    def append_prepare_responses(self, res):
        with self._prepare_responses_lock:
            self._prepare_responses.append(res)
            print("Received a response to a Prepare message")

    def clear_prepare_responses(self):
        with self._prepare_responses_lock:
            self._prepare_responses = []

    def reset_heartbeat_timeout_timer(self, timeout, job):
        with self._heartbeat_timeout_lock:
            if self.heartbeat_timeout_timer and self.heartbeat_timeout_timer.is_alive():
                self.heartbeat_timeout_timer.cancel()
            self.heartbeat_timeout_timer = Timer(timeout, job)
            self.heartbeat_timeout_timer.start()

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
            socketserver.TCPServer.__init__(self, server_address, RequestHandlerClass, bind_and_activate=True)

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            # print("Received message from %s:%s" % (self.client_address[0], self.client_address[1]))
            self.data = self.request.recv(1024).strip()
            PaxosHandler(Message.unserialize(self.data), self.server.paxos_server, self.request).process()
