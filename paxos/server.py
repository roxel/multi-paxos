import socketserver
from paxos.core import Participant, string_to_address, address_to_node_id, Message, Node, synchronized
from paxos.protocol import PaxosHandler
from threading import Timer
from time import sleep
import random


class Server(Participant):
    HEARTBEAT_PERIOD = 2
    HEARTBEAT_TIMEOUT = 3 * HEARTBEAT_PERIOD

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

        self._highest_ballot_number = None
        self._leader_id = None
        self._last_value = None
        self._prepare_responses = []

        self.send_heartbeat_timer = None
        self.heartbeat_timeout_timer = Timer(
            Server.get_randomized_timeout(),
            self.handle_heartbeat_timeout)
        
        self.nodes = []
        for idx, address in enumerate(self.servers):
            if (idx != self.id):
                self.nodes.append(
                        Node(address=address, node_id=address_to_node_id(self.servers, address))
                    )
        self.heartbeat_timeout_timer.start()
        

    def handle_heartbeat_timeout(self):
        """
        First send a test Prepare with low ballot number
        to check if there is a stable leader 
        """    
        print("Hearbeat timeout has passed!")
        print("Sending fake Prepare messages")
        low_ballot_prepare = Message(
            message_type=Message.MSG_PREPARE,
            sender_id=self.id,
            ballot_n="(-1,-1)"
        )
        for node in self.nodes:
            node.send_message(low_ballot_prepare)
        
        """
        Wait a while for NACK messages to come
        and check if there is a stable leader
        """
        (Timer(Server.HEARTBEAT_PERIOD, handle_low_ballot_checking)).start()
        
    def handle_low_ballot_checking(self):
        """
        Count reported leader IDs and heartbeat numbers 
        to find out, if there is a stable leader
        """
        pass
    
    def send_heartbeats(self):
        pass

    def run(self):
        print("Starting server {}".format(self.id))
        server = socketserver.TCPServer((self.host, self.port), Server.TCPHandler)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Terminating server {}".format(self.id))

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            print("Received message from %s:%s" % (self.client_address[0], self.client_address[1]))
            self.data = self.request.recv(1024).strip()
            self.request.sendall(b'ok')
            PaxosHandler(Message.unserialize(self.data)).process()
