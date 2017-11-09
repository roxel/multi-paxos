import datetime
from time import time

from paxos.core import Participant, Message, Node
from paxos.helpers import address_to_node_id


class Client(Participant):
    """
    Client participating in read, write operations.
    """

    def run(self, key, value=None):
        """
        Run one time operation to read or write to other nodes.
        """
        start_time = datetime.datetime.now()
        print("Starting client at {}".format(start_time))
        # should find leader here
        self.select_leader()
        if value:
            self.write(key, value)
        else:
            self.read(key)
        end_time = time()
        lasted = end_time - start_time.timestamp()
        print("Done. Took %0.3f seconds" % lasted)

    def read(self, key):
        print("READ: key={}".format(key))

    def write(self, key, value):
        print("WRITE: key={}, value={}".format(key, value))
        # TODO: finish write operation

    def select_leader(self):
        """
        Initiate communication with nodes and find leader/proposer for direct connection with him.
        """
        # TODO: implement leader selection algorithm
        for address in self.servers:
            node = Node(address=address, node_id=address_to_node_id(self.servers, address))
            message = Message(issuer_id='0', message_type=Message.MSG_READ)
            node.send_message(message)

        self.leader = None
        return self.leader
