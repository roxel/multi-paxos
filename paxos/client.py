import datetime
from time import time

from paxos.core import Participant, Message, Node


class Client(Participant):
    """
    Client participating in read, write operations.
    """
    def run(self, key, value=None):
        start_time = datetime.datetime.now()
        print("Starting client at {}".format(start_time))
        # should find leader here
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
        leader = Node(address='127.0.0.1:9999')         # TODO: algorithm
        message = Message(issuer_id='0', message_type=Message.MSG_READ)
        return leader
