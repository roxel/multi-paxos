import datetime
from time import time

from paxos.core import Participant, Message


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
        self.find_leader()
        if value:
            self.write(key, value)
        else:
            self.read(key)
        end_time = time()
        lasted = end_time - start_time.timestamp()
        print("Done. Took %0.3f seconds" % lasted)

    def read(self, key):
        """
        Reads value of a key.
        Read request is sent to all nodes.
        If quorum agrees on a value, the value is treated as correct and returned.
        """
        print("READ: key={}".format(key))
        message = Message(message_type=Message.MSG_READ, key=key)
        stats = {}
        for node in self.nodes.values():
            res = Message.unserialize(
                node.send_message(message))
            if res.value not in stats:
                stats[res.value] = 1
            else:
                stats[res.value] += 1
        value = self.choose_value(stats)
        return value

    def choose_value(self, stats):
        """
        Chooses correct value based on appearances count
        """
        stats = sorted(stats.items(), key=lambda e: e[1], reverse=True)
        if not stats:
            print('READ ERROR. No responses received.'.format(key))
        else:
            top_value = stats[0]
            if top_value[1] < self.quorum_size:
                print('READ ERROR. Quorum not satisfied.')
            else:
                print('Read value: {}'.format(str(top_value[0])))
                return top_value[0]
        return None

    def write(self, key, value):
        print("WRITE: key={}, value={}".format(key, value))
        message = Message(message_type=Message.MSG_WRITE, key=key, value=value)
        self.leader.send_message(message)

    def find_leader(self):
        """
        Initiate communication with nodes and find leader/proposer for direct connection with him.
        """
        # TODO: implement leader finding algorithm
        self.leader = self.nodes[0]
        return self.leader
