import datetime
from time import time, sleep

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
            if self.leader is None:
                print("No leader has been elected. Can't write any values")
            else:
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
        value = self.quorum_choice(message, 'value')
        print("READ: key={}, value={}".format(key, value))
        return value

    def quorum_choice(self, message, field):
        stats = {}
        for node in self.nodes.values():
            res = node.send_message(message)
            if res != 'err':
                res = Message.unserialize(res)
                field_value = getattr(res, field)
                if field_value not in stats:
                    stats[field_value] = 1
                else:
                    stats[field_value] += 1
        value = self.choose_value(stats)
        return value

    def choose_value(self, stats):
        """
        Chooses correct value based on appearances count
        """
        stats = sorted(stats.items(), key=lambda e: e[1], reverse=True)
        if not stats:
            print('READ ERROR. No responses received.')
        else:
            top_value = stats[0]
            if top_value[1] < self.quorum_size:
                print('READ ERROR. Quorum not satisfied.')
            else:
                return top_value[0]
        return None

    def saved(self, key, value):
        message = Message(message_type=Message.MSG_READ, key=key)
        result = Message.unserialize(self.leader.send_message(message))
        return result.value == bytes(value, encoding='utf-8')

    def write(self, key, value):
        print("WRITE: key={}, value={}".format(key, value))
        message = Message(message_type=Message.MSG_WRITE, key=key, value=value)
        self.leader.send_message(message)
        while not self.saved(key, value):
            sleep(0.1)
        return True

    def find_leader(self):
        """
        Initiate communication with nodes and find leader/proposer for direct connection with him.
        """
        message = Message(message_type=Message.MSG_READ, key='dummy')
        value = self.quorum_choice(message, 'leader_id')
        print("Leader node_id: %s" % value)
        if value:
            self.leader = self.nodes[value]
        return self.leader
