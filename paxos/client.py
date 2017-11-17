import datetime
from time import time

from paxos.core import Participant, Message


class Client(Participant):
    """
    Client participating in read, write operations.
    """
    ATTEMPTS = 3

    def run(self, key, value=None):
        """
        Run one time operation to read or write to other nodes.
        """
        start_time = datetime.datetime.now()
        print("Starting client at {}".format(start_time))
        result = False
        for attempt_counter in range(0, Client.ATTEMPTS):
            print("ATTEMPT {}".format(attempt_counter + 1))
            self.find_leader()
            if value:
                if self.leader is None:
                    print("No leader has been elected. Can't write any values")
                else:
                    result = self.write(key, value)
            else:
                result = self.read(key)
            if result:
                break
        if not result:
            print("ERROR. {} attempts failed".format(Client.ATTEMPTS))
        end_time = time()
        lasted = end_time - start_time.timestamp()
        print("DONE. Took %0.3f seconds" % lasted)

    def read(self, key):
        """
        Reads value of a key.
        Read request is sent to all nodes.
        If quorum agrees on a value, the value is treated as correct and returned.
        """
        print("READ REQUEST: key={}".format(key))
        message = Message(message_type=Message.MSG_READ, key=key)
        value = self.quorum_choice(message, 'value')
        if value:
            print("READ COMPLETE: key={}, value={}".format(key, value))
        else:
            print("READ ERROR: Request has failed".format(key, value))
        return value

    def quorum_choice(self, message, field):
        """
        Send message to all nodes and return value responded by majority of nodes, otherwise None.
        """
        stats = {}
        for node in self.nodes.values():
            res = Message.unserialize(node.send_immediate(message))
            if res.message_type != Message.MSG_ERROR:
                field_value = getattr(res, field)
                if field_value not in stats:
                    stats[field_value] = 1
                else:
                    stats[field_value] += 1
        value = self.choose_value(stats)
        return value

    def choose_value(self, stats):
        """
        Chooses correct value based on appearances count.
        """
        stats = sorted(stats.items(), key=lambda e: e[1], reverse=True)
        if not stats:
            print('ERROR. No responses received.')
        else:
            top_value = stats[0]
            if top_value[1] < self.quorum_size:
                print('ERROR. Quorum not satisfied.')
            else:
                return top_value[0]
        return None

    def write(self, key, value):
        print("WRITE REQUEST: key={}, value={}".format(key, value))
        message = Message(message_type=Message.MSG_WRITE, key=key, value=value)
        response = Message.unserialize(self.leader.send_awaiting(message))
        if response.message_type == Message.MSG_ACCEPTED:
            print('WRITE COMPLETE: key={}, value={}'.format(key, value))
            return True
        print('WRITE ERROR: Request has failed')
        print(response)
        return False

    def find_leader(self):
        """
        Initiate communication with nodes and find leader/proposer for direct connection with him.
        """
        message = Message(message_type=Message.MSG_READ, key='dummy')
        value = self.quorum_choice(message, 'leader_id')
        print("LEADER FOUND. Node_id: %s" % value)
        if value:
            self.leader = self.nodes[value]
        return self.leader
