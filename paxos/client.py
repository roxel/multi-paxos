import datetime
from time import time

from paxos.core import Participant, Message, Node
from paxos.helpers import address_to_node_id


class Client(Participant):
    """
    Client participating in read, write operations.
    """

    def __init__(self, *args, **kwargs):
        super(Client, self).__init__(*args, **kwargs)
        self.quorum_size = self.initial_participants // 2 + 1
        self.nodes = {}
        for idx, address in enumerate(self.servers):
            self.nodes[idx] = Node(address=address, node_id=idx)

    def run(self, key, value=None):
        """
        Run one time operation to read or write to other nodes.
        """
        start_time = datetime.datetime.now()
        print("Starting client at {}".format(start_time))
        if value:
            self.write(key, value)
        else:
            self.read(key)
        end_time = time()
        lasted = end_time - start_time.timestamp()
        print("Done. Took %0.3f seconds" % lasted)

    def read(self, key):
        print("READ: key={}".format(key))
        read_msg = Message(
            message_type=Message.MSG_READ,
            sender_i='client',
            key=key
        )
        stats = {}
        for node in self.nodes.values():
            res = Message.unserialize(
                node.send_message(read_msg))
            if res.value not in stats:
                stats[res.value] = 1
            else:
                stats[res.value] += 1
        if len(stats.keys()) == 0:
            print('No response received')
        else:
            stats = sorted(stats.items(), key=lambda e: e[1], reverse=True)[0]
            if stats[1] < self.quorum_size:
                print('Quorum not satisfied')
            else:
                print('Read value: {}'.format(str(stats[0])))

    def write(self, key, value):
        print("WRITE: key={}, value={}".format(key, value))
        # TODO: finish write operation
