import json
import socket
import sys
from collections import OrderedDict

from paxos.helpers import string_to_address


class Participant(object):
    """
    Base class for all participating processes: servers and clients.
    """

    def __init__(self, servers):
        self.servers = servers
        self._init_configuration()

    def _init_configuration(self):
        self.initial_participants = len(self.servers)
        self.nodes = {}
        for idx, address in enumerate(self.servers):
            self.nodes[idx] = Node(address=address, node_id=idx)
        self.quorum_size = self.initial_participants // 2 + 1

    def run(self, *args, **kwargs):
        """
        Run participant process. The process terminates when this method returns.
        """
        raise NotImplementedError()


class Node(object):
    """
    Stores information about other nodes.
    """

    def __init__(self, address, node_id):
        """
        :param address: node address as string, e.g. '127.0.0.1:9999'
        :param node_id: id as in Message.issuer_id
        """
        self.address = address
        self.node_id = node_id

    def _send_on_socket(self, sock, data):
        received = 'err'
        try:
            # Connect to server and send data
            sock.connect(string_to_address(self.address))
            sock.sendall(data)

            # Receive data from the server and shut down
            received = str(sock.recv(1024), "utf-8")
            # print('%s –> %s' % (self.address, received))
        except ConnectionRefusedError:
            print('%s –> %s' % (self.address, received))
        except socket.timeout:
            print('Socket connected to [ID {}: {}] has timed out'.format(self.node_id, self.address))
        finally:
            sock.close()
        return received

    def send_message(self, message):
        """
        :param message: Message instance
        """

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(1)
        received = self._send_on_socket(sock, data=message.serialize())
        return received


class MessageBase(object):
    """
    Internal message representation.
    Provides serialization mechanism.
    All attributes are stored as dictionary elements.
    """

    def __init__(self):
        self.data = OrderedDict()

    def __getattr__(self, key):
        return self.data[key]

    def __setattr__(self, key, value):
        if key != 'data':
            self.data[key] = value
        else:
            super(MessageBase, self).__setattr__(key, value)

    def serialize(self):
        return bytes(json.dumps(self.data).encode('utf-8'))

    @classmethod
    def unserialize(cls, raw_data):
        data = json.loads(raw_data, encoding='utf-8')
        obj = cls(**data)
        return obj


class Message(MessageBase):
    """
    Structures data packets sent between participating nodes.
    """

    MSG_READ = 'read'
    MSG_WRITE = 'write'
    MSG_PREPARE = 'prepare'
    MSG_PREPARE_NACK = 'prepare-nack'
    MSG_PROMISE = 'promise'
    MSG_ACCEPT_REQUEST = 'accept'
    MSG_ACCEPTED = 'accepted'
    MSG_HEARTBEAT = 'heartbeat'

    def __init__(self, message_type, sender_id=None, prop_num=None, **kwargs):
        super(Message, self).__init__()
        self.message_type = message_type
        self.sender_id = sender_id
        self.prop_num = prop_num
        self.data.update(**kwargs)


class ProposalNumber(object):
    """
    Round number is considered more important
    If ProposalNumber object A has server_id greater than
    the server_id of object B but lesser round_no
    then A < B
    """

    def __init__(self, server_id, round_no):
        self.server_id = server_id
        self.round_no = round_no

    def as_list(self):
        return [self.server_id, self.round_no]

    @staticmethod
    def from_list(t):
        return ProposalNumber(t[0], t[1])

    @staticmethod
    def get_lowest_possible():
        return ProposalNumber(-sys.maxsize - 1, -sys.maxsize - 1)

    def __str__(self):
        return "ProposalNumber<{},{}>".format(self.server_id, self.round_no)

    def __lt__(self, other):
        return (self.round_no < other.round_no) \
               or (self.round_no == other.round_no and self.server_id < other.server_id)

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __eq__(self, other):
        return self.server_id == other.server_id and self.round_no == other.round_no

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return (self.round_no > other.round_no) \
               or (self.round_no == other.round_no and self.server_id > other.server_id)

    def __ge__(self, other):
        return self.__gt__(other) or self.__eq__(other)
