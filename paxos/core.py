import json
import socket
import sys
from collections import OrderedDict

from paxos.helpers import string_to_address


IMMEDIATE_TIMOUT = 1        # in seconds
AWAITING_TIMEOUT = 10       # in seconds


class Participant(object):
    """
    Base class for all participating processes: servers and clients.
    """

    def __init__(self, servers):
        self.servers = servers
        self.leader = None
        self._init_configuration()

    def _init_configuration(self):
        self.initial_participants = len(self.servers)
        self.nodes = {}
        for idx, address in enumerate(self.servers):
            self.nodes[idx] = Node(address=address, node_id=idx)

        self.quorum_size = self.initial_participants // 2 + 1

    def answer_to(self, message, node_id):
        self.nodes[node_id].send_message(message)

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
        received = Message(message_type=Message.MSG_ERROR,
                           reason='')
        ex = None
        try:
            # Connect to server and send data
            sock.connect(string_to_address(self.address))
            sock.sendall(data)

            # Receive data from the server and shut down
            received = sock.recv(1024)
        except ConnectionRefusedError as e:
            received.reason = 'ConnectionRefusedError'
            ex = e
            print('%s –> %s' % (self.address, received))
        except socket.timeout as e:
            received.reason = 'Socket has timed out'
            ex = e
            print('Socket connected to [ID {}: {}] has timed out'.format(self.node_id, self.address))
        finally:
            sock.close()
        if ex is not None:
            received = received.serialize()
        return received

    def send_message(self, message, timeout=1):
        """
        :param timeout: socket timeout in seconds
        :type timeout: float
        :param message: Message instance
        :type message: Message
        """

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setblocking(1)
        if timeout is not None:
            sock.settimeout(timeout)
        received = self._send_on_socket(sock, data=message.serialize())
        return received

    def send_immediate(self, message):
        """
        Sends message in immediate mode, meaning the socket will have a small timeout
        allowing only short lived operations, like on_read, on_heartbeat, on_accept_request.
        Server action for these requests are not expected to last long - server can respond
        without contacting other servers.
        """
        return self.send_message(message, timeout=IMMEDIATE_TIMOUT)

    def send_awaiting(self, message):
        """
        Sends message in awaiting mode. Socket will have a bigger timeout than in immediate mode,
        allowing responding server to take long lasting actions, like for example contacting other nodes.
        """
        return self.send_message(message, timeout=AWAITING_TIMEOUT)


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

    def __str__(self):
        return str([(key, value) for key, value in self.data.items() if value is not None])

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

    MSG_READ = 'read'                       # immediate
    MSG_WRITE = 'write'                     # awaiting
    MSG_WRITE_NACK = 'write-nack'           # immediate
    MSG_PREPARE = 'prepare'                 # immediate
    MSG_PREPARE_NACK = 'prepare-nack'       # immediate TODO: handle gently terminating the socket or let it timeout
    MSG_PROMISE = 'promise'                 # immediate TODO: handle gently terminating the socket or let it timeout
    MSG_ACCEPT_REQUEST = 'accept'           # immediate
    MSG_ACCEPT_NACK = 'accept-nack'         # immediate
    MSG_ACCEPTED = 'accepted'               # immediate TODO: handle gently terminating the socket or let it timeout
    MSG_HEARTBEAT = 'heartbeat'             # immediate
    MSG_ERROR = 'error'                     # immediate, response returned by Node._send_on_socket when failed

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

    def increased(self):
        return ProposalNumber(self.server_id, self.round_no + 1)

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
