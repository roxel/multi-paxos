import socket
import json
import redis
from threading import Lock


def synchronized(fn):
    _lock = Lock()
    def wrapper():
        with _lock:
            fn()
    return wrapper


def string_to_address(address):
    """
    Change string representation of server address, e.g. 127.0.0.1:9999 to host, port tuple needed for socket API.

    >>> string_to_address('127.0.0.1:9999')
    ('127.0.0.1', 9999)
    """
    addr = address.split(":")
    host = addr[0]
    port = int(addr[1])
    return host, port


def address_to_node_id(servers, address):
    return servers.index(address)


class Participant(object):
    """
    Base class for all participating processes: servers and clients.
    """

    def __init__(self, servers):
        self.servers = servers
        self.initial_participants = len(self.servers)
        self.redis_host = 'localhost'
        self.redis_port = 6379
        self.redis_db_index = 0

    def run(self, *args, **kwargs):
        """
        Run participant process. The process terminates when this method returns.
        """
        raise NotImplementedError()


class StoreMixin(object):
    """
    Provides base for persistent storing of key-value pairs.
    """

    def redis_connection(self):
        return redis.StrictRedis(host=self.redis_host, port=self.redis_port, db=self.id)

    def set(self, key, value):
        r = self.redis_connection()
        result = r.set(key, value)
        return result

    def get(self, key):
        r = self.redis_connection()
        result = r.get(key)
        return result


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
            print('%s –> %s' % (self.address, received))
        except ConnectionRefusedError:
            print('%s –> %s' % (self.address, received))
        finally:
            sock.close()
        return received

    def send_message(self, message):
        """
        :param message: Message instance
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        received = self._send_on_socket(sock, data=message.serialize())
        return received


class Message(object):
    """
    Structures data packets sent between participating nodes.
    """

    MSG_READ = 'read'
    MSG_PREPARE = 'prepare'
    MSG_PROMISE = 'promise'
    MSG_ACCEPT_REQUEST = 'accept'
    MSG_ACCEPTED = 'accepted'

    def __init__(self, **kwargs):
        self.data = kwargs

    def __getattr__(self, key):
        return self.data[key]

    def __setattr__(self, key, value):
        if key != 'data':
            self.data[key] = value
        else:
            super(Message, self).__setattr__(key, value)

    def serialize(self):
        return bytes(json.dumps(self.data).encode('utf-8'))

    @staticmethod
    def unserialize(raw_data):
        data = json.loads(raw_data, encoding='utf-8')
        obj = Message(**data)
        return obj
