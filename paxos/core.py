import socket
import json


def string_to_address(address):
    addr = address.split(":")
    host = addr[0]
    port = int(addr[1])
    return host, port


class Participant(object):
    """
    Base class for all participating nodes and clients
    """

    def __init__(self, servers):
        self.servers = servers
        self.initial_participants = len(self.servers)

    def run(self):
        raise NotImplementedError()


NODE_CLIENT = 'c'
NODE_PROPOSER = 'p'
NODE_ACCEPTOR = 'a'
NODE_LEARNER = 'l'


class Node(object):
    """
    Stores information about other nodes.
    """

    def __init__(self, address, role=NODE_ACCEPTOR):
        self.address = address
        self.role = role

    def send_message(self, message):
        """
        :param message: Message instance
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            # Connect to server and send data
            sock.connect(string_to_address(self.address))
            sock.sendall(message.serialize())

            # Receive data from the server and shut down
            received = str(sock.recv(1024), "utf-8")
            print('received: %s' % received)
        except ConnectionRefusedError as e:
            print('socket not available: %s' % e)
        finally:
            sock.close()


class Message(object):
    """
    Structures data packets sent between participating nodes.
    """

    MSG_READ = 'read'
    MSG_PROPOSE = 'propose'
    MSG_PROMISE = 'promise'
    MSG_ACCEPT = 'accept'
    MSG_ACCEPTED = 'accepted'

    def __init__(self, **kwargs):
        self.data = kwargs

    def __getattr__(self, key):
        return self.data[key]

    def __setattr__(self, key, value):
        self.data[key] = value

    def serialize(self):
        return bytes(json.dumps(self.data).encode('utf-8'))

    @staticmethod
    def unserialize(raw_data):
        data = json.loads(raw_data, encoding='utf-8')
        obj = Message(**data)
        return obj
