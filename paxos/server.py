import socketserver
from paxos.core import Participant, string_to_address, address_to_node_id, Message, StoreMixin
from paxos.protocol import PaxosHandler


class Server(StoreMixin, Participant):
    def __init__(self, address, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.address = address
        self.host, self.port = string_to_address(address)
        self.id = address_to_node_id(self.servers, self.address)

    def run(self):
        print("Starting server {}".format(self.id))
        server = socketserver.TCPServer((self.host, self.port), Server.TCPHandler)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Terminating server {}".format(self.id))

    class TCPHandler(socketserver.BaseRequestHandler):
        def handle(self):
            print("Received message from %s:%s" % (self.client_address[0], self.client_address[1]))
            self.data = self.request.recv(1024).strip()
            self.request.sendall(b'ok')
            PaxosHandler(Message.unserialize(self.data)).process()
