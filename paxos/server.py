from paxos.core import Participant


class Server(Participant):
    """

    """

    def __init__(self, address, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.address = address
        self.id = self.servers.index(self.address)

    def run(self):
        print("Starting server {}".format(self.id))
