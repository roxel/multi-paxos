

class Participant(object):
    """
    Base class for all participating nodes and clients
    """

    def __init__(self, servers):
        self.servers = servers
        self.initial_participants = len(self.servers)

    def run(self):
        raise NotImplementedError()


class Message(object):
    """
    Structures data packets sent between participating nodes.
    """

    def __init__(self, issuer_id):
        self.issuer_id = issuer_id

        #
        # Damian:
        # - identyfikator danego węzła
        # - faza algorytmu (propose, promise, accept, accepted)
        # - operacja - read, write
        # - jeśli write, to proponowana nowa wartość
        # - jeśli read, to wartość zwracana przez węzeł w fazie accepted
        #
        # key
        # value


