from paxos.core import Message
import sys


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

    def as_tuple(self):
        return self.server_id, self.round_no

    @staticmethod
    def from_tuple(t):
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


class PaxosHandler(object):
    """
    Process Paxos protocol messages received by server.
    """
    HANDLER_FUNCTIONS = {
        Message.MSG_READ: 'on_read',
        Message.MSG_PREPARE: 'on_prepare',
        Message.MSG_PREPARE_NACK: 'on_prepare_nack',
        Message.MSG_PROMISE: 'on_promise',
        Message.MSG_ACCEPT_REQUEST: 'on_accept_request',
        Message.MSG_ACCEPTED: 'on_accepted',
        Message.MSG_HEARTBEAT: 'on_heartbeat'
    }

    def __init__(self, message, server):
        self.message = message
        self.server = server

    def process(self):
        function_name = PaxosHandler.HANDLER_FUNCTIONS[self.message.message_type]
        handler_function = getattr(self, function_name, 'on_null')
        handler_function()

    def on_null(self):
        print('Incorrect message type for message: %s' % self.message.serialize())

    def on_read(self):
        pass

    def on_prepare(self):
        """
        message_type=Message.MSG_PREPARE,
        sender_id=self.id,
        prop_num=(server_id, round_id)
        """
        prop_tuple = self.message.prop_num
        prop_num = ProposalNumber.from_tuple(prop_tuple)
        last_prop_num = self.server.get_highest_prop_num()

        if prop_num > last_prop_num:
            message = Message(
                message_type=Message.MSG_PROMISE,
                sender_id=self.server.id,
                prop_num=prop_tuple,
            )
            self.server.set_highest_prop_num(prop_num)
        else:
            message = Message(
                message_type=Message.MSG_PREPARE_NACK,
                sender_id=self.server.id,
                prop_num=prop_tuple,
                leader_id=self.server.get_leader_id(),
                last_heartbeat=self.server.get_last_heartbeat(),
            )
        self.server.answer_to(message, node_id=self.message.sender_id)

    def on_prepare_nack(self):
        self.server.append_prepare_responses(self.message)

    def on_promise(self):
        self.server.append_prepare_responses(self.message)

    def on_accept_request(self):
        pass

    def on_accepted(self):
        pass

    def on_heartbeat(self):
        self.server.handle_heartbeat(self.message)
