from paxos.core import Message


class PaxosHandler(object):
    """
    Process Paxos protocol messages received by server.
    """
    HANDLER_FUNCTIONS = {
        Message.MSG_READ: 'on_read',
        Message.MSG_PREPARE: 'on_prepare',
        Message.MSG_PROMISE: 'on_promise',
        Message.MSG_ACCEPT_REQUEST: 'on_accept_request',
        Message.MSG_ACCEPTED: 'on_accepted',
    }

    def __init__(self, message):
        self.message = message

    def process(self):
        function_name = PaxosHandler.HANDLER_FUNCTIONS[self.message.message_type]
        handler_function = getattr(self, function_name, 'on_null')
        handler_function()

    def on_null(self):
        print('Incorrect message type for message: %s' % self.message.serialize())

    def on_read(self):
        pass

    def on_prepare(self):
        pass

    def on_promise(self):
        pass

    def on_accept_request(self):
        pass

    def on_accepted(self):
        pass
