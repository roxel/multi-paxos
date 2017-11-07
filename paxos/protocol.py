from paxos.core import Message

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
        prop_num={natural number}
        """
        prop_num = self.message.prop_num
        last_prop_num = self.server.get_highest_prop_num()
        message = None

        if (prop_num > last_prop_num):
                message = Message(message_type=Message.MSG_PROMISE,
                sender_id=self.server.id,
                prop_num=prop_num
            )
        else:
            message = Message(message_type=Message.MSG_PREPARE_NACK,
                sender_id=self.server.id,
                prop_num=prop_num,
                leader_id=self.server.get_leader_id(),
                last_heartbeat=self.server.get_last_heartbeat()
            )
        self.server.answer_to(message, node_id=self.message.sender_id)

    def on_prepare_nack(self):
        res = (self.message.leader_id, self.message.last_heartbeat)
        self.server.append_prepare_responses(res)

    def on_promise(self):
        pass

    def on_accept_request(self):
        pass

    def on_accepted(self):
        pass

