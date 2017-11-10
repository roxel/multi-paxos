from paxos.core import Message, ProposalNumber


class PaxosHandler(object):
    """
    Process Paxos protocol messages received by server.
    """
    HANDLER_FUNCTIONS = {
        Message.MSG_READ: 'on_read',
        Message.MSG_WRITE: 'on_write',
        Message.MSG_PREPARE: 'on_prepare',
        Message.MSG_PREPARE_NACK: 'on_prepare_nack',
        Message.MSG_PROMISE: 'on_promise',
        Message.MSG_ACCEPT_REQUEST: 'on_accept_request',
        Message.MSG_ACCEPTED: 'on_accepted',
        Message.MSG_HEARTBEAT: 'on_heartbeat'
    }

    def __init__(self, message, server, request):
        self.message = message
        self.server = server
        self.request = request

    def process(self):
        function_name = PaxosHandler.HANDLER_FUNCTIONS.get(self.message.message_type, 'on_null')
        handler_function = getattr(self, function_name, self.on_null)
        handler_function()

    def on_null(self):
        print('Incorrect message type for message: %s' % self.message.serialize())

    def on_read(self):
        val = self.server.get(self.message.key)
        val = str(val, 'utf-8') if val is not None else ''
        message = Message(message_type=Message.MSG_READ,
                          sender_id=self.server.id, leader_id=self.server.get_leader_id(),
                          key=self.message.key, value=val)
        self.request.sendall(message.serialize())

    def quorum_achieved(self, results):
        return False

    def on_write(self):
        """
        Handles write request. Acting as a proposer.
        """
        print('Client requesting to write: key={}, value={}'.format(self.message.key, self.message.value))
        quorum_nodes = []
        message = Message(
            message_type=Message.MSG_PREPARE, sender_id=self.server.id,
            key=self.message.key, prop_num=self.server.get_highest_prop_num,
        )
        quorum_achieved = False
        while not self.quorum_achieved():
            results = []
            for node in quorum_nodes:
                result = node.send_message(message)
            quorum_achieved = False

    def on_prepare(self):
        """
        Handles prepare message. Acting as an acceptor.
        """
        prop_num = ProposalNumber.from_list(self.message.prop_num)
        last_prop_num = self.server.get_highest_prop_num()
        message = None

        if prop_num > last_prop_num:
            message = Message(message_type=Message.MSG_PROMISE,
                              sender_id=self.server.id,
                              prop_num=self.message.prop_num)
            self.server.set_highest_prop_num(prop_num)
        else:
            message = Message(message_type=Message.MSG_PREPARE_NACK,
                              sender_id=self.server.id,
                              prop_num=self.message.prop_num,
                              leader_id=self.server.get_leader_id(),
                              last_heartbeat=self.server.get_last_heartbeat())
        self.server.answer_to(message, node_id=self.message.sender_id)

    def on_prepare_nack(self):
        """
        Handles prepare_nack message. Acting as a proposer.
        """
        self.server.append_prepare_responses(self.message)

    def on_promise(self):
        """
        Handles promise message. Acting as a proposer.
        """
        self.server.append_prepare_responses(self.message)

    def on_accept_request(self):
        if len(self.server._prepare_responses) >= self.server.quorum_size:
            response = self.server._prepare_responses.get_prepare_response_with_the_highest_num()
            # value = response.value
            prop_num = response.prop_num
        else:
            # value = ...
            prop_num = self.message.prop_num

        message = Message(message_type=Message.MSG_ACCEPT_REQUEST,
                          sender_id=self.server.id,
                          # value
                          prop_num=prop_num,
                          leader_id=self.server.get_leader_id())

        for response in self.server._prepare_responses:
            self.server.answer_to(message, node_id=response.sender_id)

    def on_accepted(self):
        if self.server._highest_prop_num < self.message.prop_num:
            # value =
            message = Message(message_type=Message.MSG_ACCEPTED,
                              sender_id=self.server.id,
                              # value
                              prop_num=self.message.prop_num,
                              leader_id=self.server.get_leader_id())

        for node in self.server.nodes:
            self.server.answer_to(message, node_id=node.node_id)

    def on_heartbeat(self):
        self.server.handle_heartbeat(self.message)
