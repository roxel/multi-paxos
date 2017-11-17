from paxos.core import Message, ProposalNumber, Node
from collections import Counter


class PaxosHandler(object):
    """
    Process Paxos protocol messages received by server.
    """
    HANDLER_FUNCTIONS = {
        Message.MSG_READ: 'on_read',
        Message.MSG_WRITE: 'on_write',
        Message.MSG_PREPARE: 'on_prepare',
        Message.MSG_ACCEPT_REQUEST: 'on_accept_request',
        Message.MSG_ACCEPTED: 'on_accepted',
        Message.MSG_HEARTBEAT: 'on_heartbeat'
    }

    def __init__(self, message, server, request):
        self.message = message
        self.server = server
        self.request = request
        self.quorum_nodes = {node_id: node for node_id, node in self.server.nodes.items() if node_id != self.server.id}

    def process(self):
        function_name = PaxosHandler.HANDLER_FUNCTIONS.get(self.message.message_type, 'on_null')
        handler_function = getattr(self, function_name, self.on_null)
        handler_function()

    def respond(self, message):
        self.request.sendall(message.serialize())

    def on_null(self):
        print('Incorrect message type for message: %s' % self.message.serialize())

    def on_heartbeat(self):
        self.server.handle_heartbeat(self.message)

    def on_read(self):
        val = self.server.get(self.message.key)
        val = str(val, 'utf-8') if val is not None else ''
        message = Message(message_type=Message.MSG_ACCEPTED,
                          sender_id=self.server.id,
                          leader_id=self.server.leader_id,
                          key=self.message.key,
                          value=val)
        self.respond(message)

    def on_write(self):
        """
        Handles write request. Acting as a proposer.
        """
        print('WRITE REQUEST: key={}, value={}'.format(self.message.key, self.message.value))
        while not self.server.prepare_phase_complete:
            self.make_prepare_phase()
        write_response = self.make_accept_phase()
        self.respond(write_response)

    def make_prepare_phase(self):
        print("PREPARE {}".format(self.message.key))

        # build prepare message
        message = Message(
            message_type=Message.MSG_PREPARE, sender_id=self.server.id,
            key=self.message.key, prop_num=self.server.get_next_prop_num().as_list())

        # send messages to other nodes
        responses = []
        for node_id, node in self.quorum_nodes.items():
            response = Message.unserialize(node.send_immediate(message))
            if response.message_type == Message.MSG_PREPARE_NACK:
                print("PREPARE_NACK {}: {}".format(self.message.key, response))
            responses.append(response.message_type)

        # verify prepare phase statistics
        print("PREPARE {} results: {}".format(self.message.key, responses))
        counter = Counter(responses)
        quorum_achieved = (counter[Message.MSG_PROMISE] >= self.server.quorum_size - 1)
        self.server.prepare_phase_complete = quorum_achieved

        if not self.server.prepare_phase_complete:
            print("PREPARE {} ERROR: Quorum not achieved".format(self.message.key, message))

    def make_accept_phase(self):
        print("ACCEPT {}".format(self.message.key))

        accept_msg = Message(message_type=Message.MSG_ACCEPT_REQUEST, sender_id=self.server.id,
                             prop_num=self.server.own_prop_num.as_list(),
                             key=self.message.key, value=self.message.value)

        # send accept requests to nodes
        responses = []
        for node_id, node in self.quorum_nodes.items():
            response = Message.unserialize(node.send_immediate(accept_msg)).message_type
            responses.append(response)

        # verify accept phase statistics
        counter = Counter(responses)
        if counter[Message.MSG_ACCEPTED] >= self.server.quorum_size - 1:
            print('ACCEPT COMPLETE {}: {}'.format(self.message.key, self.message.value))
            self.server.set(self.message.key, self.message.value)
            write_response = Message(message_type=Message.MSG_ACCEPTED, sender_id=self.server.id,
                                     leader_id=self.server.leader_id,
                                     key=self.message.key, value=self.message.value)
        else:
            print('ACCEPT ERROR {}: Too few Accepted responses'.format(self.message.key))
            write_response = Message(message_type=Message.MSG_WRITE_NACK, sender_id=self.server.id,
                                     key=self.message.key, value=self.message.value)
        return write_response

    def on_prepare(self):
        """
        Handles prepare message. Acting as an acceptor.
        """
        print('PREPARE REQUEST: key={}'.format(self.message.key))

        prop_num = ProposalNumber.from_list(self.message.prop_num)
        last_prop_num = ProposalNumber.from_list(self.server.highest_prepare_msg.prop_num)

        if prop_num >= last_prop_num:
            response = Message(message_type=Message.MSG_PROMISE,
                               sender_id=self.server.id,
                               prop_num=self.message.prop_num)
            self.server.highest_prepare_msg = self.message
        else:
            response = Message(message_type=Message.MSG_PREPARE_NACK,
                               sender_id=self.server.id,
                               prop_num=self.server.highest_prepare_msg.prop_num,
                               leader_id=self.server.leader_id,
                               last_heartbeat=self.server.last_heartbeat)
        self.respond(response)

    def on_accept_request(self):
        """
        Handles accept request sent by proposer, which previously successfully ended prepare-promise phase.
        Send accepted or accepted not acknowledged to proposer by the same socket the accept request was received.
        """
        print('ACCEPT REQUEST: key={}'.format(self.message.key))

        prop_num = ProposalNumber.from_list(self.message.prop_num)
        prepare_msg = self.server.highest_prepare_msg
        condition = (prop_num == ProposalNumber.from_list(prepare_msg.prop_num))
        if condition:
            self.server.set(self.message.key, self.message.value)
            response = Message(message_type=Message.MSG_ACCEPTED,
                               sender_id=self.server.id,
                               prop_num=self.message.prop_num,
                               leader_id=self.server.leader_id,
                               key=self.message.key,
                               value=self.message.value)
            print('ACCEPT COMPLETE {}: {}'.format(self.message.key, self.message.value))
        else:
            response = Message(message_type=Message.MSG_ACCEPT_NACK,
                               sender_id=self.server.id,
                               prop_num=self.message.prop_num,
                               leader_id=self.server.leader_id,
                               leader_prop_num=self.server.highest_prepare_msg.prop_num)
            print('ACCEPT NACK {}: {}'.format(self.message.key, self.message.value))
        self.respond(response)
