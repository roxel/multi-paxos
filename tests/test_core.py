from unittest import TestCase, mock
from paxos.core import Message, Node, ProposalNumber


class CoreTest(TestCase):

    @mock.patch('paxos.core.Node._send_on_socket')
    def test_send_message_through_node(self, mock_socket):
        mock_socket.return_value = b'ok'
        node = Node(address='127.0.0.1:9999', node_id='99')
        message = Message(issuer_id='1', message_type=Message.MSG_READ)
        response = node.send_message(message)
        self.assertEqual(response, b'ok')


class MessageTest(TestCase):

    def test_serialize(self):
        msg = Message(issuer_id='3', message_type=Message.MSG_READ, key='xyz')
        s = msg.serialize()
        expected = b'{"message_type": "read", "sender_id": null, "prop_num": null, "issuer_id": "3", "key": "xyz"}'
        self.assertEqual(s, expected)

    def test_unserialize(self):
        s = b'{"issuer_id": "1", "message_type": "prepare", "key": "123"}'
        msg = Message.unserialize(s)
        self.assertEqual(msg.issuer_id, '1')
        self.assertEqual(msg.message_type, Message.MSG_PREPARE)
        self.assertEqual(msg.key, '123')

    def test_proposal_number_serialization(self):
        msg = Message(message_type=Message.MSG_PREPARE, key='abc', prop_num=[1, 10])
        s = msg.serialize()
        expected = b'{"message_type": "prepare", "sender_id": null, "prop_num": [1, 10], "key": "abc"}'
        self.assertEqual(s, expected)
        msg = Message.unserialize(s)
        self.assertEqual(msg.prop_num, [1, 10])
