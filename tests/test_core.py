import socket
from unittest import TestCase, mock
from paxos.core import Message, Node


class CoreTest(TestCase):

    @mock.patch('paxos.core.Node._send_on_socket')
    def test_send_message_through_node(self, mock_socket):
        mock_socket.return_value = b'ok'
        node = Node(address='127.0.0.1:9999', node_id='99')
        message = Message(issuer_id='1', message_type=Message.MSG_READ)
        response = node.send_message(message)
        self.assertEqual(response, b'ok')


class NodeTest(TestCase):
    pass


class MessageTest(TestCase):

    def test_serialize(self):
        msg = Message(issuer_id='3', message_type=Message.MSG_READ, key='xyz')
        s = msg.serialize()
        self.assertEqual(s, b'{"issuer_id": "3", "message_type": "read", "key": "xyz"}')

    def test_unserialize(self):
        s = b'{"issuer_id": "1", "message_type": "propose", "key": "123"}'
        msg = Message.unserialize(s)
        self.assertEqual(msg.issuer_id, '1')
        self.assertEqual(msg.message_type, Message.MSG_PROPOSE)
        self.assertEqual(msg.key, '123')
