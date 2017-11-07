from unittest import TestCase
from paxos.core import Message


class CoreTest(TestCase):
    pass


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
