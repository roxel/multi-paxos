from unittest import TestCase
from paxos.core import ProposalNumber
from paxos.core import Message
from paxos.protocol import PaxosHandler


class ProtocolTest(TestCase):

    def test_action_method(self):
        function_name = PaxosHandler.HANDLER_FUNCTIONS.get(Message.MSG_ACCEPTED, 'on_null')
        self.assertEqual(function_name, 'on_accepted')


class ProposalNumberTest(TestCase):
    def test_lt(self):
        self.assertTrue(ProposalNumber(1, 1) < ProposalNumber(1, 2))
        self.assertTrue(ProposalNumber(1, 1) < ProposalNumber(2, 1))
        self.assertTrue(ProposalNumber(1, 1) < ProposalNumber(2, 2))

    def test_le(self):
        self.assertTrue(ProposalNumber(1, 1) <= ProposalNumber(1, 1))
        self.assertTrue(ProposalNumber(1, 1) <= ProposalNumber(1, 2))
        self.assertTrue(ProposalNumber(1, 1) <= ProposalNumber(2, 1))
        self.assertTrue(ProposalNumber(1, 1) <= ProposalNumber(2, 2))

    def test_eq(self):
        self.assertTrue(ProposalNumber(1, 1) == ProposalNumber(1, 1))

    def test_ne(self):
        self.assertTrue(ProposalNumber(1, 1) != ProposalNumber(1, 2))
        self.assertTrue(ProposalNumber(1, 1) != ProposalNumber(2, 1))
        self.assertTrue(ProposalNumber(1, 1) != ProposalNumber(2, 2))

    def test_gt(self):
        self.assertTrue(ProposalNumber(2, 2) > ProposalNumber(1, 1))
        self.assertTrue(ProposalNumber(2, 2) > ProposalNumber(1, 2))

    def test_ge(self):
        self.assertTrue(ProposalNumber(2, 2) >= ProposalNumber(1, 1))
        self.assertTrue(ProposalNumber(2, 2) >= ProposalNumber(1, 2))
        self.assertTrue(ProposalNumber(2, 2) >= ProposalNumber(2, 2))

    def test_as_tuple(self):
        orig = ProposalNumber(1, 2)
        dump = orig.as_list()
        copy = ProposalNumber.from_list(dump)
        self.assertEqual(orig, copy)
