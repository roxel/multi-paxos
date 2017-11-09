from unittest import TestCase
from paxos.server import Server
from paxos.core import Message, ProposalNumber
from paxos.protocol import ProposalNumber


class LeaderElectionTest(TestCase):
    def setUp(self):
        self.LOCALHOST = "127.0.0.1"
        self.ADDR = self.LOCALHOST + ":8014"
        self.SERVERS = [self.ADDR]
        self.nack = Message(
            message_type=Message.MSG_PREPARE_NACK,
            sender_id=10,
            prop_num=ProposalNumber.get_lowest_possible().as_tuple(),
            leader_id=10,
            last_heartbeat=100)
        self.prepare_responses = [self.nack, self.nack, self.nack, self.nack]

    def test_count_nacks_top_leader(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses(self.prepare_responses)
        top_leader, _, _, _ = server.count_nacks()
        server.shutdown()
        self.assertEqual(top_leader, 10)

    def test_count_nacks_top_leader_occurrs(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses(self.prepare_responses)
        _, leader_occurrs, _, _ = server.count_nacks()
        server.shutdown()
        self.assertEqual(leader_occurrs, 4)

    def test_count_nacks_top_heartbeat(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses(self.prepare_responses)
        _, _, top_heartbeat, _ = server.count_nacks()
        server.shutdown()
        self.assertEqual(top_heartbeat, 100)

    def test_count_nacks_top_heartbeat_occurrs(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses(self.prepare_responses)
        _, _, _, heartbeat_occurrs = server.count_nacks()
        server.shutdown()
        self.assertEqual(heartbeat_occurrs, 4)

    def test_count_nacks_no_responses(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses([])
        top_leader, _, _, _ = server.count_nacks()
        server.shutdown()
        self.assertEqual(top_leader, None)

        # top_leader, leader_occurrs, top_heartbeat, heartbeat_occurrs
