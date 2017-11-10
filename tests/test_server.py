import time
from unittest import TestCase
from paxos.server import Server
from paxos.core import Message, ProposalNumber


class LeaderElectionTest(TestCase):
    def get_servers(self, addr, n=5):
        return ['{}:{}'.format(addr, port) for port in range(8000, 8000 + n)]

    def setUp(self):
        Server.HEARTBEAT_PERIOD = 10000
        self.LOCALHOST = "127.0.0.1"
        self.nodes_no = 10
        self.server_id = 0
        self.SERVERS = self.get_servers(self.LOCALHOST, self.nodes_no)
        self.ADDR = self.SERVERS[self.server_id]
        self.leader_id = 1000
        self.prop_num = ProposalNumber(self.server_id, 1)
        self.last_heartbeat = time.time()
        self.nack = Message(
            message_type=Message.MSG_PREPARE_NACK,
            sender_id=10,
            prop_num=ProposalNumber.get_lowest_possible().as_list(),
            leader_id=self.leader_id,
            last_heartbeat=self.last_heartbeat
        )
        self.heartbeat = Message(
            message_type=Message.MSG_HEARTBEAT,
            sender_id=self.leader_id,
            heartbeat=self.last_heartbeat
        )
        self.promise = Message(
            message_type=Message.MSG_PROMISE,
            sender_id=1,
            prop_num=self.prop_num.as_list()
        )
        self.nacks = [self.nack, self.nack, self.nack, self.nack]
        self.promises = [self.promise] * 10

    def test_count_nacks_top_leader(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.shutdown()
        server.set_prepare_responses(self.nacks)
        top_leader, _, _, _ = server.count_nacks()
        self.assertEqual(top_leader, self.leader_id)

    def test_count_nacks_top_leader_occurrs(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.shutdown()
        server.set_prepare_responses(self.nacks)
        _, leader_occurrs, _, _ = server.count_nacks()
        self.assertEqual(leader_occurrs, 4)

    def test_count_nacks_top_heartbeat(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.shutdown()
        server.set_prepare_responses(self.nacks)
        _, _, top_heartbeat, _ = server.count_nacks()
        self.assertEqual(top_heartbeat, self.last_heartbeat)

    def test_count_nacks_top_heartbeat_occurrs(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.set_prepare_responses(self.nacks)
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

    def test_heartbeat_new_leader(self):
        heartbeat = Message(
            message_type=Message.MSG_HEARTBEAT,
            sender_id=self.leader_id,
            heartbeat=time.time()
        )
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.id = 0
        server.handle_heartbeat(heartbeat)
        leader_id = server.get_leader_id()
        server.shutdown()
        self.assertEqual(self.leader_id, leader_id)

    def test_heartbeat_leader_unchanged(self):
        heartbeat = Message(
            message_type=Message.MSG_HEARTBEAT,
            sender_id=1,
            heartbeat=time.time()
        )
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.id = 10
        old_leader = server.get_leader_id()
        server.handle_heartbeat(heartbeat)
        leader_id = server.get_leader_id()
        server.shutdown()
        self.assertEqual(old_leader, leader_id)

    def test_heartbeat_last_heartbeat_set(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server.handle_heartbeat(self.heartbeat)
        server.shutdown()
        self.assertEqual(self.last_heartbeat, server.get_last_heartbeat())

    def test_next_heartbeat_increasing(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        hb1 = server.next_heartbeat()
        time.sleep(0.05)
        hb2 = server.next_heartbeat()
        server.shutdown()
        self.assertGreater(hb2, hb1)

    def test_handle_prepare_success(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server._prepare_responses = self.promises
        server.handle_prepare_timeout()
        server.shutdown()
        self.assertTrue(server.prepare_phase_completed)

    def test_handle_prepare_failure(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        server._prepare_responses = self.nacks
        server.handle_prepare_timeout()
        server.shutdown()
        self.assertFalse(server.prepare_phase_completed)

    def test_next_prop_num(self):
        server = Server(servers=self.SERVERS, address=self.ADDR)
        prop_num = server.next_proposal_num()
        expected = ProposalNumber(self.server_id, 1)
        server.shutdown()
        self.assertEqual(expected, prop_num)
        self.assertEqual(expected, server.highest_prop_num())
