"""Run with python -m unittest discover -s tests -v (no database required)."""

from copy import deepcopy
import time
import unittest

from ddb.raft import Node, RaftError


class MemoryStore:
    def __init__(self):
        self.state = {"term": 0, "voted_for": None, "commit_index": 0, "log": []}
        self.students = {}
        self.available = True

    def load(self):
        self.check_health()
        return deepcopy(self.state)

    def check_health(self):
        if not self.available:
            raise OSError("database unavailable")

    def save_hard_state(self, term, voted_for):
        self.state.update(term=term, voted_for=voted_for)

    def append_entries(self, after, entries):
        assert after >= self.state["commit_index"]
        self.state["log"][after:] = deepcopy(entries)

    def commit_through(self, index):
        for item in self.state["log"][self.state["commit_index"]:index]:
            command = item["command"]
            if command["action"] == "upsert":
                self.students[command["student_id"]] = deepcopy(command)
            elif command["action"] == "delete":
                self.students.pop(command["student_id"], None)
        self.state["commit_index"] = index

    def read_student(self, student_id):
        return deepcopy(self.students.get(student_id))


class Link:
    def __init__(self, cluster, source):
        self.cluster, self.source = cluster, source

    def call(self, node_id, rpc, payload):
        if self.source in self.cluster.isolated or node_id in self.cluster.isolated:
            raise OSError("partitioned")
        try:
            return getattr(self.cluster.nodes[node_id], rpc)(payload)
        except RaftError as error:
            raise OSError("peer unavailable") from error


class RaftTests(unittest.TestCase):
    def make_cluster(self, size, pre_vote=True):
        self.nodes, self.isolated = {}, set()
        members = {str(i): str(i) for i in range(size)}
        for node_id in members:
            node = Node(node_id, members, MemoryStore(), Link(self, node_id),
                        heartbeat_interval=.045, election_min=.19,
                        election_max=.36, operation_timeout=.65, pre_vote=pre_vote)
            self.nodes[node_id] = node
            self.addCleanup(node.stop)
        for node in self.nodes.values():
            node.start()

    def wait_for(self, condition, message):
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            result = condition()
            if result:
                return result
            time.sleep(.02)
        self.fail(message)

    def leader(self):
        def current():
            leaders = [node for name, node in self.nodes.items()
                       if name not in self.isolated and node.status()["role"] == "leader"]
            return leaders[0] if len(leaders) == 1 else None
        return self.wait_for(current, "no unique connected leader")

    @staticmethod
    def write(node, score, student_id=1):
        return node.mutate({"action": "upsert", "student_id": student_id, "name": "Alice",
                            "score": score, "updated_at": "2026-01-01T00:00:00"})

    def test_partition_rejects_old_leader_and_repairs_conflicting_suffix(self):
        self.make_cluster(3)
        old = self.leader()
        self.write(old, 80)
        self.isolated = {old.node_id}
        with self.assertRaises(RaftError):
            self.write(old, 51, student_id=2)
        with self.assertRaises(RaftError):
            old.read(1)
        self.assertEqual(old.read(1, "stale")["record"]["score"], 80)
        new = self.leader()
        self.assertNotEqual(new.node_id, old.node_id)
        index = self.write(new, 95)["index"]
        self.assertEqual(new.read(1)["record"]["score"], 95)
        self.isolated = set()
        self.wait_for(lambda: all(node.status()["commit_index"] >= index
                                  for node in self.nodes.values()), "replicas did not catch up")
        for node in self.nodes.values():
            self.assertEqual(node.read(1, "stale")["record"]["score"], 95)
            self.assertIsNone(node.read(2, "stale")["record"])

    def test_five_members_progress_with_two_missing(self):
        self.make_cluster(5)
        leader = self.leader()
        self.assertEqual(leader.status()["quorum"], 3)
        followers = [name for name in self.nodes if name != leader.node_id]
        self.isolated = set(followers[:2])
        self.write(leader, 78)
        self.assertEqual(leader.read(1)["record"]["score"], 78)
        self.isolated = set(followers[:3])
        with self.assertRaises(RaftError):
            self.write(leader, 90)
        self.assertEqual(leader.read(1, "stale")["record"]["score"], 78)

    def test_pre_vote_keeps_an_isolated_follower_from_advancing_term(self):
        self.make_cluster(3)
        leader = self.leader()
        peer = next(node for node in self.nodes.values() if node is not leader)
        self.wait_for(lambda: peer.status()["leader_id"] == leader.node_id, "no leader heartbeat")
        term = peer.status()["term"]
        self.isolated = {peer.node_id}
        self.wait_for(lambda: peer.status()["leader_id"] is None, "peer did not time out")
        # Allow more than one round of pre-votes to time out without a quorum.
        time.sleep(peer.election_max * 2)
        self.assertEqual(peer.status()["term"], term)
        self.isolated = set()
        self.wait_for(lambda: peer.status()["leader_id"] == leader.node_id, "peer failed to rejoin")
        self.assertEqual(leader.status()["term"], term)

    def test_without_pre_vote_an_isolated_follower_advances_term(self):
        self.make_cluster(3, pre_vote=False)
        leader = self.leader()
        peer = next(node for node in self.nodes.values() if node is not leader)
        self.wait_for(lambda: peer.status()["leader_id"] == leader.node_id, "no leader heartbeat")
        term = peer.status()["term"]
        self.isolated = {peer.node_id}
        self.wait_for(lambda: peer.status()["term"] > term + 1, "peer did not start new terms")

    def test_idle_leader_with_failed_storage_steps_down_and_rejoins(self):
        self.make_cluster(3)
        old = self.leader()
        self.write(old, 80)
        old.store.available = False
        self.wait_for(lambda: not old.status()["healthy"], "idle leader did not detect storage loss")
        new = self.leader()
        self.assertNotEqual(new.node_id, old.node_id)
        self.write(new, 95)
        old.store.available = True
        self.wait_for(lambda: old.status()["healthy"] and old.status()["leader_id"] == new.node_id,
                      "node did not reload and rejoin")
        def caught_up():
            record = old.read(1, "stale")["record"]
            return record is not None and record["score"] == 95
        self.wait_for(caught_up, "recovered node did not apply new write")


if __name__ == "__main__":
    unittest.main()
