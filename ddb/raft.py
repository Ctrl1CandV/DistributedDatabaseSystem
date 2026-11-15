"""Raft consensus for a small, fixed membership cluster.

The transport supplies bounded HTTP RPC calls; this module never holds the
state lock while performing network I/O. Log indexes exposed on the wire are
one based, with index zero representing the empty prefix.
"""
import copy
import random
import threading
import time


class RaftError(Exception):
    def __init__(self, message, status=503, leader_id=None):
        super().__init__(message)
        self.status = status
        self.leader_id = leader_id


class Node:
    def __init__(self, node_id, peers, store, transport, *, heartbeat_interval=0.15,
                 election_min=0.55, election_max=0.9, operation_timeout=4.0, pre_vote=True):
        if node_id not in peers or not peers:
            raise ValueError('peers 必须包含本节点')
        if not 0 < heartbeat_interval < election_min <= election_max or operation_timeout <= 0:
            raise ValueError('无效的超时配置')
        self.node_id, self.peers = node_id, dict(peers)
        self.store, self.transport = store, transport
        self.heartbeat_interval, self.election_min = heartbeat_interval, election_min
        self.election_max, self.operation_timeout = election_max, operation_timeout
        self.pre_vote_enabled = pre_vote
        self.quorum = len(peers) // 2 + 1
        self._cv = threading.Condition(threading.RLock())
        self._running = False
        self._healthy = False
        self._thread = None
        self._inflight = set()
        self._pre_vote_inflight = set()
        self._vote_inflight = set()
        self._health_generation = 0
        self._probe_inflight = False
        self._recovery_inflight = False
        self._probe_at = 0.0
        self._campaign = 0
        self._last_leader_contact = None
        self.role, self.leader_id = 'follower', None
        self.term, self.voted_for, self.commit_index, self.log = 0, None, 0, []
        self.next_index, self.match_index = {}, {}
        self._deadline = 0.0
        self._heartbeat_due = 0.0
        self._retry_at = 0.0
        self._reset_deadline()

    def _reset_deadline(self):
        self._deadline = time.monotonic() + random.uniform(self.election_min, self.election_max)

    def _recover(self, state):
        term, voted_for = state['term'], state['voted_for']
        commit, log = state['commit_index'], copy.deepcopy(state['log'])
        if not isinstance(term, int) or not isinstance(commit, int) or not 0 <= commit <= len(log):
            raise OSError('invalid persistent state')
        self.term, self.voted_for, self.commit_index, self.log = term, voted_for, commit, log
        self.role, self.leader_id = 'follower', None
        self.next_index, self.match_index = {}, {}
        self._campaign += 1
        self._last_leader_contact = None
        self._healthy = True
        self._health_generation += 1
        self._probe_at = time.monotonic() + 0.5
        self._reset_deadline()
        self._cv.notify_all()

    def _failed(self):
        self._health_generation += 1
        self._healthy = False
        self._retry_at = time.monotonic() + 0.5
        self.role, self.leader_id = 'follower', None
        self._cv.notify_all()

    def _load_worker(self, generation):
        try:
            state = self.store.load()
        except OSError:
            state = None
        with self._cv:
            self._recovery_inflight = False
            if not self._running or generation != self._health_generation or self._healthy:
                return
            if state is not None:
                try:
                    self._recover(state)
                except OSError:
                    self._retry_at = time.monotonic() + 0.5
            else:
                self._retry_at = time.monotonic() + 0.5
            self._cv.notify_all()

    def _probe_worker(self, generation):
        try:
            self.store.check_health()
            failed = False
        except OSError:
            failed = True
        with self._cv:
            self._probe_inflight = False
            if self._running and self._healthy and generation == self._health_generation:
                if failed:
                    self._failed()
                else:
                    self._probe_at = time.monotonic() + 0.5
            self._cv.notify_all()

    def start(self):
        with self._cv:
            if self._running:
                return
            self._health_generation += 1
            self._running, self._healthy = True, False
            try:
                self._recover(self.store.load())
            except OSError:
                self._failed()
            self._thread = threading.Thread(target=self._run, name=f'raft-{self.node_id}', daemon=True)
            self._thread.start()

    def stop(self):
        with self._cv:
            self._running = False
            self._health_generation += 1
            self._healthy = False
            self.role, self.leader_id = 'follower', None
            self._cv.notify_all()
            thread = self._thread
        if thread and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.heartbeat_interval * 2))

    def _last_term(self):
        return self.log[-1]['term'] if self.log else 0

    def _step_down(self, term, leader=None):
        self._campaign += 1
        if term > self.term:
            self.store.save_hard_state(term, None)
            self.term, self.voted_for = term, None
        self.role, self.leader_id = 'follower', leader
        self._reset_deadline()
        self._cv.notify_all()

    def _require_healthy(self):
        if not self._running or not self._healthy:
            raise RaftError('节点存储不可用')

    def pre_vote(self, payload):
        """Ask about a prospective election without changing persistent state."""
        with self._cv:
            self._require_healthy()
            recent_leader = (self.role == 'leader' or
                             self._last_leader_contact is not None and
                             time.monotonic() - self._last_leader_contact < self.election_min)
            up_to_date = ((payload['last_log_term'], payload['last_log_index']) >=
                          (self._last_term(), len(self.log)))
            granted = (payload['candidate_id'] in self.peers and
                       payload['term'] >= self.term + 1 and up_to_date and not recent_leader)
            return {'term': self.term, 'vote_granted': granted, 'campaign': payload['campaign']}

    def vote(self, payload):
        with self._cv:
            self._require_healthy()
            try:
                term = payload['term']
                if term > self.term:
                    self._step_down(term)
                granted = False
                if term == self.term and payload['candidate_id'] in self.peers:
                    current = (self._last_term(), len(self.log))
                    candidate = (payload['last_log_term'], payload['last_log_index'])
                    if candidate >= current and self.voted_for in (None, payload['candidate_id']):
                        self.store.save_hard_state(self.term, payload['candidate_id'])
                        self.voted_for = payload['candidate_id']
                        self._reset_deadline()
                        granted = True
                return {'term': self.term, 'vote_granted': granted}
            except OSError:
                self._failed()
                raise RaftError('节点存储不可用')

    def append(self, payload):
        with self._cv:
            self._require_healthy()
            try:
                term, leader = payload['term'], payload['leader_id']
                if leader not in self.peers:
                    return {'term': self.term, 'success': False}
                if term < self.term:
                    return {'term': self.term, 'success': False}
                self._step_down(term, leader)
                self._last_leader_contact = time.monotonic()
                prev, prev_term = payload['prev_log_index'], payload['prev_log_term']
                if prev < 0 or prev > len(self.log):
                    return {'term': self.term, 'success': False, 'conflict_index': len(self.log) + 1}
                if prev and self.log[prev - 1]['term'] != prev_term:
                    conflict_term = self.log[prev - 1]['term']
                    first = prev
                    while first > 1 and self.log[first - 2]['term'] == conflict_term:
                        first -= 1
                    return {'term': self.term, 'success': False, 'conflict_index': first}
                entries = payload['entries']
                offset = 0
                while offset < len(entries) and prev + offset < len(self.log):
                    if self.log[prev + offset]['term'] != entries[offset]['term']:
                        break
                    offset += 1
                after = prev + offset
                if offset < len(entries):
                    if after < self.commit_index:
                        return {'term': self.term, 'success': False, 'conflict_index': self.commit_index + 1}
                    tail = copy.deepcopy(entries[offset:])
                    self.store.append_entries(after, tail)
                    self.log = self.log[:after] + tail
                # Only the prefix covered by this RPC is known to agree.
                target = min(payload['leader_commit'], prev + len(entries))
                if target > self.commit_index:
                    self.store.commit_through(target)
                    self.commit_index = target
                    self._cv.notify_all()
                return {'term': self.term, 'success': True, 'match_index': prev + len(entries)}
            except OSError:
                self._failed()
                raise RaftError('节点存储不可用')

    def _start_pre_vote(self):
        self._campaign += 1
        campaign = self._campaign
        self.role, self.leader_id = 'follower', None
        self._reset_deadline()
        self._prevotes = {self.node_id}
        payload = {'term': self.term + 1, 'candidate_id': self.node_id,
                   'last_log_index': len(self.log), 'last_log_term': self._last_term(),
                   'campaign': campaign}
        if self.quorum == 1:
            self._start_election()
            return
        for peer in self.peers:
            if peer != self.node_id and peer not in self._pre_vote_inflight:
                self._pre_vote_inflight.add(peer)
                threading.Thread(target=self._request_pre_vote, args=(peer, payload), daemon=True).start()

    def _request_pre_vote(self, peer, payload):
        try:
            reply = self.transport.call(peer, 'pre_vote', payload)
        except OSError:
            reply = None
        with self._cv:
            self._pre_vote_inflight.discard(peer)
            if not self._running or not self._healthy or reply is None:
                return
            try:
                if reply['term'] > self.term:
                    self._step_down(reply['term'])
                elif (self._campaign == payload['campaign'] and
                      reply.get('campaign') == payload['campaign'] and
                      self.term + 1 == payload['term'] and self.role == 'follower' and
                      reply.get('vote_granted')):
                    self._prevotes.add(peer)
                    if len(self._prevotes) >= self.quorum:
                        self._start_election()
            except OSError:
                self._failed()

    def _start_election(self):
        self._campaign += 1
        try:
            term = self.term + 1
            self.store.save_hard_state(term, self.node_id)
            self.term, self.voted_for = term, self.node_id
            self.role, self.leader_id = 'candidate', None
            self._reset_deadline()
            self._votes = {self.node_id}
            payload = {'term': term, 'candidate_id': self.node_id,
                       'last_log_index': len(self.log), 'last_log_term': self._last_term()}
            if len(self._votes) >= self.quorum:
                self._become_leader()
            else:
                for peer in self.peers:
                    if peer != self.node_id and peer not in self._vote_inflight:
                        self._vote_inflight.add(peer)
                        threading.Thread(target=self._request_vote, args=(peer, payload), daemon=True).start()
        except OSError:
            self._failed()

    def _request_vote(self, peer, payload):
        try:
            reply = self.transport.call(peer, 'vote', payload)
        except OSError:
            reply = None
        with self._cv:
            self._vote_inflight.discard(peer)
            if not self._running or not self._healthy or reply is None:
                return
            try:
                if reply['term'] > self.term:
                    self._step_down(reply['term'])
                elif self.role == 'candidate' and self.term == payload['term'] and reply.get('vote_granted'):
                    self._votes.add(peer)
                    if len(self._votes) >= self.quorum:
                        self._become_leader()
            except OSError:
                self._failed()

    def _become_leader(self):
        self.role, self.leader_id = 'leader', self.node_id
        self.next_index = {p: len(self.log) + 1 for p in self.peers if p != self.node_id}
        self.match_index = {p: 0 for p in self.peers}
        # A committed entry in the new term establishes authority and commits
        # any inherited prefix once a quorum has accepted it.
        self._append_local({'action': 'noop'})
        self._heartbeat_due = 0
        self._cv.notify_all()

    def _append_local(self, command):
        entry = {'term': self.term, 'command': copy.deepcopy(command)}
        self.store.append_entries(len(self.log), [entry])
        self.log.append(entry)
        self.match_index[self.node_id] = len(self.log)
        self._advance_commit()
        return len(self.log)

    def _advance_commit(self):
        # The quorum-th largest replicated index is the largest index known
        # to be stored on a majority. Raft may directly commit it only when
        # it belongs to this leader's term.
        replicated = sorted((self.match_index.get(p, 0) for p in self.peers), reverse=True)
        index = replicated[self.quorum - 1]
        if index > self.commit_index and self.log[index - 1]['term'] == self.term:
            self.store.commit_through(index)
            self.commit_index = index
            self._cv.notify_all()

    def _replicate(self, peer, payload, term):
        try:
            reply = self.transport.call(peer, 'append', payload)
        except OSError:
            reply = None
        with self._cv:
            self._inflight.discard(peer)
            if not self._running or not self._healthy or reply is None:
                return
            try:
                if reply['term'] > self.term:
                    self._step_down(reply['term'])
                elif self.role == 'leader' and self.term == term:
                    if reply.get('success'):
                        matched = payload['prev_log_index'] + len(payload['entries'])
                        self.match_index[peer] = max(self.match_index.get(peer, 0), matched)
                        self.next_index[peer] = matched + 1
                        old = self.commit_index
                        self._advance_commit()
                        if self.commit_index != old or matched < len(self.log):
                            self._heartbeat_due = 0
                    else:
                        old_next = self.next_index[peer]
                        hint = reply.get('conflict_index', old_next - 1)
                        self.next_index[peer] = max(1, min(old_next - 1, hint))
                        self._heartbeat_due = 0
                self._cv.notify_all()
            except OSError:
                self._failed()

    def _broadcast(self):
        for peer in self.peers:
            if peer == self.node_id or peer in self._inflight:
                continue
            index = self.next_index[peer]
            prev = index - 1
            payload = {'term': self.term, 'leader_id': self.node_id, 'prev_log_index': prev,
                       'prev_log_term': self.log[prev - 1]['term'] if prev else 0,
                       'entries': copy.deepcopy(self.log[prev:prev + 64]), 'leader_commit': self.commit_index}
            self._inflight.add(peer)
            threading.Thread(target=self._replicate, args=(peer, payload, self.term), daemon=True).start()

    def _run(self):
        while True:
            with self._cv:
                if not self._running:
                    return
                now = time.monotonic()
                if not self._healthy:
                    if now >= self._retry_at and not self._recovery_inflight:
                        self._recovery_inflight = True
                        threading.Thread(target=self._load_worker, args=(self._health_generation,), daemon=True).start()
                else:
                    if now >= self._probe_at and not self._probe_inflight:
                        self._probe_inflight = True
                        threading.Thread(target=self._probe_worker, args=(self._health_generation,), daemon=True).start()
                if self._healthy and self.role == 'leader':
                    if now >= self._heartbeat_due:
                        self._heartbeat_due = now + self.heartbeat_interval
                        self._broadcast()
                elif self._healthy and now >= self._deadline:
                    if self.pre_vote_enabled:
                        self._start_pre_vote()
                    else:
                        self._start_election()
                self._cv.wait(timeout=min(self.heartbeat_interval / 3, 0.05))

    def mutate(self, command):
        deadline = time.monotonic() + self.operation_timeout
        with self._cv:
            self._require_healthy()
            if self.role != 'leader':
                raise RaftError('当前节点不是领导者', leader_id=self.leader_id)
            term = self.term
            try:
                index = self._append_local(command)
            except OSError:
                self._failed()
                raise RaftError('节点存储不可用')
            self._heartbeat_due = 0
            self._cv.notify_all()
            while self.commit_index < index:
                self._require_healthy()
                if self.role != 'leader' or self.term != term:
                    raise RaftError('领导者已变更，写入结果未确认', leader_id=self.leader_id)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RaftError('等待多数副本提交超时，写入结果未确认', leader_id=self.leader_id)
                self._cv.wait(remaining)
            # A former leader's uncommitted entry may have been overwritten
            # and another entry committed at the same index while we waited.
            if len(self.log) < index or self.log[index - 1]['term'] != term:
                raise RaftError('领导者已变更，写入结果未确认', leader_id=self.leader_id)
            return {'index': index, 'term': term}

    def read(self, student_id, mode='linearizable'):
        if mode not in ('linearizable', 'stale'):
            raise RaftError('未知读取模式', status=400)
        barrier = self.mutate({'action': 'noop'}) if mode == 'linearizable' else None
        with self._cv:
            self._require_healthy()
            if barrier and (self.role != 'leader' or self.term != barrier['term']):
                raise RaftError('领导者已变更', leader_id=self.leader_id)
            try:
                record = self.store.read_student(student_id)
            except OSError:
                self._failed()
                raise RaftError('节点存储不可用')
            return {'record': record, 'index': self.commit_index, 'node_id': self.node_id, 'mode': mode}

    def status(self):
        with self._cv:
            return {'node_id': self.node_id, 'role': self.role, 'term': self.term,
                    'leader_id': self.leader_id, 'commit_index': self.commit_index,
                    'last_log_index': len(self.log), 'quorum': self.quorum,
                    'healthy': self._healthy, 'pre_vote': self.pre_vote_enabled, 'next_index': dict(self.next_index),
                    'match_index': dict(self.match_index)}
