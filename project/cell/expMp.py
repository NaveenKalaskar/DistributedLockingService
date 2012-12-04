import multiprocessing
import time
import sys
import types
import traceback
import os
import stat
import signal
import time
import logging
import threading
import warnings
from logging import DEBUG
from logging import INFO
from logging import ERROR
from logging import CRITICAL
from logging import FATAL
from distalgo.runtime.udp import UdpEndPoint
from distalgo.runtime.tcp import TcpEndPoint
from distalgo.runtime.event import *
from distalgo.runtime.util import *
from distalgo.runtime import DistProcess
from random import randint
import sys
NOPS = 10
operations = {i: lambda state: (state + (i), state + (i)) for i in 
range(NOPS)}


class Replica(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'Request', [], [(1, 'p')], [self._event_handler_0]), EventPattern(Event.receive, 'Decision', [], [(1, 's'), (2, 'p')], [self._event_handler_1])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, leaders, initial_state):
        (self.state, self.slot_num) = (initial_state, 1)
        (self.proposals, self.decisions) = (set(), set())
        self.initial_state = initial_state
        self.leaders = leaders

    def main(self):
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def propose(self, p):
        if (not {s for (s, p1) in self.decisions if (p1 == p)}):
            maxs = max({0} | (set((s for (s, p1) in self.proposals))) | (set((s for (s, p1) in self.decisions))))
            s1 = min((s for s in range(1, maxs + (1) + (1)) if (not set((p1 for (s0, p1) in self.proposals if (s0 == s))) | (set((p1 for (s0, p1) in self.decisions if (s0 == s)))))))
            self.proposals.add((s1, p))
            self.send(('Propose', s1, p), self.leaders)

    def perform(self, p):
        (k, cid, op) = p
        if {s for (s, p0) in self.decisions if ((p0 == p) and (s < self.slot_num))}:
            self.slot_num+=1
        else:
            (next, result) = operations[op](self.state)
            (self.state, self.slot_num) = (next, self.slot_num + (1))
            self.send(('Response', cid, result), k)

    def _event_handler_0(self, p, _timestamp, _source):
        self.propose(p)

    def _event_handler_1(self, s, p, _timestamp, _source):
        self.decisions.add((s, p))
        while {p1 for (s0, p1) in self.decisions if (s0 == self.slot_num)}:
            p1 = {p1 for (s0, p1) in self.decisions if (s0 == self.slot_num)}.pop()
            for p2 in {p2 for (s0, p2) in self.proposals if (s0 == self.slot_num) if (p2 != p1)}:
                self.propose(p2)
            self.perform(p1)


class Acceptor(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'P1a', [], [(1, 'lam'), (2, 'b')], [self._event_handler_0]), EventPattern(Event.receive, 'P2a', [], [(1, 'lam'), (2, 'load')], [self._event_handler_1])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self):
        self.ballot_num = ((-1), (-1))
        self.accepted = set()

    def main(self):
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, lam, b, _timestamp, _source):
        if (b > self.ballot_num):
            self.ballot_num = b
        self.send(('P1b', self._id, self.ballot_num, self.accepted), lam)

    def _event_handler_1(self, lam, load, _timestamp, _source):
        (b, s, p) = load
        if (b >= self.ballot_num):
            self.ballot_num = b
            self.accepted.add((b, s, p))
        self.send(('P2b', self._id, self.ballot_num), lam)


class Leader(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'Propose', [], [(1, 's'), (2, 'p')], [self._event_handler_0]), EventPattern(Event.receive, 'Adopted', [], [(1, 'ballot_num_'), (2, 'pvals')], [self._event_handler_1]), EventPattern(Event.receive, 'Preempted', [], [(1, 'b')], [self._event_handler_2])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, acceptors, replicas):
        self.ballot_num = (0, self._id)
        self.active = False
        self.proposals = set()
        self.replicas = replicas
        self.acceptors = acceptors

    def main(self):
        self.spawn(Scout, [self._id, self.acceptors, self.ballot_num])
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, s, p, _timestamp, _source):
        if (not {p1 for (s1, p1) in self.proposals if (s1 == s)}):
            self.proposals.add((s, p))
            if self.active:
                self.spawn(Commander, [self._id, self.acceptors, self.replicas, (self.ballot_num, s, p)])

    def _event_handler_1(self, ballot_num_, pvals, _timestamp, _source):
        if (ballot_num_ == self.ballot_num):
            self.proposals = self.circle_plus(self.proposals, self.pmax(pvals))
            for (s, p) in self.proposals:
                self.spawn(Commander, [self._id, self.acceptors, self.replicas, (self.ballot_num, s, p)])
            self.active = True

    def _event_handler_2(self, b, _timestamp, _source):
        (r1, lam1) = b
        if (b > self.ballot_num):
            self.active = False
            self.ballot_num = (r1 + (1), self._id)
            self.spawn(Scout, [self._id, self.acceptors, self.ballot_num])

    def circle_plus(self, x, y):
        return y | ({(s, p) for (s, p) in x if (not {p1 for (s0, p1) in y if (s0 == s)})})

    def pmax(self, pvals):
        return {(s, p) for (b, s, p) in pvals if all(((b1 <= b) for (b1, s0, p1) in pvals if (s0 == s)))}


class Commander(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'P2b', [], [(1, 'a'), (2, 'b1')], [self._event_handler_0])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, lam, acceptors, replicas, load):
        self.waitfor = set(acceptors)
        (self.b, self.s, self.p) = load
        self.done = False
        self.load = load
        self.lam = lam
        self.replicas = replicas
        self.acceptors = acceptors

    def main(self):
        self.send(('P2a', self._id, (self.b, self.s, self.p)), self.acceptors)
        while (not self.done):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, a, b1, _timestamp, _source):
        if (b1 == self.b):
            if (a in self.waitfor):
                self.waitfor.remove(a)
            if (len(self.waitfor) < len(self.acceptors) / (2)):
                self.send(('Decision', self.s, self.p), self.replicas)
                self.done = True
        else:
            self.send(('Preempted', b1), self.lam)
            self.done = True


class Scout(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'P1b', [], [(1, 'a'), (2, 'b1'), (3, 'r')], [self._event_handler_0])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, lam, acceptors, b):
        self.waitfor = set(acceptors)
        self.pvalues = set()
        self.done = False
        self.lam = lam
        self.b = b
        self.acceptors = acceptors

    def main(self):
        import time
        import random
        time.sleep(
        random.random())
        self.send(('P1a', self._id, self.b), self.acceptors)
        while (not self.done):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, a, b1, r, _timestamp, _source):
        if (b1 == self.b):
            self.pvalues|=r
            if (a in self.waitfor):
                self.waitfor.remove(a)
            if (len(self.waitfor) < len(self.acceptors) / (2)):
                self.send(('Adopted', self.b, self.pvalues), self.lam)
                self.done = True
        else:
            self.send(('Preempted', b1), self.lam)
            self.done = True


class Client(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'Response', [], [(1, 'cid'), (2, 'result')], [self._event_handler_0])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, replicas):
        self.cid = 0
        self.results = dict()
        self.count = dict()
        self.replicas = replicas

    def main(self):
        while True:
            self.send(('Request', (self._id, self.cid, 
            randint(0, NOPS - (1)))), self.replicas)
            while (not ((self.results.get(self.cid) != None) and (self.count.get(self.cid) == len(self.replicas)))):
                self._process_event(self._event_patterns, True, None)
            self.output('Received result %d:%d' % ((self.cid, self.results[self.cid])))
            self.cid+=1

    def _event_handler_0(self, cid, result, _timestamp, _source):
        if (self.results.get(cid) == None):
            self.results[cid] = result
            self.count[cid] = 1
        elif (self.results[cid] != result):
            self.output('Replicas out of sync at cid(%d) : %d - %d ' % ((cid, self.results[cid], result)))
        else:
            self.count[cid]+=1

def main():
    nacceptors = 3
    nreplicas = 3
    nleaders = 1
    nclients = 3
    nops = 5
    use_channel('tcp')
    acceptors = createprocs(Acceptor, nacceptors, [])
    replicas = createprocs(Replica, nreplicas)
    leaders = createprocs(Leader, nleaders, (acceptors, replicas))
    clients = createprocs(Client, nclients, (replicas,))
    setupprocs(replicas, (leaders, 0))
    setupprocs(acceptors, [])
    setupprocs(leaders, [acceptors, replicas])
    setupprocs(clients, [replicas])
    startprocs(acceptors)
    startprocs(replicas | (leaders))
    inpt = input('Enter')
    startprocs(clients)
    for p in acceptors | (replicas) | (leaders) | (clients):
        p.join()