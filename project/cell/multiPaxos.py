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
from time import sleep
import sys
NOPS = 10


class HeartBeat(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = []
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, replicas):
        self.t_s = 4
        self.replicas = replicas

    def main(self):
        heatBeat()
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def heartBeat(self):
        self.output('this is the heart beat ')
        self.send(('hb',), self.replicas)
        sleep(self.t_s)
        return False


class Replica(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'hb', [], [], [self._event_handler_0]), EventPattern(Event.receive, 'LeaderElect', [], [(1, 'leader1')], [self._event_handler_1]), EventPattern(Event.receive, 'Request', [], [(1, 'p')], [self._event_handler_2]), EventPattern(Event.receive, 'Decision', [], [(1, 's'), (2, 'p')], [self._event_handler_3])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, acceptors, replicas, initial_state, myself, lmyid):
        self.lmyid = lmyid
        self.initialized = False
        self.leader = None
        self.isLeader = False
        (self.state, self.slot_num) = (initial_state, 1)
        (self.proposals, self.decisions) = (set(), set())
        self.output('len of acceptors ' + (str(len(acceptors))))
        self.output('len of replicas ' + (str(len(replicas))))
        self.output('initial_state ' + (str(initial_state)))
        self.output('myself .. ')
        self.output(myself)
        self.output('myid ..' + (str(lmyid)))
        if (self.lmyid == 1):
            self.leader = self.spawn(Leader, [acceptors, replicas])
            self.isLeader = True
        self.initial_state = initial_state
        self.myself = myself
        self.replicas = replicas
        self.lmyid = lmyid
        self.acceptors = acceptors

    def main(self):
        if (self.lmyid == 1):
            self.leaderAnnounce()
            while (not False):
                self._process_event(self._event_patterns, True, None)
        else:
            while (not False):
                self._process_event(self._event_patterns, True, None)

    def leaderAnnounce(self):
        if (self.lmyid == 1):
            self.send(('LeaderElect', self.leader), self.replicas - (set([self.myself])))
        sleep(2)
        return False

    def operations(self, op):
        llock = op % (5)
        oper = op % (2)
        self.state[llock] = str(oper)

    def _event_handler_0(self, _timestamp, _source):
        self.output('This is the heart beat that i was talking about')

    def _event_handler_1(self, leader1, _timestamp, _source):
        self.leader = leader1
        self.output('I am told this is my leader' + (str(self.leader)))

    def propose(self, p):
        if (not {s for (s, p1) in self.decisions if (p1 == p)}):
            maxs = max({0} | (set((s for (s, p1) in self.proposals))) | (set((s for (s, p1) in self.decisions))))
            s1 = min((s for s in range(1, maxs + (1) + (1)) if (not set((p1 for (s0, p1) in self.proposals if (s0 == s))) | (set((p1 for (s0, p1) in self.decisions if (s0 == s)))))))
            self.proposals.add((s1, p))
            self.send(('Propose', s1, p), self.leader)

    def perform(self, p):
        (k, cid, op) = p
        if {s for (s, p0) in self.decisions if ((p0 == p) and (s < self.slot_num))}:
            self.slot_num+=1
        else:
            self.operations(op)
            result = self.state
            self.slot_num = self.slot_num + (1)
            self.send(('Completed', self.slot_num), self.leader)
            self.send(('Response', cid, result), k)

    def _event_handler_2(self, p, _timestamp, _source):
        self.propose(p)

    def _event_handler_3(self, s, p, _timestamp, _source):
        self.decisions.add((s, p))
        while {p1 for (s0, p1) in self.decisions if (s0 == self.slot_num)}:
            p1 = {p1 for (s0, p1) in self.decisions if (s0 == self.slot_num)}.pop()
            for p2 in {p2 for (s0, p2) in self.proposals if (s0 == self.slot_num) if (p2 != p1)}:
                self.propose(p2)
            self.perform(p1)


class Acceptor(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'P1a', [], [(1, 'lam'), (2, 'b'), (3, 's')], [self._event_handler_0]), EventPattern(Event.receive, 'P2a', [], [(1, 'lam'), (2, 'load')], [self._event_handler_1]), EventPattern(Event.receive, 'GcCollect', [], [(1, 'slot')], [self._event_handler_2])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self):
        self.ballot_num = (-1)
        self.accepted = {}
        self.gc_slot = 0

    def main(self):
        self.output('the ballot number in acceptor')
        self.output(self.ballot_num)
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, lam, b, s, _timestamp, _source):
        if (b > self.ballot_num):
            self.ballot_num = b
        temp_accepted = {}
        for (t_s, t_pval) in temp_accepted.items():
            if (int(t_s) > int(s)):
                temp_accepted[t_s] = t_pval
        self.send(('P1b', self._id, self.ballot_num, temp_accepted), lam)

    def _event_handler_1(self, lam, load, _timestamp, _source):
        (b, s, p) = load
        if (b >= self.ballot_num):
            self.ballot_num = b
            self.accepted[s] = (b, s, p)
        self.send(('P2b', self._id, self.ballot_num), lam)

    def _event_handler_2(self, slot, _timestamp, _source):
        if (slot in self.accepted):
            if (self.gc_slot < slot):
                del self.accepted[slot]
                self.gc_slot = slot


class Leader(DistProcess):

    def __init__(self, parent, initq, channel, log):
        DistProcess.__init__(self, parent, initq, channel, log)
        self._event_patterns = [EventPattern(Event.receive, 'Propose', [], [(1, 's'), (2, 'p')], [self._event_handler_0]), EventPattern(Event.receive, 'Adopted', [], [(1, 'ballot_num_'), (2, 'pvals')], [self._event_handler_1]), EventPattern(Event.receive, 'Decided', [], [(1, 'slot')], [self._event_handler_2]), EventPattern(Event.receive, 'Completed', [], [(1, 'slot')], [self._event_handler_3]), EventPattern(Event.receive, 'Preempted', [], [(1, 'b')], [self._event_handler_4])]
        self._sent_patterns = []
        self._label_events = {}

    def setup(self, acceptors, replicas):
        self.ballot_num = randint(0, 500)
        self.ballot_num = self.ballot_num % (30)
        self.gc_collector = {}
        self.active = False
        self.proposals = set()
        self.slot_num = 0
        self.nreplicas = len(replicas)
        self.replicas = replicas
        self.acceptors = acceptors

    def main(self):
        self.spawn(Scout, [self._id, self.acceptors, self.ballot_num, self.slot_num])
        while (not False):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, s, p, _timestamp, _source):
        if (not {p1 for (s1, p1) in self.proposals if (s1 == s)}):
            self.proposals.add((s, p))
            if self.active:
                self.spawn(Commander, [self._id, self.acceptors, self.replicas, (self.ballot_num, s, p)])

    def _event_handler_1(self, ballot_num_, pvals, _timestamp, _source):
        if (self.ballot_num == ballot_num_):
            self.proposals = self.circle_plus(self.proposals, self.pmax(pvals))
            for (s, p) in self.proposals:
                self.spawn(Commander, [self._id, self.acceptors, self.replicas, (self.ballot_num, s, p)])
            self.active = True

    def _event_handler_2(self, slot, _timestamp, _source):
        self.slot_num = slot

    def _event_handler_3(self, slot, _timestamp, _source):
        if (slot in self.gc_collector):
            count = self.gc_collector[slot]
            if (count == self.nreplicas - (1)):
                del self.gc_collector[slot]
                self.send(('GcCollect', slot), self.acceptors)
            else:
                self.gc_collector[slot] = count + (1)
        else:
            self.gc_collector[slot] = 1

    def _event_handler_4(self, b, _timestamp, _source):
        r1 = b
        self.active = False
        self.ballot_num = r1 + (1)
        self.spawn(Scout, [self._id, self.acceptors, self.ballot_num, self.slot_num])

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
                self.send(('Decided', self.s), self.lam)
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

    def setup(self, lam, acceptors, b, slot_num):
        self.waitfor = set(acceptors)
        self.pvalues_set = set()
        self.pvalues = {}
        self.done = False
        self.ballot_num = b
        self.lam = lam
        self.b = b
        self.slot_num = slot_num
        self.acceptors = acceptors

    def main(self):
        import time
        import random
        time.sleep(
        random.random())
        self.send(('P1a', self._id, self.ballot_num, self.slot_num), self.acceptors)
        while (not self.done):
            self._process_event(self._event_patterns, True, None)

    def _event_handler_0(self, a, b1, r, _timestamp, _source):
        if (b1 == self.ballot_num):
            for (slot, pval) in r.items():
                print(slot)
                print(pval)
                temp_p = self.pvalues.get(slot)
                if (temp_p is None):
                    print('yes it was None')
                    self.pvalues[slot] = pval
                else:
                    (balr, slr, pslr) = pval
                    (balp, slp, pslp) = self.pvalues[slot]
                    if (int(balp) < int(balr)):
                        self.pvalues[slot] = pval
            if (a in self.waitfor):
                self.waitfor.remove(a)
            if (len(self.waitfor) < len(self.acceptors) / (2)):
                for (slt1, pval1) in self.pvalues.items():
                    self.pvalues_set.add(pval1)
                self.send(('Adopted', self.b, self.pvalues_set), self.lam)
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
            self.output('Received result %d:' % (self.cid))
            self.output(self.results[self.cid])
            self.cid+=1

    def _event_handler_0(self, cid, result, _timestamp, _source):
        if (self.results.get(cid) == None):
            self.results[cid] = result
            self.count[cid] = 1
        elif (self.results[cid] != result):
            self.output('Replicas out of sync at cid(%d) : %d - %d ' % ((cid, self.results[cid], result)))
            print(' ')
        else:
            self.count[cid]+=1

def main():
    nacceptors = 3
    nreplicas = 3
    nleaders = 1
    nclients = 8
    nops = 5
    use_channel('tcp')
    acceptors = createprocs(Acceptor, nacceptors, [])
    replicas = createprocs(Replica, nreplicas)
    clients = createprocs(Client, nclients, (replicas,))
    myid = 1
    initial_state = {}
    initial_state[0] = str(0)
    initial_state[1] = str(1)
    initial_state[2] = str(2)
    initial_state[3] = str(3)
    initial_state[4] = str(4)
    for rep in replicas:
        setupprocs([rep], (acceptors, replicas, initial_state, rep, myid))
        myid = myid + (1)
    setupprocs(acceptors, [])
    setupprocs(clients, [replicas])
    startprocs(acceptors)
    startprocs(replicas)
    sleep(5)
    startprocs(clients)
    for p in acceptors | (replicas) | (clients):
        p.join()
from random import randint