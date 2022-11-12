import pickle
import hashlib
import threading
import socket
import sys

M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n


class ModRange(object):
    """
    Range-like object that wraps around 0 at some divisor using modulo arithmetic.

    >>> mr = ModRange(1, 4, 100)
    >>> mr
    <mrange [1,4)%100>
    >>> 1 in mr and 2 in mr and 4 not in mr
    True
    >>> [i for i in mr]
    [1, 2, 3]
    >>> mr = ModRange(97, 2, 100)
    >>> 0 in mr and 99 in mr and 2 not in mr and 97 in mr
    True
    >>> [i for i in mr]
    [97, 98, 99, 0, 1]
    >>> [i for i in ModRange(0, 0, 5)]
    [0, 1, 2, 3, 4]
    """

    def __init__(self, start, stop, divisor):
        self.divisor = divisor
        self.start = start % self.divisor
        self.stop = stop % self.divisor
        # we want to use ranges to make things speedy, but if it wraps around the 0 node, we have to use two
        if self.start < self.stop:
            self.intervals = (range(self.start, self.stop),)
        elif self.stop == 0:
            self.intervals = (range(self.start, self.divisor),)
        else:
            self.intervals = (range(self.start, self.divisor), range(0, self.stop))

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return ''.format(self.start, self.stop, self.divisor)

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        for interval in self.intervals:
            if id in interval:
                return True
        return False

    def __len__(self):
        total = 0
        for interval in self.intervals:
            total += len(interval)
        return total

    def __iter__(self):
        return ModRangeIter(self, 0, -1)


class ModRangeIter(object):
    """ Iterator class for ModRange """
    def __init__(self, mr, i, j):
        self.mr, self.i, self.j = mr, i, j

    def __iter__(self):
        return ModRangeIter(self.mr, self.i, self.j)

    def __next__(self):
        if self.j == len(self.mr.intervals[self.i]) - 1:
            if self.i == len(self.mr.intervals) - 1:
                raise StopIteration()
            else:
                self.i += 1
                self.j = 0
        else:
            self.j += 1
        return self.mr.intervals[self.i][self.j]


class FingerEntry(object):
    """
    Row in a finger table.

    >>> fe = FingerEntry(0, 1)
    >>> fe
    
    >>> fe.node = 1
    >>> fe
    
    >>> 1 in fe, 2 in fe
    (True, False)
    >>> FingerEntry(0, 2, 3), FingerEntry(0, 3, 0)
    (, )
    >>> FingerEntry(3, 1, 0), FingerEntry(3, 2, 0), FingerEntry(3, 3, 0)
    (, , )
    >>> fe = FingerEntry(3, 3, 0)
    >>> 7 in fe and 0 in fe and 2 in fe and 3 not in fe
    True
    """
    def __init__(self, n, k, node=None):
        if not (0 <= n < NODES and 0 < k <= M):
            raise ValueError('invalid finger entry values')
        self.start = (n + 2**(k-1)) % NODES
        self.next_start = (n + 2**k) % NODES if k < M else n
        self.interval = ModRange(self.start, self.next_start, NODES)
        self.node = node

    def __repr__(self):
        """ Something like the interval|node charts in the paper """
        return print(f"{self.start}, {self.next_start}, {self.node}")

    def __contains__(self, id):
        """ Is the given id within this finger's interval? """
        return id in self.interval


class ChordNode():
    def __init__(self, n):
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.address = ('localhost', TEST_BASE + n)
        self.listener = self.start_server(self.address) 
        self.start_listening()

    def start_server(self, address):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(address)
        listener.listen(BACKLOG)  
        return listener

    def start_listening(self):
        print("Starting a listening thread at {}".format(self.address))
        listen_thr = threading.Thread(target=self.listen, args=())
        listen_thr.start()

    def listen(self):		
        while True:
            conn, addr = self.listener.accept()
            handle_rpc_thr = threading.Thread(target = self.handle_rpc, args = (conn,))
            handle_rpc_thr.start()

    @property
    def successor(self):
        return self.finger[1].node

    @successor.setter
    def successor(self, id):
        self.finger[1].node = id

    def find_successor(self, id):
        """ Ask this node to find id's successor = successor(predecessor(id))"""
        print(f"node {self.node}: finding successor of {id}...")
        np = self.find_predecessor(id)
        return self.call_rpc(np, 'successor')
    
    def call_rpc(self, n_prime, method, arg1= None, arg2= None):
        print(f"{self.node}: Calling RPC to {n_prime}  with method = {method} and args {(arg1,arg2)}")
        address = ('localhost', TEST_BASE+n_prime)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps((method, arg1, arg2)))
                return pickle.loads(sock.recv(BUF_SZ))
            except Exception as e:
                return None


    def join(self, n_prime = None):
        if n_prime == None:
            for i in range(1, M+1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        else:
            print(f"Initializing Finger Table with the help of {n_prime}")
            self.init_finger_table(n_prime)
            self.update_others()
            self.pr_finger_table()

    def pr_finger_table(self):
        for i in range(1, M+1):
            print(f"index: {i} , start: {self.finger[i].start} , interval: {self.finger[i].start},{self.finger[i].next_start}, node: {self.finger[i].node}")

    def init_finger_table(self, n_prime):
        self.finger[1].node = self.call_rpc(n_prime, 'find_successor', self.finger[1].start)
        print(f"my successor {self.successor}")
        self.predecessor = self.call_rpc(self.successor, 'get_predecessor') # this should be successor.predecessor
        print(f"my predecessor: {self.predecessor}")
        self.call_rpc(self.successor, "set_predecessor", self.node)
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(n, self.finger[i].node, NODES):
                self.finger[i+1].node = self.finger[i].node
                print(f"elawalaneya {i+1}")
            else:
                self.finger[i+1].node = self.call_rpc(n_prime, 'find_successor', self.finger[i+1].start)

    def get_predecessor(self):
        return self.predecessor

    def set_predecessor(self, n):
        self.predecessor = n

    def handle_rpc(self, client):
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        client.sendall(pickle.dumps(result))
        client.close()
        self.pr_finger_table()

    def dispatch_rpc(self, method, arg1=None, arg2=None):
        if method == 'find_successor':
            return self.find_successor(arg1)
        elif method == 'predecessor':
            return self.find_predecessor(arg1)
        elif method == 'closest_preceding_finger':
            return self.closest_preceding_finger(arg1)
        elif method == 'get_predecessor':
            return self.predecessor
        elif method == 'set_predecessor':
            self.predecessor = arg1
            return "OK"
        elif method == 'successor':
            return self.successor
        elif method == 'update_finger_table':
            self.update_finger_table(arg1, arg2)
            return "OK"
        else:
            print(f"Received invalid request {method} with args: {(arg1, arg2)}")

    def find_predecessor(self, id):
        """
        We are looking for n' such that id falls between n' and the successor for n'
        """
        n_prime = self.node
        # print(f"n_prime successor {n_prime_successor}")
        while id not in ModRange(n_prime+1, self.call_rpc(n_prime, 'successor')+1, NODES):
            n_prime = self.call_rpc(n_prime, 'closest_preceding_finger', id)
        return n_prime

    def closest_preceding_finger(self, id):
        for i in reversed(range(1, M+1)): #M+1 because finger table is 1-indexed
            if self.finger[i].node in ModRange(n+1, id, NODES): 
                return self.finger[i].node
        return self.node
    
    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            # FIXME: bug in paper, have to add the 1 +
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES)
            self.call_rpc(p, 'update_finger_table', self.node, i)

    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        # FIXME: don't want e.g. [1, 1) which is the whole circle
        if (self.finger[i].start != self.finger[i].node
                 # FIXME: bug in paper, [.start
                 and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})'.format(
                     s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            print('#', self)
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, 'update_finger_table', s, i)
            return str(self)
        else:
            return 'did nothing {}'.format(self)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please enter valid command i.e python3 chord_node.py NODEID [NODEID]")
        exit(1)

    n = int(sys.argv[1])
    node = ChordNode(n)
    if len(sys.argv) > 2:
        n_prime = int(sys.argv[2])
        node.join(n_prime)
    else:
        node.join()
  