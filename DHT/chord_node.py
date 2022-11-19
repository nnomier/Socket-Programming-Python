"""
CPSC 5520, Seattle University
This is an implementation of Chord Distributed Hashtable assignment in distributed systems
Lab4: chord

Run notes:
If it's the first node: 
Run python3 chord_node.py [node_id], 
node_id is just an int such as 0,1,2.. and the equivalent 
port will be calculated by adding the id to the base_port
if it's another node:
Run python3 chord_node.py [new_node_id] [existing_node_id] 

:Authors: Noha Nomier
"""
import pickle
import threading
import socket
import sys

M = 4  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
NODES = 2**M
BUF_SZ = 4096  # socket recv arg
BACKLOG = 100  # socket listen arg
TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n

NOT_FOUND_MSG = "KEY DOESN'T EXIST"

"""RPC Methods To Avoid Typos"""
FIND_SUCCESSOR = 'find_successor'
FIND_PREDECESSOR = 'find_predecessor'
CLOSEST_PRECEDING_FINGER = 'closest_preceding_finger'
GET_PREDECESSOR = 'get_predecessor'
SET_PREDECESSOR = 'set_predecessor'
SUCCESSOR = 'successor'
UPDATE_FINGER_TABLE = 'update_finger_table'
PUT_DATA = 'put_data'
UPDATE_KEYS = 'update_keys'
FIND_DATA = 'find_data'
GET_VALUE = 'get_value'

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
    """
    An object that represents a node in a Chord P2P system 
    that makes RPC to other nodes by the assistance of its finger 
    table and stores keys some keys that exceed K/N
    """
    def __init__(self, n):
        self.node = n
        self.finger = [None] + [FingerEntry(n, k) for k in range(1, M+1)]  # indexing starts at 1
        self.predecessor = None
        self.keys = {}
        self.address = ('localhost', TEST_BASE + n)
        self.listener = self.start_server(self.address) 
        self.start_listening()

    def start_server(self, address):
        """Creates a TCP/IP socket to listen and reply to incoming requests"""
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(address)
        listener.listen(BACKLOG)  
        return listener

    def start_listening(self):
        """Starts a new listener thread"""
        print("Starting a listening thread at {}".format(self.address))
        listen_thr = threading.Thread(target=self.listen, args=())
        listen_thr.start()

    def listen(self):	
        """
        Listener loop that handles each request in a separate thread
        """	
        while True:
            conn, _ = self.listener.accept()
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
        return self.call_rpc(np, SUCCESSOR)
    
    def call_rpc(self, n_prime, method, arg1= None, arg2= None):
        """
        This method handles calling an RPC to another node by sending @n_prime
        the required method to be executed on that node and the associated parameters
        """
        print(f"Self: Calling RPC to {n_prime}  with method = {method} and args {(arg1,arg2)}")
        address = ('localhost', TEST_BASE+n_prime)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps((method, arg1, arg2)))
                return pickle.loads(sock.recv(BUF_SZ))
            except Exception as e:
                return None

    def join(self, n_prime = None):
        """
        A method called whenever a new node joins,
        If it's the very first node, it will initialize all the finger table with 
        itself as successor
        Otherwise, It make RPCs to another existing node @n_prime to make it help
        with the initialization
        """
        if n_prime == None:
            for i in range(1, M+1):
                self.finger[i].node = self.node
            self.predecessor = self.node
        else:
            print(f"Initializing Finger Table with the help of node {n_prime}\n\n")
            self.init_finger_table(n_prime)
            self.update_others()

        self.pr_finger_table()

    def pr_finger_table(self):
        """Prints node's finger table"""
        print("*"*30)
        print(f"Node {self.node} finger table:\n")
        print("start\tint.\t\tsucc.\n")
        for i in range(1, M+1):
            print(f"{i}: {self.finger[i].start} | [{self.finger[i].start}, {self.finger[i].next_start}) | {self.finger[i].node}")
        print(f"\nPREDECESSOR:{self.predecessor}\nSUCCESSOR:{self.successor}")
        print("*"*30)
        print("\n")

    def init_finger_table(self, n_prime):
        """
        A method to initialize finger table with the help of one other node @n_prime
        by calling RPC to @n_prime to update each finger table entry
        """
        self.finger[1].node = self.call_rpc(n_prime, FIND_SUCCESSOR, self.finger[1].start)
        self.predecessor = self.call_rpc(self.successor, GET_PREDECESSOR) # this should be successor.predecessor
        self.call_rpc(self.successor, SET_PREDECESSOR, self.node)
        for i in range(1, M):
            if self.finger[i+1].start in ModRange(n, self.finger[i].node, NODES):
                self.finger[i+1].node = self.finger[i].node
            else:
                self.finger[i+1].node = self.call_rpc(n_prime, FIND_SUCCESSOR, self.finger[i+1].start)

    def put_data(self, key, value):
        """
        This method is called whenever a request comes to add a new key
        It works by finding the valid node (successor of the key) which on 
        its end adds the required @key,@value to their stored keys
        """
        successor_node = self.find_successor(key)
        self.call_rpc(successor_node, UPDATE_KEYS, key, value)
        self.pr_keys()

    def pr_keys(self):
        """prints the keys stored on this node"""
        print("*"*30)
        print(f"Keys Stored on this node: {self.keys.keys()}\n")
        
    def get_predecessor(self):
        """returns node's predecessor"""
        return self.predecessor

    def set_predecessor(self, n):
        """sets node's predecessor value"""
        self.predecessor = n

    def handle_rpc(self, client):
        """
        A method that handles receing an RPC from another node
        by loading the data received which should include the required 
        method and its arguments and returning back the result
        """
        rpc = client.recv(BUF_SZ)
        method, arg1, arg2 = pickle.loads(rpc)
        result = self.dispatch_rpc(method, arg1, arg2)
        if result != None:
            client.sendall(pickle.dumps(result))
        client.close()
        
    def dispatch_rpc(self, method, arg1=None, arg2=None):
        """
        A method that handles calling the actual local method coming from an
        RPC with the given @method and arguments if applicable @arg1, arg2
        """
        if method == FIND_SUCCESSOR:
            return self.find_successor(arg1)
        elif method == FIND_PREDECESSOR:
            return self.find_predecessor(arg1)
        elif method == CLOSEST_PRECEDING_FINGER:
            return self.closest_preceding_finger(arg1)
        elif method == GET_PREDECESSOR:
            return self.predecessor
        elif method == SET_PREDECESSOR:
            self.predecessor = arg1
            return "OK"
        elif method == SUCCESSOR:
            return self.successor
        elif method == UPDATE_FINGER_TABLE:
            return self.update_finger_table(arg1, arg2)
        elif method == PUT_DATA:
            self.put_data(arg1, arg2)
        elif method == UPDATE_KEYS:
            self.update_keys(arg1, arg2)
        elif method == FIND_DATA:
            return self.find_data(arg1, arg2)
        elif method == GET_VALUE:
            return self.get_value(arg1, arg2)
        else:
            print(f"Received invalid request {method} with args: {(arg1, arg2)}")

    def find_data(self, hashed_id, key):
        if hashed_id>NODES:
            return NOT_FOUND_MSG
        successor_node = self.find_successor(hashed_id)
        return self.call_rpc(successor_node, GET_VALUE, hashed_id, key)

    def get_value(self, hashed_id, key):
        if hashed_id not in self.keys:
            print(f"self: key {key} doesn't exist")
            return NOT_FOUND_MSG
        
        entries_for_hash = self.keys[hashed_id]

        if key not in entries_for_hash:
            print(f"self: key {key} doesn't exist")
            return NOT_FOUND_MSG
        
        print(f"returning value for id {hashed_id} and key {key} .....")
        return entries_for_hash[key]
        
    def update_keys(self, key, value):
        entries = {}
        if key in self.keys.keys():
           entries = self.keys[key]      
        entries.update(value)
        self.keys[key] = entries  
        self.pr_keys()
        
    def find_predecessor(self, id):
        """
        We are looking for n' such that id falls between n' and the successor for n'
        """
        n_prime = self.node
        # print(f"n_prime successor {n_prime_successor}")
        while id not in ModRange(n_prime+1, self.call_rpc(n_prime, SUCCESSOR)+1, NODES):
            n_prime = self.call_rpc(n_prime, CLOSEST_PRECEDING_FINGER, id)
        return n_prime

    def closest_preceding_finger(self, id):
        for i in reversed(range(1, M+1)): #M+1 because finger table is 1-indexed
            if self.finger[i].node in ModRange(n+1, id, NODES): 
                return self.finger[i].node
        return self.node
    
    def update_others(self):
        """ Update all other node that should have this node in their finger tables """
        for i in range(1, M+1):  # find last node p whose i-th finger might be this node
            p = self.find_predecessor((1 + self.node - 2**(i-1) + NODES) % NODES)
            self.call_rpc(p, UPDATE_FINGER_TABLE, self.node, i)

    def update_finger_table(self, s, i):
        """ if s is i-th finger of n, update this node's finger table with s """
        if (self.finger[i].start != self.finger[i].node 
                 and s in ModRange(self.finger[i].start, self.finger[i].node, NODES)):
            print('update_finger_table({},{}): {}[{}] = {} since {} in [{},{})\n'.format(
                     s, i, self.node, i, s, s, self.finger[i].start, self.finger[i].node))
            self.finger[i].node = s
            p = self.predecessor  # get first node preceding myself
            self.call_rpc(p, UPDATE_FINGER_TABLE, s, i)
            self.pr_finger_table()
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
  