import pickle
import hashlib
import socket
import sys

TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
M = 4  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
BUF_SZ = 4096  # socket recv arg

class ChordQuery:
    """
    An Object responsible to retrieve a value for a given key
    from the DHT-Chord system by the help of a start_node
    """
    def __init__(self, start_node, key):
        self.start_node = start_node
        self.node_address = ('localhost', TEST_BASE + start_node)
        self.target = key
        self.find_data()

    def find_data(self):
        """
        This method hashes the given key to find its position on Chord and makes a conn 
        to the start node giving it the hashed id and the given key
        """
        hashed = int.from_bytes(self.sha1(self.target), "big") % (2**M)
        print(f"Sending request for key {self.target} to Node {self.start_node} ... ")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(self.node_address)
                sock.sendall(pickle.dumps(("find_data", hashed, self.target)))
                response = pickle.loads(sock.recv(BUF_SZ))
                print(f"response:\n\n{response} \n\n")
            except Exception as e:
                print(f"Error receiving value from Node {self.start_node}: {e}")

    def sha1(self, data):
        """returns sha1 digest of @data"""
        return hashlib.sha1(data.encode()).digest()
        
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please enter valid command i.e python3 chord_query.py NODEID KEY")
        exit(1)

    node = int(sys.argv[1])
    target_key = sys.argv[2]
    populate = ChordQuery(node, target_key)

