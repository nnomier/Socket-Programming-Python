import pickle
import hashlib
import threading
import socket
import sys
import csv
import time 

TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
M = 3  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
BUF_SZ = 4096  # socket recv arg

class ChordQuery:
    def __init__(self, start_node, key):
        self.start_node = start_node
        self.node_address = ('localhost', TEST_BASE + start_node)
        self.target = key
        self.find_data()

    def find_data(self):
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

    def call_helper_node(self, key, value):
        """
        key is the hash value and value is a dict of {original key, entire row data}
        """
        print(f"Sending data for key {value.keys()} to Node {self.start_node} ... ")


    def sha1(self, data):
        return hashlib.sha1(data.encode()).digest()
        
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please enter valid command i.e python3 chord_query.py NODEID KEY")
        exit(1)

    node = int(sys.argv[1])
    target_key = sys.argv[2]
    populate = ChordQuery(node, target_key)

