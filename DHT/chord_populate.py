"""
Chord Populate

This class is run by running the command
python3 chord_populate.py [NODE_ID] [FILE_NAME]
NODE_ID is ID of the node in Chord

:Authors: Noha Nomier
"""
import pickle
import hashlib
import threading
import socket
import sys
import csv
import time 

TEST_BASE = 43544  # for testing use port numbers on localhost at TEST_BASE+n
M = 4  # FIXME: Test environment, normally = hashlib.sha1().digest_size * 8
BUF_SZ = 4096  # socket recv arg

class ChordPopulate:
    """
    An object that wishes to add some data from a file
    into a DHT-Chord system by connecting to one start node
    """
    def __init__(self, start_node, file_name):
        self.start_node = start_node
        self.node_address = ('localhost', TEST_BASE + start_node)
        self.file_name = file_name
        self.populate_data(file_name)

    def populate_data(self, file_name):
        """
        Reads the file with the given @file_name and use 
        column_0+column_3 as the key after hashing it with SHA-1
        and sends each key,value to the start_node to be put in 
        the DHT
        """
        with open(file_name) as fp:
            next(fp)
            reader = csv.reader(fp, delimiter=",")
            for row in reader:
                time.sleep(0.001) #This delay is used to fix openning sockets more than os file limit
                key = row[0] + row[3]
                hashed = int.from_bytes(self.sha1(key), "big") % (2**M)
                self.send_data(hashed, key, row)

    def send_data(self, hashed_key, key, row):
        """
        For each key value a new client socket thread
        is created and sends the K,V to the
        start node to be put in the chord DHT
        """
        populate_thr = threading.Thread(target=self.call_helper_node, args=(hashed_key, {key:row},)) 
        populate_thr.start()

    def call_helper_node(self, key, value):
        """
        Makes a connection to the start node and sends it the data 
        to be put in the DHT

        key is the hash value and value is a dict of {original key, entire row data}

        """
        print(f"Sending data for key {value.keys()} to Node {self.start_node} ... ")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.connect(self.node_address)
                sock.sendall(pickle.dumps(("put_data", key, value)))
                print("Sending Done!")
            except Exception as e:
                print(f"Error connecting to Node {self.start_node}: {e}")

    def sha1(self, data):
        """returns sha1 digest of @data"""
        return hashlib.sha1(data.encode()).digest()
        
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Please enter valid command i.e python3 chord_populate.py NODEID FILE_NAME")
        exit(1)

    node = int(sys.argv[1])
    file_name = sys.argv[2]
    populate = ChordPopulate(node, file_name)

