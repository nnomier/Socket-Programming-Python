"""
Computer Networks, RDT3.0 Simulation - Receiver 
:Authors: Noha Nomier
"""

from socket import *
from time import sleep
import util

RECEIVER_ADDRESS = ('127.0.0.1', 50503)
BUFF_SIZE = 2048
ACK = 1
SLEEP_TIME_SEC = 3

class Receiver:
    """ 
    Constructs a receiver object that follows RDT3.0 sender protocol
    """
    def __init__(self):
        self.seq_num = 0 
        self.counter = 1
        self.run()

    def run(self):
        with socket(AF_INET, SOCK_DGRAM) as receiver:
            receiver.bind(RECEIVER_ADDRESS)
            print( f'starting receiver up on {RECEIVER_ADDRESS[0]} port {RECEIVER_ADDRESS[1]}')
            while True:                
                msg, sender_socket = receiver.recvfrom(BUFF_SIZE)
                is_valid = util.verify_checksum(msg)
                seq_num_received = msg[11] & 1
                print(f"packet num.{self.counter} received: {msg}")
                ack_prev = False
                timed_out = False
                if self.counter % 6 == 0:
                    print('simulating packet loss: sleep a while to trigger timeout event on the sender side…')
                    timed_out = True
                    sleep(SLEEP_TIME_SEC)
                elif self.counter % 3 == 0:
                    print('simulating packet bit errors/corruputed:  ACK the previous packet!')
                    ack_prev = True 
                if not is_valid or seq_num_received != self.seq_num or ack_prev == True:
                    response = util.make_packet("", ACK, seq_num=self.seq_num^1) 
                    receiver.sendto(response, sender_socket)
                elif not timed_out:
                    print(f"packet is expected, message string delivered: {msg[12:].decode()}")
                    print("packet is delivered, now creating and sending the ACK packet...")
                    response = util.make_packet("", ACK, seq_num=self.seq_num) 
                    receiver.sendto(response, sender_socket)
                    self.seq_num = self.seq_num^1
                print('All done for this packet\n')
                self.counter += 1


if __name__ == "__main__":
    receiver = Receiver()