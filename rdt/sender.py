"""
Computer Networks, RDT3.0 Simulation - Sender
:Authors: Noha Nomier
"""
from socket import *
import util

RECEIVER_ADDRESS = ('127.0.0.1', 50503)
BUFF_SIZE = 2048
TIMEOUT_IN_SECONDS = 2

class Sender:
  def __init__(self):
      """ 
      Constructs a sender object that follows RDT3.0 sender protocol
      """
      self.seq_num = 0
      self.counter = 1
      self.sender = self.create_udp_socket()

  def create_udp_socket(self):
      """Creates a UDP socket object""" 
      return socket(AF_INET, SOCK_DGRAM)

  def transmit_message(self, packet, app_msg_str):
      """
      sends @packet to the receiver with the @app_msg_str as payload
      simulates a timer using socket.settimeout() and keeps retransmitting 
      the packet each time timeout occurs in a recursive call
      """
      self.sender.sendto(packet, RECEIVER_ADDRESS)
      self.sender.settimeout(TIMEOUT_IN_SECONDS)
      print(f'packet num {self.counter} is successfully sent to the receiver')
      data_bytes = b''
      try:
          data_bytes, _ = self.sender.recvfrom(BUFF_SIZE)
          return data_bytes
      except timeout:
        print('socket timeout! Resend\n\n')
        print(f'[timeout retransmission]: {app_msg_str}')
        self.counter += 1
        data_bytes = self.transmit_message(packet, app_msg_str)  #keep calling it until no timeout occurs
      
      return data_bytes

  def rdt_send(self, app_msg_str):
      """realibly send a message to the receiver

      Args:
        app_msg_str: the message string (to be put in the data field of the packet)

      """
      print(f'original message string: {app_msg_str}')
      packet = util.make_packet(data_str = app_msg_str, ack_num=0, seq_num=self.seq_num)
      print(f'packet created: {packet}')

      data_bytes = self.transmit_message( packet, app_msg_str)
      is_valid_packet = util.verify_checksum(data_bytes)
      received_seq_num = (data_bytes[11]) & 1

      while not is_valid_packet or received_seq_num != self.seq_num:
        if not is_valid_packet:
          print('receiver sent corrupted message, resend!\n\n')
          print(f'[corrupted message retransmission]: {app_msg_str}')
        elif received_seq_num != self.seq_num:
          print('receiver acked the previous pkt, resend!\n\n')
          print(f'[ACK-Previous retransmission]: {app_msg_str}')

        self.counter += 1
        data_bytes = self.transmit_message(packet, app_msg_str)
        is_valid_packet = util.verify_checksum(data_bytes)
        received_seq_num = (data_bytes[11]) & 1

      print(f"packet is received correctly, seq. num {self.seq_num} = ACK {received_seq_num}. all done!\n\n")
      self.seq_num = self.seq_num^1
      self.counter +=1

