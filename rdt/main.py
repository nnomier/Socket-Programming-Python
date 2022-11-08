from sender import Sender

# note: no arguments will be passed in
sender = Sender() 

for i in range(1, 15):
    # this is where your rdt_send will be called
    sender.rdt_send('msg' + str(i))

# self.sender.sendto(packet, RECEIVER_ADDRESS)
# self.sender.settimeout(TIMEOUT_IN_SECONDS)
# print(f'packet num {self.counter} is successfully sent to the receiver')
# data_bytes = b''
# try:
#     data_bytes, _ = self.sender.recvfrom(BUFF_SIZE)
# except timeout:
#   print('socket timeout! Resend\n\n')
#   print(f'[timeout retransmission]: {app_msg_str}')
#   self.counter += 1
#   print(f'packet num {self.counter} is successfully sent to the receiver')
#   self.sender.sendto(packet, RECEIVER_ADDRESS)
#   data_bytes, _ = self.sender.recvfrom(BUFF_SIZE)