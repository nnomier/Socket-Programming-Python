"""
Computer Networks, RDT3.0 Simulation - utility class
:Authors: Noha Nomier
"""

def create_checksum(packet_wo_checksum):
    """create the checksum of the packet (MUST-HAVE DO-NOT-CHANGE)

    Args:
      packet_wo_checksum: the packet byte data (including headers except for checksum field)

    Returns:
      the checksum in bytes

    """
    checksum = 0
    data_len = len(packet_wo_checksum)
    if (data_len % 2):
        data_len += 1
        packet_wo_checksum += b'\x00'
    
    for i in range(0, data_len, 2):
        w = (packet_wo_checksum[i] << 8) + (packet_wo_checksum[i + 1])
        checksum += w

    checksum = (checksum >> 16) + (checksum & 0xFFFF)
    checksum = ~checksum & 0xFFFF
    return checksum.to_bytes(2, byteorder='big')

def verify_checksum(packet):
    """verify packet checksum (MUST-HAVE DO-NOT-CHANGE)

    Args:
      packet: the whole (including original checksum) packet byte data

    Returns:
      True if the packet checksum is the same as specified in the checksum field
      False otherwise

    """
    calculated_checksum = (create_checksum(packet[0:8] + packet[10:]))
    return (int.from_bytes(packet[8:10],'big') + ~(int.from_bytes(calculated_checksum, 'big', signed=True))) == 0xFFFF

def make_packet(data_str, ack_num, seq_num):
    """Make a packet (MUST-HAVE DO-NOT-CHANGE)

    Args:
      data_str: the string of the data (to be put in the Data area)
      ack: an int tells if this packet is an ACK packet (1: ack, 0: non ack)
      seq_num: an int tells the sequence number, i.e., 0 or 1

    Returns:
      a created packet in bytes

    """
    first_8_bytes = bytes('COMPNETW', 'utf-8')

    total_packet_len = len(data_str) + 12 # 12 = 8(first_bytes) + 2(checksum) + 2(length,ack,seq)

    length_byte =  (total_packet_len << 2 | ack_num << 1 | seq_num).to_bytes(2, byteorder='big')
    message_bytes = bytes(data_str, 'utf-8')
    checksum = create_checksum(first_8_bytes + length_byte + message_bytes)

    packet = first_8_bytes + checksum + length_byte + message_bytes

    return packet
    