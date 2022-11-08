"""
Forex Provider Subscriber
This module contains utility functions to marshal/unmarshal packet contents 
sent or received from the subscriber side
:Authors: Noha Nomier
"""
import struct
from datetime import datetime ,timedelta

SIZE_OF_ONE_MESSAGE = 32 #bytes
MICROS_PER_SECOND = 1_000_000

def serialize_address(ip_address, port_number):
    """
    serializes the host, port address into a byte array to be subscribed to the the publisher

    :param ip_address: client ip address
    :param port_number: client port
    :return: 6-byte sequence in subscription request
    """
    ip_as_bytes = bytes(map(int, ip_address.split('.')))
    port_as_bytes = struct.pack('>H', port_number)
    return ip_as_bytes + port_as_bytes

def unmarshal_message(message: bytes) -> list:
    """
    Unmarshals publisher message to retrive the needed information
    Steps:
    Check len and divide by 32 to get number of partitions
    Every 32 bytes unmarshal them and append to quotes_list
    For every 32 bytes, divide them such that 
    Bytes[0:8] The timestamp is a 64-bit integer number of microseconds
    that have passed since 00:00:00 UTC on 1 January 1970 (excluding leap seconds).
    Sent in big-endian network format.

    Bytes[8:14] The currency names are the three-character ISO codes 
    ('USD', 'GBP', 'EUR', etc.) transmitted in 8-bit ASCII from left to right.

    Bytes[14:22] The exchange rate is 64-bit floating point number represented
    in IEEE 754 binary64 little-endian format. The rate is number of currency2 units 
    to be exchanged per one unit of currency1. So, for example, if currency1 is USD and currency2 is JPY,
    we'd expect the exchange rate to be around 100.

    Bytes[22:32] Reserved. These are not currently used (typically all set to 0-bits).
    """
    number_of_quotes = len(message) / SIZE_OF_ONE_MESSAGE
    unmarshaled_quotes = []
    for i in range(int(number_of_quotes)):
        curr_quote = {}
        curr_quote['timestamp']= deserialize_utcdatetime(message[(i*SIZE_OF_ONE_MESSAGE): ((i*SIZE_OF_ONE_MESSAGE)+8)])
        currency_1 = message[((i*SIZE_OF_ONE_MESSAGE)+8):((i*SIZE_OF_ONE_MESSAGE)+11)].decode('utf-8')
        currency_2 = message[((i*SIZE_OF_ONE_MESSAGE)+11):((i*SIZE_OF_ONE_MESSAGE)+14)].decode('utf-8')
        curr_quote['curr1'] = currency_1
        curr_quote['curr2'] = currency_2
        curr_quote['price'] = deserialize_price(message[((i*SIZE_OF_ONE_MESSAGE) + 14) : ((i*SIZE_OF_ONE_MESSAGE) + 22)])

        unmarshaled_quotes.append(curr_quote)
    return unmarshaled_quotes

def deserialize_utcdatetime(utc_bytes: bytes) -> datetime:
    """ Converts a byte stream into  UTC datetime """
    epoch = datetime(1970, 1, 1)
    total_seconds = struct.unpack('>Q', utc_bytes)[0] / MICROS_PER_SECOND
    return epoch + timedelta(seconds=total_seconds)

def deserialize_price(price_bytes: bytes) -> float:
    """
    Convert a byte array in little-endian into a floating number representing the price
    """
    return struct.unpack('<d', price_bytes)[0]  
