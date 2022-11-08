"""
CPSC 5520, Seattle University
This is an implementation of Arbitrage Opportunities Pub/Sub assignment in distributed systems
Lab3: Pub/Sub
:Authors: Noha Nomier
"""
import sys
import socket
from datetime import datetime
from bellman_ford import BellmanFord
import fxp_bytes_subscriber
import math

LISTENER_ADDRESS = (socket.gethostbyname(socket.gethostname()), 0)
BUFF_SIZE = 4096
QUOTE_TIMEOUT = 1.5

class ArbitrageSubscriber:
    """
    Constructs an Arbitrage Subscriber Object to listen to published messages
    and detect arbitrage ooportunities
    """
    def __init__(self, provider_address):
        self.provider_address = provider_address
        self.quotes_timestamps = {}  # key is curr1, val is {curr2: timestamp, curr3: timestamp, ...}
        self.graph = BellmanFord()
        self.lastest_timestamp = datetime.now()

    def run(self):
        """
        The main runner method that subscribes by sending its address serialized to 
        the publisher and handles received quotes by checking arbitrage opportunities
        """
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as subscriber:
            subscriber.bind(LISTENER_ADDRESS)
            listener_address = subscriber.getsockname()
            print( f'starting up on {listener_address[0]} port {listener_address[1]}')

            serailized_address = fxp_bytes_subscriber.serialize_address(listener_address[0], listener_address[1])
            subscriber.sendto(serailized_address, self.provider_address)

            while True:
                data_bytes, _ = subscriber.recvfrom(BUFF_SIZE)
                unmarshaled_data = fxp_bytes_subscriber.unmarshal_message(data_bytes)
                self.remove_outdated_quotes()
                self.handle_quotes_to_graph(unmarshaled_data)
                self.detect_arbitrage()

    def detect_arbitrage(self):
        """
        Checks arbitrage opportunity by calling Bellman Ford shortest path algorithm to 
        detect negative cycles, if there's a negative cycle then there's an arbitrage
        """
        for node in self.graph.get_nodes():
            distances, predecessor, negative_edge = self.graph.shortest_paths( node, 1e-9)
            if negative_edge is not None:
                cycle = self.get_cycle(negative_edge, predecessor)
                if cycle is not None:
                    break

    def get_cycle(self, negative_edge, predecessor, start_amount=100):
        """
        After detecting the existance of a negative cycle,
        we need to retrieve the exact cycle and print out the path 
        that be taken from the starting currency
        """
        graph = self.graph.get_graph()
        cycle = []
        # start with an arbitrary node in the cycle
        cycle_node = negative_edge[1]
        v = cycle_node

        # set to see which currency is duplicated in the cycle to know what currency to start at
        seen = {v}
        start = None
        while True:
            v = predecessor[v]
            if v is None:
                return None
            if v in seen:  # this would be the start of our cycle
                start = v
                break
            seen.add(v)

        print(f'ARBITRAGE DETECTED:')
        print(f"\tstart with {start} {str(start_amount)}")
        v = start
        while True:
            if v is None:
                return None
            cycle.append(v)

            # we've reached the end of the cycle
            if v == start and len(cycle) > 1:
                break
            v = predecessor[v]

        cycle.reverse()

        amount_exchanged = start_amount

        for i in range(0, len(cycle) - 1):
            price = math.exp(-1 * graph[cycle[i]][cycle[i+1]])
            amount_exchanged = amount_exchanged * price
            print(f'\texchange {cycle[i]} for {cycle[i+1]} at {price} --> {cycle[i+1]}  {str(amount_exchanged)}')

        print('\n')
        return cycle

    def remove_outdated_quotes(self):
        """
        Removes any published quotes that exceeded QUOTE_TIMEOUT
        """
        curr_utc_time = datetime.utcnow()
        temp = {}
        for curr1, val_dict in self.quotes_timestamps.items():
            for curr2, last_timestamp in val_dict.items():
                if (curr_utc_time - last_timestamp).total_seconds() > QUOTE_TIMEOUT:
                    print(
                        f'removing stale quote for (\'{curr1}\', \'{curr2}\')')
                    self.graph.remove_edge(curr1, curr2)
                else:
                    if curr1 not in temp.keys():
                        temp[curr1] = {
                            curr2: self.quotes_timestamps[curr1][curr2]}
                    else:
                        temp[curr1][curr2] = self.quotes_timestamps[curr1][curr2]

        self.quotes_timestamps = temp

    def handle_quotes_to_graph(self, received_quotes):
        """
        Handles received quotes from publisher by adding them to the graph 
        or updating values if they already existed and removing any out-of-sequence
        message
        """
        for quote in received_quotes:
            quote_time = quote['timestamp']
            curr1 = quote['curr1']
            curr2 = quote['curr2']
            rate = quote['price']
            print(f'{str(quote_time)} {curr1} {curr2} {str(rate)}')

            if self.lastest_timestamp > quote_time:
                print('ignoring out-of-sequence message')
            else:
                self.graph.add_edge(curr1, curr2, rate)

                if curr1 not in self.quotes_timestamps.keys():
                    self.quotes_timestamps[curr1] = {curr2: quote_time}
                else:
                    self.quotes_timestamps[curr1][curr2] = quote_time

                self.lastest_timestamp = quote_time


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Please enter valid command i.e python3 lab2.py [PROVIDER_HOST] [PROVIDER_PORT]")
        exit(1)

    address = (sys.argv[1], int(sys.argv[2]))
    subscriber = ArbitrageSubscriber(address)
    subscriber.run()
