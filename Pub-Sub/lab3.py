import sys
import socket
from datetime import datetime
from bellman_ford import BellmanFord
import fxp_bytes_subscriber
import math

LISTENER_ADDRESS = (socket.gethostbyname(socket.gethostname()), 60602) #TODO: CHANGE THIS TO BE MORE DYNAMIC
BUFF_SIZE = 4048
QUOTE_TIMEOUT = 1.5

class Lab3:
    def __init__(self, provider_address):
        self.provider_address = provider_address
        self.quotes_timestamps = {}  #key is curr1, val is {curr2, timestamp}
        self.graph = BellmanFord()
        self.lastest_timestamp = datetime.now()

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.bind(LISTENER_ADDRESS)  # subscriber binds the socket to the publishers address
            serailized_address = fxp_bytes_subscriber.serialize_address(LISTENER_ADDRESS[0], LISTENER_ADDRESS[1])
            sock.sendto(serailized_address, self.provider_address)
            while True:
                data_bytes, _ = sock.recvfrom(BUFF_SIZE)

                unmarshaled_data = fxp_bytes_subscriber.unmarshal_message(data_bytes)

                self.remove_outdated_quotes()
                self.handle_quotes_to_graph(unmarshaled_data)
                self.detect_arbitrage()

    def detect_arbitrage(self):
        #TODO: start with usd for now and maybe add tolerance
        distances, predecessor, negative_edge = self.graph.shortest_paths('USD', 1e-12)
        if negative_edge is not None:
            print(f'ARBITRAGE DETECTED:')
            self.get_cycle(negative_edge, predecessor, distances)



    def get_cycle(self, negative_edge, predecessor, distances, start_amount=100):
        graph = self.graph.get_graph()
        cycle = []      
        cycle_node = negative_edge[1] #start with an arbitrary node in the cycle 
        v = cycle_node 

        seen = {v} # set to see which currency is duplicated in the cycle to know what currency to start at
        start = None
        while True:
            v = predecessor[v]
            if v is None:
                return None
            if v in seen: #this would be the start of our cycle
               start = v
               break 
            seen.add(v)

        print(f"\tstart with {start} {str(start_amount)}")
        v = start
        while True:
            if v is None:
                return None
            cycle.append(v)
            #TODO: Remove this after making sure there is no infinity loop
            if len(cycle) > len(distances):
                print(f'distances: {distances}')
                print(f'{cycle}')
                print('THERE COULD HAVE BEEN AN INFINITY LOOP IDK WHY')
                break
            if v == start and len(cycle) > 1: #we've reached the end of the cycle 
                break
            v = predecessor[v]
            
        cycle.reverse()

        amount_exchanged = start_amount

        for i in range(0, len(cycle) - 1):
            price = math.exp(-1 * graph[cycle[i]][cycle[i+1]])
            amount_exchanged = amount_exchanged * price 
    
            print(f'\texchange {cycle[i]} for {cycle[i+1]} at {price} --> {cycle[i+1]}  {str(amount_exchanged)}')

        return cycle

    def remove_outdated_quotes(self):
        curr_utc_time =  datetime.utcnow()
        temp = {}
        for curr1, val_dict in self.quotes_timestamps.items():
            for curr2, last_timestamp in val_dict.items():
                if  (curr_utc_time - last_timestamp).total_seconds() > QUOTE_TIMEOUT:
                    print(f'removing stale quote for (\'{curr1}\', \'{curr2}\')')
                    self.graph.remove_edge(curr1, curr2)
                else:
                    if curr1 not in temp.keys():
                        temp[curr1] = {curr2: self.quotes_timestamps[curr1][curr2]}
                    else:
                         temp[curr1][curr2] = self.quotes_timestamps[curr1][curr2]

        self.quotes_timestamps = temp
    def handle_quotes_to_graph(self, received_quotes):
        for quote in received_quotes:
            print(f'{str()}')

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
	subscriber = Lab3(address)
	subscriber.run()