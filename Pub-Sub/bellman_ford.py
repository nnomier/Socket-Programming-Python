import math

class BellmanFord:

    def __init__(self):
        self.graph = {}

    def add_edge(self, curr1, curr2, price):
        rate = -1 * math.log(price)
        if curr1 not in self.graph.keys():
            self.graph[curr1] = {curr2: rate}
        else:
            self.graph[curr1][curr2] = rate

        if curr2 not in self.graph.keys():
            self.graph[curr2] = {curr1: -1 * rate}
        else:
            self.graph[curr2][curr1] = -1 * rate

    def get_graph(self):
        return self.graph
    
    def remove_edge(self, curr1, curr2):
        if curr1 not in self.graph:
            print(f'Invalid removal, \'{curr1}\' doesn\'t exist in graph')
            return
        if curr2 not in self.graph:
            print(f'Invalid removal, \'{curr2}\' doesn\'t exist in graph')
            return

        """
        {curr1 : {curr2:price, curr2:price},
         curr1 : {curr2:price} 
        }
        """

        del self.graph[curr1][curr2]
        del self.graph[curr2][curr1]

        if len(self.graph[curr1]) == 0 :
            del self.graph[curr1]
        if len(self.graph[curr2]) == 0 :
            del self.graph[curr2]
  


    def get_nodes(self):
        return list(self.graph.keys())
    
    def shortest_paths(self, start_vertex, tolerance=0):

        # Dictionary to store lowest sum of edge weights
        distances = {}

        for curr in self.graph.keys():
            distances[curr] = float("Inf")

        distances[start_vertex] = 0

        V = len(self.graph) #number of vertices in the graph

        predecessor = {start_vertex : None}
        # print(self.graph.items())
        for _ in range(V - 1):
            for u, v_dict in self.graph.items():
                for v, weight in v_dict.items():
                    #relaxe edges if possible
                    if distances[u] != float("Inf") and distances[u] + weight - tolerance < distances[v] and distances[u] + weight + tolerance < distances[v]:
                        distances[v] = distances[u] + weight
                        predecessor[v] = u

        negative_cycle = None
        # i = 0
        # do one more iteration to detect cycle
        for u, v_dict in self.graph.items():
            for v, weight in v_dict.items():
                if distances[u] != float("Inf") and distances[u] + weight - tolerance < distances[v] and distances[u] + weight + tolerance < distances[v]:
                    negative_cycle = (u, v)
                    return distances, predecessor, negative_cycle
        
        return distances, predecessor, negative_cycle
