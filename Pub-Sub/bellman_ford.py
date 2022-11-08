"""
Bellman Ford
:Authors: Noha Nomier
"""
import math

class BellmanFord:
    """
    Constructs a BellmanFord object that maintains a graph and runs 
    bellman ford algorithm on it to tetect negative cycle
    """
    def __init__(self):
        self.graph = {} #graph would look like {c1: {c2:price, c3,price} , c2: {c1,price, c4,price}, ....}

    def add_edge(self, curr1, curr2, price):
        """
        adds two new edges to the graph : (curr1, curr2, -log(price))
        and (curr2, curr1, log(price)) to the graph dictionary
        """
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
        """returns a reference to the current graph object"""
        return self.graph
    
    def remove_edge(self, curr1, curr2):
        """removes the two edges (curr1,curr2) and (curr2,curr1)"""
        if curr1 not in self.graph:
            print(f'Invalid removal, \'{curr1}\' doesn\'t exist in graph')
            return
        if curr2 not in self.graph:
            print(f'Invalid removal, \'{curr2}\' doesn\'t exist in graph')
            return

        del self.graph[curr1][curr2]
        del self.graph[curr2][curr1]

        if len(self.graph[curr1]) == 0 :
            del self.graph[curr1]
        if len(self.graph[curr2]) == 0 :
            del self.graph[curr2]
  

    def get_nodes(self):
        """returns list of nodes in the graph"""
        return list(self.graph.keys())
    
    def shortest_paths(self, start_vertex, tolerance=0):
        """
        Runs Bellman Ford shortest path algorithm on the graph starting at @start_vertex
        to detect negative cycle, a tolerance value is added to solve floating number rounding
        errors
        """
        # Dictionary to store lowest sum of edge weights
        distances = {}

        for curr in self.graph.keys():
            distances[curr] = float("Inf")

        distances[start_vertex] = 0

        V = len(self.graph) #number of vertices in the graph

        predecessor = {start_vertex : None}
        for _ in range(V - 1):
            for u, v_dict in self.graph.items():
                for v, weight in v_dict.items():
                    #relaxe edges if possible
                    if distances[u] != float("Inf") and distances[u] + weight - tolerance < distances[v] and distances[u] + weight + tolerance < distances[v]:
                        distances[v] = distances[u] + weight
                        predecessor[v] = u

        negative_cycle = None

        for u, v_dict in self.graph.items():
            for v, weight in v_dict.items():
                if distances[u] != float("Inf") and distances[u] + weight - tolerance < distances[v] and distances[u] + weight + tolerance < distances[v]:
                    negative_cycle = (u, v)
                    return distances, predecessor, negative_cycle
        
        return distances, predecessor, negative_cycle
