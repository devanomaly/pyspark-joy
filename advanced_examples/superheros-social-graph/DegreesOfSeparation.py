# Finds the degrees of separation between two given heroes in the "marvel-graph.txt" file;
# Just for fun, let's see how close two of the most obscure heroes are: COOPER, TERI (id=1167) and CHAKRA II (id=982) -- since we've learned about them in "MostPopularAndObscure.py"

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, split, size, sum as sqlSUM, broadcast
from pyspark.sql.types import *
from pyspark import SparkContext
from numpy import (
    Inf,
)  # we use numpy's infinity to mark unvisited nodes's distances from the source as infinite

# We'll use a Breadth-First-Search to solve the problem; in order to do so in an iterative way, a Queue comes in handy, so we make our own implementation of it based on linked-lists.


class Node:
    def __init__(self, node_ID):
        self.node_ID = node_ID
        self.next = None


class Queue:
    def __init__(self):
        self.first = None
        self.last = None
        self.size = 0

    def enqueue(self, node_ID, NodeClass, node=None):
        newNode = node if node else NodeClass(node_ID)
        if not self.first:
            self.first = newNode
        else:
            self.last.next = newNode
        self.last = newNode
        self.size += 1
        return self.size

    def dequeue(self):
        if not self.first:
            return None
        dequeued = self.first.node_ID
        if self.first == self.last:
            self.last = None
        self.first = self.first.next
        self.size -= 1
        return dequeued


# Sanity-checks
myQueue = Queue()
print("queue size =", myQueue.size)
myQueue.enqueue(1, Node)
print("queue size =", myQueue.size)
myQueue.enqueue(2, Node)
print("queue size =", myQueue.size)
myQueue.enqueue(3, Node)
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())

# Now, we define HeroNode class, made of connections defined by its parent class.
class HeroNode(Node):
    def __init__(self, hero_ID):
        super().__init__(hero_ID)
        self.source_distance = Inf
        self.visited = False
        
    @property
    def hero_ID(self):
      return self.node_ID
    
    def markAsVisited(self):
        self.visited = True
        return True

    def setDistance(self, newDist):
        if newDist < self.source_distance:
            self.source_distance = newDist
            return True
        self.markAsVisited()
        return False


class HeroGraph:
    def __init__(self):
        self.adjList = (
            {}
        )  # this adjList should have node_ID as key and a list of the former's neighbors in complete HeroNode format

    def addVertex(self, key):
        try:
            if self.adjList[key]:
                return False
        except KeyError:
            self.adjList[key] = []  # FIXME:>> maybe it should be an RDD?
        return True

    def addEdge(self, heroNode1, heroNode2):
        self.addVertex(heroNode1.hero_ID)
        self.addVertex(heroNode2.hero_ID)
        # TODO:>> think about using assymetric adjList in order to avoid "ahead of time visiting"... maybe this could help avoid revisiting HeroNodes?
        # FIXME:>> avoid duplicates! what if at least one of the edges already exists?
        self.adjList[heroNode1.hero_ID].push(heroNode2)
        self.adjList[heroNode2.hero_ID].push(heroNode1)
        return True

    def removeEdge(self, heroNode1, heroNode2):
      # FIXME:>> add exceptions for non existing keys!
        self.adjList[heroNode1.hero_ID] = self.adjList[heroNode1.hero_ID].filter(lambda heroNode: heroNode != heroNode2)
        self.adjList[heroNode2.hero_ID] = self.adjList[heroNode2.hero_ID].filter(lambda heroNode: heroNode != heroNode1)
        return True

    def removeHeroNode(self, heroNode):
        while self.adjList[heroNode.hero_ID].length:
            self.removeEdge(heroNode, self.adjList[heroNode.hero_ID].pop())
        del self.adjList[heroNode.hero_ID]
        return True
