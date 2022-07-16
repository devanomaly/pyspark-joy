# Finds the degrees of separation between two given heroes in the marvel-graph.txt;
# Just for fun, let's see how close two of the most obscure heroes are: COOPER, TERI (id=1167) and CHAKRA II (id=982)

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, split, size, sum as sqlSUM, broadcast
from pyspark.sql.types import *
from pyspark import SparkContext
from numpy import Inf # we use numpy's infinity to mark unvisited nodes's distances from the source as infinite

# We'll use a Breadth-First-Search to solve the problem; in order to do so in an iterative way, a Queue comes in handy, so we make our own implementation of it based on linked-lists.


class Node:
    def __init__(self, val):
        self.val = val
        self.next = None


class Queue:
    def __init__(self):
        self.first = None
        self.last = None
        self.size = 0

    def enqueue(self, val):
        newNode = Node(val)
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
        dequeued = self.first.val
        if self.first == self.last:
            self.last = None
        self.first = self.first.next
        self.size -= 1
        return dequeued


# Sanity-checks
myQueue = Queue()
print("queue size =", myQueue.size)
myQueue.enqueue(1)
print("queue size =", myQueue.size)
myQueue.enqueue(2)
print("queue size =", myQueue.size)
myQueue.enqueue(3)
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())
print("queue size =", myQueue.size)
print("dequeued =>", myQueue.dequeue())

