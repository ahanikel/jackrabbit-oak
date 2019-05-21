from diagrams import Cluster, Diagram, Edge
from diagrams.onprem.compute import Server

with Diagram("Oak Streaming (fine-grained)", show=False, direction="RL"):
    zmq = Edge(color="brown", label="ZeroMQ")
    inProcess = Edge(color="grey", label="in-process")
    nodeStore = Server("NodeStore")
    with Cluster("Write"):
        log = Server("Log (or Kafka topic)")
        nodeStore >> zmq >> log
    with Cluster("Read"):
        agg = Server("NodeStateAggregator")
        backendStore = Server("BackendStore")
        log >> zmq >> agg
        agg >> inProcess >> backendStore
        backendStore >> zmq >> nodeStore
