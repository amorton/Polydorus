from twisted.internet import defer, reactor
from telephus.protocol import ManagedCassandraClientFactory, ManagedThriftClientProtocol
from telephus.cassandra.ttypes import *

from telephus.porcelain_client import PorcelainClient

@defer.inlineCallbacks
def connect(keyspace='system', hosts=['localhost:9160'], connections=5, consistency=ConsistencyLevel.ONE):
    manager = ManagedCassandraClientFactory()
    client = PorcelainClient(manager, consistency=consistency)
    
    for i in xrange(connections):
        for host, port in [x.split(':') for x in hosts]:
            reactor.connectTCP(host, int(port), manager)    
    
    yield manager.deferred
    yield client.set_keyspace(keyspace)
   
    defer.returnValue(client)
