from twisted.internet import defer, reactor
from telephus.protocol import ManagedCassandraClientFactory, ManagedThriftClientProtocol
from telephus.cassandra.ttypes import *
from telephus.client import CassandraClient

def connect(keyspace='system', hosts=['localhost:9160'], connections=5, consistency=ConsistencyLevel.ONE):
	manager = ManagedCassandraClientFactory(keyspace=keyspace)
	client = CassandraClient(manager, consistency=consistency)
	
	for i in xrange(connections):
	    for host, port in [x.split(':') for x in hosts]:
	        reactor.connectTCP(host, int(port), manager)    
	return client