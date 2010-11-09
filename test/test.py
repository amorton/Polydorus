from twisted.internet import defer, reactor
import cassandra
import polydorus
import models
# from models import init_models

polydorus.Settings.client = cassandra.connect()

# init_models(client)

def x():
	t = models.TestModel()
	t.test()
	
reactor.callLater(0, x)

reactor.run()