class Configuration(object):
    cassandra_client = None
    
    def __init__(self):
        raise Exception('Cannot create instances of Configuration -- use the class!')