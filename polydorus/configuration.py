class Configuration(object):
    cassandra_client = None
    count = 10000
    column_count = count
    
    def __init__(self):
        raise Exception('Cannot create instances of Configuration -- use the class!')