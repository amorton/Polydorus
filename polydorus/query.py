class QueryResult(object):
    _results = None
    _count = None
    
    def __init__(self, results, count):
        self._results = results
        self._count = count
    
    def __len__(self):
        return len(self._results)
        
    # Get all results
    def __iter__(self):
        return iter(self._results)
        
    def __getslice__(self, i, j):
        return self._results[i:j]
        
    def __getitem__(self, key):
        return self._results[key]

    def __str__(self):
        if self._count is None:
            return "Empty QueryResult object"
        else:
            return "QueryResult object (total results: %s)" % (self._count)
            
    def __len__(self):
        return len(self._results)

    @property
    def total(self):
        return self._count


class Query(object):
    """Usage: 
        expression = IndexExpression(MyModel.name, IndexOperator.EQ, other)
        q = Query(MyModel, expression)
        for x in q[5:10]:
            print x
        print len(q)
    """
    _expressions = []
    _sorts = []
    _offset = None
    _limit = None
    _attributes = []
    
    def __init__(self, cls, expression=None, sort=None, attributes=None):
        self._model_class = cls
        self._expressions = [expression] if expression else []
        self._sorts = [sort] if sort else []
        self._attributes = [attributes] if attributes else []
        
    def __and__(self, other):
        self._expressions.extend(other._expressions)
        self._sorts += other._sorts
        self._attributes += other._attributes
        self._offset = other._offset or self._offset
        self._limit = other._limit or self._limit
        return self
        
    def filter(self, expression):
        return self & Query(self._model_class, expression=expression)
        
    def __str__(self):
        return "Query object (expressions: %s; sorts: %s; offset: %s, limit: %s, attributes: %s)" % (self._expressions, self._sorts, self._offset, self._limit, self._attributes)

    def sort(self, sort):
        return self & Query(self._model_class, sort=sort)
        
    def attributes(self, attributes):
        return self & Query(self._model_class, attributes=attributes)
        
    def offset(self, new_offset):
        self._offset = new_offset
        return self
        
    def limit(self, new_limit):
        self._limit = new_limit
        return self
    
    def execute(self):
        return self._model_class.execute_query(self)