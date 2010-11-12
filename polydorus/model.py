from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue
from twisted.internet import reactor, defer
import uuid
import logging
import datetime
from pytz import utc
from telephus.cassandra.ttypes import InvalidRequestException, CfDef, ColumnDef, IndexExpression, IndexOperator
import copy
import math
import decimal
import re
from netaddr.ip import IPAddress
from netaddr.strategy import ipv4
from dateutil import parser
from operator import attrgetter, itemgetter
from attributes import *
from configuration import Configuration
from query import Query, QueryResult

class ModelMeta(type):
    """Meta class to set the _attributes attribute on each class instance without bubbling up to the super class"""
    def __init__(cls, name, bases, attrs):
        super(ModelMeta, cls).__init__(name, bases, attrs)

        if name == 'Model':
            cls._attributes = {}
            cls._rich_attributes = {}
        else:
            cls._attributes = copy.copy(cls._attributes)
            cls._rich_attributes = copy.copy(cls._rich_attributes)
            
        if getattr(cls.Meta, 'column_family', None) is None:
            cls.Meta.column_family = None
        if getattr(cls.Meta, 'comparator_type', None) is None:
            cls.Meta.comparator_type = 'UTF8Type'
        if getattr(cls.Meta, 'subcomparator_type', None) is None:
            cls.Meta.subcomparator_type = 'UTF8Type'
            
        delete_attributes = []
        for k, v in attrs.items():
            if isinstance(v, GenericAttribute):
                v.name = k
                v._model_class = cls.__class__
                cls._attributes[k] = v
                delete_attributes.append(k)
            elif isinstance(v, RichAttribute):
                v.Meta.base_name = "%s_%s" % (cls.Meta.column_family, k)
                v._model_class = cls.__class__
                cls._rich_attributes[k] = v
                delete_attributes.append(k)
        for k in delete_attributes:
            delattr(cls, k)
    
    def __getattr__(cls, attr):
        if attr not in cls._attributes: 
            raise AttributeError('%s does not exist' % attr)
        return cls._attributes[attr]

class Model(object):
    """This is the base model for twisted & cassandra"""
    __metaclass__ = ModelMeta

    id = UUIDAttribute(read_only=True)
    date_created = DateTimeAttribute(read_only=True, indexed=True)
    date_modified = DateTimeAttribute(read_only=True, indexed=True)
    
    _attribute_values = {}
    _rich_proxies = {}
    _client = None
    
    def __init__(self, *args, **kwargs):
        self._attribute_values = {}
            
        for k, v in self._attributes.items():
            self._attribute_values[k] = getattr(v, 'default', None)
        for k, v in self._rich_attributes.items():
            self._rich_proxies[k] = RichAttributeProxy(self, v)
        for k, v in kwargs.items():
            setattr(self, k, v)

    class Meta:
        """This is model (user space) metadata, not python metadata"""
        pass
    
    def _setattr(self, name, value, filter=True):
        self.__setattr__(name, value, filter=filter)
    
    def __setattr__(self, name, value, filter=True):
        if name == '_attribute_values':
            super(Model, self).__setattr__(name, value)
        else:
            attr = self._attributes[name]
            v = attr.validate(value)
            if filter:
                v = attr.filter_input(self, v)
            self._attribute_values[name] = v
        
        
    def __getattr__(self, name):
        v = self._attribute_values[name] if name in self._attribute_values else self._rich_proxies[name]
        return v
        
    def _setattr_from_db(self, name, value):
        attr = self._attributes[name]
        self._attribute_values[name] = attr.from_db_value(value)

    def _getattr_for_db(self, name):
        attr = self._attributes[name]
        value = self._attribute_values[name]
        if value is None and attr.required:
            raise Exception("%s is required." % name)
        return attr.to_db_value(value)
    
    def update(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    @inlineCallbacks
    def save(self, retry=0, configuration=Configuration):
        if retry:
            logging.warn('Retry #%s' % retry)
        now = datetime.datetime.now(tz=utc).replace(microsecond=0)

        self._setattr('date_modified', now, filter=False)
        if self.id is None:
            self._setattr('id', uuid.uuid1(), filter=False)
            self._setattr('date_created', now, filter=False)
        
        
        insert_dict = {}
        for k, p in self._attributes.items():
            v = self._attribute_values[k]
            yield defer.maybeDeferred(p.before_save, self)
            if k != 'id':
                insert_dict[k] = self._getattr_for_db(k)
        id = self._getattr_for_db('id')
        
        mutation_map = {}
        
        for p in self._rich_proxies.values():
            mutation_map.update(p.db_data())
        
        mutation_map.update({id: {self.Meta.column_family: insert_dict}})
        
        yield configuration.cassandra_client.batch_mutate(mutation_map)
        
        for k, p in self._attributes.items():
            v = self._attribute_values[k]
            yield defer.maybeDeferred(p.after_save, self)

        logging.debug('Saved.')
        returnValue(True)
    
    @inlineCallbacks
    def delete(self, configuration=Configuration):
        if self.id is None:
            returnValue(False)
            yield
        else:
            yield configuration.cassandra_client.remove(self.id.bytes, self.Meta.column_family)
            self._setattr('id', None, filter=False)
            self._setattr('date_modified', None, filter=False)
            self._setattr('date_created', None, filter=False)
            returnValue(True)

    @classmethod
    @inlineCallbacks
    def list(cls, predicate=None, start='', finish='', configuration=Configuration):
        key_slice = yield configuration.cassandra_client.get_range_slices(cls.Meta.column_family, start=start, finish=finish)
        objects = []
        for record in key_slice:
            o = cls()
            setattr(o, 'id', uuid.UUID(record.key).hex)
            for column in record.columns:
                setattr(o, column.column.name, column.column.value)
            if predicate is None or predicate(o):
                objects.append(o)
        
        returnValue(objects)
    
    @classmethod
    def _result_to_instance(cls, key, result):
        o = cls()
        o._setattr('id', key, filter=False)
        for column in result:
            o._setattr_from_db(column.column.name, column.column.value)
        return o
        
    @classmethod
    def _result_to_dict(cls, key, result):
        columns = {'id': key}
        for column in result:
            attribute_name = column.column.name
            p = cls._attributes[attribute_name] if attribute_name in cls._attributes else None
            if p is None: raise Exception('Unknown attribute: %s' % attribute_name)
            value = column.column.value
            columns[attribute_name] = p.from_db_value(value)
        return columns
        
    @classmethod
    @inlineCallbacks
    def get(cls, key, configuration=Configuration):
        assert(isinstance(key, uuid.UUID))
        names = cls._attributes.keys()
        record = yield configuration.cassandra_client.get_slice(key.bytes, cls.Meta.column_family, names=names)
        if record == []:
            returnValue(None)
        o = cls._result_to_instance(key, record)
        returnValue(o)

    @classmethod
    def filter(cls, expression=None):
        return Query(cls, expression)



    @classmethod
    @inlineCallbacks
#     def filter(cls, filters=None, sorts=None, page=None, limit=None, configuration=Configuration):
    def execute_query(cls, query=None, configuration=Configuration):
        if query is None:
            raise Exception('query is None!')
            
        sorts = query._sorts or ['id']
        offset = query._offset or 0
        limit = query._limit or 25
        
        # get from memcache
        # if not gotten from memcache:
        preliminary_columns = []
        for x in sorts:
            preliminary_columns.append(x.lstrip('+-'))
        if len(sorts) > 1:
            raise Exception("Multiple order clauses not supported.")
        order = sorts[0]
        reverse_sort = order.startswith('-')
        order_key_name = order.lstrip('+-')

        excludes = {}
        for i, e in enumerate(query._expressions):
            if e.op == IndexOperator.NE:
                excludes[e.column_name] = e.value
                preliminary_columns.append(e.column_name)
                del query._expressions[i]
                    
        preliminary_results = yield configuration.cassandra_client.get_indexed_slices(cls.Meta.column_family, query._expressions, names=preliminary_columns, start_key='')
        
        preliminary_results = [cls._result_to_dict(r.key, r.columns) for r in preliminary_results]
        
        def check_excludes(cols):
            for k, v in excludes.items():
                p = cls._attributes[k] if k in cls._attributes else None
                if p is None: raise Exception('Unknown attribute: %s' % k)
                attr = cols[k] if k in cols else None
                if attr is not None and cols[k] == v:
                    return False
            return True
            
        matching_results = filter(check_excludes, preliminary_results)
                             
        sorted_matching_results = sorted(matching_results, key=itemgetter(order_key_name))
        matching_keys = [r['id'] for r in sorted_matching_results]
        l = len(matching_keys)
       
#         fetch_keys = matching_keys[(page-1)*limit:page*limit] if not reverse_sort else matching_keys[l-(page-1)*limit:l-(page*limit):-1]        
        fetch_keys = matching_keys[offset:offset+limit] if not reverse_sort else matching_keys[l-offset-1:l-offset-limit-1:-1]        
        search_results = yield configuration.cassandra_client.multiget_slice(fetch_keys, cls.Meta.column_family, count=limit)
        
        results = []
        for (key, columns) in search_results.items():
            results.append(cls._result_to_instance(uuid.UUID(bytes=key), columns))

        sorted_results = sorted(results, key=attrgetter(order_key_name))
        if reverse_sort: sorted_results = sorted_results[::-1]
        
        returnValue(QueryResult(sorted_results, l))
#         returnValue((l, sorted_results))

    def as_dict(self, properties=None):
        if properties is None:
            properties = self._attributes.keys()
        return dict((name, getattr(self, name, None)) for name in properties)        
