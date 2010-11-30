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
from base_model import BaseModel, BaseModelMeta


class RowModelMeta(BaseModelMeta):
    def __init__(cls, name, bases, attrs):
        super(RowModelMeta, cls).__init__(name, bases, attrs)
        for k, v in cls._attributes.items():
            if getattr(v, 'row_key', False):
                cls._row_key = (k, v)
        if cls._row_key is None and cls.Meta.column_family:
            raise Exception('No row_key found for non-primitive model.')
        
class RowModel(BaseModel):
    __metaclass__ = RowModelMeta
    _row_key = None
    
    def _pre_save(self):
        if self._is_new:
            self._setattr(self._row_key[0], uuid.uuid1(), filter=False)        
    
    def _mutation_map_for_save(self):
        insert_dict = {}
        for k in self._dirty:
            v = self._attribute_values[k]
            if k != self._row_key[0]:
                insert_dict[k] = self._getattr_for_db(k)
        
        row_key = self._getattr_for_db(self._row_key[0])
        
        mutation_map = {}
        mutation_map.update({row_key: {self.Meta.column_family: insert_dict}})
        return mutation_map
        
    
    @inlineCallbacks
    def delete(self, configuration=Configuration):
        if self._row_key[1] is None:
            returnValue(False)
            yield
        else:
            yield configuration.cassandra_client.remove(getattr(self, self._row_key[0]).bytes, self.Meta.column_family)
            self._setattr(self._row_key[0], None, filter=False)
            self._setattr('date_modified', None, filter=False)
            self._setattr('date_created', None, filter=False)
            returnValue(True)

#     @classmethod
#     @inlineCallbacks
#     def list(cls, predicate=None, start='', finish='', configuration=Configuration):
#         key_slice = yield configuration.cassandra_client.get_range_slices(cls.Meta.column_family, start=start, finish=finish)
#         objects = []
#         for record in key_slice:
#             o = cls()
#             setattr(o, 'id', uuid.UUID(record.key).hex)
#             for column in record.columns:
#                 setattr(o, column.column.name, column.column.value)
#             if predicate is None or predicate(o):
#                 objects.append(o)
#         
#         returnValue(objects)
    
    @classmethod
    def _result_to_instance(cls, key, result):
        o = cls(is_new=False)
        o._setattr(o._row_key[0], key, filter=False)
        for column in result:
            o._setattr_from_db(column.column.name, column.column.value)
        return o
        
    @classmethod
    def _result_to_dict(cls, key, result):
        columns = {cls._row_key[0]: key}
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
#         assert(isinstance(key, uuid.UUID))
        #TODO assert key is same type as row_key attribute / support not UUID key
        names = cls._attributes.keys()
        record = yield configuration.cassandra_client.get_slice(key.bytes, cls.Meta.column_family, names=names)
        if record == []:
            returnValue(None)
        o = cls._result_to_instance(key, record)
        o._post_get()
        returnValue(o)


    @classmethod
    @inlineCallbacks
#     def filter(cls, filters=None, sorts=None, page=None, limit=None, configuration=Configuration):
    def execute_query(cls, query=None, configuration=Configuration):
        if query is None:
            raise Exception('query is None!')
            
        sorts = query._sorts or [cls._row_key[0]]
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
        matching_keys = [r[cls._row_key[0]] for r in sorted_matching_results]
        l = len(matching_keys)
       
        fetch_keys = matching_keys[offset:offset+limit] if not reverse_sort else matching_keys[l-offset-1:l-offset-limit-1:-1]        
        search_results = yield configuration.cassandra_client.multiget_slice(fetch_keys, cls.Meta.column_family, count=limit)
        
        results = []
        for (key, columns) in search_results.items():
            results.append(cls._result_to_instance(uuid.UUID(bytes=key), columns))

        sorted_results = sorted(results, key=attrgetter(order_key_name))
        if reverse_sort:
            sorted_results.reverse()
        
        returnValue(QueryResult(sorted_results, l))

    def as_dict(self, properties=None):
        if properties is None:
            properties = self._attributes.keys()
        return dict((name, getattr(self, name, None)) for name in properties)        
