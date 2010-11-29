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

class ColumnModelMeta(BaseModelMeta):
    def __init__(cls, name, bases, attrs):
        if getattr(cls.Meta, 'comparator_type', None) is None:
            # class Meta is not inherited if it is redefined in a subclass
            setattr(cls.Meta, 'comparator_type', 'BytesType')

        super(ColumnModelMeta, cls).__init__(name, bases, attrs)
        for k, v in cls._attributes.items():
            if getattr(v, 'row_key', False):
                cls._row_key = (k, v)
            if getattr(v, 'column_key', False):
                cls._column_key = (k, v)
                
        if cls._row_key is None and cls.Meta.column_family:
            raise Exception('No row_key found for non-primitive model.')

        if cls._column_key is None and cls.Meta.column_family:
            raise Exception('No row_key found for non-primitive model.')


class ColumnModel(BaseModel):
    __metaclass__ = ColumnModelMeta
    _row_key = None
    _column_key = None
    
    class Meta(BaseModel.Meta):
        comparator_type = 'BytesType'

    @classmethod
    def _pack_column(cls, column_key, attribute):
        return '%s%s' % (cls._column_key[1]._db_format(column_key), attribute)
    
    @classmethod    
    def _unpack_column(cls, column):
        id = uuid.UUID(bytes=column[0:16])
        attribute_name = column[16:]
        return (id, attribute_name)
    
    def _mutation_map_for_save(self):
        insert_dict = {}
        for k, p in self._attributes.items():
            v = self._attribute_values[k]
            if k not in (self._row_key[0], self._column_key[0]):
                insert_dict[self._pack_column(getattr(self, self._column_key[0]), k)] = self._getattr_for_db(k)
        
        row_key = self._getattr_for_db(self._row_key[0])
        mutation_map = {}
        mutation_map.update({row_key: {self.Meta.column_family: insert_dict}})
        return mutation_map


    @classmethod
    @inlineCallbacks
    def get(cls, row_key, column_key, configuration=Configuration):
        names = [cls._pack_column(column_key, x) for x,v in cls._attributes.items() if not x in (cls._row_key[0], cls._column_key[0])]
                
        record = yield configuration.cassandra_client.get_slice(row_key.bytes, cls.Meta.column_family, names=names)
                
        if record == []:
            returnValue(None)
        o = cls._result_to_instance(row_key, column_key, record)
        o._post_get()
        returnValue(o)


    @inlineCallbacks
    def delete(self, configuration=Configuration):
        pass

    @classmethod
    def _result_to_instance(cls, row_key, column_key, result):
        o = cls(is_new=False)
        o._setattr(o._row_key[0], row_key, filter=False)
        o._setattr(o._column_key[0], column_key, filter=False)
        for column in result:
            id, name = cls._unpack_column(column.column.name)
            if name not in (o._row_key[0], o._column_key[0]):
                o._setattr_from_db(name, column.column.value)
        return o

    
    def _result_to_instances(cls, row_key, result):
        results = {}
        for column in result:
            column_key, name = cls._unpack_column(column.column.name)
            o = results.get(column_key)
            if o is None:
                o = cls(is_new=False)
                o._setattr(o._row_key[0], row_key, filter=False)
                o._setattr(o._column_key[0], column_key, filter=False)
                results[column_key] = o
                
            if name not in (o._row_key[0], o._column_key[0]):
                o._setattr_from_db(name, column.column.value)
        return results
        
        
#     @classmethod
#     def _result_to_dict(cls, key, result):
#         columns = {'id': key}
#         for column in result:
#             attribute_name = column.column.name
#             p = cls._attributes[attribute_name] if attribute_name in cls._attributes else None
#             if p is None: raise Exception('Unknown attribute: %s' % attribute_name)
#             value = column.column.value
#             columns[attribute_name] = p.from_db_value(value)
#         return columns


    @classmethod
    @inlineCallbacks
    def execute_query(cls, query=None, configuration=Configuration):

        if query is None:
            raise Exception('query is None!')        
        
        
        for e in query._expressions:
            print e
            
        return
        
        row_key, kwargs = cls._slice_def_for_query(query)
        
        search_results = yield configuration.cassandra_client.get_slice(row_key, cls.Meta.column_family, **kwargs)
        
        results = cls._results_to_instances(search_results)

        sorted_results = sorted(results, key=attrgetter(order_key_name))
        if reverse_sort:
            sorted_results.reverse()
        
        returnValue(QueryResult(sorted_results, None))
