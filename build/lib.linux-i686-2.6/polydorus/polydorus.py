# from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue
# from twisted.internet import reactor, defer
# import uuid
# import logging
# import time
# from .lib.database import dbconnection, keyspace
# from .lib import LateBindingProperty
# from telephus.cassandra.ttypes import InvalidRequestException, CfDef, ColumnDef, IndexExpression, IndexOperator
# import time
# from pytz import utc
# import copy
# import datetime
# import math
# import decimal
# import re
# import struct
# import pprint
# from netaddr.ip import IPAddress
# from netaddr.strategy import ipv4
# from dateutil import parser
# from operator import attrgetter, itemgetter
# from lib.types import EncryptedString, PhoneNumber
# 
# from lib.config import Config
# 
# config = Config.get_config()
# 
# secret = config.get('encryption', 'secret')
# salt = config.get('encryption', 'salt')
# 
# # Pack and Unpack are based on code from PyCassa <http://github.com/pycassa/pycassa/blob/master/pycassa/columnfamily.py#L272>
# 
# def init(conn):
#     global dbconnection
#     dbconnection = conn
# 
# def _pack(value, data_type):
#     """
#     Packs a value into the expected sequence of bytes that Cassandra expects.
#     """
#     if value is None:
#         return value
#     elif data_type == long:
#         return struct.pack('>q', long(value))  # q is 'long long'
#     elif data_type == int:
#         return struct.pack('>i', int(value))
#     elif data_type == str:
#         return struct.pack(">%ds" % len(value), value)
#     elif data_type == unicode:
#         try:
#             st = value.encode('utf-8')
#         except UnicodeDecodeError:
#             # value is already utf-8 encoded
#             st = value
#         return struct.pack(">%ds" % len(st), st)
# #     elif data_type == uuid.UUID:
# #         if not hasattr(value, 'bytes'):
# #             raise TypeError("%s not valid for %s" % (value, data_type))
# #         return struct.pack('>16s', value.bytes)
# #     elif data_type == IPAddress:
# #         if not hasattr(value, 'packed'):
# #             raise TypeError("%s not valid for %s" % (value, data_type))
# #         return struct.pack('>4s', value.packed)
#     else:
#         raise Exception("Unkown data_type:" + data_type)
# 
# def _unpack(b, data_type):
#     """
#     Unpacks Cassandra's byte-representation of values into their Python
#     equivalents.
#     """
# 
#     if data_type == long:
#         return struct.unpack('>q', b)[0]
#     elif data_type == int:
#         return struct.unpack('>i', b)[0]
#     elif data_type == str:
#         return struct.unpack('>%ds' % len(b), b)[0]
#     elif data_type == unicode:
#         unic = struct.unpack('>%ds' % len(b), b)[0]
#         return unic.decode('utf-8')
# #     elif data_type == uuid.UUID:
# #         temp_bytes = struct.unpack('>16s', b)[0]
# #         return uuid.UUID(bytes=temp_bytes)
# #     elif data_type == IPAddress:
# #         temp_bytes = struct.unpack('>4s', b)[0]
# #         return IPAddress(ipv4.packed_to_int(temp_bytes))
#     else: # BytesType
#         return b
# 
#         yield dbconnection.batch_mutate({id: {self.Meta.column_family: insert_dict}})
#         
#             
# class BaseModelMeta(type):
#     """Meta class to set the _attributes attribute on each class instance without bubbling up to the super class"""
#     def __init__(self, name, bases, attrs):
#         super(BaseModelMeta, self).__init__(name, bases, attrs)
# 
#         if name == 'BaseModel':
#             self._attributes = {}
#             self._rich_attributes = {}
#         else:
#             self._attributes = copy.copy(self._attributes)
#             self._rich_attributes = copy.copy(self._rich_attributes)
#             
#         if getattr(self.Meta, 'column_family', None) is None:
#             self.Meta.column_family = None
#         if getattr(self.Meta, 'comparator_type', None) is None:
#             self.Meta.comparator_type = 'UTF8Type'
#         if getattr(self.Meta, 'subcomparator_type', None) is None:
#             self.Meta.subcomparator_type = 'UTF8Type'
#             
#         delete_attributes = []
#         for k, v in attrs.items():
#             if isinstance(v, GenericAttribute):
#                 v.name = k
#                 self._attributes[k] = v
#                 delete_attributes.append(k)
#             elif isinstance(v, RichAttribute):
#                 v.Meta.base_name = "%s_%s" % (self.Meta.column_family, k)
#                 self._rich_attributes[k] = v
#                 delete_attributes.append(k)
#         for k in delete_attributes:
#             delattr(self, k)
# 
# 
# class BaseModel(object):
#     """This is the base model for twisted & cassandra"""
#     __metaclass__ = BaseModelMeta
# 
#     id = UUIDAttribute(read_only=True)
#     date_created = DateTimeAttribute(read_only=True, indexed=True)
#     date_modified = DateTimeAttribute(read_only=True, indexed=True)
#     
#     _attribute_values = {}
#     _rich_proxies = {}
#     
#     
#     def __init__(self, *args, **kwargs):
#         self._attribute_values = {}
#         for k, v in self._attributes.items():
#             self._attribute_values[k] = getattr(v, 'default', None)
#         for k, v in self._rich_attributes.items():
#             self._rich_proxies[k] = RichAttributeProxy(self, v)
#         
#         
# #             self._attribute_values[k] = getattr(v, 'default', None)
#             
#             
# #         print "self._attribute_values before update:"
# #         pprint.pprint(self._attribute_values)
#         for k, v in kwargs.items():
#             setattr(self, k, v)
# #         print "self._attribute_values after update:"
# #         pprint.pprint(self._attribute_values)
# #         self.id = None
# 
#     class Meta:
#         """This is model (user space) metadata, not python metadata"""
#         pass
# #         column_family = None
# #         comparator_type = 'UTF8Type'
# #         subcomparator_type = 'UTF8Type'
# #     
#     #id = PrimaryKeyAttribute()
#     
#     def _setattr(self, name, value, filter=True):
#         self.__setattr__(name, value, filter=filter)
#     
#     def __setattr__(self, name, value, filter=True):
#         if name == '_attribute_values':
#             super(BaseModel, self).__setattr__(name, value)
#         else:
# #             print '__setattr__(%s, %s)' % (name, value)
#             attr = self._attributes[name]
#             v = attr.validate(value)
#             if filter:
#                 v = attr.filter_input(self, v)
#             self._attribute_values[name] = v
#         
#         
#     def __getattr__(self, name):
# #         print "__getattr__(self, %s)" % name
#         v = self._attribute_values[name] if name in self._attribute_values else self._rich_proxies[name]
# #         print "return ", v
#         return v
#         
#     def _setattr_from_db(self, name, value):
#         attr = self._attributes[name]
#         self._attribute_values[name] = attr.from_db_value(value)
# 
#     def _getattr_for_db(self, name):
#         attr = self._attributes[name]
#         value = self._attribute_values[name]
#         if value is None and attr.required:
#             raise Exception("%s is required." % name)
#         return attr.to_db_value(value)
#     
#     #@inlineCallbacks
#     def update(self, *args, **kwargs):
#         for k, v in kwargs.items():
#             setattr(self, k, v)
# #             try:
# #                 setattr(self, k, v)
# #             except Exception:
# #                 print 'Error calling update on %s for %s with value: %s' % (k, self, v)
#     
#     @inlineCallbacks
#     def save(self, retry=0):
#         if retry:
#             logging.warn('Retry #%s' % retry)
#            # print 'Retry #%s' % retry
#         # Try to insert the columns
#         
#         
#         now = datetime.datetime.now(tz=utc).replace(microsecond=0)
# 
#         self._setattr('date_modified', now, filter=False)
#         if self.id is None:
#             self._setattr('id', uuid.uuid1(), filter=False)
#             self._setattr('date_created', now, filter=False)
#         
#         
#         insert_dict = {}
#         for k, p in self._attributes.items():
#             v = self._attribute_values[k]
#             yield defer.maybeDeferred(p.before_save, self)
#             if k != 'id':
#                 insert_dict[k] = self._getattr_for_db(k)
#         id = self._getattr_for_db('id')
#         
#         mutation_map = {}
#         
#         for p in self._rich_proxies.values():
#             mutation_map.update(p.db_data())
#         
#         print mutation_map
#         
#         mutation_map.update({id: {self.Meta.column_family: insert_dict}})
#         
#         yield dbconnection.batch_mutate(mutation_map)
#         
#         for k, p in self._attributes.items():
#             v = self._attribute_values[k]
#             yield defer.maybeDeferred(p.after_save, self)
# 
#         logging.debug('Saved.')
#         returnValue(True)
#     
#     @inlineCallbacks
#     def delete(self):
#         if self.id is None:
#             returnValue(False)
#             yield
#         else:
#             yield dbconnection.remove(self.id.bytes, self.Meta.column_family)
#             self._setattr('id', None, filter=False)
#             self._setattr('date_modified', None, filter=False)
#             self._setattr('date_created', None, filter=False)
#             returnValue(True)
# 
#     @classmethod
#     @inlineCallbacks
#     def list(cls, predicate=None, start='', finish=''):
# #         yield cls.create_column_families()
#         # all_users[0].columns[0].column.value = 'Matt'
#         key_slice = yield dbconnection.get_range_slices(cls.Meta.column_family, start=start, finish=finish)
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
#     
#     @classmethod
#     def _result_to_instance(cls, key, result):
#         o = cls()
# #         print "o:", id(o)
#         o._setattr('id', key, filter=False)
# #         o.id = key
#         for column in result:
#             o._setattr_from_db(column.column.name, column.column.value)
#         
# #             if attribute_name == 'int_test':
# #                 print '_result_to_instance:', o.int_test
#         return o
#         
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
#         
#     @classmethod
#     @inlineCallbacks
#     def get(cls, key):
# #         yield cls.create_column_families()
#         assert(isinstance(key, uuid.UUID))
#         names = cls._attributes.keys()
# #         names = properties.keys()
#         #names = tuple([v.name if v.name else k for k, v in properties])
#        # print "Looking for key %s (%s)", (str(key), [key.bytes])
#         record = yield dbconnection.get_slice(key.bytes, cls.Meta.column_family, names=names)
#         if record == []:
#             returnValue(None)
#             
#         o = cls._result_to_instance(key, record)
#         
#             
#         returnValue(o)
# 
#     @classmethod
#     @inlineCallbacks
#     def filter(cls, filters=None, sorts=None, page=None, limit=None):
#         sorts = sorts or ['id']
#         filters = filters or []
#         page = page or 1
#         limit = limit or 25
#         # get from memcache
#         # if not gotten from memcache:
#         query = None
#         excludes = {}
#         preliminary_columns = []
#         for x in sorts:
#             preliminary_columns.append(x.lstrip('+-'))
# #         print "sorts:", sorts
#         if len(sorts) > 1:
#             raise Exception("Multiple order clauses not supported.")
#         order = sorts[0]
#         reverse_sort = order.startswith('-')
#         order_key_name = order.lstrip('+-')
#         
#         for [name, comparator, value] in filters:
#             p = cls._attributes[name] if name in cls._attributes else None
#             if p is None: raise Exception('Unknown attribute: %s' % name)
#             
#             if comparator == "=":
#                 q = (p == value)
#             elif comparator == "!=":
#                 excludes[name] = value
#                 preliminary_columns.append(name)
#             elif comparator == ">":
#                 q = (p > value)
#             elif comparator == ">=":
#                 q = (p >= value)
#             elif comparator == "<":
#                 q = (p < value)
#             elif comparator == "<=":
#                 q = (p <= value)
#             else:
#                 raise Exception('Unsupported comparator: %s', comparator)
#                 
#             if query is None:
#                 query = q
#             else:
#                 query = (query & q)
#                     
# #         print 'preliminary_columns:', preliminary_columns
#         
# #         print "query.expressions:", query.expressions
#         
#         preliminary_results = yield dbconnection.get_indexed_slices(cls.Meta.column_family, query.expressions, names=preliminary_columns, start_key='')
#         
#         preliminary_results = [cls._result_to_dict(r.key, r.columns) for r in preliminary_results]
#         
# #         print "preliminary_results:", preliminary_results
#         
#         # check for != values
#         def check_excludes(cols):
#             for k, v in excludes.items():
#                 p = cls._attributes[k] if k in cls._attributes else None
#                 if p is None: raise Exception('Unknown attribute: %s' % k)
# #                 print "cols:", cols
#                 attr = cols[k] if k in cols else None
# #                 print 'attr %s: %s' % (k, attr)
#                 if attr is not None and cols[k] == p.validate(v):
# #                     print 'filtered out:', attr
#                     return False
#             return True
#             
#         matching_results = filter(check_excludes, preliminary_results)
#              
# #         print "matching_results:", matching_results
#                 
#         sorted_matching_results = sorted(matching_results, key=itemgetter(order_key_name))
# #         try:
# #             print [r['int_test'] for r in sorted_matching_results]
# #         except:
# #             pass
#             
#         matching_keys = [r['id'] for r in sorted_matching_results]
#         l = len(matching_keys)
#        
#         ### save matching_results to memcached ###
#         fetch_keys = matching_keys[(page-1)*limit:page*limit] if not reverse_sort else matching_keys[l-(page-1)*limit:l-(page*limit):-1]        
#         search_results = yield dbconnection.multiget_slice(fetch_keys, cls.Meta.column_family, count=limit)
#         
#         results = []
#         for (key, columns) in search_results.items():
#             results.append(cls._result_to_instance(uuid.UUID(bytes=key), columns))
# 
#         sorted_results = sorted(results, key=attrgetter(order_key_name))
#         if reverse_sort: sorted_results = sorted_results[::-1]
#         returnValue((l, sorted_results))
# 
#     def as_dict(self, properties=None):
#         if properties is None:
#             properties = self._attributes.keys()
#         return dict((name, getattr(self, name, None)) for name in properties)        
