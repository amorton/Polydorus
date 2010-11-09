from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue
from twisted.internet import reactor, defer
import uuid
import logging
import time
from telephus.cassandra.ttypes import InvalidRequestException, CfDef, ColumnDef, IndexExpression, IndexOperator
import time
from pytz import utc
import copy
import datetime
import math
import decimal
import re
import struct
from netaddr.ip import IPAddress
from netaddr.strategy import ipv4
from dateutil import parser
from operator import attrgetter, itemgetter
from types import *

def _pack(value, data_type):
    """
    Packs a value into the expected sequence of bytes that Cassandra expects.
    """
    if value is None:
        return value
    elif data_type == long:
        return struct.pack('>q', long(value))  # q is 'long long'
    elif data_type == int:
        return struct.pack('>i', int(value))
    elif data_type == str:
        return struct.pack(">%ds" % len(value), value)
    elif data_type == unicode:
        try:
            st = value.encode('utf-8')
        except UnicodeDecodeError:
            st = value
        return struct.pack(">%ds" % len(st), st)
    else:
        raise Exception("Unkown data_type:" + data_type)

def _unpack(b, data_type):
    """
    Unpacks Cassandra's byte-representation of values into their Python
    equivalents.
    """

    if data_type == long:
        return struct.unpack('>q', b)[0]
    elif data_type == int:
        return struct.unpack('>i', b)[0]
    elif data_type == str:
        return struct.unpack('>%ds' % len(b), b)[0]
    elif data_type == unicode:
        unic = struct.unpack('>%ds' % len(b), b)[0]
        return unic.decode('utf-8')
    else: # BytesType
        return b      
        
        
class Query(object):
    expressions = []
    
    def __init__(self, expression=None):
        super(Query, self).__init__()
        self.expressions = [expression] if expression else []
        
    def __and__(self, other):
        new_copy = Query()
        new_copy.expressions.extend(self.expressions)
        new_copy.expressions.extend(other.expressions)
        return new_copy
        
    def __str__(self):
        return "Query object: expressions: %s" % self.expressions

class GenericAttribute(object):
    """This is used to denote a attribute in a Model"""
    default = None
    indexed = False
    required = False
    _type = str
    _db_type = str
    read_only = False
    write_once = False
    rich_kid = False
    
    def __init__(self, *args, **kwargs):
        self.__dict__.update(kwargs)
            
    def __eq__(self, other):
        return Query(IndexExpression(self.name, IndexOperator.EQ, self._db_format(other)))

    def __lt__(self, other):
        return Query(IndexExpression(self.name, IndexOperator.LT, self._db_format(other)))

    def __le__(self, other):
        return Query(IndexExpression(self.name, IndexOperator.LTE, self._db_format(other)))

    def __gt__(self, other):
        return Query(IndexExpression(self.name, IndexOperator.GT, self._db_format(other)))

    def __ge__(self, other):
        return Query(IndexExpression(self.name, IndexOperator.GTE, self._db_format(other)))
    
    def _coerce(self, value):
        return self._type(value)
        
    def _coerce_from_db(self, value):
        if self._db_type != self._type:
            return self._type(value)
        else:
            return value
        
    def _coerce_to_db(self, value):
        if value is None:
            return None
        elif self._db_type == self._type:
            return value
        else:
            return self._db_type(value) 
        
    def _pack(self, value):
        return None if value is None else _pack(value, self._db_type)

    def _unpack(self, value):
        return _unpack(value, self._db_type)

    def _db_format(self, value):
        return self._pack(self._coerce_to_db(self._coerce(value)))

    # Filter input when properties are set
    def input_filter(self, model, value): return value
    
    def before_save(self, model): pass

    def after_save(self, model): pass
    
    def filter_input(self, model, value):
        if self.read_only:
            raise Exception("Cannot write to a read-only attribute.")
        elif self.write_once and model.id is not None:
            raise Exception("Cannot update a write-once attribute.")
        elif self.required and value is None:
            raise TypeError('Property can not be null for "%s" column' % (self.name))
        else:
            return self.input_filter(model, value)
            
    def validate(self, value):
        value = self._coerce(value)
        if value is not None:
            if not isinstance(value, self._type):
                raise TypeError('Attribute "%s" needs to be type: %s (got %s)' % (self.name, self._type, type(value)))
        return value
        
    def to_db_value(self, value):
        return self._pack(self._coerce_to_db(value))
        
    def from_db_value(self, value):
        return self._coerce_from_db(self._unpack(value))

        
class UUIDAttribute(GenericAttribute):
    _type = uuid.UUID
    _db_type = str
    
    def _coerce(self, value):
        return uuid.UUID(value) if isinstance(value, str) else value

    def _coerce_to_db(self, value):
        return None if value is None else value.bytes
        
    def _coerce_from_db(self, value):
        return uuid.UUID(bytes=value)

class ForeignKeyAttribute(UUIDAttribute):
    # TODO: Allow late binding by passing the name of the class instead of the class itself
    # to allow circular references
    indexed = True
    write_once = True
    required = True
    
    def __init__(self, foreign_class, *args, **kwargs):
        super(ForeignKeyAttribute, self).__init__(self, *args, **kwargs)
        self.foreign_class = foreign_class

class IntegerAttribute(GenericAttribute):
    _type = int
    _db_type = int
        
class LongAttribute(GenericAttribute):
    _type = long
    _db_type = long
        
class StringAttribute(GenericAttribute):
    _type = unicode
    _db_type = unicode
    
    def _coerce(self, value):
        max_length = getattr(self, 'max_length', None)
        if max_length and len(value) > max_length:
            raise ValueError('Value "%s" is longer than max_length (%s) for column `%s`' % (value, max_length, name))
        if value is not None and getattr(self, '_regex', None):
            assert(self._regex.match(value))
        return unicode(value)

class EncryptedStringAttribute(GenericAttribute):
    _type = EncryptedString
    _db_type = str
    
    def _coerce(self, value):
        return value if (value is None or isinstance(value, self._type)) else EncryptedString(secret, value)

    def _coerce_to_db(self, value):
        return None if value is None else value.bytes
        
    def _coerce_from_db(self, bytes):
        return EncryptedString(secret, bytes=bytes)

class BooleanAttribute(IntegerAttribute):
    _type = bool
    _db_type = int
        
class DateTimeAttribute(GenericAttribute):
    _type = datetime.datetime
    _db_type = long
    
    def _coerce(self, value):
        if value is None:
            return None
        else:
            d = value if isinstance(value, self._type) else parser.parse(value)
            if d.tzinfo is None:
                raise Exception("No timezone data provided!")
            else:
                return d.astimezone(utc).replace(microsecond=0)
                
            
     
    def _coerce_from_db(self, value):
        return datetime.datetime.fromtimestamp(value, tz=utc)
        
    def _coerce_to_db(self, value):
        return None if value is None else time.mktime(value.timetuple())
        
class PhoneNumberAttribute(GenericAttribute):
    _type = PhoneNumber
    _db_type = unicode
        
    def _coerce(self, value):
        return value if (value is None or isinstance(value, self._type)) else PhoneNumber(value)

    def _coerce_to_db(self, value):
        return None if value is None else value.number
        
    def _coerce_from_db(self, value):
        return PhoneNumber(value)

class DecimalAttribute(GenericAttribute):
    _type = decimal.Decimal
    _db_type = long
    
    def __init__(self, max_digits, decimal_places, *args, **kwargs):
        super(DecimalAttribute, self).__init__(self, *args, **kwargs)
        self.max_digits = max_digits
        self.decimal_places = decimal_places
                    
    def _coerce_from_db(self, value):
        return decimal.Decimal(value) / decimal.Decimal(long(math.pow(10, self.decimal_places)))
        
    def _coerce_to_db(self, value):
        return None if value is None else `long(value * decimal.Decimal(long(math.pow(10, self.decimal_places))))`
        
class EmailAttribute(StringAttribute):
    _regex = re.compile(r"(?:^|\s)[-a-z0-9_.]+@(?:[-a-z0-9]+\.)+[a-z]{2,6}(?:\s|$)", re.IGNORECASE)
    
class IPAddressAttribute(GenericAttribute):
    _type = IPAddress
    _db_type = str
        
        
class RichAttributeMeta(type):
    def __init__(self, name, bases, attrs):
        super(RichAttributeMeta, self).__init__(name, bases, attrs)
        
        if name == 'RichAttribute':
            self._attributes = {}
        else:
            self._attributes = copy.copy(self._attributes)

        delete_attributes = []
        for k, v in attrs.items():
            if isinstance(v, GenericAttribute):
                v.name = k
                self._attributes[k] = v
                delete_attributes.append(k)
        for k in delete_attributes:
            delattr(self, k)



class RichAttribute(object):
    id = UUIDAttribute()
    __metaclass__ = RichAttributeMeta
    


class RichAttributeProxy(object):
    _search_cache = {}
    _staged_data = {}
    
    def __init__(self, parent, attribute):
        self._parent = parent
        self._attribute = attribute

    def __call__(self, sort=None, start_key='', limit=100, attributes=None):
        search_key = {'sort': sort, 'start': start, 'limit': limit, 'attributes': attributes}
        if not attributes:
            raise AttributeError('"attributes" is a required argument.')
        if not sort:
            raise AttributeError('"sort" is a required argument.')
        
            
    def update(self, data, by=None):
        attr = self._attribute
        natural_key = attr.Meta.natural_key
        processed_data = {}
        
        for key, values in data.items():
            if by != natural_key:
                setattr(values, by, key)
                key = getattr(values, natural_key, None)
                delattr(values, natural_key)
            rich_kid = attr._attributes[natural_key]
            key = rich_kid.filter_input(self._parent, rich_kid.validate(key))
            for k, v in values.items():
                rich_kid = attr._attributes[k]
                values[k] = rich_kid.filter_input(self._parent, rich_kid.validate(v))
            processed_data[key] = values
        self._staged_data.update(processed_data)

        
    def db_data(self):
        attr = self._attribute
        natural_key = attr.Meta.natural_key
        id = self._parent.id
        mutation_map = {}
        for primary, secondary in self._attribute.Meta.indices:
            cfname = '%s_by_%s_and_%s' % (attr.Meta.base_name, primary, secondary)
                                    
            for key, values in self._staged_data.items():
                if primary == 'id':
                    key1_base = attr._attributes['id'].to_db_value(id)
                elif primary == natural_key:
                    key1_base = attr._attributes[primary].to_db_value(key)
                    
                if secondary == natural_key:
                    key2 = attr._attributes[secondary].to_db_value(key)
                else:
                    key2 = attr._attributes[secondary].to_db_value(values[secondary])
                
                for k, v in values.items():
                    if k in (primary, secondary):
                        pass
                    else:
                        key1 = '%s_%s' % (key1_base, k)
                        if not mutation_map.has_key(key1): 
                            mutation_map[key1] = {cfname: {}}
                        insert_dict = mutation_map[key1][cfname]
                        insert_dict.update({key2: attr._attributes[k].to_db_value(v)})
        return mutation_map
                    
            
