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

class BaseModelMeta(type):
    """Meta class to set the _attributes attribute on each class instance without bubbling up to the super class"""
    def __init__(cls, name, bases, attrs):
        super(BaseModelMeta, cls).__init__(name, bases, attrs)

        if name == 'BaseModel':
            cls._attributes = {}
        else:
            cls._attributes = copy.copy(cls._attributes)
            
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
        for k in delete_attributes:
            delattr(cls, k)
    
    def __getattr__(cls, attr):
        if attr not in cls._attributes: 
            raise AttributeError('%s does not exist' % attr)
        return cls._attributes[attr]

class BaseModel(object):
    """This is the base model for twisted & cassandra"""
    __metaclass__ = BaseModelMeta

    _attribute_values = {}
    _dirty = set()
    _fetched = set()
    _is_new = True
    _protected_attributes = set(['_attribute_values', '_is_new', '_dirty', '_fetched', 'Meta'])
    
    
    def __init__(self, is_new=True, *args, **kwargs):
        self._attribute_values = {}
        self._is_new = is_new
        self._fetched = set()
        
        if is_new:
            self._dirty = set(kwargs.keys())
            for k, v in self._attributes.items():
                default = getattr(v, 'default', None)
                self._attribute_values[k] = default
                if not default is None:
                    self._dirty.add(k)
            for k, v in kwargs.items():
                self._setattr(k, v)
        else:
            self._dirty = set()
            
    class Meta:
        """This is model (user space) metadata, not python metadata"""
        pass
    
    def _setattr(self, name, value, filter=True):
        self.__setattr__(name, value, filter=filter)
    
    def __setattr__(self, name, value, filter=True):
        if name in self._protected_attributes:
            super(BaseModel, self).__setattr__(name, value)
        else:
            attr = self._attributes[name]
            v = attr.validate(value)
            if filter:
                v = attr.filter_input(self, v)
            self._attribute_values[name] = v
            self._dirty.add(name)
        
        
    def __getattr__(self, name):
        if name not in self._attributes:
            raise AttributeError('%s not a property of %s' % (name, type(self)))
            
        v = self._attribute_values[name] if name in self._attribute_values else None
        return v
        
    def _setattr_from_db(self, name, value):
        attr = self._attributes[name]
        self._attribute_values[name] = attr.from_db_value(value)
        self._fetched.add(name)

    def _getattr_for_db(self, name):
        attr = self._attributes[name]
        value = self._attribute_values[name]
        if name in self._dirty:
            return attr.to_db_value(value)
        else:
            raise Exception("%s is not dirty." % name)
    
    def update(self, *args, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
    
    @classmethod
    def filter(cls, expression=None):
        return Query(cls, expression)


    def as_dict(self, properties=None):
        if properties is None:
            properties = self._attributes.keys()
        return dict((name, getattr(self, name, None)) for name in properties)        

    def _pre_save(self):
        pass
    
    def _mutation_map_for_save(self):
        raise NotImplementedError("_mutation_map_for_save must be implemented by the subclass.")
    
    @inlineCallbacks    
    def save(self, configuration=Configuration):
        for k, p in self._attributes.items():
            yield defer.maybeDeferred(p.pre_save, self)

        self._pre_save()
        
        for k, a in self._attributes.items():
            if a.required and self._attribute_values[k] is None:
                raise Exception("%s is required." % k)
        
        mutation_map = self._mutation_map_for_save()
        yield configuration.cassandra_client.batch_mutate(mutation_map)
        
        for k, p in self._attributes.items():
            yield defer.maybeDeferred(p.post_save, self)

        self._post_save()
        returnValue(True)
        
    def _post_save(self):
        self._is_new = False
    
    
    @classmethod
    def _pre_get(cls):
        pass
        
    def _post_get(self):
        self._is_new = False