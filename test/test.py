# -*- coding: utf-8 -*-

from twisted.trial import unittest
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, maybeDeferred, returnValue
from twisted.internet.protocol import ClientCreator
import decimal
import datetime
from pytz import utc
from dateutil import parser
import uuid
import time
import random
from telephus.protocol import ManagedCassandraClientFactory, ManagedThriftClientProtocol
from telephus.cassandra.ttypes import *
from telephus.client import CassandraClient

from polydorus import RowModel, ColumnModel
from polydorus.attributes import *
from polydorus.utils import generate_cfdef, generate_cfdef_cli
from polydorus.configuration import Configuration


def connect(keyspace='system', hosts=['localhost:9160'], connections=5, consistency=ConsistencyLevel.ONE):
	manager = ManagedCassandraClientFactory(keyspace=keyspace)
	client = CassandraClient(manager, consistency=consistency)
	
	for i in xrange(connections):
	    for host, port in [x.split(':') for x in hosts]:
	        reactor.connectTCP(host, int(port), manager)    
	return (client, manager)


class TestRowModel(RowModel):
    foo = UUIDAttribute(read_only=True, row_key=True)
    date_created = DateTimeAttribute(read_only=True, indexed=True)
    date_modified = DateTimeAttribute(read_only=True, indexed=True)
    
    def _pre_save(self):
        super(TestRowModel, self)._pre_save()
        now = datetime.datetime.now(tz=utc).replace(microsecond=0)
        self._setattr('date_modified', now, filter=False)
        if self._is_new:
            self._setattr('date_created', now, filter=False)


class TestColumnModel(ColumnModel):
    class Meta(ColumnModel.Meta):
        column_family = 'col_test'
    
    test2_id = UUIDAttribute(row_key=True)
    id = UUIDAttribute(column_key=True)
    int_test = IntegerAttribute()
    long_test = LongAttribute()
    
                    
class TestModel2(TestRowModel):
    class Meta:
        column_family = 'test2'
    name = StringAttribute(indexed=True)

class TestModel1(TestRowModel):
    class Meta:
        column_family = 'test1'
    
    def _date_test2_input_filter(self, value):
        self._setattr('duration', (value - self.date_test).seconds, filter=False)
        return value

    def generate_long_pin(self):
        if self.long_pin is None:
            self.long_pin = str(random.randint(1000000000,9999999999))
        
    first_name = StringAttribute(indexed=True)
    last_name = StringAttribute(indexed=True)
    test2_id = ForeignKeyAttribute(TestModel2)
    decimal_test = DecimalAttribute(16, 10, indexed=True)
    bool_test = BooleanAttribute(indexed=True, default=True)
    date_test = DateTimeAttribute(indexed=True)
    date_test2 = DateTimeAttribute(indexed=True, input_filter=_date_test2_input_filter)
    duration = LongAttribute(indexed=True, read_only=True)
    long_pin = StringAttribute(indexed=True, pre_save=generate_long_pin)
    int_test = IntegerAttribute(indexed=True)
    long_test = LongAttribute(indexed=True)
    write_once_test = IntegerAttribute(write_once=True)
    ip_test = IPAddressAttribute(indexed=True)
    json_test = JSONAttribute()

keyspace = 'PolydorusTrial'

cf_defs = generate_cfdef(TestModel1, keyspace)
cf_defs.extend(generate_cfdef(TestModel2, keyspace))
cf_defs.extend(generate_cfdef(TestColumnModel, keyspace))
keyspace_def = KsDef(name=keyspace, replication_factor=1, strategy_class='org.apache.cassandra.locator.SimpleStrategy', cf_defs=cf_defs)

@inlineCallbacks
def reset_keyspace(cassandra_client):
    try:
        yield cassandra_client.set_keyspace('system')
        yield cassandra_client.system_drop_keyspace(keyspace)
    except Exception as e:
        pass

    try:
        yield cassandra_client.system_add_keyspace(keyspace_def)
    except Exception as e:
        print 'keyspace already exists!!!?', e
    
    try:
        yield cassandra_client.set_keyspace(keyspace)
    except Exception as e:
        print 'keyspace was not set!!!', e
        raise e
        
@inlineCallbacks
def tear_down(cassandra_client, manager):
    try:
        yield cassandra_client.set_keyspace('system')
#         yield cassandra_client.system_drop_keyspace(keyspace)
    except Exception as e:
        print 'Couldnt drop keyspace', e
    yield manager.shutdown()
    for c in reactor.getDelayedCalls():
        c.cancel()
    reactor.removeAll()

    

class ModelTest(unittest.TestCase):
    '''Test cases for models'''
    timeout = 25
    
    @inlineCallbacks
    def setUp(self):
        Configuration.cassandra_client, self.manager = connect()
        
        try:
            yield Configuration.cassandra_client.set_keyspace('system')
            yield Configuration.cassandra_client.system_drop_keyspace(keyspace)
        except Exception as e:
            pass

        try:
            yield Configuration.cassandra_client.system_add_keyspace(keyspace_def)
        except Exception as e:
            print 'keyspace already exists!!!?', e
        
        try:
            yield Configuration.cassandra_client.set_keyspace(keyspace)
        except Exception as e:
            print 'keyspace was not set!!!', e
            raise e
        
        
        t2 = TestModel2(name='Test2')
        yield t2.save()
        self.test2_id = t2.foo
        self.failUnless(isinstance(self.test2_id, uuid.UUID))
        
        t1 = TestModel1(test2_id=self.test2_id, first_name=u'Матфей', last_name=u'Вильямс')
        t1.json_test = {'a':1, 'b':[1,2,3]}
        yield t1.save()
        self.test1_id = t1.foo
        self.failUnless(isinstance(self.test2_id, uuid.UUID))
            
    @inlineCallbacks
    def tearDown(self):
        try:
            yield Configuration.cassandra_client.set_keyspace('system')
#             yield Configuration.cassandra_client.system_drop_keyspace(keyspace)
        except Exception as e:
            print 'Couldnt drop keyspace', e
        yield self.manager.shutdown()
        for c in reactor.getDelayedCalls():
            c.cancel()
        reactor.removeAll()
    
    def test_cli_script(self):
        script = generate_cfdef_cli([TestModel1, TestModel2], "Test")
        
        self.assertTrue(script.startswith("create keyspace Test"))
        self.assertTrue(script.find("create column family %s" % (
            TestModel1.Meta.column_family)) > -1)
        self.assertTrue(script.find("create column family %s" % (
            TestModel2.Meta.column_family)) > -1)
            
    @inlineCallbacks
    def test_column_model(self):
        id = uuid.uuid1()
        m = TestColumnModel(test2_id=self.test2_id, id=id, int_test=123, long_test = 123L)
        yield m.save()
        
        id2 = uuid.uuid1()
        m2 = TestColumnModel(test2_id=self.test2_id, id=id2, int_test=124, long_test = 124L)
        yield m.save()
    
        m = yield TestColumnModel.get(self.test2_id, id)
        
        self.failUnlessEquals(m.test2_id, self.test2_id)
        self.failUnlessEquals(m.id, id)
        self.failUnlessEquals(m.int_test, 123)
        self.failUnlessEquals(m.long_test, 123L)
        
        rs = yield TestColumnModel.get(self.test2_id)
        self.failUnlessEquals(len(rs), 1)
        for r in rs:
            self.failUnless(r.id in (id, id2))
    
    
    @inlineCallbacks
    def test_get(self):
        i = yield TestModel1.get(self.test1_id)
        self.failUnlessEquals(i.foo, self.test1_id)
        self.failUnlessEquals(i.first_name, u'Матфей')
        self.failUnlessEquals(i.last_name, u'Вильямс')
        self.failUnlessEquals(i.bool_test, True)
        self.failUnlessEquals(i.json_test, {'a':1, 'b':[1,2,3]})
    
    
    @inlineCallbacks
    def test_insert_mutate(self):
        i = TestModel1(test2_id=self.test2_id, first_name=u'Матфей', last_name=u'Вильямс', write_once_test=1)
        date_created = datetime.datetime.now(tz=utc).replace(microsecond=0)
        yield i.save()
        id = i.foo
        long_pin = i.long_pin
        self.failUnless(isinstance(self.test2_id, uuid.UUID))
        self.failUnless(len(long_pin) == 10)
        created_diff = (i.date_created - date_created).seconds
        self.failUnless(created_diff < 1 and created_diff > -1)
        self.failUnlessEqual(i.date_created, i.date_modified)
        date_created = i.date_created
        
        
        i = yield TestModel1.get(id)
        self.failUnlessEquals(i.foo, id)
        
        created_diff = (date_created - i.date_created).seconds
        modified_diff = (date_created - i.date_modified).seconds
        self.failUnless(created_diff < 1 and created_diff > -1)
        self.failUnless(modified_diff < 1 and modified_diff > -1)
                
        i.first_name = u'Иона'
        i.date_test = '2010-10-21T14:44:33Z'
        i.date_test2 = '2010-10-21T15:44:33Z'
        i.decimal_test = '10.25'
        i.int_test = '15'
        i.bool_test = False
        i.ip_test = '192.168.0.1'
        
        self.failUnlessEqual(i.duration, 3600)

        try:
            i.duration = 123
            self.fail('Duration should be read only.')
        except:
            pass
        
        try:
            i.write_once_test = 2
            self.fail('write_once column should be read-only after initial save.')
        except:
            pass
        
        # give the modification time a chance to advance
        time.sleep(3)
        
        yield i.save()
        
        i = yield TestModel1.get(id)
        self.failUnlessEquals(i.first_name, u'Иона')
                
        self.failUnlessEquals(i.date_test, parser.parse('2010-10-21T14:44:33Z'))
        self.failUnlessEquals(i.decimal_test, decimal.Decimal('10.25'))
        self.failUnlessEquals(i.bool_test, False)
        self.failUnlessEquals(i.int_test, 15)
        self.failUnlessEqual(i.duration, 3600)
        self.failUnlessEqual(str(i.ip_test), '192.168.0.1')
        self.failUnlessEqual(i.long_pin, long_pin)

        self.failUnlessEqual(date_created, i.date_created)
        self.failIfEqual(i.date_modified, date_created)

        modified_diff = (i.date_modified - date_created).seconds
        self.failUnless(modified_diff < 6 and modified_diff > 1)


    @inlineCallbacks
    def test_insert_with_missing_foreign_key(self):
        i = TestModel1(first_name=u'Матфей', last_name=u'Вильямс')
        try:
            yield i.save()
            self.fail('succeeded in saving without foriegn key')
        except:
            pass
            
    def test_modify_date_created_and_modified(self):
        try:
            i = TestModel1(first_name=u'Матфей', last_name=u'Вильямс', date_created='2010-10-21T14:44:33Z')
            self.fail('succeeded in modifying date_created')
        except:
            pass
            
        try:
            i.date_created='2010-10-21T14:44:33Z'
            self.fail('succeeded in modifying date_created')
        except:
            pass
            
        try:
            i = TestModel1(first_name=u'Матфей', last_name=u'Вильямс', date_modified='2010-10-21T14:44:33Z')
            self.fail('succeeded in modifying date_modified')
        except:
            pass
            
        try:
            i.date_modified='2010-10-21T14:44:33Z'
            self.fail('succeeded in modifying date_modified')
        except:
            pass
            
    @inlineCallbacks
    def test_search(self):
        for i in range(50):
            d = parser.parse('2010-10-21T14:44:33Z')
            m1 = TestModel1(test2_id=self.test2_id, first_name='John', last_name='Jacob')
            m1.decimal_test = decimal.Decimal('3.1415992961')
            m1.date_test = d
            yield m1.save()
            
            m2 = TestModel1(test2_id=self.test2_id, first_name='Jane', last_name='Schmidt')
            m2.bool_test = False
            m2.int_test = i
            m2.long_test = i
            yield m2.save()
            
        search_results = yield TestModel1.filter(TestModel1.first_name == 'John').execute()
        self.failUnlessEquals(search_results.total, 50)
        self.failUnlessEquals(len(search_results), 25)
        for r in search_results:
            self.failUnlessEquals(r.last_name, 'Jacob')
            self.failUnlessEquals(r.bool_test, True)
            self.failUnlessEquals(r.test2_id, self.test2_id)
            self.failUnlessEquals(d, r.date_test)
            
        search_results = yield TestModel1.filter(TestModel1.last_name == 'Schmidt').sort('int_test').offset(10).limit(10).execute()
        self.failUnlessEquals(len(search_results), 10)
        for i, r in enumerate(search_results):
            self.failUnlessEquals(r.first_name, 'Jane')
            self.failUnlessEquals(r.bool_test, False)
            self.failUnlessEquals(r.long_test, i+10)
            self.failUnlessEquals(r.int_test, i+10)
            
        search_results = yield TestModel1.filter(TestModel1.last_name == 'Schmidt').sort('-int_test').offset(10).limit(10).execute()
        self.failUnlessEquals(len(search_results), 10)
        for i, r in enumerate(search_results):
            self.failUnlessEquals(r.first_name, 'Jane')
            self.failUnlessEquals(r.bool_test, False)
            self.failUnlessEquals(r.long_test, (40-i-1))
            self.failUnlessEquals(r.int_test, (40-i-1))
            
        search_results = yield TestModel1.filter(TestModel1.first_name == 'Jane').filter(TestModel1.int_test != 5).execute()
        for r in search_results:
            self.failUnless(r.int_test != 5)
        
        
        search_results = yield TestModel1.filter(TestModel1.last_name == 'Schmidt').filter(TestModel1.int_test > 5).execute()
        for r in search_results:
            self.failUnless(r.int_test > 5)
            
        