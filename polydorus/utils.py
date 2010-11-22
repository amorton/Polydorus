from telephus.cassandra.ttypes import *
import uuid
import struct

validators = {
    unicode: 'UTF8Type',
    str: 'BytesType',
    uuid.UUID: 'LexicalUUIDType',
    long: 'LongType',
    int: 'LongType',
}

def generate_cfdef(cls, keyspace):
        cf_defs = []
        column_defs = []
        for key, value in cls._attributes.items():
            if not key == 'id':
                prop_cls = value.__class__
                validator = validators[value._db_type]
                if value.indexed:
                    index_name = 'idx_' + key
                    index_type = 0
                else:
                    index_name = None
                    index_type = None
                    
                column_def = ColumnDef(name=key, validation_class='org.apache.cassandra.db.marshal.' + validator, index_type=index_type, index_name=index_name)
                column_defs.append(column_def)
        cf_defs.append(CfDef(keyspace=keyspace, name=cls.Meta.column_family, comparator_type=cls.Meta.comparator_type, column_metadata=column_defs))
        
#         for name, rich_attr in cls._rich_attributes.items():
#             for primary, secondary in rich_attr.Meta.indices:
#                 idx_part = '%s_and_%s' % (primary, secondary)
#                 cf_name = '%s_by_%s' % (rich_attr.Meta.base_name, idx_part)
#                 cf_defs.append(CfDef(keyspace=keyspace, name=cf_name))
#                 
        return cf_defs
        
        
def pack(value, data_type):
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

def unpack(b, data_type):
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
        
