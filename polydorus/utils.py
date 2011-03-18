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
        cf_def = CfDef(keyspace=keyspace, name=cls.Meta.column_family, comparator_type=cls.Meta.comparator_type, column_metadata=column_defs)
        cf_def.memtable_throughput_in_mb = getattr(cls.Meta, "memtable_throughput_in_mb", 64)
        cf_def.memtable_flush_after_mins = getattr(cls.Meta, "memtable_flush_after", 1440)
        cf_def.memtable_operations_in_millions = getattr(cls.Meta, "memtable_operations", 0.25)
        
        cf_defs.append(cf_def)
        
#         for name, rich_attr in cls._rich_attributes.items():
#             for primary, secondary in rich_attr.Meta.indices:
#                 idx_part = '%s_and_%s' % (primary, secondary)
#                 cf_name = '%s_by_%s' % (rich_attr.Meta.base_name, idx_part)
#                 cf_defs.append(CfDef(keyspace=keyspace, name=cf_name))
#                 
        return cf_defs

def generate_cfdef_cli(classes, keyspace, rf=1, 
    strategy='org.apache.cassandra.locator.SimpleStrategy'):
    """Generates a cassandra-cli script to build a keyspace that contains 
    column families for all the classes.
    
    :param classes: List of model classes to include in the schema. 
    :param keyspace: Name for the keyspace.
    :param rf: Replication factor for the keyspace. 
    :param stategy: Name of the replica placement strategy to use. 
    """

    cf_defs = [generate_cfdef(c, keyspace)[0] for c in classes]

    buffer = []
    write = buffer.append

    write("create keyspace %s with replication_factor = %s "\
        "and placement_strategy = '%s';" % (keyspace, rf, strategy))
    write("\n\n")

    write('use %s;' % keyspace)
    write("\n\n")
    
    for cf_def in cf_defs:
        
        write("create column family %s \n" % cf_def.name)
        write("with column_type = Standard \n")
        write("and comparator = %s " % cf_def.comparator_type)
        write("and keys_cached = 0 \n")
        write("and rows_cached = 0 \n")
        write("and memtable_flush_after = %s \n" % cf_def.memtable_flush_after_mins)
        write("and memtable_throughput = %s \n" % cf_def.memtable_throughput_in_mb)
        write("and memtable_operations = %s \n" % cf_def.memtable_operations_in_millions)
        
        if cf_def.column_metadata:
            write("and column_metadata = [")
            
            for col_def in cf_def.column_metadata:
                write("{column_name : %s, " % col_def.name)
                _, _, val_class = col_def.validation_class.rpartition(".")
                write("validation_class : %s" % val_class)
                if col_def.index_name:
                    write(", index_name : %s, " % col_def.index_name)
                    #Only one type of index for now
                    write("index_type : 0")
                write("},\n")
            write("]")
        write(";\n\n")
    return "".join(buffer)
    
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
        
