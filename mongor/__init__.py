from random import randint
from sys import stderr as log_error
import gzip
import datetime

#needed external deps
import pymongo
from pymongo.errors import CollectionInvalid
from pymongo.errors import ConnectionFailure 
from dateutil.parser import parse
import bson


#utilities
def datetime_to_ObjectId(dt_object=None):
    '''
    giving this function an obvious name
    '''
    return bson.objectid.ObjectId.from_datetime(dt_object)

def string_datetime_to_ObjectId(string_dt_object):
    '''
    giving this function an obvious name
    '''
    return datetime_to_ObjectId(parse(string_dt_object))

def string_ObjectId_to_datetime(string_ObjectId_object):
    '''
    giving this function an obvious name
    '''
    return bson.objectid.ObjectId(string_ObjectId_object).generation_time




class Config(object):
    """
    Used to configure the mongodb curator.
    
    Configuration class should only be used directly
    when performing the original setup
    """
    def __init__(self, host="localhost", port=27017, ssl=False):
        self.mongo_client = pymongo.MongoClient(host=host, port=port, ssl=ssl)
        self.database = pymongo.database.Database(self.mongo_client, "config_db")
        
    def setup(self, background=False, unique_uids=False):
        '''
        few hard coded keywords in this function:
          - nodes, a node is a target database for document insertion
          - schedule, the order in which the node will be available for insertions
          - db_type, arbitrary name used for grouping nodes.
        '''
        try:
            self.database.create_collection("nodes")
        except CollectionInvalid:
            self.database.drop_collection("nodes")
            self.database.create_collection("nodes")
            
        self.database['nodes'].ensure_index("uid", 
                                      background=background, 
                                      unique=unique_uids)
        self.database['nodes'].ensure_index([("name", 1), 
                                       ("host", 1), 
                                       ("port", 1)], 
                                       background=background, 
                                       unique=True)
        self.database['nodes'].ensure_index("schedule")
        self.database['indexes'].ensure_index("db_type")
        self.database['QFD'].ensure_index("db_type")
        
    def close(self):
        '''
        give a function to allow the client to close resources
        '''
        self.mongo_client.disconnect()
        
    def add_index(self, db_type="", #<str>  
                  fields=None,     #<list>
                  background=False, #<bool>
                  unique=False,     #<bool>
                  sparse=True,       #<bool>
                  text=False       #<bool>
                  ): 
        if isinstance(fields, basestring):
            fields = [fields] #load into a list so the loop works
        for field in fields:
            self.database['indexes'].insert({"db_type":db_type,
                                             "field":field,
                                             "background":background, 
                                             "unique":unique,
                                             "sparse":sparse,
                                             "text":text,
                                             })
    
    def remove_index(self, db_type="", #<str>  
                  fields=None): #<List>
        if isinstance(fields, basestring):
            fields = [fields] #load into a list so the loop works
        for field in fields:
            self.database['indexes'].remove({"db_type":db_type, "field":field})
    
    def get_indexes(self, db_type, fields_only=False):
        indexes = list(self.database['indexes'].find({"db_type":db_type}))
        for index in indexes:
            if isinstance(index['field'], list):
                fields = []
                for field in index['field']:
                    fields.append(tuple(field))
                index['field'] = fields
        if fields_only:
            indexes = [x['field'] for x in indexes]
        return indexes
    
    def add_qfd(self,
                field,
                uid="",
                db_type="",
                window = 60*24, #<int> in minutes default 1 day
                ):
        if field:
            self.database['QFD'].insert({"db_type":db_type, 
                                         "window":window, 
                                         "field":field, 
                                         "uid":uid, 
                                         "last_id":bson.objectid.ObjectId.from_datetime(datetime.datetime(2000, 1, 1))})
        return field
    
    def add_node(self,  uid="", 
                        name="",
                        db_type="",
                        db_tags=None, #list<str>
                        capability='r', #r: read, rw=read/write
                        host="localhost",
                        port=27017,
                        max_size= 21474836480, #20GB in bytes 
                        ssl=False, #boolean
                        passwd_file="", #file that should contain "username:password"
                        raw_args=None):
        '''
        Could make a Node class, but that is probably overkill (zen#3) since it would be internal only
        
        '''
        node = {}
        if isinstance(raw_args, dict):
            node = raw_args
        node['uid'] = uid
        node['name'] = name
        node['host'] = host
        node['port'] = port
        node['ssl'] = ssl
        node['max_size'] = max_size
        node['passwd_file'] = passwd_file
        node['db_type'] = db_type
        node['capability'] = capability
        if db_tags:
            if not isinstance(db_tags, list): 
                node['db_tags'] = [db_tags]
            else:
                node['db_tags'] = db_tags
        
        if "rw" in capability: #only schedule rotations for write nodes
            node['schedule'] = self._get_next_sequence_number(db_type=db_type)
            
        self.database['nodes'].insert(node)
        return self.get_node(uid)
    
    def set_node_tags(self, uid, db_tags):
        nodes = self.get_node(uid)
        if isinstance(db_tags, list):
            for node in nodes:
                node['db_tags']  = db_tags
                self.database['nodes'].save(node)
                
    def rotate_schedule(self, db_type=""):
        '''
        assumes that appropriate maintenence has already been done 
        and the incoming database is clean/prepared for inserts
        '''
        nodes = list(self.database['nodes'].find({"$and":[
                                             {"host":"localhost"},
                                             {"db_type":db_type},
                                             {'schedule':{"$exists":True}}]}))
        for node in nodes:
            node["schedule"] = (node["schedule"] + 1) % len(nodes)
            self.database['nodes'].save(node)
    
    def get_current_local_node(self, db_type="", limit1=True):
        log_error.write( "USING OLD FUNCTION, move to get_current_write_node\n")
        return self.get_current_write_node(db_type=db_type, limit1=limit1)
    
    
    def get_current_write_node(self, db_type="", limit1=True):
        selected_nodes = None
        current_nodes = list(self.database['nodes'].find({"$and":[
                                                    {"db_type":db_type},
                                                    {"capability":"rw"}, 
                                                    {'schedule':0}]}))
        if limit1:
            selected_nodes = self.select_random_from_nodes(current_nodes)
        else:
            selected_nodes = current_nodes
        return selected_nodes
    
    
    def get_local_nodes(self, db_type=""):
        log_error.write( "USING OLD FUNCTION, move to get_write_nodes\n")
        return self.get_write_nodes(db_type)
        
    def get_write_nodes(self, db_type=""):
        return self.get_nodes({"$and":[{"capability":"rw"},{"db_type":db_type}]})
    
    def get_read_nodes(self, db_type=""):
        return self.get_nodes({"$and":[{"db_type":db_type}]})
        
    def get_nodes(self, criteria):
        return list(self.database['nodes'].find(criteria).sort("schedule", pymongo.ASCENDING))
    
    def select_random_from_nodes(self, current_nodes):
        return current_nodes[randint(0, len(current_nodes)-1)] #allow possible randomization
    
    def get_node(self, uid):
        nodes = list(self.database['nodes'].find({"uid":uid}))
        for node in nodes:
            node['host'] = self.mongo_client.host #ensure all node hosts are relative to the caller.
        return nodes
        
    def remove_node(self, db_type, uid):
        '''removes the nodes from the rotation
        this has a giant atomicity problem
        any command seeking to read from the database inluding:
            get the current node
            rotate the schedule
            add/delete other nodes
        during this command may have uncontrolled output
        
        It would be best to lock all applications from accessing mongor 
        while this command takes place
        '''
        delete_nodes = self.get_node(uid)
        self.database['nodes'].remove({"uid":uid})
        for node in delete_nodes:
            self.database.connection.drop_database(node['name'])
        nodes_to_reorder = self.get_write_nodes(db_type = db_type)
        schedule = 0
        for node in nodes_to_reorder:
            node['schedule'] = schedule
            schedule += 1
            self.database['nodes'].save(node)
        return self.get_node(uid)

    def _get_next_sequence_number(self, db_type=""):
        try:
            return list(self.database['nodes'].find({"$and":[
                                      {"db_type":db_type},
                                      {'schedule':{"$exists":True}}]}).
                                      sort("schedule", pymongo.DESCENDING).
                                      limit(1))[0]['schedule'] + 1
        except KeyError:
            return 0
        except IndexError:
            return 0

class Maintenence:
    """
    Used to maintain the mongodb curator.
    
    Maintenece class should be called by an external script
    """
    def __init__(self, config_host, config_port, config_ssl):
        self.config = Config(host=config_host, port=config_port, ssl=config_ssl)
        
    def close(self):
        '''
        give a function to allow the client to close resources
        '''
        self.config.mongo_client.disconnect()
        
    def ensure_indexes(self, collection, 
                            indexes,
                            db_type=""):
        assert isinstance(indexes, list)
        for node in self.config.get_write_nodes(db_type=db_type):
            database = Database().node_to_database(node)
            for index in indexes:
                database[collection].ensure_index(index['field'], 
                                                  background=index['background'], 
                                                  sparse=index['sparse'], 
                                                  unique=index['unique'])
    
    def clean_incoming(self, db_type="", dump_bson=False):
        nodes = self._index_of_schedule_change(self.config.get_write_nodes(db_type=db_type)[::-1]) #reverse the order to get the next nodes to the front of the list
        for node in nodes:
            database = Database().node_to_database(node)
            database.connection.drop_database(node['name'])
            
    def current_tailable_cursor(self, db_type, collection, _id="", node=None):
        ''' 
        _id <bson.objectid.ObjectId> start somewhere other than the beginning of the database
        db_type <basestring> which type of database to use
        node <dict> In the node format, allow the 
        
        Returns:
            None - Returns None <NoneType> at the end of each while loop
            Dict - Returns a Dictionary representative of the mongo document
             
        Notes: caller is responsible for throttling if required (at end of cursor)
        TODO: No good way to start the cursor at a time other than the live or immediately previous database 
        '''
        if not _id:
            _id = bson.objectid.ObjectId('000000000000000000000000')
        while True: #this while loop supports both cursor catching up to live and switching db
            if not node:
                node = self.config.get_current_write_node(db_type=db_type)
            database = Database().node_to_database(node)
            node = None
            print database
            for document in database[collection].find({"_id":{'$gt':_id}}).sort('$natural', pymongo.ASCENDING):
                _id = document['_id']
                yield document
            yield None
    
    def _index_of_schedule_change(self, nodes):
        index = 0
        schedule = nodes[0]['schedule']
        for position in range(len(nodes)): #do this way in order to maintain the index variable
            if nodes[position]['schedule'] < schedule:
                index = position
                break
        return nodes[:index]
    
    def need_to_rotate(self, db_type=""):
        rotate = False
        current_nodes = self.config.get_current_write_node(db_type=db_type, limit1=False)
        for node in current_nodes:
            database = Database().node_to_database(node)
            data_size = database.command("dbstats")["fileSize"]
            if int(node['max_size']) < int(data_size):
                rotate = True #if any one database in the current 'random' databases is full, rotate them all
        return rotate, data_size
    
    def rotate_schedule(self, db_type=""):
        self.config.rotate_schedule(db_type=db_type)
    
    def dump_bson(self, node, filename):
        if ".gz" not in filename[len(filename)-4:]:
            filename += ".gz" #put a 'meaningless' extension on there for readability
        database = Database().node_to_database(node)
        #TODO: Add some error handling around this to ensure f gets closed.  
        f = gzip.open(filename, 'wb')#gzip strea
        #dump in reverse chronological order as the newer stuff may be more important than older stuff
        for document in database.find().sort('$natural', pymongo.DESCENDING):
            f.write(bson.BSON.encode(document))

        return filename
        
class Database:
    """
    The most common used class.
    
    A client will generally call this class looking for one or more database handles
    The purpose of this library is to abstract and curate the database and return
    database handles
    
    it is up to client applications to obtain new handles every now and then
    
    mongoclient under the hook will happily maintain persistent connections forever
    this library makes no attempt to force the client into a new handle.
    """
    def __init__(self, config_host="", config_port=27017, config_ssl=False):
        self.config = None
        self.mongo_clients = {} #allows MongoClient to manage the connection pool for each host
        if config_host:
            self.config = Config(host=config_host, port=config_port, ssl=config_ssl)
    
    def close(self):
        '''
        give a function to allow the client to close resources
        '''
        self.config.mongo_client.disconnect() #only resource that needs closed
        
    def get_current_write_node(self, db_type=""):
        return self.node_to_database(self.config.get_current_write_node(db_type))
    
    def get_write_nodes(self, db_type="metadata"):
        return [self.node_to_database(node) for node in self.config.get_write_nodes(db_type=db_type)]
    
    def get_read_nodes(self, db_type="metadata", with_bson=False):
        return [self.node_to_database(node) for node in self.config.get_read_nodes(db_type=db_type)]
    
    def node_to_database(self, node, protect=False):
        node_name = "%s:%s" %(node['host'], node['port'])
        if node_name not in self.mongo_clients:
            self.mongo_clients[node_name] = pymongo.MongoClient(host=node['host'], 
                                                                port=node['port'], 
                                                                ssl=node['ssl'])
        return_database = None #worst case, a None will be returned. 
        if protect: #hand back a database that has customized protections enabled
            return_database = ProtectedDatabase(self.mongo_clients[node_name], node['name'])
        else:#by default, hand back a standard database handle.
            return_database = pymongo.database.Database(self.mongo_clients[node_name], node['name'])
        return return_database
    
    def node_to_file_handle(self, node):
        return 
        
class Query:
    def __init__(self, databases=None, config_host="", config_port=27017, config_ssl=False, db_type=""):
        self.databases = databases
        self.cursors   = {}
        if not self.databases:
            self.databases = Database(config_host=config_host, 
                                      config_port=config_port, 
                                      config_ssl=config_ssl).get_read_nodes(db_type)

                                      
    def count(self, collection, criteria, date_start=None, date_end=None):
        """
        Returns a count of the number of documents which match the provided criteria.
        """
        total_count = 0
        
        for db in self.databases:
            if isinstance(db, pymongo.database.Database): 
                # Get the count for this database
                result_count = db[collection].find(criteria).count()
                
                # Add it to the total count
                total_count += result_count
        
        return total_count
                                      
    def find(self, collection, criteria, projection=None, sort=None, limit=101, skip=0, bson_pre_match="", date_start=None, date_end=None):
        '''

        '''
        if not sort:
            sort = [("_id", pymongo.DESCENDING)] #sort in reverse chronological order
        for db in self.databases:
            if isinstance(db, pymongo.database.Database): #allow growth to query raw bson documents.
                #limit +1 to be absolutely sure that getMore isnt called until your really mean it. 
                cursor = db[collection].find(criteria, projection).sort(sort).batch_size(limit+1)
                result_count = cursor.count()
                next_skip = skip - result_count
                if next_skip > 0: #no ramaining documents in this database
                    skip = next_skip #send updated skip to next loop
                    continue #on to the next loop, there was nothing in this db
                else: #there are remaining items in this db
                    cursor = cursor.skip(skip) #apply the skip to this db lookup
                    skip = 0 #set the skip to 0 for the next loop       
                    for item in cursor: #begin dereferencing the cursor
                        yield item  #allows the caller to determine when to stop requesting data

class GlobalQuery:
    def __init__(self, config_host, config_port=27017, config_ssl=False, db_type="", db_tags=None):
        if not db_tags:
            db_tags = []
            
        self.local_config = Config(host=config_host, port=config_port, ssl=config_ssl)
        self.remote_databases = {} #what users will probably iterate
        self.remote_configs   = {} #what system uses to refresh
        self.load_remote_configs(db_type, db_tags=db_tags)
    
    def load_remote_configs(self, db_type, db_tags):
        config_servers = self.local_config.get_nodes({ "db_tags": { '$all': db_tags }})
        for node in config_servers:
            node_name = "%s:%s" %(node['host'], node['port'])
            self.remote_databases[node_name] = []
            try:
                self.remote_configs[node_name] = Config(host=node['host'], 
                                                        port=node['port'], 
                                                        ssl=node['ssl'])
            except ConnectionFailure:
                self.remote_databases[node_name] = None
                continue #move to the next possible node
            
            database = Database()
            node_databases = []
            for remote_read_node in self.remote_configs[node_name].get_read_nodes(db_type):
                remote_read_node['host'] = node['host']
                node_databases.append(database.node_to_database(remote_read_node))
            if node_databases:
                self.remote_databases[node_name] = Query(databases=node_databases)
        return self



class ProtectedDatabase(pymongo.database.Database):
    def __getattr__(self, name):
        '''
        may be worth performing some type of logging
        or decision making based on the user that got to this point.
        somewhat pointless since savvy users could just open up a real database handle
        
        perhaps has worthyness in an enterprise environment, maybe not
        '''
        return ProtectedCollection(self, name)
    
    def drop(self):
        raise ValueError("Drop Not Allowed")
    
    def drop_collection(self, name_or_collection):
        raise ValueError("Drop Not Allowed")
        
        
class ProtectedCollection(pymongo.collection.Collection):
    def find(self, *args, **kwargs):
        '''
        might be worth doing a bit of logging here to monitor queries as they go by
        or take action based on the user and the criteria
        '''
        #syslog.openlog("TKSearch", 0, syslog.LOG_LOCAL0)
        #info = self.__dict__
        #syslog.syslog(syslog.LOG_DEBUG, "%s|%s|%s|%s"%(str(getpass.getuser()), info['_Collection__database'], info['_Collection__full_name'], str(args)))
        #syslog.closelog()
        if not 'slave_okay' in kwargs:
            kwargs['slave_okay'] = self.slave_okay
        if not 'read_preference' in kwargs:
            kwargs['read_preference'] = self.read_preference
        return pymongo.cursor.Cursor(self, *args, **kwargs)
    
    
    def drop(self):
        raise ValueError("Drop Not Allowed")
    
    def drop_index(self, index_or_name):
        raise ValueError("Drop Not Allowed")
    
    def drop_indexes(self):
        raise ValueError("drop_indexes Not Allowed")
        
    def reindex(self):
        raise ValueError("reindex Not Allowed")
    
    def find_and_modify(self, query={}, update=None, upsert=False, **kwargs):
        #interesting that query is set to a blank dict here, 
        #that may cause problems in the future if this class does anything other than raise
        raise ValueError("find_and_modify Not Allowed")
        
    def create_index(self, key_or_list, deprecated_unique=None,
                     ttl=300, **kwargs):
        raise ValueError("create_index Not Allowed")






















