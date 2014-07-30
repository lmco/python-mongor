#!/usr/bin/python
'''manages mongor
'''
from mongor import Maintenence
import argparse
from pprint import pprint
from bson.json_util import loads
from distutils.util import strtobool

def add_node(args):
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    m.config.add_node(uid = args.uid,
                      name = args.name,
                      db_type = args.db_type,
                      db_tags = loads(args.db_tags),
                      capability = args.capability,
                      host = args.host,
                      port = args.port,
                      max_size = args.max_size,
                      ssl = bool(strtobool(args.ssl)),
                      passwd_file = args.passwd_file
                      )
    return
def remove_node(args):
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    m.config.remove_node(args.db_type, args.uid)
    return

def set_db_tags(args):
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    tags = loads(args.db_tags)
    m.config.set_node_tags(args.uid,
                    tags)
    return


def add_index(args):
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    fields = loads(args.fields)
    m.config.add_index(db_type = args.db_type,
                       fields = fields,
                       background = bool(strtobool(args.background)), #bool
                       unique = bool(strtobool(args.unique)),
                       sparse = bool(strtobool(args.sparse)),
                       text = bool(strtobool(args.text)))
    if bool(strtobool(args.rebuild)):
        build_index(args)
    return

def build_index(args):
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    m.ensure_indexes(args.collection, 
                     m.config.get_indexes(args.db_type), 
                     db_type=args.db_type)
    return

def remove_index():
    m = Maintenence(args.config_host, args.config_port, args.config_ssl)
    fields = loads(args.fields)
    m.remove_index(args.db_type,
                   fields)
    return

if __name__ == '__main__':
    parser = argparse.ArgumentParser("MongoR manager")
    #parser.add_argument('-o', '--output')
    #parser.add_argument('-v', dest='verbose', action='store_true')
    parser.add_argument('--host', type=str, dest='config_host', required=True)
    parser.add_argument('--port', type=int, dest='config_port', required=True)
    parser.add_argument('--ssl', dest='config_ssl', default=False, action='store_true')
    parser.add_argument('--listnodes', dest='list_nodes', default=False, action='store_true')
    
    
    subparsers = parser.add_subparsers(help='python manage.py <command> -h')
    
    #add a node
    parser_add_node = subparsers.add_parser('addnode', help='adds a node to mongor')
    parser_add_node.set_defaults(which='addnode')
    parser_add_node.add_argument('uid', type=str, help="uniqueID for the node (unique)")
    parser_add_node.add_argument('name', type=str, help="name of the node(unique)")
    parser_add_node.add_argument('db_type', type=str, help="the major category of the database")
    parser_add_node.add_argument('db_tags', type=str, help="json list '[\"tag1\",\"tag2\",etc]'")
    parser_add_node.add_argument('capability', type=str, help="r=readonly, rw=read/write")
    parser_add_node.add_argument('max_size', type=int, help="size after which should rotate")
    parser_add_node.add_argument('passwd_file', type=str, help="the local (to the caller) file containing auth creds")
    parser_add_node.add_argument('host', type=str, help="dns name or IP address of the mongod")
    parser_add_node.add_argument('port', type=int, help="port of the mongod")
    parser_add_node.add_argument('ssl', type=str, help="use SSL for connection")
    
    parser_rm_node = subparsers.add_parser('removenode', help='removes a node to mongor')
    parser_rm_node.set_defaults(which='removenode')
    parser_rm_node.add_argument('uid', type=str, help="the unique identifier of the node to remove")
    parser_rm_node.add_argument('db_type', type=str, help="the major category of the database")
    
    
    parser_add_index = subparsers.add_parser('addindex', help='adds an index to mongor')
    parser_add_index.set_defaults(which='addindex')
    parser_add_index.add_argument('db_type', type=str, help="db_type the index applies")
    parser_add_index.add_argument('collection', type=str, help="collection the index applies")
    parser_add_index.add_argument('fields', type=str, help="JSON list of namespaces")
    parser_add_index.add_argument('background', type=str, help="build index in background")
    parser_add_index.add_argument('unique', type=str, help="force a unique index")
    parser_add_index.add_argument('sparse', type=str, help="use a sparse index")
    parser_add_index.add_argument('text', type=str, help="use text index engine")
    parser_add_index.add_argument('rebuild', type=str, help="rebuild indexes on previous databases")
    
    
    
    parser_rm_index = subparsers.add_parser('removeindex', help='removes index from future built buckets')
    parser_rm_index.set_defaults(which='removeindex')
    parser_rm_index.add_argument('db_type', type=str, help="db_type the index applies")
    parser_rm_index.add_argument('fields', type=str, help="JSON list of namespaces")
    
    
    parser_build_index = subparsers.add_parser('buildindex', help='build the index on all databases')
    parser_build_index.set_defaults(which='buildindex')
    parser_build_index.add_argument('db_type', type=str, help="db_type the index applies")
    parser_build_index.add_argument('collection', type=str, help="collection the index applies")
    
    
    
    parser_set_db_tags = subparsers.add_parser('setdbtags', help='sets the db_tags field for an existing node')
    parser_set_db_tags.set_defaults(which='setdbtags')
    parser_set_db_tags.add_argument('uid', type=str, help="unique id for the node to change")
    parser_set_db_tags.add_argument('db_tags', type=str, help="JSON list of namespaces")
    
    
    
    
    args = parser.parse_args()
    if args.which is "addnode":
        add_node(args)
    elif args.which is "removenode":
        remove_node(args)
    elif args.which is "addindex":
        add_index(args)
    elif args.which is "removeindex":
        remove_index(args)
    elif args.which is "setdbtags":
        set_db_tags(args)
    elif args.which is "buildindex":
        build_index(args)
