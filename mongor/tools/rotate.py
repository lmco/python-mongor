from mongor import Maintenence
from optparse import OptionParser

if __name__ == "__main__":
    # Set up options
    #This should be moved to argparse for python 2.7+
    parser = OptionParser(usage="usage: %prog [options]")
    parser.add_option("-s", "--servername",
                      action="store",
                      dest="servername",
                      help="[REQUIRED] 'localhost' or 'mongodb.host.com'")
    parser.add_option("-p", "--port",
                      action="store",
                      type="int",
                      dest="port",
                      help="[REQUIRED] integer 27017 or other")
    parser.add_option("-c", "--collection",
                      action="store",
                      dest="collection",
                      help="[REQUIRED] collection name for indexes")
    parser.add_option("-t", "--type",
                      action="store",
                      dest="db_type",
                      help="[REQUIRED] database type")
    parser.add_option('--ssl',
                      dest='ssl',
                      action='store_true',
                      help="[optional] if the mongod requres ssl")
    parser.set_defaults(ssl=False)
    (options, args) = parser.parse_args()
    
    
    maint = Maintenence(config_host=options.servername, config_port=options.port, config_ssl=options.ssl)
    need, data_size = maint.need_to_rotate(db_type=options.db_type)
    if need:
        maint.clean_incoming(db_type=options.db_type)
        maint.ensure_indexes(options.collection, maint.config.get_indexes(options.db_type), db_type=options.db_type)
        maint.rotate_schedule(db_type=options.db_type)
