import os, sys, json, tarfile, string

def get_db_path(arango_root, db_name):
    db_path = arango_root + '/arangodb/databases'
    databases = os.listdir(db_path)
    for i in databases:
        json_path = db_path + '/' + i + '/parameter.json'
        with open(json_path) as data_file:    
            data = json.load(data_file)
            if not data['deleted'] and data['name'] == db_name:
                return db_path + '/database-' + data['id']
    return None
    
    
arango_root_path = 'ARANGO_DB_ROOT_PATH'
output_file = 'OUTPUT_TAR_GZ_PATH'
target_dbs = ['_system', 'NAME_OF_OTHER_DBS']


target_dbs_paths = []
for i in target_dbs:
    output =  get_db_path(arango_root_path, i)
    if output is None:
        print('Could not find database ' + i)
        sys.exit(1)
    target_dbs_paths.append(output)
    

tar = tarfile.open(output_file, "w:gz")

for i in target_dbs_paths:
    tar.add(i, 'databases/' + i.split('/')[-1])
    
tar.close()

