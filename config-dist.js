var config = {

    'mysql' : {
        'host': '',
        'port': '',
        'user': '',
        'password': '',
        'database': ''
    },

    'arango': {
        "url"     : '',
        "username": '',
        "password": '',
        "database": '',
        "graph_name": '',
        "entity_collection": '',
        "relation_collection": ''
    },

    'entities' : ['place', 'tribe', 'person', 'transcript', 'event']

};

module.exports = config;