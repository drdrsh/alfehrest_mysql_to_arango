var mysql = require('mysql');
var Promise = require("bluebird");
var ID = require('shortid');
var config = require('./config.js');

var pool  = mysql.createPool({
    connectionLimit : 10,
    host            : config.mysql.host,
    user            : config.mysql.username,
    password        : config.mysql.password
});
var database = config.mysql.database;
var entities = config.entities;


function createPersonPersonRelRecord(relationshipData) {
    var typeDefinition = [
        ['sehr', 'sehr'],
        ['zawj', 'zawj'],
        ['am', 'wld_akh'],
        ['khal', 'wld_okht'],
        ['akh_shaqiq', 'akh_shaqiq'],
        ['akh_ab', 'akh_ab'],
        ['akh_om', 'akh_om'],
        ['waled', 'wld'],
        ['waled_rada', 'wld_rada'],
        ['akh_rada', 'akh_rada'],

        ['wld_am_a', 'wld_am_a'],
        ['wld_khalh_k', 'wld_khalh_k'],

        ['wld_khal_a', 'wld_ama_k'],
        ['wld_am_k', 'wld_khalh_a'],

        ['other', 'other']
    ];

    var obj = {
        '_key' : 'rel_' + ID.generate(),
        '_from': 'entity/' + relationshipData.firstEntityId,
        '_to'  : 'entity/' + relationshipData.secondEntityId,
        'firstEntityId' : relationshipData.firstEntityId,
        'secondEntityId' : relationshipData.secondEntityId,
        'firstEntityType' : 'person',
        'secondEntityType' : 'person',
        'type' :  "person." + typeDefinition[relationshipData.type][1],
        'relationship': typeDefinition[relationshipData.type][1],
        'strings' : {
            'ar' : {
                'comments' : relationshipData.properties['annotation'],
                'references' : []
            }
        }
    };

    if(relationshipData.type == 0 || relationshipData.type == 1) {
        var x = {
            'dateStarted': {
                first_date: relationshipData.properties['date_started'] || null,
                mode: 'approx',
                second_date: null
            },
            'dateEnded': {
                first_date: relationshipData.properties['date_ended'] || null,
                mode: 'approx',
                second_date: null
            }
        };
        Object.assign(obj, x);
    }
    return obj;
}

function createPersonTribeRelRecord(relationshipData) {

    var typeDefinition = ['sareeh', 'montaqel', 'mostalhaq', 'abd', 'mawla'];

    var obj = {
        '_key' : 'rel_' + ID.generate(),
        '_from': 'entity/' + relationshipData.firstEntityId,
        '_to'  : 'entity/' + relationshipData.secondEntityId,
        'firstEntityId' : relationshipData.firstEntityId,
        'secondEntityId' : relationshipData.secondEntityId,
        'firstEntityType' : 'person',
        'secondEntityType' : 'tribe',
        'type' :  "tribe." + typeDefinition[relationshipData.type],
        'relationship': typeDefinition[relationshipData.type],
        'strings' : {
            'ar' : {
                'comments' : relationshipData.properties['annotation'],
                'references' : []
            }
        }
    };

    if(relationshipData.type != 0) {
        var x = {
            'dateStarted': {
                first_date: relationshipData.properties['date_started'] || null,
                mode: 'approx',
                second_date: null
            }
        };
        Object.assign(obj, x);
    }
    return obj;
}

function createPersonPlaceRelRecord(relationshipData) {
    //TODO: By default make all relationships of the type ?? ??
}

function createTribeStateRelRecord(relationshipData) {
    //TODO: Implement relationship handling
    return null;
}

function createTribeTribeRelRecord(relationshipData) {
    //TODO: add support for ally and follower
    var typeDefinition = [
        ['ally', 'ally'],
        ['follower', 'followed'],
        ['branch', 'root']
    ];


    var subtypeDefinition = [
        'shaab', 'qabila', 'emara', 'batn',
        'fakhz', 'faseela', 'dawla', 'other'];


    var obj = {
        '_key' : 'rel_' + ID.generate(),
        '_from': 'entity/' + relationshipData.firstEntityId,
        '_to'  : 'entity/' + relationshipData.secondEntityId,
        'firstEntityId' : relationshipData.firstEntityId,
        'secondEntityId' : relationshipData.secondEntityId,
        'firstEntityType' : 'tribe',
        'secondEntityType' : 'tribe',
        'type' :  "tribe." + typeDefinition[relationshipData.type][1],
        'relationship': typeDefinition[relationshipData.type][1],
        'strings' : {
            'ar' : {
                'comments' : relationshipData.properties['annotation'],
                'references' : []
            }
        }
    };

    return obj;
}

function createPersonRelRecord(relationshipData) {
    if(relationshipData.secondEntityType == 'tribe') {
        return createPersonTribeRelRecord(relationshipData);
    }
    if(relationshipData.secondEntityType == 'place') {
        return createPersonPlaceRelRecord(relationshipData);
    }
    if(relationshipData.secondEntityType == 'person') {
        return createPersonPersonRelRecord(relationshipData);
    }
    return null;
}

function createTribeRelRecord(relationshipData) {
    if(relationshipData.secondEntityType == 'tribe') {
        return createTribeTribeRelRecord(relationshipData);
    }
    if(relationshipData.secondEntityType == 'state') {
        return createTribeStateRelRecord(relationshipData);
    }
    return null;
}

function loadAllDeepRel(entityData, relationshipData, databaseStructure) {

    var recordHandlers = {
        'person': createPersonRelRecord,
        'tribe' : createTribeRelRecord
    };

    var promises = [];
    for(var idx in databaseStructure) {
        var tableNames = databaseStructure[idx].deep_rel;
        for(var i=0; i<tableNames.length; i++) {
            var tableName = tableNames[i];
            //promises.push(loadSingleEntityData(tableName));
            var q = `
                SELECT "${tableName}" as tableName, e.*, s.annotation FROM ${database}.${tableName} e
                LEFT JOIN ${database}.${tableName}_string s ON s.entity_id = e.id
            `;
            promises.push(query(q));
        }
    }

    return new Promise( (resolve, reject) => {
        Promise.all(promises).then(resultSets => {
            for (var i = 0; i < resultSets.length; i++) {
                var resultSet = resultSets[i];
                for (var j = 0; j < resultSet.length; j++) {

                    var row = resultSet[j];
                    var relationshipParts = row.tableName.split('_');

                    var relObj = {
                        'type': row['type'],
                        'properties' : {

                        }
                    };


                    if(relationshipParts[0] == relationshipParts[1]){
                        //tribe_tribe
                        let firstEntityId = row['first_' + relationshipParts[0]];
                        let firstEntityType = relationshipParts[0];
                        let secondEntityId = row['second_' + relationshipParts[1]];
                        let secondEntityType = relationshipParts[1];
                        let x = {
                            firstEntity: entityData[firstEntityType][firstEntityId],
                            firstEntityId: entityData[firstEntityType][firstEntityId]['graphId'],
                            firstEntityType: firstEntityType,
                            secondEntity: entityData[secondEntityType][secondEntityId],
                            secondEntityId: entityData[secondEntityType][secondEntityId]['graphId'] ,
                            secondEntityType: secondEntityType
                        };

                        delete row['first_' + relationshipParts[0]];
                        delete row['second_' + relationshipParts[1]];
                        Object.assign(relObj, x);
                    } else {
                        //person_tribe
                        let firstEntityId = row[relationshipParts[0] + '_id'];
                        let firstEntityType = relationshipParts[0];
                        let secondEntityId = row[relationshipParts[1] + '_id'];
                        let secondEntityType = relationshipParts[1];

                        let x = {
                            firstEntity: entityData[firstEntityType][firstEntityId],
                            firstEntityId: entityData[firstEntityType][firstEntityId]['graphId'],
                            firstEntityType: firstEntityType,
                            secondEntity: entityData[secondEntityType][secondEntityId],
                            secondEntityId: entityData[secondEntityType][secondEntityId]['graphId'],
                            secondEntityType: secondEntityType
                        };

                        delete row[relationshipParts[0] + '_id'];
                        delete row[relationshipParts[1] + '_id'];
                        Object.assign(relObj, x);
                    }

                    delete row['id'];
                    delete row['type'];
                    delete row['tableName'];

                    Object.assign(relObj.properties, row);

                    var relRecord = (recordHandlers[relObj.firstEntityType])(relObj);
                    if(relRecord) {
                        relationshipData.push(relRecord);
                    }
                }
            }
            resolve([entityData, relationshipData, databaseStructure]);
        }, err => {
            reject(err);
        });
    });
}

function createShallowRelRecords(relationshipData) {

    var entityTypes = [];
    var entityIds   = [];

    for(var idx in relationshipData) {
        entityTypes.push(idx);
        entityIds.push(relationshipData[idx]);
    }

    return {
        '_key' : 'rel_' + ID.generate(),
        '_from': 'entity/' + entityIds[0],
        '_to'  : 'entity/' + entityIds[1],
        'firstEntityId' : entityIds[0],
        'secondEntityId' : entityIds[1],
        'firstEntityType' : entityTypes[0],
        'secondEntityType' : entityTypes[1],
        'type' :  entityTypes[1] + ".related",
        'relationship': 'related'
    };

}
function loadAllShallowRel(entityData, relationshipData, databaseStructure) {

    var promises = [];
    for(var idx in databaseStructure) {
        var tableNames = databaseStructure[idx].shallow_rel;
        for(var i=0; i<tableNames.length; i++) {
            var tableName = tableNames[i];
            var q = `SELECT * FROM ${database}.${tableName}`;
            promises.push(query(q));
        }
    }

    return new Promise( (resolve, reject) => {
        Promise.all(promises).then(resultSets => {
            for(var i=0; i<resultSets.length; i++) {
                var resultSet = resultSets[i];
                for(var j=0; j<resultSet.length; j++) {
                    var row = resultSet[j];
                    var relObj = {};
                    for(var idx in row) {
                        if(idx == 'id'){
                            continue;
                        }
                        var entityType = idx.replace('_id', '');
                        var entityId = row[idx];
                        relObj[entityType] = entityData[entityType][entityId]['graphId'];
                    }
                    relationshipData.push(createShallowRelRecords(relObj));
                }
            }
            resolve([entityData, relationshipData, databaseStructure]);
        }, err => {
            reject(err);
        })
    });
}

function preparePerson(entityData, fullEntityData, relationshipData) {
    var dates = ['born', 'died', 'hijrah', 'islam', 'redda'];
    for(var i=0; i<dates.length; i++) {
        var d = dates[i];
        entityData[d] = {
            first_date: entityData[d] || null,
            mode: 'approx',
            second_date: null
        };
    }
    return entityData;
}

function prepareTribe(entityData, fullEntityData, relationshipData) {

    var subtypeDefinition = [
        'shaab', 'qabila', 'emara', 'batn',
        'fakhz', 'faseela', 'dawla', 'other'];
    entityData.type = subtypeDefinition[entityData.type];
    return entityData;

}

function preparePlace(entityData, fullEntityData, relationshipData) {

    var dates = ['establishment', 'destruction'];
    for(var i=0; i<dates.length; i++) {
        var d = dates[i];
        entityData[d] = {
            first_date: entityData[d] || null,
            mode: 'approx',
            second_date: null
        };
    }
    entityData.geopoints = JSON.parse(entityData.geopoints);
    entityData.images = [];
    entityData.type = 'generic';
    return entityData;
}

function prepareEvent(entityData, fullEntityData, relationshipData) {
    var eventPlace = fullEntityData['place'][entityData.place_id];
    var event_types = ['other', 'battle', 'treaty', 'establishment', 'destruction', 'uprising'];
    relationshipData.push({
        '_key' : 'rel_' + ID.generate(),
        '_from': 'entity/' + entityData['_key'],
        '_to'  : 'entity/' + eventPlace['_key'],
        'firstEntityId' : entityData['_key'],
        'secondEntityId' : eventPlace['_key'],
        'firstEntityType' : 'event',
        'secondEntityType' : 'place',
        'type' :  "place.took_place_in",
        'relationship': 'took_place_in',
        'strings' : {
            'ar' : {'comments' : ''}
        }
    });

    entityData.type = event_types[entityData.type];
    entityData.date = {
        first_date: entityData.date || null,
        mode: 'approx',
        second_date: null
    };
    delete entityData.place_id;
    return entityData;
}

function prepareTranscript(entityData, fullEntityData, relationshipData) {

    if(entityData.person_id) {
        var transcriptPerson = fullEntityData['person'][entityData.person_id];
        relationshipData.push({
            '_key' : 'rel_' + ID.generate(),
            '_from': 'entity/' + entityData['_key'],
            '_to'  : 'entity/' + transcriptPerson['_key'],
            'firstEntityId' : entityData['_key'],
            'secondEntityId' : transcriptPerson['_key'],
            'firstEntityType' : 'transcript',
            'secondEntityType' : 'person',
            'type' :  "person.said_by",
            'relationship': 'said_by',
            'strings' : {
                'ar' : {'comments' : ''}
            }
        });
    }

    if(entityData.place_id) {
        var transcriptPlace = fullEntityData['place'][entityData.place_id];
        relationshipData.push({
            '_key' : 'rel_' + ID.generate(),
            '_from': 'entity/' + entityData['_key'],
            '_to'  : 'entity/' + transcriptPlace['_key'],
            'firstEntityId' : entityData['_key'],
            'secondEntityId' : transcriptPlace['_key'],
            'firstEntityType' : 'transcript',
            'secondEntityType' : 'place',
            'type' :  "place.said_in",
            'relationship': 'said_in',
            'strings' : {
                'ar' : {'comments' : ''}
            }
        });
    }

    entityData.date = {
        first_date: entityData.date || null,
        mode: 'approx',
        second_date: null
    };
    delete entityData.place_id;
    delete entityData.person_id;
    return entityData;
}
function prepareEntityData(entityData, relationshipData, databaseStructure){

    var entityHandlers = {
        'person' : preparePerson,
        'tribe' : prepareTribe,
        'transcript' : prepareTranscript,
        'event' : prepareEvent,
        'place' : preparePlace
    };
    var entityArray = [];
    for(var entityType in entityData) {
        var entityList = entityData[entityType];
        var handler = entityHandlers[entityType];
        for(var entityId in entityList ) {
            var entity = entityList[entityId];
            entityData[entityType][entityId]['_key'] = entity['graphId'];
            entityData[entityType][entityId]['id'] = entity['graphId'];
            entityData[entityType][entityId]['_entity_type'] = entityType;

            delete entityData[entityType][entityId]['graphId'];
            if(entityData[entityType][entityId]['strings']['ar']['references'] ){
                entityData[entityType][entityId]['strings']['ar']['references'] =
                    entityData[entityType][entityId]['strings']['ar']['references'].trim().split("\n");
            } else {
                entityData[entityType][entityId]['strings']['ar']['references'] = [];
            }

            entityData[entityType][entityId] = handler(entityData[entityType][entityId], entityData, relationshipData);
            entityArray.push(entityData[entityType][entityId]);
        }
    }


    return Promise.resolve([entityArray, relationshipData, databaseStructure]);
}
function loadAllEntityData() {
    var promises = [];
    for(var i=0; i<entities.length; i++){
        promises.push(loadSingleEntityData(entities[i]));
    }

    return new Promise( (resolve, reject) => {
        Promise.all(promises).then( entityData => {
            var consolidatedObject = {};
            for(var i=0; i<entityData.length; i++) {
                var entityEntry = entityData[i];
                consolidatedObject[entityEntry.id] = entityEntry.data;
            }
            resolve(consolidatedObject);
        });
    });
}

function loadSingleEntityData(entityName) {

    var entityTablename = entityName;
    var stringsTablename= entityName +  '_string';
    var entities = {};

    return new Promise( function(resolve, reject) {
        Promise.all([
            query(`SHOW COLUMNS FROM ${database}.${stringsTablename}`),
            query(`SELECT e.id as main_entity_id, e.*, s.* FROM ${database}.${entityTablename} e JOIN ${database}.${stringsTablename} s ON e.id = s.entity_id`)
        ]).then( (responses) => {
                var columns = responses[0];
                var rows = responses[1];

                var lang = 'ar';
                var stringIgnoreCols = ['id', 'entity_id', 'outdated', 'lid'];
                var entityIgnoreCols =
                    ['id', 'entity_id', 'lid', 'outdated', 'created_by', 'created_date',
                        'modified_date', 'modified_by', 'locked_by', 'locked_date', 'locked_lang'];
                var translatable = [];
                for(let i=0; i<columns.length; i++) {
                    var fieldName = columns[i]['Field'];
                    if(stringIgnoreCols.indexOf(fieldName) != -1) {
                        continue;
                    }
                    translatable.push(fieldName);
                }

                for(let i=0; i<rows.length; i++) {
                    var entity = rows[i];
                    var newEntity = {
                        'strings' : {
                            'ar' : {}
                        }
                    };
                    newEntity['id'] = entity.main_entity_id;
                    delete entity.main_entity_id;
                    for(var idx in entity) {
                        if(translatable.indexOf(idx) == -1) {
                            if(entityIgnoreCols.indexOf(idx) == -1) {
                                newEntity[idx] = entity[idx];
                            }
                            continue;
                        }
                        newEntity['strings'][lang][idx] = entity[idx];
                    }
                    newEntity['graphId'] = entityName + "_" + ID.generate();
                    entities[newEntity.id] = newEntity;
                }
                resolve({
                    id  : entityName,
                    data: entities
                });
            },
            () => {
                reject();
            }
        );

    });

}

function query(sql) {
    return new Promise((resolve, reject) => {
        pool.query(sql, function (err, rows, fields) {
            if (err) {
                reject(err);
                return;
            }
            resolve(rows, fields);
        });
    });
}

function readDatabaseStructure() {
    return new Promise( (resolve, reject) => {
        query(`SHOW TABLES FROM ${database}`)
            .then( (rows, fields) => {
                resolve(parseDatabaseStructure(rows));
            }, (err) => {
                reject(err);
            });
    });
}
function parseDatabaseStructure(rows) {

    var tables = {};
    for(var i=0; i<entities.length; i++) {
        var entity = entities[i];
        tables[entity] = {
            'main': entity,
            'strings': entity + '_string',
            'shallow_rel': [],
            'deep_rel': []
        };
    }
    var fieldName = `Tables_in_${database}`;
    for(var j=0; j<rows.length; j++) {

        var tableName = rows[j][fieldName];
        if (tableName.indexOf('_') == -1 || tableName.indexOf('string') != -1) {
            continue;
        }

        var startEntity = tableName.split('_');
        if(entities.indexOf(startEntity[0]) == -1){
            continue;
        }

        if(tableName.indexOf('_to_') != -1) {
            tables[startEntity[0]]['shallow_rel'].push(tableName);
        } else {
            tables[startEntity[0]]['deep_rel'].push(tableName);
        }
    }

    return tables;
}




function dropDatabase(db, entityData, relationshipData) {

    return new Promise( function(resolve, reject) {
        db.dropDatabase(config.arango.database)
        .then(
            () => { resolve([db, entityData, relationshipData]); },
            () => { resolve([db, entityData, relationshipData]); }
        );
    });

}

function createDatabase(db, entityData, relationshipData) {

    return new Promise( function(resolve, reject) {
        db.createDatabase(config.arango.database)
            .then( () => {
                db.useDatabase(config.arango.database);
                resolve([db, entityData, relationshipData])
            });
    });

}

function createCollections(db, entityData, relationshipData) {

    var promises = [];
    promises.push(db.collection(config.arango.entity_collection).create());
    promises.push(db.edgeCollection(config.arango.relation_collection).create());

    return new Promise((resolve, reject) => {
        Promise.all(promises)
            .then(
            () => {
                resolve([db, entityData, relationshipData]);
            },
            () => {
                reject();
            }
        );
    });

}

function createGraph(db, entityData, relationshipData) {


    return new Promise( function(resolve, reject) {
        db.graph(config.arango.graph_name).create({
            'name': config.arango.graph_name,
            'edgeDefinitions': [{
                to: [config.arango.entity_collection],
                from: [config.arango.entity_collection],
                collection: config.arango.relation_collection
            }],
            'orphanCollections': []
        }).then(() => {
            resolve([db, entityData, relationshipData])
        });

    });

}


function insertData(db, entityData, relationshipData) {

    var eCol = config.arango.entity_collection;
    var rCol = config.arango.relation_collection;

    var eData = JSON.stringify(entityData);
    var rData = JSON.stringify(relationshipData);

    var q1 =`
        LET entities = ${eData}
        FOR e IN entities
            INSERT e IN ${eCol}`;

    var q2 = `
    LET relations = ${rData}
    FOR r IN relations
        INSERT r IN ${rCol}`;

    var promises = [db.query(q1), db.query(q2)];

    return new Promise( (resolve, reject) => {
        Promise.all(promises).then(
            () => { resolve([db, entityData, relationshipData]); },
            () => { reject(); }
        );
    });

}


Promise
    .all([ loadAllEntityData(), [], readDatabaseStructure()])
    .spread(loadAllShallowRel)
    .spread(loadAllDeepRel)
    .spread(prepareEntityData)
    .spread( (entityData, relationshipData, databaseStructure) => {
        var db = (require('arangojs'))(config.arango.url);
        return dropDatabase(db, entityData, relationshipData)
    })
    .spread(createDatabase)
//    .spread(createCollections)
    .spread(createGraph)
    .spread(insertData)
    .spread((db, entityData, relationshipData) => {
        console.log('Done!');
    });
