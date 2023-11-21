import json
from xml.etree import ElementTree as ET
import os

import pymongo

from ControllerUtils import checkAttributeNames, checkPrimaryKeyForInsert, \
    checkPrimaryKeyAlreadyExistsInDb, joinAttributePrimaryKeys, joinAttributeValues, getTable, searchKeyUnique, \
    searchKeyLength, checkIndexName, testTypeMatches, testAttributeTypes, checkAttributeNamesDelete, \
    checkPrimaryKeyForDelete, testAttributeTypesForDelete, checkForeignKeyForDeleteRow, checkPrimaryKeyDoesNotExist, \
    getIndexFiles, getIndexAttributes, joinAttributeIndex, checkIfIndexUnique, checkIfIndexKeyExists, \
    checkForForeignKeys, checkIfForeignKeyExists, joinAttributeForeignKeys, checkForeignKeyForDelete


class CatalogController:
    myClient = pymongo.MongoClient("mongodb://localhost:27017/")
    myDb = None

    def __init__(self):
        # header: et.write(f, encoding='utf-8', xml_declaration=True)
        if os.path.exists('Catalog.xml'):
            pass
        else:
            databases = ET.Element('Databases')
            tree = ET.ElementTree(databases)
            tree.write('Catalog.xml')

    def useDatabase(self, name):
        with open('Catalog.xml', 'r') as f:
            doc = ET.parse(f)
            found = False
            for elem in doc.iter("Database"):
                if elem.attrib['dataBaseName'] == name:
                    found = True
                    self.myDb = self.myClient[name]
                    # dblist = self.myClient.list_database_names()
                    # if name in dblist:
                    #     self.myDb = self.myClient[name]
                    # else:
                    # raise Exception("database does not exist in mongo")
                    break
        if not found:
            raise Exception("database does not exist in the catalog")

    def createDatabase(self, name):
        with open('Catalog.xml', 'r') as f:
            doc = ET.parse(f)
            for elem in doc.iter("Database"):
                if elem.attrib['dataBaseName'] == name:
                    raise Exception("The database already exists")
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            database = ET.SubElement(myRoot, 'Database', dataBaseName=name)
            tables = ET.SubElement(database, 'Tables')
            ET.dump(tables)
            myTree.write('Catalog.xml')
            # mongo
            self.myDb = self.myClient[name]
        return "Database was created"

    def dropDatabase(self, name):
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myTree.iter("Database"):
                if db.attrib['dataBaseName'] == name:
                    myRoot.remove(db)
                    myTree = ET.ElementTree(myRoot)
                    myTree.write('Catalog.xml')
                    # mongo
                    self.myClient.drop_database(name)
                    return "Database dropped successfully"
        raise Exception("Database could not be found")

    def createTable(self, dataBaseName, dataJson):
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == dataBaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == dataJson["tableName"]:
                            raise Exception("Table name already exists")
                    table = ET.SubElement(tables, 'Table', tableName=dataJson["tableName"],
                                          fileName=dataJson["tableName"] + ".bin", rowLength="0")
                    structure = ET.SubElement(table, 'Structure')
                    primaryKeys = ET.SubElement(table, 'PrimaryKey')
                    foreignKeys = ET.SubElement(table, 'ForeignKeys')
                    uniqueKeys = ET.SubElement(table, "UniqueKeys")
                    ET.SubElement(table, "IndexFiles")
                    columnNames = []
                    uniqueColumns=[]
                    foreignColumns=[]
                    for attribute in dataJson["attributes"]:
                        ET.SubElement(structure, 'Attribute', attributeName=attribute["name"],
                                      type=attribute["type"].upper(), length=attribute["size"],
                                      isNull=("0" if attribute["notNull"] else "1"))
                        if attribute["primaryKey"]:
                            pk = ET.SubElement(primaryKeys, "pkAttribute")
                            pk.text = attribute["name"]
                            columnNames.append(attribute["name"])
                        if attribute["unique"]:
                            unq = ET.SubElement(uniqueKeys, "uniqueAttribute")
                            unq.text = attribute["name"]
                            uniqueColumns.append(attribute["name"])
                        fk = attribute["foreignKey"]
                        if fk["tableName"]:
                            refs = []
                            # check if fk is valid
                            for ref in db.findall("Tables"):
                                r = ref.find("refTable")
                                if r and r.text == fk["tableName"]:
                                    refs.append(ref)
                            tables = db.find("Tables")
                            for tb in tables.iter("Table"):
                                if tb.attrib["tableName"] == fk["tableName"]:
                                    refs.append(tb)
                                    break
                            if len(refs) == 0:
                                raise Exception("Could not find table referenced by the fk")
                            else:
                                struc = tb.find("Structure")
                                found = False
                                for atrib in struc.iter("Attribute"):
                                    if atrib.attrib["attributeName"] == fk["columnName"]:
                                        found = True
                                        break
                                if not found:
                                    raise Exception("Could not find column name in table referenced by the fk")

                            foreignKey = ET.SubElement(foreignKeys, "ForeignKey")
                            attr = ET.SubElement(foreignKey, "fkAttribute")
                            attr.text = attribute["name"]
                            references = ET.SubElement(foreignKey, "references")
                            refTable = ET.SubElement(references, "refTable")
                            refTable.text = fk["tableName"]
                            refAttribute = ET.SubElement(references, "refAttribute")
                            refAttribute.text = fk["columnName"]
                            foreignColumns.append(fk["columnName"])

                    myTree = ET.ElementTree(myRoot)
                    myTree.write('Catalog.xml')
                    # ===== TODO sa mai las asta ...index pt cheia primara?
                    if len(columnNames) > 0:
                        x = {
                            "command": 55,
                            "tableName": dataJson["tableName"],
                            "indexName": "primary" + dataJson["tableName"] + "".join(columnNames),
                            "columnName": columnNames,
                            "unique": "1"
                        }
                        # convert into JSON:
                        response = json.dumps(x)
                        self.createIndexWithName(dataBaseName, x)
                    # =========
                        # =====  index pt chei unice
                    if len(uniqueColumns) > 1:
                        for uniqueK in uniqueColumns:
                            listc=[uniqueK]
                            x = {
                                "command": 55,
                                "tableName": dataJson["tableName"],
                                "indexName": "unique"+dataJson["tableName"]+uniqueK,
                                "columnName": listc,
                                "unique": "1"
                            }
                            # convert into JSON:
                            response = json.dumps(x)
                            self.createIndexWithName(dataBaseName, x)
                    elif len(uniqueColumns) == 1:
                        x = {
                            "command": 55,
                            "tableName": dataJson["tableName"],
                            "indexName": "unique" + dataJson["tableName"] + "".join(uniqueColumns),
                            "columnName": uniqueColumns,
                            "unique": "1"
                        }
                        # convert into JSON:
                        response = json.dumps(x)
                        self.createIndexWithName(dataBaseName, x)
                        # =========
                    # ===== foreign index
                    if len(foreignColumns) > 0:
                        x = {
                            "command": 55,
                            "tableName": dataJson["tableName"],
                            "indexName": "foreign" + dataJson["tableName"] + "".join(foreignColumns),
                            "columnName": foreignColumns,
                            "unique": "0"
                        }
                        # convert into JSON:
                        response = json.dumps(x)
                        self.createIndexWithName(dataBaseName, x)
                        # =========
                    # mongo
                    self.myDb.create_collection(dataJson["tableName"])
                    return "Successfully created table"
        return "Could not find database"

    def dropTable(self, databaseName, name):
        indexFiles=getIndexFiles(databaseName, name)
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    refs = []
                    # search for fk
                    for ref in db.findall("Tables"):
                        r = ref.find("refTable")
                        if r and r.text == name:
                            refs.append(ref)
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        fks = tb.find("ForeignKeys")
                        for fk in fks.iter("ForeignKey"):
                            rfr = fk.find("references")
                            tbl = rfr.find("refTable")
                            if tbl is not None and tbl.text == name:
                                refs.append(tbl)
                    # remove if possible
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == name:
                            if len(refs) > 0:
                                raise Exception("Could not delete table because it contains references to other tables")
                            tables.remove(tb)
                            myTree = ET.ElementTree(myRoot)
                            myTree.write('Catalog.xml')
                            # mongo
                            self.myDb.drop_collection(name)
                            #TODO DROP ALL INDEXES
                            for index in indexFiles:
                                self.myDb.drop_collection(index)
                            return "Table dropped successfully"
        return "Table could not be found be found"

    def createIndex(self, databaseName, dataJson):
        if checkIndexName(databaseName,
                          databaseName + dataJson["tableName"] + dataJson["columnName"][0]):
            return "Index name already exists"
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == dataJson["tableName"]:
                            indexName = databaseName + dataJson["tableName"] + dataJson["columnName"][
                                0] + ".ind"  # aici se poate adauga numele la fiecare coloana...
                            indexFile = tb.find("IndexFiles")
                            indx = ET.SubElement(indexFile, 'IndexFile', indexName=indexName,
                                                 keylength=searchKeyLength(databaseName,
                                                                           dataJson["tableName"],
                                                                           dataJson["columnName"][
                                                                               0]),
                                                 isUnique=searchKeyUnique(databaseName,
                                                                          dataJson["tableName"],
                                                                          dataJson["columnName"][
                                                                              0]),
                                                 indexType="BTree")
                            indatrib = ET.SubElement(indx, 'IndexAttributes')
                            for atrib in dataJson["columnName"]:
                                iatrib = ET.SubElement(indatrib, 'IAttribute')
                                iatrib.text = atrib
                            myTree = ET.ElementTree(myRoot)
                            myTree.write('Catalog.xml')

                            f = open(indexName, "w")
                            f.write("indexfile")
                            f.close()

                            return "Successfully created index with name " + indexName
            raise Exception("Could not find database")

    def createIndexWithName(self, currentDatabase, dataJson):
        if checkIndexName(currentDatabase, dataJson["indexName"]):
            return "Index name already exists"
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == currentDatabase:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == dataJson["tableName"]:
                            indexName = dataJson["indexName"] + ".ind"
                            indexFile = tb.find("IndexFiles")
                            indx = ET.SubElement(indexFile, 'IndexFile', indexName=indexName,
                                                 keylength=searchKeyLength(currentDatabase,
                                                                           dataJson["tableName"],
                                                                           dataJson["columnName"][0]),
                                                 isUnique=str(dataJson["unique"]),
                                                 indexType="BTree")
                            indatrib = ET.SubElement(indx, 'IndexAttributes')
                            for atrib in dataJson["columnName"]:
                                iatrib = ET.SubElement(indatrib, 'IAttribute')
                                iatrib.text = atrib
                            myTree = ET.ElementTree(myRoot)
                            myTree.write('Catalog.xml')

                            f = open(indexName, "w")
                            f.write("indexfile")
                            f.close()

                            # mongo
                            self.myDb.create_collection(indexName)#dataJson["indexName"])
                            return "Successfully created index with name " + indexName
            raise Exception("Could not find database")

    def dropIndex(self, databaseName, dataJson):
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        indexFile = tb.find("IndexFiles")
                        for idf in indexFile.iter("IndexFile"):
                            if idf.attrib["indexName"] == dataJson["indexName"] + ".ind":
                                indexFile.remove(idf)
                                myTree = ET.ElementTree(myRoot)
                                myTree.write('Catalog.xml')

                                os.remove(dataJson["indexName"] + ".ind")
                                # mongo
                                self.myDb.drop_collection(dataJson["indexName"] + ".ind")
                                return "Index dropped successfully"
                        raise Exception("Could not find index")
        raise Exception("Index could not be removed")

    def insert(self, dataBaseName, dataJson):
        with open('Catalog.xml', 'r'):
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            table = getTable(myRoot, dataBaseName, dataJson)
            if table:
                structure = table.find("Structure")
                attributes = structure.findall("Attribute")
                primaryKeys = table.find("PrimaryKey")
                primaryKeyAttributes = primaryKeys.findall("pkAttribute")

                # check attributes
                checkAttributeNames(attributes, dataJson)
                checkPrimaryKeyForInsert(primaryKeyAttributes, dataJson)

                collection = self.myDb.get_collection(dataJson["tableName"])
                if collection is None:
                    raise Exception("Collection not found in db")

                key = None
                for k in dataJson["values"][0].keys():
                    key = k
                response = []
                for i in range(len(dataJson["values"][0][key])):
                    try:
                        testAttributeTypes(attributes, dataJson, i)
                        # check primary key is unique
                        checkPrimaryKeyAlreadyExistsInDb(primaryKeyAttributes, collection, dataJson, i)
                        pk = joinAttributePrimaryKeys(primaryKeyAttributes, dataJson, i)
                        value = joinAttributeValues(attributes, primaryKeyAttributes, dataJson, i)
                        tr = {"pk": pk, "value": value}

                        # TODO foreign key check =================================================
                        foreignKeyList = checkForForeignKeys(dataBaseName,dataJson["tableName"])
                        for foreignKey in foreignKeyList:
                            pkFilename = "primary" + foreignKey["refTable"] + "".join(foreignKey["refAttribute"])+".ind"
                            foreignKeyAttributes=foreignKey["fkAttribute"]
                            fk=joinAttributeForeignKeys(foreignKeyAttributes, dataJson, i)
                            if checkIfForeignKeyExists(fk,self.myDb.get_collection(pkFilename)) == False:
                                raise Exception("Foreign key "+"".join(foreignKey["refAttribute"])+" with value "+fk+" does not exist,cannot insert")

                        # TODO foreign key check =================================================
                        #TODO insert for indexes =================================================
                        indexList=getIndexFiles(dataBaseName,dataJson["tableName"])
                        for index in indexList:
                            isUnique = checkIfIndexUnique(dataBaseName, dataJson["tableName"], index)
                            collection1 = self.myDb.get_collection(index)
                            indexAtrib = getIndexAttributes(dataBaseName, index)
                            pk1 = joinAttributeIndex(indexAtrib, dataJson, i)
                            value1 = joinAttributePrimaryKeys(primaryKeyAttributes, dataJson, i)
                            tr1 = {"pk": pk1, "value": value1}
                            existsAlready = checkIfIndexKeyExists(collection1, tr1)
                            # check fo unique
                            if existsAlready == True:
                                if isUnique == True:
                                    raise Exception("Unique Index key already exists ,cannot insert")
                        #==================================================================
                        for index in indexList:
                            isUnique=checkIfIndexUnique(dataBaseName,dataJson["tableName"],index)
                            collection1 = self.myDb.get_collection(index)
                            indexAtrib=getIndexAttributes(dataBaseName,index)
                            pk1=joinAttributeIndex(indexAtrib,dataJson,i)
                            value1=joinAttributePrimaryKeys(primaryKeyAttributes, dataJson, i)
                            tr1 = {"pk": pk1, "value": value1}
                            existsAlready= checkIfIndexKeyExists(collection1,tr1)
                            #check fo unique
                            if existsAlready == True:
                                if isUnique == True:
                                    raise Exception("Unique Index key already exists ,cannot insert")
                                else:
                                    #update key
                                    myquery=collection1.find_one({"pk": pk1})
                                    filter = {'pk': pk1}
                                    # Values to be updated.
                                    newvalues = {"$set": {'value': myquery["value"]+value1}}
                                    collection1.update_one(filter, newvalues)
                            else:
                                collection1.insert_one(tr1)
                                response.append("\nSuccessfully inserted index row :" + pk)
                        # TODO insert for indexes ===========================================================
                        collection.insert_one(tr)
                        response.append("Successfully inserted table row :" + pk)

                    except Exception as e:
                        response.append("Error:" + str(e))
        return "\n".join(response)

    def delete(self, dataBaseName, dataJson):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        table = getTable(myRoot, dataBaseName, dataJson)
        if table:
            structure = table.find("Structure")
            attributes = structure.findall("Attribute")
            primaryKeys = table.find("PrimaryKey")
            primaryKeyAttributes = primaryKeys.findall("pkAttribute")
            checkAttributeNamesDelete(attributes, dataJson)
            checkPrimaryKeyForDelete(primaryKeyAttributes, dataJson)
            testAttributeTypesForDelete(attributes, dataJson)
            collection = self.myDb.get_collection(dataJson["tableName"])
            if collection is None:
                raise Exception("Collection not found in db")
            checkPrimaryKeyDoesNotExist(collection, dataJson)

            #TODO CHECK FOR FOREIGN KEYS==================================
            #NU MAI E VALABIL checkForeignKeyForDeleteRow(myRoot, dataBaseName, dataJson, self.myDb)
            checkForeignKeyForDelete(dataBaseName,dataJson,self.myDb,primaryKeyAttributes)

            # TODO CHECK FOR FOREIGN KEYS==================================

            collection.delete_one({"pk": dataJson["primaryKeyValue"]})
            #TODO delete from indexes values
            indexList = getIndexFiles(dataBaseName, dataJson["tableName"])
            for index in indexList:
                collection1 = self.myDb.get_collection(index)
                #indexAtrib = getIndexAttributes(dataBaseName, index)
                #pk1 = joinAttributeIndex(indexAtrib, dataJson, 0)
                #existsAlready = checkIfIndexKeyExists(collection1, {"pk": pk1})
                #TODO de sters atunci cand index key contine valori multiple
                #collection1.foo.find({"value": { $regex: dataJson["primaryKeyValue"]}}, {_id: 0})
                collection1.delete_one({"value": dataJson["primaryKeyValue"]})
                toBeDeleted=collection1.find({"value": {"$regex": dataJson["primaryKeyValue"]}})
                for row in toBeDeleted:
                    newv=row["value"]
                    # Values to be updated.
                    newvalue = {"$set": {'value': newv.replace(dataJson["primaryKeyValue"],"")}}
                    collection1.update_one(row,newvalue)

            return "Successfully removed table row"



