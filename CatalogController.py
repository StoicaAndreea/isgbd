import json
from xml.etree import ElementTree as ET
import os





class CatalogController:
    def __init__(self):
        # header: et.write(f, encoding='utf-8', xml_declaration=True)
        if os.path.exists('Catalog.xml'):
            pass
        else:
            databases = ET.Element('Databases')
            tree = ET.ElementTree(databases)
            tree.write('Catalog.xml')

    def createDatabase(self, name):
        with open('Catalog.xml', 'r') as f:
            doc = ET.parse(f)
            for elem in doc.iter("Database"):
                if elem.attrib['dataBaseName'] == name:
                    return "The database already exists"
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            database = ET.SubElement(myRoot, 'Database', dataBaseName=name)
            tables = ET.SubElement(database, 'Tables')
            ET.dump(tables)
            myTree.write('Catalog.xml')
        return "Database was created"

    def dropDatabase(self, name):
        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myTree.iter("Database"):
                if db.attrib['dataBaseName'] == name:
                    myRoot.remove(db)
                    myTree = ET.ElementTree(myRoot)
                    myTree.write('Catalog.xml')
                    return "Database dropped successfully"
        return "Database could not be found"

    def createTable(self, dataBaseName, dataJson):
        with open('Catalog.xml', 'r') as f:
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
                    indexFiles = ET.SubElement(table, "IndexFiles")
                    columnName="-"
                    for attribute in dataJson["attributes"]:
                        attb = ET.SubElement(structure, 'Attribute', attributeName=attribute["name"],
                                             type=attribute["type"], length=attribute["size"],
                                             isNull=("0" if attribute["notNull"] else "1"))
                        if attribute["primaryKey"]:
                            pk = ET.SubElement(primaryKeys, "pkAttribute")
                            pk.text = attribute["name"]
                            columnName=attribute["name"]
                        if attribute["unique"]:
                            unq = ET.SubElement(uniqueKeys, "uniqueAttribute")
                            unq.text = attribute["name"]
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
                    myTree = ET.ElementTree(myRoot)
                    myTree.write('Catalog.xml')
                    #===== index pt cheia primara
                    if columnName != "-":
                        x = {
                            "command": 5,
                            "tableName": dataJson["tableName"],
                            "columnName": columnName
                        }
                        # convert into JSON:
                        response = json.dumps(x)
                        self.createIndex(dataBaseName, x)
                    #=========
                    return "Successfully created table"
        return "Could not find database"

    def dropTable(self, databaseName, name):
        with open('Catalog.xml', 'r') as f:
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
                            return "Table dropped successfully"
        return "Table could not be found be found"

    def createIndex(self, databaseName, dataJson):
        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == dataJson["tableName"]:
                            indexName=databaseName+dataJson["tableName"]+dataJson["columnName"]+".ind"
                            indexFile = tb.find("IndexFiles")
                            indx = ET.SubElement(indexFile, 'IndexFile', indexName=indexName,
                                                 keylength=self.searchKeyLength(databaseName, dataJson["tableName"], dataJson["columnName"]),
                                                 isUnique=self.searchKeyUnique(databaseName, dataJson["tableName"], dataJson["columnName"]),
                                                 indexType="BTree")
                            indatrib=ET.SubElement(indx, 'IndexAttributes')
                            iatrib=ET.SubElement(indatrib, 'IAttribute')
                            iatrib.text = dataJson["columnName"]
                            myTree = ET.ElementTree(myRoot)
                            myTree.write('Catalog.xml')

                            f = open(indexName, "w")
                            f.write("indexfile")
                            f.close()

                            return "Successfully created index with name "+indexName
            return "Could not find database"

    def searchKeyLength(self,databaseName, tablename, columname):

        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == tablename:
                            structure = tb.find("Structure")
                            for at in structure.iter("Attribute"):
                                if at.attrib['attributeName'] == columname:
                                    return at.attrib['length']
        return "-"

    def searchKeyUnique(self,databaseName, tablename, columname):
        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == tablename:
                            uniq = tb.find("UniqueKeys")
                            for at in uniq.iter("uniqueAttribute"):
                                if at.text == columname:
                                    return "1"
                            return "0"
        return "0"
    def dropIndex(self, databaseName,dataJson):

        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        indexFile = tb.find("IndexFiles")
                        for idf in indexFile.iter("IndexFile"):
                            if idf.attrib["indexName"] == dataJson["indexName"]+".ind":
                                indexFile.remove(idf)
                                myTree = ET.ElementTree(myRoot)
                                myTree.write('Catalog.xml')

                                os.remove(dataJson["indexName"]+".ind")

                                return "Index dropped successfully"
                        return "could not find index"
        return "index could not be removed"


