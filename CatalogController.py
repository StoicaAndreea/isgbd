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
                            return "tabelul exista deja"
                    # myTree = ET.parse('Catalog.xml')
                    # myRoot = db.getparent()

                    table = ET.SubElement(tables, 'Table', tableName=dataJson["tableName"],
                                          fileName=dataJson["tableName"] + ".bin", rowLength="0")
                    structure = ET.SubElement(table, 'Structure')
                    # todo for in care parcurc si adaug atributele
                    primaryKeys = ET.SubElement(table, 'primaryKey')
                    uniqueKeys = ET.SubElement(table, "uniqueKeys")
                    indexFiles = ET.SubElement(table, "IndexFiles")
                    myTree = ET.ElementTree(myRoot)
                    myTree.write('Catalog.xml')
                    return "Successfully created table"
        return "Could not find database"

    def dropTable(self, databaseName, name):
        with open('Catalog.xml', 'r') as f:
            myTree = ET.parse('Catalog.xml')
            myRoot = myTree.getroot()
            for db in myRoot.iter("Database"):
                if db.attrib['dataBaseName'] == databaseName:
                    tables = db.find("Tables")
                    for tb in tables.iter("Table"):
                        if tb.attrib['tableName'] == name:
                            # myRoot = db.getparent()
                            tables.remove(tb)
                            print(myRoot)
                            # ET.dump(parent)

                            myTree = ET.ElementTree(myRoot)
                            myTree.write('Catalog.xml')
                            return "Table dropped successfully"
        return "Table could not be found be found"

    def createIndex(self, dataJson):
        pass

    def dropIndex(self, dataJson):
        pass
