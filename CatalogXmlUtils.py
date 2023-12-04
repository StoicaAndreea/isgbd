from xml.etree import ElementTree as ET


def searchKeyLength(databaseName, tablename, columname):
    with open('Catalog.xml', 'r'):
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


def searchKeyUnique(databaseName, tablename, columname):
    with open('Catalog.xml', 'r'):
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


def checkIndexName(databaseName, indexName):
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    indexFile = tb.find("IndexFiles")
                    for idf in indexFile.iter("IndexFile"):
                        if idf.attrib["indexName"] == indexName + ".ind":
                            return True
    return False


def checkIfIndexUnique(databaseName, tableName, indexName):
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        indexFile = tb.find("IndexFiles")
                        for idf in indexFile.iter("IndexFile"):
                            if idf.attrib['indexName'] == indexName:
                                if idf.attrib['isUnique'] == '1':
                                    return True
                                else:
                                    return False


def checkForForeignKeys(databaseName, tableName):
    foreignKeyList = []
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        fKeys = tb.find("ForeignKeys")
                        for fkey in fKeys.iter("ForeignKey"):
                            atrib = []
                            for fatrib in fkey.iter("fkAttribute"):
                                atrib.append(fatrib.text)
                            reff = fkey.find("references")
                            refTable = reff.find("refTable").text
                            refAttributes = []
                            for refat in reff.iter("refAttribute"):
                                refAttributes.append(refat.text)
                            y = {"fkAttribute": atrib, "refTable": refTable, "refAttribute": refAttributes}
                            foreignKeyList.append(y)
    return foreignKeyList


def getIndexAttributes(databaseName, indexName):
    attributeList = []
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    indexFile = tb.find("IndexFiles")
                    for idf in indexFile.iter("IndexFile"):
                        if idf.attrib["indexName"] == indexName:
                            indexAtrib = idf.find("IndexAttributes")
                            for atrib in indexAtrib.iter("IAttribute"):
                                attributeList.append(atrib.text)
    return attributeList


def getIndexFiles(databaseName, tableName):
    indexesList = []
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        indexFile = tb.find("IndexFiles")
                        for idf in indexFile.iter("IndexFile"):
                            indexesList.append(idf.attrib['indexName'])
    return indexesList


def getIndexAttributesAndFiles(databaseName, tableName):
    indexesList = []
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        indexFile = tb.find("IndexFiles")
                        for idf in indexFile.iter("IndexFile"):
                            a = []
                            for att in idf.find("IndexAttributes").iter("IAttribute"):
                                a.append(att.text)
                            indexesList.append({'fileName': idf.attrib['indexName'], 'attributes': a})
                        break
    return indexesList


def getTableAttributes(databaseName, tableName):
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        structure = tb.find("Structure")
                        return structure.findall("Attribute")
    return []


def getTablePrimaryKeys(databaseName, tableName):
    with open('Catalog.xml', 'r'):
        myTree = ET.parse('Catalog.xml')
        myRoot = myTree.getroot()
        for db in myRoot.iter("Database"):
            if db.attrib['dataBaseName'] == databaseName:
                tables = db.find("Tables")
                for tb in tables.iter("Table"):
                    if tb.attrib['tableName'] == tableName:
                        primaryKeys = tb.find("PrimaryKey")
                        return primaryKeys.findall("pkAttribute")
    return []


def getAttributeListWithoutPk(attributeList, pkList):
    attributeList = list(
        map(lambda attribute: attribute.attrib["attributeName"], attributeList))
    pkList = list(map(lambda pkItem: pkItem.text, pkList))
    attributes = []
    for att in attributeList:
        if att not in pkList:
            attributes.append(att)
    return attributes
