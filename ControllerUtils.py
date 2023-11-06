import re
import datetime
from xml.etree import ElementTree as ET

# insert
from AttributeTypeEnum import AttributeTypeEnum


def getTable(myRoot, dataBaseName, dataJson):
    for db in myRoot.iter("Database"):
        if db.attrib['dataBaseName'] == dataBaseName:
            tables = db.find("Tables")
            for tb in tables.iter("Table"):
                if tb.attrib['tableName'] == dataJson["tableName"]:
                    return tb
            raise Exception("Could not find table")
    raise Exception("could not find database")


def checkAttributeNames(attributes, dataJson):
    result = ""
    for val in dataJson["values"]:
        found = False
        for attribute in attributes:
            if attribute.attrib["attributeName"] in val.keys():
                found = True
                break
        if not found:
            raise Exception("Attribute does not exist in table schema")
    return result


def checkPrimaryKeyForInsert(primaryKeyAttributes, dataJson):
    for pk in primaryKeyAttributes:
        found = False
        for att in dataJson["values"]:
            if pk.text in att:
                found = True
                break
        if not found:
            raise Exception("Missing primary key " + pk.text)


def joinAttributePrimaryKeys(primaryKeyAttributes, dataJson, i):
    result = ""
    for attribute in primaryKeyAttributes:
        for val in dataJson["values"]:
            if attribute.text in val.keys():
                result = result + "#" + val[attribute.text][i]
                break
    return result


def checkPrimaryKeyAlreadyExistsInDb(primaryKeyAttributes, collection, dataJson, i):
    pkval = joinAttributePrimaryKeys(primaryKeyAttributes, dataJson, i)
    dbPkValue = list(collection.find({"pk": pkval}))
    if len(dbPkValue) > 0:
        raise Exception("Duplicate primary key for " + pkval)


def joinAttributeValues(attributes, primaryKeyAttributes, dataJson, i):
    result = ""
    atts = []
    for pk in primaryKeyAttributes:
        for attribute in attributes:
            if attribute.attrib["attributeName"] != pk.text:
                atts.append(attribute)
    for attribute in atts:
        found = False
        for val in dataJson["values"]:
            if attribute.attrib["attributeName"] in val.keys():
                found = True
                result = result + "#" + val[attribute.attrib["attributeName"]][i]
                break
        if not found:
            if attribute.attrib["isNull"] == "0":
                raise Exception("Attribute " + attribute.attrib["attributeName"] + " cannot be null")
            result = result + "#null"
    return result


def testAttributeTypes(attributes, dataJson, i):
    for attribute in attributes:
        for val in dataJson["values"]:
            if attribute.attrib["attributeName"] in val.keys():
                testTypeMatches(attribute, val[attribute.attrib["attributeName"]][i])


def testTypeMatches(attribute, value):
    attribLength = attribute.attrib["length"]
    attribType = attribute.attrib["type"]
    nrOfBytes = len(value.encode('utf-8'))
    if attribType.upper() == AttributeTypeEnum.CHAR.value:
        if value[0] != "\"" and value[len(value) - 1] != "\"":
            raise Exception("Char values should be contained in double quotes")
        if attribLength != "-" and nrOfBytes > int(attribLength):
            raise Exception("Type char should have nr of bytes less than " + attribLength)
        elif attribLength != "-" and nrOfBytes > 8000:
            raise Exception("Type char should have nr of bytes less than 8000")
    elif attribType.upper() == AttributeTypeEnum.VARCHAR.value:
        if value[0] != "\"" and value[len(value) - 1] != "\"":
            raise Exception("Varchar values should be contained in double quotes")
        if attribLength != "-" and nrOfBytes > int(attribLength):
            raise Exception("Type char should have nr of bytes less than " + attribLength)
        elif attribLength != "-" and nrOfBytes > 8000:
            raise Exception("Type char should have nr of bytes less than 8000")
    elif attribType.upper() == AttributeTypeEnum.BIT.value:
        try:
            int(value)
        except ValueError:
            raise Exception("Bit value not valid")
        if attribLength != "-" and nrOfBytes != int(attribLength):
            raise Exception("Type varchar should have nr of bytes " + attribLength)
        elif attribLength != "-" and nrOfBytes > 64:
            raise Exception("Type varchar should have nr of bytes exactly 64")
    elif attribType.upper() == AttributeTypeEnum.BOOLEAN.value:
        try:
            if int(value) not in [0, 1]:
                raise Exception("For type boolean, values can be 0 or 1")
        except ValueError:
            raise Exception("Boolean not valid")
    elif attribType.upper() == AttributeTypeEnum.INTEGER.value:
        try:
            int(value)
        except ValueError:
            raise Exception("Integer value not valid")
        if -2147483647 > int(value) > 2147483647:
            raise Exception("For type integer, values must be from -2,147,483,647 to 2,147,483,647")
        if attribLength != "-" and nrOfBytes > int(attribLength):
            raise Exception("Type integer should have nr of bytes less than" + attribLength)
    elif attribType.upper() == AttributeTypeEnum.DOUBLE.value:
        if not re.search("(\d+\.\d+)", value, re.IGNORECASE):
            raise Exception("Type double should have format 'dd.dd'")
    elif attribType.upper() == AttributeTypeEnum.DATE.value:
        if not re.search("^\"(0[1-9]|[1-2][0-9]|3[0-1])-(0[1-9]|1[0-2])-(\d{4})\"$", value, re.IGNORECASE):
            raise Exception("Type date should have format 'dd-mm-yyy'")
        try:
            date = value.replace("\"", "").split("-")
            datetime.datetime(int(date[2]), int(date[1]), int(date[0]))
        except ValueError:
            raise Exception("Date not valid")
    elif attribType.upper() == AttributeTypeEnum.TIME.value:
        if not re.search("^\"([01][0-9]|2[0-3]):([0-5][0-9])\"$", value, re.IGNORECASE):
            raise Exception("Type time should have format 'hh:mm'")
    elif attribType.upper() == AttributeTypeEnum.DATETIME.value:
        if not re.search("^\"(0[1-9]|[1-2][0-9]|3[0-1])-(0[1-9]|1[0-2])-(\d{4}) ([01][0-9]|2[0-3]):([0-5][0-9])\"$",
                         value,
                         re.IGNORECASE):
            raise Exception("Type datetime should have format 'dd-mm-yyy hh:mm'")
        try:
            datetimeval = value.replace("\"", "").split(" ")
            date = datetimeval[0].split("-")
            time = date[1].split(":")
            datetime.datetime(int(date[2]), int(date[1]), int(date[0]), int(time[0]), int(time[1]))
        except ValueError:
            raise Exception("DateTime not valid")
    else:
        raise Exception("type not okay")


# delete
def checkAttributeNamesDelete(attributes, dataJson):
    names = list(filter(lambda att: att not in ["", " "], dataJson["primaryKey"].split("#")))
    for val in names:
        found = False
        for attribute in attributes:
            if attribute.attrib["attributeName"] in val:
                found = True
                break
        if not found:
            raise Exception("Attribute does not exist in table schema")


def checkPrimaryKeyForDelete(primaryKeyAttributes, dataJson):
    names = list(filter(lambda att: att not in ["", " "], dataJson["primaryKey"].split("#")))
    for pk in primaryKeyAttributes:
        found = False
        for att in names:
            if pk.text in att:
                found = True
                break
        if not found:
            raise Exception("Missing primary key " + pk.text)


def testAttributeTypesForDelete(attributes, dataJson):
    values = list(filter(lambda att: att not in ["", " "], dataJson["primaryKeyValue"].split("#")))
    pks = list(filter(lambda att: att not in ["", " "], dataJson["primaryKey"].split("#")))
    for attribute in attributes:
        for i in range(len(values)):
            if attribute.attrib["attributeName"] == pks[i]:
                testTypeMatches(attribute, values[i])


def checkForeignKeyForDeleteRow(myRoot, databaseName, dataJson, myDb):
    for db in myRoot.iter("Database"):
        if db.attrib['dataBaseName'] == databaseName:
            refs = []
            # search for fk
            tables = db.find("Tables")
            for tb in tables.iter("Table"):
                fks = tb.find("ForeignKeys")
                for fk in fks.iter("ForeignKey"):
                    rfr = fk.find("references")
                    tbl = rfr.find("refTable")
                    if tbl is not None and tbl.text == dataJson["tableName"]:
                        refs.append(tb)
            for ref in refs:
                col = myDb.get_collection(ref.attrib["tableName"])
                findings = col.find_one({"value": {'$regex': dataJson["primaryKeyValue"]}})
                if findings is not None:
                    raise Exception("Could not delete row, as it is referenced in table " + ref.attrib["tableName"])


def checkPrimaryKeyDoesNotExist(collection, dataJson):
    dbPkValue = list(collection.find({"pk": dataJson["primaryKeyValue"]}))
    if len(dbPkValue) == 0:
        raise Exception("Key " + dataJson["primaryKeyValue"] + " does not exist in table")


# indexes


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
