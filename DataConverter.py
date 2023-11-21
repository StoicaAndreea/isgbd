import json
import re

from AttributeTypeEnum import AttributeTypeEnum
from AttributeWithSizeTypeEnum import AttributeWithSizeTypeEnum


class DataConverter:
    # USE DB - 0
    # CREATE DB - 1
    # DROP DB   - 2
    # CREATE TB - 3
    # DROP TB   - 4
    # CREATE INDEX - 5
    # DROP INDEX - 6
    # INSERT - 7
    # DELETE - 8

    CREATE_DATABASE_COMMAND = "CREATE DATABASE"
    DROP_DATABASE_COMMAND = "DROP DATABASE"
    USE_DATABASE_COMMAND = "USE DATABASE"
    CREATE_TABLE_COMMAND = "CREATE TABLE"
    DROP_TABLE_COMMAND = "DROP TABLE"
    CREATE_INDEX_COMMAND = "CREATE INDEX"
    CREATE_UNIQUE_INDEX_COMMAND = "CREATE UNIQUE INDEX"
    DROP_INDEX_COMMAND = "DROP INDEX"
    INSERT_COMMAND = "INSERT INTO"
    DELETE_COMMAND = "DELETE FROM"
    VALUES_INSERT_COMMAND = "VALUES"
    PRIMARY_KEY_SYNTAX = "PRIMARY KEY"
    NOT_NULL_SYNTAX = "NOT NULL"
    UNIQUE_SYNTAX = "UNIQUE"
    REFERENCES_SYNTAX = "REFERENCES"
    WHERE_SYNTAX = "WHERE"

    def __init__(self):
        pass

    def parseExpression(self, expression):
        exp = expression.upper()
        if self.CREATE_DATABASE_COMMAND in exp:
            return self.createDatabase(expression)
        elif self.DROP_DATABASE_COMMAND in exp:
            return self.dropDatabase(expression)
        elif self.USE_DATABASE_COMMAND in exp:
            return self.useDatabase(expression)
        elif self.CREATE_TABLE_COMMAND in exp:
            return self.createTable(expression)
        elif self.DROP_TABLE_COMMAND in exp:
            return self.dropTable(expression)
        elif re.search("^(CREATE\s(UNIQUE\s)?INDEX)\s\w+\sON\s\w+\s*\(\s*\w+\s*(\s*,\s*\w+\s*)*\s*\);$", expression, re.IGNORECASE):
             return self.createIndexWithNameGiven(expression)
        elif self.CREATE_INDEX_COMMAND in exp:
            return self.createIndex(expression)
        elif self.DROP_INDEX_COMMAND in exp:
            return self.dropIndex(expression)
        elif self.INSERT_COMMAND in exp:
            return self.insertRow(expression)
        elif self.DELETE_COMMAND in exp:
            return self.deleteRow(expression)
        else:
            raise Exception("Unknown command")

    def createDatabase(self, expression):
        endChar = expression.upper().index(";")
        databaseName = expression[len(self.CREATE_DATABASE_COMMAND) + 1: endChar]
        x = {
            "command": 1,
            "databaseName": databaseName
        }
        # convert into JSON:
        return json.dumps(x)

    def dropDatabase(self, expression):
        endChar = expression.upper().index(";")
        databaseName = expression[len(self.DROP_DATABASE_COMMAND) + 1: endChar]
        x = {
            "command": 2,
            "databaseName": databaseName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def useDatabase(self, expression):
        endChar = expression.upper().index(";")
        databaseName = expression[len(self.USE_DATABASE_COMMAND) + 1: endChar]
        x = {
            "command": 0,
            "databaseName": databaseName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def createTable(self, expression):
        expression = expression.replace(self.CREATE_TABLE_COMMAND, "") \
            .replace(self.CREATE_TABLE_COMMAND.lower(), "").replace(";", "").strip()
        endName = expression.upper().index("(")
        tableName = expression[0:endName].strip()
        expression = expression[endName + 1:-1]

        attributes = []
        lines = list(filter(lambda item: item not in [",", "", " "], expression.split(",")))
        names = []
        for line in lines:
            isPrimaryKey, isNotNull, isUnique, isForeignKey = False, False, False, False
            line = line.strip()
            primaryKeyIndex = line.upper().find(self.PRIMARY_KEY_SYNTAX)
            if primaryKeyIndex != -1:
                isPrimaryKey = True
                line = line.replace(self.PRIMARY_KEY_SYNTAX, "").replace(self.PRIMARY_KEY_SYNTAX.lower(), "").strip()

            notNullIndex = line.upper().find(self.NOT_NULL_SYNTAX)
            if notNullIndex != -1:
                isNotNull = True
                line = line.replace(self.NOT_NULL_SYNTAX, "").replace(self.NOT_NULL_SYNTAX.lower(), "").strip()

            isUniqueIndex = line.upper().find(self.UNIQUE_SYNTAX)
            if isUniqueIndex != -1:
                isUnique = True
                line = line.replace(self.UNIQUE_SYNTAX, "").replace(self.UNIQUE_SYNTAX.lower(), "").strip()

            isForeignKeyIndex = line.upper().find(self.REFERENCES_SYNTAX)
            foreignKey = {
                "tableName": "",
                "columnName": ""
            }
            if isForeignKeyIndex != -1:
                if isPrimaryKey:
                    raise Exception("Cannot have foreign key and primary key at the same time")
                fk = line[isForeignKeyIndex:]
                fk = fk.replace(self.REFERENCES_SYNTAX, "").replace(self.REFERENCES_SYNTAX.lower(), "").strip()
                fk = list(filter(lambda item: item not in [",", "", " "], fk.split(" ")))
                if len(fk) == 2:
                    foreignKey = {
                        "tableName": fk[0].strip(),
                        "columnName": fk[1].replace("(", "").replace(")", "").strip()
                    }
                else:
                    raise Exception("Foreign key not properly declared")
                line = line[0:isForeignKeyIndex].strip()

            data = list(filter(lambda item: item not in ["", " "], line.split(" ")))
            if len(data) == 2:
                attName = data[0]
                if attName in names:
                    raise Exception("Attribute names need to be unique")
                else:
                    names.append(attName)
                typeEnd = data[1].upper().find("(")
                attType = data[1]
                size = "-"
                if typeEnd != -1:
                    attType = data[1][0:typeEnd].strip()
                    size = data[1][typeEnd + 1:-1]
                attTypes = [member.value for member in AttributeTypeEnum]
                attWithSizeTypes = [member.value for member in AttributeWithSizeTypeEnum]
                if attType.upper() not in attTypes:
                    raise Exception("argument type: " + attType + " not valid")
                if size != "-" and attType.upper() not in attWithSizeTypes:
                    raise Exception("argument type: " + attType + " should not have a size defined")
                attributes.append({
                    "name": attName,
                    "type": attType,
                    "size": size,
                    "primaryKey": isPrimaryKey,
                    "unique": isUnique,
                    "notNull": isNotNull,
                    "foreignKey": foreignKey
                })
            else:
                raise Exception("create table syntax not valid")

        x = {
            "command": 3,
            "tableName": tableName,
            "attributes": attributes
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def dropTable(self, expression):
        endChar = expression.upper().index(";")
        tableName = expression[len(self.DROP_TABLE_COMMAND) + 1: endChar]
        x = {
            "command": 4,
            "tableName": tableName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def createIndex(self, expression):
        expression = expression.replace(self.CREATE_INDEX_COMMAND, "") \
            .replace(self.CREATE_INDEX_COMMAND.lower(), "").replace(";", "")
        expContent = list(filter(lambda item: item not in ["", " ", "(", ")", ","], expression.split(" ")))
        indexName, tableName = "", ""
        columnName = []
        if len(expContent) > 1:
            tableName = expContent[0].replace('(', '').strip()
            columnName.append(expContent[1].replace('(', '').replace(')', '').strip())
            i = 2
            while i != len(expContent):
                columnName.append(expContent[i].replace('(', '').replace(')', '').strip())
                i = i + 1
                if i == len(expContent):
                    break
        else:
            raise Exception("create index syntax not valid")
        x = {
            "command": 5,
            "tableName": tableName,
            "columnName": columnName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def dropIndex(self, expression):
        endChar = expression.upper().index(";")
        indexName = expression[len(self.DROP_INDEX_COMMAND) + 1: endChar]
        x = {
            "command": 6,
            "indexName": indexName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def createIndexWithNameGiven(self, expression):
        unique = 0
        if re.search("UNIQUE", expression, re.IGNORECASE):
            unique=1
        if unique == 0:
            expression = expression.replace(self.CREATE_INDEX_COMMAND, "") \
                .replace(self.CREATE_INDEX_COMMAND.lower(), "").replace(";", "")
        else:
            expression = expression.replace(self.CREATE_UNIQUE_INDEX_COMMAND, "") \
                .replace(self.CREATE_UNIQUE_INDEX_COMMAND.lower(), "").replace(";", "")
        expContent = list(filter(lambda item: item not in ["", " ", "(", ")", "ON", "on", ","], expression.split(" ")))
        indexName, tableName = "", ""

        columnName = []
        if len(expContent) > 2:
            indexName = expContent[0].replace('(', '').strip()
            tableName = expContent[1].replace('(', '').strip()
            columnName.append(expContent[2].replace('(', '').replace(')', '').strip())
            i = 3
            while i != len(expContent):
                columnName.append(expContent[i].replace('(', '').replace(')', '').strip())
                i = i + 1
                if i == len(expContent):
                    break
        else:
            raise Exception("create index syntax not valid")

        x = {
            "command": 55,
            "tableName": tableName,
            "indexName": indexName,
            "columnName": columnName,
            "unique": unique
        }

        # convert into JSON:
        response = json.dumps(x)
        return response

    def insertRow(self, expression):
        expression = expression.replace(self.INSERT_COMMAND, "") \
            .replace(self.INSERT_COMMAND.lower(), "").replace(";", "").strip()
        endChar = expression.index("(")
        tableName = expression[:endChar].strip()
        expression = expression[endChar:].strip()
        whereIndex = expression.upper().index(self.VALUES_INSERT_COMMAND)
        attributes = expression[:whereIndex].replace("(", "").replace(")", "").strip().split(",")
        attributes = list(map(lambda att: att.strip(), attributes))

        values = expression[whereIndex + len(self.VALUES_INSERT_COMMAND):].strip().split(", (")
        values = list(map(lambda val: val.replace(")", "").replace("(", "").strip(), values))

        attVal = []
        for i in range(len(attributes)):
            attVal.append({attributes[i]: []})
        for val in values:
            vals = list(map(lambda att: att.strip(), val.split(",")))
            if len(attributes) != len(vals):
                raise Exception("The number of attributes and values do not match")
            for i in range(len(attributes)):
                attVal[i][attributes[i]].append(vals[i])
        x = {
            "command": 7,
            "tableName": tableName,
            "values": attVal
        }
        # convert into JSON:
        response = json.dumps(x)
        return response

    def deleteRow(self, expression):
        expression = expression.replace(self.DELETE_COMMAND, "") \
            .replace(self.DELETE_COMMAND.lower(), "").replace(";", "").strip()
        endChar = expression.upper().index(self.WHERE_SYNTAX)
        tableName = expression[:endChar].strip()
        expression = expression[endChar:].strip()
        expression = expression.replace(self.WHERE_SYNTAX, "") \
            .replace(self.WHERE_SYNTAX.lower(), "").replace("(", "").replace(")", "").strip()
        pks = list(
            filter(lambda item: item not in ["", " ", "and", "AND"], re.split("AND", expression, flags=re.IGNORECASE)))
        primaryKey = ""
        primaryKeyVal = ""
        for pk in pks:
            attributes = list(map(lambda att: att.strip(), pk.strip().split("=")))
            primaryKey = primaryKey + "#" + attributes[0]
            primaryKeyVal = primaryKeyVal + "#" + attributes[1]

        x = {
            "command": 8,
            "tableName": tableName,
            "primaryKey": primaryKey,
            "primaryKeyValue": primaryKeyVal
        }
        # convert into JSON:
        response = json.dumps(x)
        return response
