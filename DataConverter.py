import json

from AttributeTypeEnum import AttributeTypeEnum


class DataConverter:
    # USE DB - 0
    # CREATE DB - 1
    # DROP DB   - 2
    # CREATE TB - 3
    # DROP TB   - 4
    # CREATE INDEX - 5
    # DROP INDEX - 6

    CREATE_DATABASE_COMMAND = "CREATE DATABASE"
    DROP_DATABASE_COMMAND = "DROP DATABASE"
    USE_DATABASE_COMMAND = "USE DATABASE"
    CREATE_TABLE_COMMAND = "CREATE TABLE"
    DROP_TABLE_COMMAND = "DROP TABLE"
    CREATE_INDEX_COMMAND = "CREATE INDEX"
    DROP_INDEX_COMMAND = "DROP INDEX"
    PRIMARY_KEY_SYNTAX = "PRIMARY KEY"
    NOT_NULL_SYNTAX = "NOT NULL"
    UNIQUE_SYNTAX = "UNIQUE"

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
        elif self.CREATE_INDEX_COMMAND in exp:
            return self.createIndex(expression)
        elif self.DROP_INDEX_COMMAND in exp:
            return self.dropIndex(expression)
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
        for line in lines:
            isPrimaryKey, isNotNull, isUnique = False, False, False
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

            data = list(filter(lambda item: item not in ["", " "], line.split(" ")))
            if len(data) == 2:
                attName = data[0]
                typeEnd = data[1].upper().find("(")
                attType = data[1]
                size = 0
                if typeEnd != -1:
                    attType = data[1][0:typeEnd].strip()
                    size = data[1][typeEnd + 1:-1]
                attTypes = [member.value for member in AttributeTypeEnum]
                if attType.upper() not in attTypes:
                    raise Exception("argument type: ", attType, " not valid")
                attributes.append({
                    "name": attName.strip(),
                    "type": attType.strip(),
                    "size": size.strip(),
                    "primaryKey": isPrimaryKey,
                    "unique": isUnique,
                    "notNull": isNotNull
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
        expContent = list(filter(lambda item: item not in ["", " ", "(", ")"], expression.split(" ")))
        indexName, tableName, columnName = "", "", ""
        if len(expContent) == 2:
            tableName = expContent[0].replace('(', '').strip()
            columnName = expContent[1].replace('(', '').replace(')', '').strip()
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
