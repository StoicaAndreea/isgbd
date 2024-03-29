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
        elif re.search("^(CREATE\s(UNIQUE\s)?INDEX)\s\w+\sON\s\w+\s*\(\s*\w+\s*(\s*,\s*\w+\s*)*\s*\);$", expression,
                       re.IGNORECASE):
            return self.createIndexWithNameGiven(expression)
        elif self.CREATE_INDEX_COMMAND in exp:
            return self.createIndex(expression)
        elif self.DROP_INDEX_COMMAND in exp:
            return self.dropIndex(expression)
        elif self.INSERT_COMMAND in exp:
            return self.insertRow(expression)
        elif self.DELETE_COMMAND in exp:
            return self.deleteRow(expression)
        elif re.search(
                'SELECT',
                expression, re.IGNORECASE):
            return self.createSelect(expression)
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
            unique = 1
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

    def createSelect(self, expression):
        tableNames = []
        tableAliases = []
        columns = {}
        distinct = 0
        conditions = []
        joins = []
        if re.search("DISTINCT", expression, re.IGNORECASE):
            distinct = 1

        tableNamesWithAlias = re.search(
            'FROM\s(\w+(\s(AS)\s\w+)?)((\s*,\s(\w+(\s(AS)\s\w+)?))*)(\s((INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN)|(WHERE)))',
            expression,
            re.IGNORECASE).group()
        if not tableNamesWithAlias:
            raise Exception("could not find table name")
        tableNamesWithAlias = tableNamesWithAlias.replace("FROM", "").replace("from", "").replace("inner join",
                                                                                                  "").replace(
            "INNER JOIN", "").replace("outer join", "").replace("OUTER JOIN", "").replace("left join", "").replace(
            "LEFT JOIN", "").replace("right join", "").replace("RIGHT JOIN", "").replace("WHERE", '').replace("where",
                                                                                                              '').strip()
        tableNamesWithAlias = tableNamesWithAlias.split(",")
        for tableNameWithAlias in tableNamesWithAlias:
            if re.search(' AS ', tableNameWithAlias, re.IGNORECASE):
                tna = re.split("AS", tableNameWithAlias, flags=re.IGNORECASE)
                if tna[1].strip() in tableAliases:
                    raise Exception('cannot use the same alias for two tables')
                tableAliases.append(tna[1].strip())
                tableNames.append(tna[0].strip())
            else:
                if '-' in tableAliases:
                    raise Exception('you need to use aliases')
                tableAliases.append('-')
                tableNames.append(tableNameWithAlias)

        # todo: lab 5 table names and aliases from joins
        if re.search('(INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN)', expression, re.IGNORECASE):
            joinList = re.search('((INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN))(.*)WHERE', expression,
                                 re.IGNORECASE).group().replace("WHERE", '').replace("where", '').strip()
            joinList = joinList.split(",")
            for i, joinItem in enumerate(joinList):
                if re.search('AS \w+ ON', joinItem, re.IGNORECASE):
                    tableName = re.search('((INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN)) (.*) AS', joinItem,
                                          re.IGNORECASE).group().replace("inner join", "").replace("INNER JOIN", "") \
                        .replace("outer join", "").replace("OUTER JOIN", "").replace("left join", "").replace(
                        "LEFT JOIN", "").replace("right join", "").replace("RIGHT JOIN", "").replace("as", '').replace(
                        "AS", '').strip()
                    alias = re.search("AS (.*) ON", joinItem, flags=re.IGNORECASE).group().replace("on", "").replace("ON", '').replace("as", "").replace("AS", '').strip()
                    if alias in tableAliases:
                        raise Exception('cannot use the same alias for two tables')
                    tableAliases.append(alias)
                    tableNames.append(tableName)
                    joinType = re.search('(INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN)', joinItem, re.IGNORECASE).group().strip().upper()
                    condition = re.search('ON(.*)', joinItem, re.IGNORECASE).group().replace("on", '').replace("ON", '').strip()
                    joins.append({
                        "join": joinType,
                        "table": tableName,
                        "alias": alias,
                        "condition": condition
                    })
                else:
                    if '-' in tableAliases:
                        raise Exception('you need to use aliases')
                    tableAliases.append('-')
                    tableName = re.search('(INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN) (.*) ON', joinItem,
                                          re.IGNORECASE).group().replace("inner join", "").replace("INNER JOIN", "") \
                        .replace("outer join", "").replace("OUTER JOIN", "").replace("left join", "").replace(
                        "LEFT JOIN", "").replace("right join", "").replace("RIGHT JOIN", "").replace("on", '').replace(
                        "ON", '').strip()
                    tableNames.append(tableName)
                    joinType = re.search('(INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN)', joinItem, re.IGNORECASE).group().strip().upper()
                    condition = re.search('ON(.*)', joinItem, re.IGNORECASE).group().replace("on", '').replace("ON", '').strip()
                    joins.append({
                        "join": joinType,
                        "table1": tableNames[0], #initial
                        "table2": tableName, #joined
                        "alias": '-',
                        "condition": condition
                    })
        columnNames = re.search('SELECT(.*)FROM', expression, re.IGNORECASE)
        columnNames = columnNames.group(1).split(",")
        for columnName in columnNames:
            columnName = columnName.replace("DISTINCT", "").replace("Distinct", "").replace("distinct", "").strip()
            if re.search('\.', columnName, re.IGNORECASE):
                colnamewa = columnName.split(".")
                if colnamewa[0] in tableAliases:
                    if colnamewa[0] in columns:
                        columns[colnamewa[0]].append(colnamewa[1])
                    else:
                        columns[colnamewa[0]] = [colnamewa[1]]
                else:
                    raise Exception('unknown alias used for column name')
            else:
                if '-' in columns:
                    columns['-'].append(columnName)
                else:
                    columns['-'] = [columnName]
        for tal in tableAliases:
            if tal not in columns:
                columns[tal] = []

        conditionList = re.search('WHERE(.*);', expression, re.IGNORECASE).group().replace("where", "").replace("WHERE",
                                                                                                                "").replace(
            ";", "").strip()
        conditions = re.split("AND", conditionList, flags=re.IGNORECASE)

        x = {
            "command": 9,
            "tableNames": tableNames,
            "tableAliases": tableAliases,
            "columns": columns,
            "distinct": distinct,
            "conditions": conditions,
            "joins": joins,
            "expression":expression
        }
        response = json.dumps(x)
        return response

# index
# select a.t1id, a.sunet, a.pozitie, b.t2id, b.row, b.column from table1 as a inner join table2 as b on b.row = a.sunet where a.pozitie > 1;
# select a.t1id, a.sunet, a.pozitie, b.t2id, b.row, b.column from table1 as a inner join table2 as b on b.row = a.sunet where a.pozitie < 6;
# select a.t1id, a.sunet, a.pozitie, b.t2id, b.row, b.column from table1 as a inner join table2 as b on b.row = a.sunet where b.row = "12";


# select a.t1id, a.sunet, a.pozitie, b.t2id, b.row, b.column from table1 as a inner join table2 as b on b.column = a.pozitie where a.pozitie > 1;
# select a.t1id, a.sunet, a.pozitie from table1 as a inner join table2 as b on b.row = a.sunet where a.pozitie > 1;
# select a.t1id, a.sunet, a.pozitie, b.t2id, b.row, b.column from table1 as a inner join table2 as b on b.row = a.sunet where a.pozitie > 1;
# select distinct c.t1id, c.sunet, c.pozitie from table1 as c where c.pozitie = 2 and c.sunet = "a";
# select distinct t1id, sunet, pozitie from table1 where pozitie = 2 and sunet = "a";
# select c.t1id, c.sunet, c.pozitie from table1 as c where c.pozitie = 2 and c.sunet = "a";
# select c.t1id, c.sunet, c.pozitie, a.row from table1 as c, table2 as a where c.pozitie = 2 and c.sunet = "a" and a.row = "1";
# select t1id, sunet, pozitie from table1 where sunet = "a" and pozitie = 2;
# select t1id, sunet, pozitie from table1 where pozitie = 2;
# select sunet from table1 where sunet = "a" and pozitie = 2;
# select sunet from table1 where sunet = "a" and pozitie = 2 and t1id = 1;
if __name__ == '__main__':
    # TODO poti sa rulezi main ca sa vezi cum ar arata JSON-ul pt createSelect si la lab 5 mai trebuie adaugat in x numai partea de join

    converter = DataConverter()
    converter.createSelect(
        'SELECT DISTINCT c.CustomerName, b.City FROM Customers as c , animals inner join ana as a on a.c = b.c, outer join mama as b on a.c = b.c WHERE c.a BEtween (1,2) and c.a < 3 and c.c in [1,1] and d.c between (1,2);')
