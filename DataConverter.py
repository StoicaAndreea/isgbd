import json
import re


class DataConverter:
    # USE DB - 0
    # CREATE DB - 1
    # DROP DB   - 2
    # CREATE TB - 3
    # DROP TB   - 4
    # CREATE INDEX - 5

    CREATE_DATABASE_COMMAND = "CREATE DATABASE"
    DROP_DATABASE_COMMAND = "DROP DATABASE"
    USE_DATABASE_COMMAND = "USE DATABASE"
    CREATE_TABLE_COMMAND = "CREATE TABLE"
    DROP_TABLE_COMMAND = "DROP TABLE"
    CREATE_INDEX_COMMAND = "CREATE INDEX"

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
        else:
            print("UNKNOWN COMMAND")
            return False

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


    # {
    #     command:
    #     tableName:
    #     attributes: [
    #         {
    #             name = ""
    #             type = AttributeTypeEnum.VARCHAR
    #             size = ""
    #             primaryKey = False
    #         }
    #         {
    #             name = ""
    #         type = AttributeTypeEnum.VARCHAR
    #         size = ""
    #         primaryKey = False
    #         }
    #     ]
    #     indexes: [
    #         {
    #           name
    #            pkNAme:
    #         }
    #     ]
    # }
    def createTable(self, expression):
        endChar = expression.upper().index("(")
        tableName = expression[len(self.USE_DATABASE_COMMAND) + 1: endChar].strip()

        attributes = []

        x = {
            "command": 3,
            "tableName": tableName,
            "arrtributes":[]
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
        endChar = expression.upper().index(";")
        databaseName = expression[len(self.USE_DATABASE_COMMAND) + 1: endChar]
        attributeString = re.search("\(\s*((\w+\s+\w+(?:\(\d+\))?(\s+PRIMARY KEY)?(?:\s*,\s*)?)+)\s*\)")
        attributes = attributeString.split(" ")
        x = {
            "command": 5,
            "databaseName": databaseName
        }
        # convert into JSON:
        response = json.dumps(x)
        return response


    def convertSimple(self, type, sentence):
        # initializing substrings
        sub1 = ""
        sub2 = ";"
        if type == 1 or type == 2:
            sub1= "DATABASE"
        elif type == 4:
            sub1= "TABLE"

        # getting index of substrings
        idx1 = sentence.upper().index(sub1)
        idx2 = sentence.upper().index(sub2)
        res = ''
        res = sentence[idx1 + len(sub1) + 1: idx2]
        x = {
            "command": type,
            "databaseName": res
        }
        # convert into JSON:
        y = json.dumps(x)

        # the result is a JSON string:
        return y

    def find_between(text, first, last):
        start = text.find(first) + len(first)
        end = text.find(last, start)
        return text[start:end] if start != -1 and end != -1 else None

    def convertCreateTable(self,type,sentence):
        sub1 = ""
        sub2 = ";"

        result = []
        #todo de extras tablename ul
        #ia numai stringul dintre paranteze
        result = re.search('\((.*)\);$', sentence)
        # print(result.group(1))
        answ=result.group(1).split(", ")
        # print(answ)
        r=[]
        for z in answ:
            r.append(z.split())
        #print(r)
        x = {
            "command": type,
            "tableName": "NUME"
        }
        for item in r:
            x[item[0]]=item[1]
        # convert into JSON:
        #print(x)
        y = json.dumps(x)

        # the result is a JSON string:
        return y


if __name__ == '__main__':
    c = DataConverter()
    sentence="CREATE TABLE Persons ( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255));"
    print(c.convertCreateTable(3,sentence))

