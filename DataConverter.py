import json
import re
class DataConverter:
    '''
    CREATE DB -1
    DROP DB   -2
    CREATE TB -3
    DROP TB   -4
    '''
    def __init__(self):
        pass

    def convertSimple(self,type,sentence):
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
    c=DataConverter()
    sentence="CREATE TABLE Persons ( PersonID int, LastName varchar(255), FirstName varchar(255), Address varchar(255), City varchar(255));"
    print(c.convertCreateTable(3,sentence))

