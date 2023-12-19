import itertools
import re

from CatalogXmlUtils import getIndexAttributesAndFiles, getTableAttributes, getTablePrimaryKeys, \
    getAttributeListWithoutPk
from ConditionEnum import ConditionEnum


def parseData(dataJson, databaseName, databaseConnection):
    if len(dataJson['joins']) == 0:
        return parseDataWithoutJoin(dataJson, databaseName, databaseConnection)
    else:
        return parseDataJoin(dataJson, databaseName, databaseConnection)


def parseDataJoin(dataJson, databaseName, databaseConnection):
    result = []
    for _ in dataJson['tableAliases']:
        result.append(["-"])
    for join in dataJson['joins']:
        res = parseJoin(join, dataJson, databaseName, databaseConnection)
        for i in range(len(result)):
            if result[i] != res[i] and res[i] != ["-"]:
                if result[i] == ["-"]:
                    result[i] = res[i]
                else:
                    result[i] = intersection(result[i], res[i])
    print(result, "\nbefore distinct\n")
    result = handleDistinct(dataJson, result)
    #TODO result = doWhereCondition here!!!!
    print(result, "\nafter distinct\n")
    result = finalJoinOfData(dataJson, result, databaseName)
    result = arrangeDataJoin(dataJson, result, databaseName)
    #=========
    result = parseConditionsForJoin(dataJson, result)
    #======
    print(result, "\nafter arrange\n")
    return result


def parseConditionsForJoin(dataJson,before):
    result=[]
    for item in before:
        k=1
        for condition in dataJson["conditions"]:
            triplet=getCondition(condition)
            if checkCond(triplet,item,checkCol(triplet,dataJson["columns"])) == False:
                k=0
        if k==1:
            result.append(item)
    return result


def checkCol(triplet,columns):
    for alias in columns:
        if is_float(triplet[0]) == False:
            if is_integer(triplet[0]) == False:
                if triplet[0] in columns[alias]:
                    return "stanga"
        if is_float(triplet[2]) == False:
            if is_integer(triplet[2]) == False:
                if triplet[2] in columns[alias]:
                   return "dreapta"
    return "stanga"

def checkCond(triplet,item,poz):
    if poz=="stanga":
        p1=0
    else:
        p1=2
    if triplet[1] == "=":
        if p1 == 0:
            if is_integer(triplet[2]) == True:
                return int(item[triplet[p1]]) == triplet[2]
            elif is_float(triplet[2]) == True:
                return float(item[triplet[p1]]) == triplet[2]
            else:return item[triplet[p1]] == triplet[2]
        else:
            if is_integer(triplet[0]) == True:
                return triplet[0] == int(item[triplet[p1]])
            elif is_float(triplet[0]) == True:
                return triplet[0] == int(item[triplet[p1]])
            else:return triplet[0] == item[triplet[p1]]
    elif triplet[1] == "<":
        if p1 == 0:
            if is_integer(triplet[2]) == True:
                return int(item[triplet[p1]]) < triplet[2]
            elif is_float(triplet[2]) == True:
                return float(item[triplet[p1]]) < triplet[2]
            else:
                return item[triplet[p1]] < triplet[2]
        else:
            if is_integer(triplet[0]) == True:
                return triplet[0] < int(item[triplet[p1]])
            elif is_float(triplet[0]) == True:
                return triplet[0] < int(item[triplet[p1]])
            else:
                return triplet[0] < item[triplet[p1]]
    elif triplet[1] == ">":
        if p1 == 0:
            if is_integer(triplet[2]) == True:
                return int(item[triplet[p1]]) > triplet[2]
            elif is_float(triplet[2]) == True:
                return float(item[triplet[p1]]) > triplet[2]
            else:
                return item[triplet[p1]] > triplet[2]
        else:
            if is_integer(triplet[0]) == True:
                return triplet[0] > int(item[triplet[p1]])
            elif is_float(triplet[0]) == True:
                return triplet[0] > int(item[triplet[p1]])
            else:
                return triplet[0] > item[triplet[p1]]

def getCondition(condition):
    triplet=condition.split(" ")
    final=[]

    for item in triplet:
        if is_integer(item) == True:
            final.append(int(item))
        elif is_float(item) == True:
            final.append(float(item))
        else:
            final.append(re.sub(r'^.*?[.]', '', item))

    #print(final)
    return final


def parseDataWithoutJoin(dataJson, databaseName, databaseConnection):
    result = []
    for _ in dataJson['tableAliases']:
        result.append(["-"])
    for condition in dataJson['conditions']:
        res = parseCondition(condition, dataJson, databaseName, databaseConnection)
        for i in range(len(result)):
            if result[i] != res[i] and res[i] != ["-"]:
                if result[i] == ["-"]:
                    result[i] = res[i]
                else:
                    result[i] = intersection(result[i], res[i])
    result = handleDistinct(dataJson, result)
    return arrangeData(dataJson, result, databaseName)


def parseJoin(join, dataJson, databaseName, databaseConnection):
    if re.search('inner join', join["join"], re.IGNORECASE):
        return parseConditionByJoin(join, dataJson, databaseName, databaseConnection)
    elif re.search('outer join', join["join"], re.IGNORECASE):
        return parseConditionByJoin(join, dataJson, databaseName, databaseConnection)
    elif re.search('left join', join["join"], re.IGNORECASE):
        return parseConditionByJoin(join, dataJson, databaseName, databaseConnection)
    elif re.search('right join', join["join"], re.IGNORECASE):
        return parseConditionByJoin(join, dataJson, databaseName, databaseConnection)
    raise Exception(join + " join is not valid")


def parseConditionByJoin(condition, dataJson, databaseName, databaseConnection):
    result = []
    for _ in dataJson['tableAliases']:
        result.append(["-"])
    cond = condition["condition"].split("=")
    columnName = cond[0].strip()
    expectedValue = cond[1].strip()
    cnwa = columnName.split(".")
    evwa = expectedValue.split(".")
    if cnwa[0] not in dataJson['tableAliases'] or evwa[0] not in dataJson['tableAliases']:
        raise Exception('unknown alias used for column name in command')
    else:
        aliasIndex1 = dataJson["tableAliases"].index(cnwa[0])
        aliasIndex2 = dataJson["tableAliases"].index(evwa[0])
        result = runSearchJoin(result, dataJson["tableNames"][aliasIndex1], aliasIndex1,
                               dataJson["tableNames"][aliasIndex2], aliasIndex2, dataJson,
                               databaseConnection, databaseName, cnwa[1], evwa[1])
    return result


def runSearchJoin(result, tableName1, i1, tableName2, i2, dataJson, databaseConnection, databaseName,
                  columnName1,
                  columnName2):
    table1 = databaseConnection.get_collection(tableName1)
    table2 = databaseConnection.get_collection(tableName2)
    indexList1 = list(filter(lambda idx: "foreign" not in idx["fileName"] and columnName1 in idx["attributes"],
                             getIndexAttributesAndFiles(databaseName, tableName1)))
    attributeList1 = getTableAttributes(databaseName, tableName1)
    pkList1 = getTablePrimaryKeys(databaseName, tableName1)
    indexList2 = list(filter(lambda idx: "foreign" not in idx["fileName"] and columnName2 in idx["attributes"],
                             getIndexAttributesAndFiles(databaseName, tableName2)))
    # todo fk index
    attributeList2 = getTableAttributes(databaseName, tableName2)
    pkList2 = getTablePrimaryKeys(databaseName, tableName2)
    # cautare secventiala in db
    if len(indexList1) == 0 and len(indexList2) == 0:
        sortMergeJoin(result, table1, table2, attributeList1, pkList1, attributeList2, pkList2, i1, i2, columnName1, columnName2)
    # cautare cu indecsi => indexed nested loop
    elif len(indexList1) != 0:
        result = indexedNestedLoop(result, table2, table1, indexList1, attributeList2, pkList2, i1, i2, dataJson,
                                   databaseConnection, columnName2, columnName1)
    elif len(indexList2) != 0:
        result = indexedNestedLoop(result, table1, table2, indexList2, attributeList1, pkList1, i2, i1, dataJson,
                                   databaseConnection, columnName1, columnName2)
    return result


def sortMergeJoin(result, table1, table2, attributeList1, pkList1, attributeList2, pkList2, i1, i2, columnName1, columnName2):
    attributeList1 = getAttributeListWithoutPk(attributeList1, pkList1)
    pkList1 = list(map(lambda pkItem: pkItem.text, pkList1))
    attributeList2 = getAttributeListWithoutPk(attributeList2, pkList2)
    pkList2 = list(map(lambda pkItem: pkItem.text, pkList2))
    list1 = list(table1.find({}, {"_id": False, "pk": True, "value": True}))
    list2 = list(table2.find({}, {"_id": False, "pk": True, "value": True}))
    if columnName1 in attributeList1 and columnName2 in attributeList2:
        list1 = sorted(list1, key=lambda item: item["value"].split('#')[attributeList1.index(columnName1)+1])
        l1v = list(map(lambda item: item["value"].split('#')[attributeList1.index(columnName1)+1], list1))
        list2 = sorted(list2, key=lambda item: item["value"].split('#')[attributeList2.index(columnName2)+1])
        l2v = list(map(lambda item: item["value"].split('#')[attributeList2.index(columnName2)+1], list2))
        j1 = 0
        j2 = 0
        while j1 < len(list1) and j2 < len(list2):
            while j1 < len(list1) and j2 < len(list2) and l1v[j1] == l2v[j2]:
                print("3", l1v[j1], l2v[j2])
                if result[i2] == ["-"]:
                    result[i2].remove("-")
                    if list2[j2] not in result[i2]:
                        result[i2].append(list2[j2])
                if result[i1] == ["-"]:
                    result[i1].remove("-")
                if list2[j1] not in result[i1]:
                    result[i1].append(list1[j1])
                j1 += 1
                #j2 += 1
            while j1 < len(list1) and j2 < len(list2) and l1v[j1] < l2v[j2]:
                print("1", l1v[j1], l2v[j2])
                j1 += 1
            while j1 < len(list1) and j2 < len(list2) and l1v[j2] < l2v[j1]:
                print("2", l1v[j1], l2v[j2])
                j2 += 1
                print(result)
            if j1 >= len(list1) or j2 >= len(list2):
                break
    elif columnName1 in pkList1 and columnName2 in pkList2:
        list1 = sorted(list1, key=lambda item: item["value"].split('#')[pkList1.index(columnName1)+1])
        l1v = list(map(lambda item: item["value"].split('#')[pkList1.index(columnName1)+1], list1))
        list2 = sorted(list2, key=lambda item: item["value"].split('#')[pkList2.index(columnName2)+1])
        l2v = list(map(lambda item: item["value"].split('#')[pkList2.index(columnName2)+1], list2))
        j1 = 0
        j2 = 0
        while j1 < len(list1) and j2 < len(list):
            while l1v[j1] == l2v[j2]:
                if result[i2] == ["-"]:
                    result[i2].remove("-")
                result[i2].append(list2[j2])
                if result[i1] == ["-"]:
                    result[i1].remove("-")
                result[i1].append(list1[j1])
                j1 += 1
                j2 += 1
            while l1v[j1] < l2v[j2]:
                j1 += 1
            while l1v[j2] < l2v[j1]:
                j2 += 1

    print(result)
    return result

    # resvalues = list(filter(lambda rvi: rvi not in ["", " "], r["value"].split('#')))
    # respk = list(filter(lambda rvi: rvi not in ["", " "], r["pk"].split('#')))


def indexedNestedLoop(result, table1, table2, indexList, attributeList, pkList, i1, i2, dataJson, databaseConnection,
                      columnName1, columnName2):
    attributeList = getAttributeListWithoutPk(attributeList, pkList)
    pkList = list(map(lambda pkItem: pkItem.text, pkList))
    for r in table1.find({}, {"_id": False, "pk": True, "value": True}):
        resvalues = list(filter(lambda rvi: rvi not in ["", " "], r["value"].split('#')))
        respk = list(filter(lambda rvi: rvi not in ["", " "], r["pk"].split('#')))
        for attributeListIndex, att in enumerate(attributeList):
            if att == columnName1:
                lung = 0
                if result[i1] != ["-"]:
                    lung = len(result[i1])
                result = indexSearchInTableJoin(result, indexList, table2, i1, dataJson,
                                                databaseConnection,
                                                columnName2, resvalues[attributeListIndex])
                if lung != len(result[i1]):
                    if result[i2] == ["-"]:
                        result[i2].remove("-")
                    result[i2].append(r)
                break
        for pkIndex, pk in enumerate(pkList):
            if pk == columnName1:
                lung = 0
                if result[i2] != ["-"]:
                    lung = len(result[i2])
                result = indexSearchInTableJoin(result, indexList, table2, i1, dataJson,
                                                databaseConnection,
                                                columnName2, respk[pkIndex])
                if lung != len(result[i1]):
                    if result[i2] == ["-"]:
                        result[i2].remove("-")
                    result[i2].append(r)
                break
    return result


def indexSearchInTableJoin(result, indexList, table, i, dataJson, databaseConnection, columnName,
                           expectedValue):
    for index in indexList:
        if columnName in index["attributes"]:
            file = databaseConnection.get_collection(index["fileName"])
            if file is not None:
                for r in file.find({"pk": conditionValidationIndex(ConditionEnum.EQ, expectedValue)},
                                   {"_id": False, "pk": True, "value": True}):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(table.find_one({"pk": {"$regex": r["value"]}},
                                                        {"_id": False, "pk": True,
                                                         "value": True}))
                if result[i] != ["-"]:
                    break
    return result


def sequentialSearchInTableJoin(conditionType, result, attributeList, pkList, table, i, dataJson, columnName,
                                expectedValue):
    attributeList = getAttributeListWithoutPk(attributeList, pkList)
    pkList = list(map(lambda pkItem: pkItem.text, pkList))
    for r in table.find({}, {"_id": False, "pk": True, "value": True}):
        resvalues = list(filter(lambda rvi: rvi not in ["", " "], r["value"].split('#')))
        respk = list(filter(lambda rvi: rvi not in ["", " "], r["pk"].split('#')))
        for attributeListIndex, att in enumerate(attributeList):
            if att == columnName:
                if conditionValidationSequential(conditionType, resvalues[attributeListIndex], expectedValue):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(r)
                break
        for pkIndex, pk in enumerate(pkList):
            if pk == columnName:
                if conditionValidationSequential(conditionType, respk[pkIndex], expectedValue):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(r)
                break
    return result


def parseCondition(condition, dataJson, databaseName, databaseConnection):
    if re.search('<=', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.LTE, dataJson, databaseName, databaseConnection)
    elif re.search('>=', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.GTE, dataJson, databaseName, databaseConnection)
    elif re.search('=', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.EQ, dataJson, databaseName, databaseConnection)
    elif re.search('<', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.LT, dataJson, databaseName, databaseConnection)
    elif re.search('>', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.GT, dataJson, databaseName, databaseConnection)
    elif re.search('LIKE', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.LIKE, dataJson, databaseName, databaseConnection)
    elif re.search('IN', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.IN, dataJson, databaseName, databaseConnection)
    elif re.search('BETWEEN', condition, re.IGNORECASE):
        return parseConditionByType(condition, ConditionEnum.BETWEEN, dataJson, databaseName, databaseConnection)
    raise Exception(condition + " condition is not valid")


def parseConditionByType(condition, conditionType, dataJson, databaseName, databaseConnection):
    result = []
    for _ in dataJson['tableAliases']:
        result.append(["-"])
    if checkCondition(condition, conditionType):
        cond = splitCondition(condition, conditionType)
        columnName = cond[0].strip()
        expectedValue = cond[1].strip()
        if re.search('\.', columnName, re.IGNORECASE):
            cnwa = columnName.split(".")
            if cnwa[0] not in dataJson['tableAliases']:
                raise Exception('unknown alias used for column name in command')
            else:
                aliasIndex = dataJson["tableAliases"].index(cnwa[0])
                result = runSearch(conditionType, result, dataJson["tableNames"][aliasIndex], aliasIndex, dataJson,
                                   databaseConnection, databaseName, cnwa[1], expectedValue)
        else:
            for i, tableName in enumerate(dataJson['tableNames']):
                result = runSearch(conditionType, result, tableName, i, dataJson, databaseConnection, databaseName,
                                   columnName,
                                   expectedValue)
        return result


def splitCondition(condition, conditionType):
    condition = condition.strip()
    if conditionType == ConditionEnum.EQ:
        return condition.split("=")
    elif conditionType == ConditionEnum.LT:
        return condition.split("<")
    elif conditionType == ConditionEnum.GT:
        return condition.split(">")
    elif conditionType == ConditionEnum.LTE:
        return condition.split("<=")
    elif conditionType == ConditionEnum.GTE:
        return condition.split(">=")
    elif conditionType == ConditionEnum.LIKE:
        return re.split("LIKE", condition, flags=re.IGNORECASE)
    elif conditionType == ConditionEnum.IN:
        return re.split("IN", condition, flags=re.IGNORECASE)
    elif conditionType == ConditionEnum.BETWEEN:
        return re.split("BETWEEN", condition, flags=re.IGNORECASE)
    raise Exception("issue with condition split")


def checkCondition(condition, conditionType):
    condition = condition.strip()
    if conditionType == ConditionEnum.EQ:
        if re.search('\s*(\w+|\w+\.\w+)\s*=\s*(("[^"]*")|(\d+\.\d+)|\d+|null)\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.LT:
        if re.search('\s*(\w+|\w+\.\w+)\s*<\s*(("[^"]*")|(\d+\.\d+)|\d+)\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.GT:
        if re.search('\s*(\w+|\w+\.\w+)\s*>\s*(("[^"]*")|(\d+\.\d+)|\d+)\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.LTE:
        if re.search('\s*(\w+|\w+\.\w+)\s*<=\s*(("[^"]*")|(\d+\.\d+)|\d+)\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.GTE:
        if re.search('\s*(\w+|\w+\.\w+)\s*>=\s*(("[^"]*")|(\d+\.\d+)|\d+)\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.LIKE:
        if re.search('\s*(LIKE\s("[^"]*"))\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.IN:
        if re.search('\s*(IN\s\[(("[^"]*")|(\d+\.\d+)|\d+|null)((\s*,\s*(("[^"]*")|(\d+\.\d+)|\d+|null))*)])\s*',
                     condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.BETWEEN:
        if re.search('\s*(BETWEEN\s\(((("[^"]*")|\d+\.\d+)|\d+)\s*,\s*((("[^"]*")|\d+\.\d+)|\d+)\))\s*', condition,
                     re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    return True


def runSearch(conditionType, result, tableName, i, dataJson, databaseConnection, databaseName, columnName,
              expectedValue):
    table = databaseConnection.get_collection(tableName)
    indexList = list(filter(lambda idx: "foreign" not in idx["fileName"] and columnName in idx["attributes"],
                            getIndexAttributesAndFiles(databaseName, tableName)))
    attributeList = getTableAttributes(databaseName, tableName)
    pkList = getTablePrimaryKeys(databaseName, tableName)
    # cautare secventiala in db
    if len(indexList) == 0:
        result = sequentialSearchInTable(conditionType, result, attributeList, pkList, table, i, dataJson, columnName,
                                         expectedValue)
    # cautare cu indecsi
    else:
        result = indexSearchInTable(conditionType, result, indexList, table, i, dataJson, databaseConnection,
                                    columnName, expectedValue)
    return result


def sequentialSearchInTable(conditionType, result, attributeList, pkList, table, i, dataJson, columnName,
                            expectedValue):
    attributeList = getAttributeListWithoutPk(attributeList, pkList)
    pkList = list(map(lambda pkItem: pkItem.text, pkList))
    for r in table.find({}, {"_id": False, "pk": True, "value": True}):
        resvalues = list(filter(lambda rvi: rvi not in ["", " "], r["value"].split('#')))
        respk = list(filter(lambda rvi: rvi not in ["", " "], r["pk"].split('#')))
        for attributeListIndex, att in enumerate(attributeList):
            if att == columnName:
                if conditionValidationSequential(conditionType, resvalues[attributeListIndex], expectedValue):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(r)
                break
        for pkIndex, pk in enumerate(pkList):
            if pk == columnName:
                if conditionValidationSequential(conditionType, respk[pkIndex], expectedValue):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(r)
                break
    return result


def conditionValidationSequential(conditionType, value, expectedValue):
    expectedValue = expectedValue.strip()
    if conditionType == ConditionEnum.EQ:
        return value == expectedValue
    elif conditionType == ConditionEnum.LT:
        return value < expectedValue
    elif conditionType == ConditionEnum.GT:
        return value > expectedValue
    elif conditionType == ConditionEnum.LTE:
        return value <= expectedValue
    elif conditionType == ConditionEnum.GTE:
        return value >= expectedValue
    elif conditionType == ConditionEnum.LIKE:
        return re.search(expectedValue.replace("\"""", ""), value.replace("\"""", ""), re.IGNORECASE) is not None
    elif conditionType == ConditionEnum.IN:
        expectedValue = expectedValue.replace("[", "").replace("]", "").strip().split(",")
        expectedValue = list(map(lambda v: v.strip(), expectedValue))
        return value.strip() in expectedValue
    elif conditionType == ConditionEnum.BETWEEN:
        expectedValue = expectedValue.replace("(", "").replace(")", "").strip().split(",")
        expectedValue = list(map(lambda v: v.strip(), expectedValue))
        return expectedValue[0] < value < expectedValue[1]


def indexSearchInTable(conditionType, result, indexList, table, i, dataJson, databaseConnection, columnName,
                       expectedValue):
    for index in indexList:
        if columnName in index["attributes"]:
            file = databaseConnection.get_collection(index["fileName"])
            if file is not None:
                for r in file.find({"pk": conditionValidationIndex(conditionType, expectedValue)},
                                   {"_id": False, "pk": True, "value": True}):
                    if dataJson['columns'][dataJson['tableAliases'][i]]:
                        if result[i] == ["-"]:
                            result[i].remove("-")
                        result[i].append(table.find_one({"pk": {"$regex": r["value"]}},
                                                        {"_id": False, "pk": True,
                                                         "value": True}))
    return result


# todo fix the logic for index search
def conditionValidationIndex(conditionType, expectedValue):
    expectedValue = expectedValue.strip()
    if conditionType == ConditionEnum.EQ:
        return {"$regex": '&' + expectedValue}
    elif conditionType == ConditionEnum.LT:
        return {"$lt": '&' + expectedValue}
    elif conditionType == ConditionEnum.GT:
        return {"$gt": '&' + expectedValue}
    elif conditionType == ConditionEnum.LTE:
        return {"$lte": '&' + expectedValue}
    elif conditionType == ConditionEnum.GTE:
        return {"$gte": '&' + expectedValue}
    elif conditionType == ConditionEnum.LIKE:
        return {"$regex": '&' + expectedValue}
    elif conditionType == ConditionEnum.IN:
        expectedValue = expectedValue.replace("[", "").replace("]", "").strip().split(",")
        expectedValue = list(map(lambda v: v.strip(), expectedValue))
        return {"$in": expectedValue}
    elif conditionType == ConditionEnum.BETWEEN:
        expectedValue = expectedValue.replace("(", "").replace(")", "").strip().split(",")
        expectedValue = list(map(lambda v: v.strip(), expectedValue))
        return {"$gt": '&' + expectedValue[0], "$lt": '&' + expectedValue[1]}


def handleDistinct(dataJson, result):
    if dataJson["distinct"] == 1:
        for i, r in enumerate(result):
            res = []
            for item1 in r:
                if item1 not in res:
                    res.append(item1)
            result[i] = res
    return result


def arrangeData(dataJson, result, databaseName):
    for i, tableName in enumerate(dataJson["tableNames"]):
        tableResult = []
        pkList = getTablePrimaryKeys(databaseName, tableName)
        attributeList = getAttributeListWithoutPk(getTableAttributes(databaseName, tableName), pkList)
        pkList = list(map(lambda pkItem: pkItem.text, pkList))
        for item in result[i]:
            if item != "-":
                resvalue = list(filter(lambda rvi: rvi not in ["", " "], item["value"].split('#')))
                respk = list(filter(lambda rvi: rvi not in ["", " "], item["pk"].split('#')))
                res = {}
                for pki, pk in enumerate(pkList):
                    if pk in dataJson["columns"][dataJson["tableAliases"][i]]:
                        res[pk] = respk[pki]
                for ai, a in enumerate(attributeList):
                    if a in dataJson["columns"][dataJson["tableAliases"][i]]:
                        res[a] = resvalue[ai]
                tableResult.append(res)
        result[i] = tableResult
    print(result)
    return result


def finalJoinOfData(dataJson, result, databaseName):
    interm = result[0]
    for i in range(1, len(result)):
        interm = list(itertools.product(interm, result[i]))
    interm = list(map(lambda item: list(item), interm))
    print("interm", interm)
    return interm


def arrangeDataJoin(dataJson, result, databaseName):
    resultFinal = []
    for r in result:
        res = {}
        for i, tableName in enumerate(dataJson["tableNames"]):
            pkList = getTablePrimaryKeys(databaseName, tableName)
            attributeList = getAttributeListWithoutPk(getTableAttributes(databaseName, tableName), pkList)
            pkList = list(map(lambda pkItem: pkItem.text, pkList))

            if r[i] != "-":
                resvalue = list(filter(lambda rvi: rvi not in ["", " "], r[i]["value"].split('#')))
                respk = list(filter(lambda rvi: rvi not in ["", " "], r[i]["pk"].split('#')))
                for pki, pk in enumerate(pkList):
                    if pk in dataJson["columns"][dataJson["tableAliases"][i]]:
                        res[pk] = respk[pki]
                for ai, a in enumerate(attributeList):
                    if a in dataJson["columns"][dataJson["tableAliases"][i]]:
                        res[a] = resvalue[ai]
        resultFinal.append(res)
    print(result)
    return resultFinal


def intersection(list1, list2):
    return [value for value in list1 if value in list2]

def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

def is_integer(string):
    try:
        int(string)
        return True
    except ValueError:
        return False


# if __name__ == '__main__':
    # getCondition("ceva = sunet")
    # print(checkCol(getCondition("sunet > 1"),{"a": ["t1id", "sunet", "pozitie"], "b": ["t2id", "row", "column"]}))
    #print(checkCond(getCondition("pozitie < 25"), {'t1id': '2', 'sunet': '"ana"', 'pozitie': '50', 't2id': '1', 'row': '"ana"', 'column': '1'},checkCol(getCondition("pozitie = 50"),{"a": ["t1id", "sunet", "pozitie"], "b": ["t2id", "row", "column"]})))