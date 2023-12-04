import json
import re

from CatalogXmlUtils import getIndexAttributesAndFiles, getTableAttributes, getTablePrimaryKeys, \
    getAttributeListWithoutPk
from ConditionEnum import ConditionEnum


def parseData(dataJson, databaseName, databaseConnection):
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
    raise Exception(condition+" condition is not valid")


def parseConditionByType(condition, conditionType, dataJson, databaseName, databaseConnection):
    result = []
    for _ in dataJson['tableAliases']:
        result.append(["-"])
    if checkCondition(condition, conditionType):
        cond = splitCondition(condition, conditionType)
        print(cond)
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
                result = runSearch(conditionType, result, tableName, i, dataJson, databaseConnection, databaseName, columnName,
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
        if re.search('\s*(IN\s\[(("[^"]*")|(\d+\.\d+)|\d+|null)((\s*,\s*(("[^"]*")|(\d+\.\d+)|\d+|null))*)])\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    elif conditionType == ConditionEnum.BETWEEN:
        if re.search('\s*(BETWEEN\s\(((("[^"]*")|\d+\.\d+)|\d+)\s*,\s*((("[^"]*")|\d+\.\d+)|\d+)\))\s*', condition, re.IGNORECASE) is None:
            raise Exception(condition + " condition is not correct")
    return True


def runSearch(conditionType, result, tableName, i, dataJson, databaseConnection, databaseName, columnName, expectedValue):
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


def sequentialSearchInTable(conditionType, result, attributeList, pkList, table, i, dataJson, columnName, expectedValue):
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
    print(value, expectedValue)
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
                for r in file.find({"pk": conditionValidationIndex(conditionType, expectedValue)}, {"_id": False, "pk": True, "value": True}):
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
    return result


def intersection(list1, list2):
    return [value for value in list1 if value in list2]
