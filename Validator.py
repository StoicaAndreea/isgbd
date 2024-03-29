import re


class Validator:
    # CREATE DB       -1
    # DROP DB         -2
    # CREATE TB       -3
    # DROP TB         -4
    # CREATE INDEX    -5
    GENERAL_REGEX = "(\s*([\0\b\'\"\n\r\t\%\_\\]*\s*(((select\s+\S.*\s+from\s+\S+)|(use\s+\S+)|(insert\s+into\s+\S+)|(update\s+\S+\s+set\s+\S+)|(delete\s+from\s+\S+)|(create table (\s+\S+)|(\s*[\(](\s|\S)*[\)]))|(((drop)|(create)|(alter))\s+((database)|(table)|(index))\s+\S+)|(\/\*)|(--)))(\s*[\;]\s*)*)+)"

    def __init__(self):
        pass

    def validate(self, sentence):
        if not sentence.strip()[-1] == ';':
            return "Missing ';'"
        if re.search("^(CREATE DATABASE)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        elif re.search("^(DROP DATABASE)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        elif re.search("^(USE DATABASE)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        # ordinea este primary key, not null, unique!!!!!
        elif re.search(
                "^CREATE TABLE (\w+)\s*\(\s*((\w+\s+\w+(?:\(\d+\))?(\s+PRIMARY KEY)?(\s+NOT NULL)?(\s+UNIQUE)?(?:\s+REFERENCES\s\w+\s\(\w+\))?(?:\s*,\s*)?)+)\s*\);$",
                sentence, re.IGNORECASE):
            return False
        elif re.search("^(DROP TABLE)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        elif re.search("^(CREATE INDEX)\s\w+\s*\(\s*\w+\s*(\s*,\s*\w+\s*)*\s*\);$", sentence, re.IGNORECASE):
            return False
        elif re.search("^(CREATE\s(UNIQUE\s)?INDEX)\s\w+\sON\s\w+\s*\(\s*\w+\s*(\s*,\s*\w+\s*)*\s*\);$", sentence,
                       re.IGNORECASE):
            return False
        elif re.search("^(DROP INDEX)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        elif re.search(
                '^(INSERT INTO)\s+\w+\s*\(\s*(\w+\s*,\s*)*\w+\s*\)\s*VALUES\s+(\(\s*((("[^"]*")|(\d+\.\d+)|\d+)\s*,\s*)*(("[^"]*")|(\d+\.\d+)|\d+)\s*\),\s{1})*(\(\s*((("[^"]*")|(\d+\.\d+)|\d+|null)\s*,\s*)*(("[^"]*")|(\d+\.\d+)|\d+|null)\s*\));$',
                sentence, re.IGNORECASE):
            return False
        elif re.search(
                '^(DELETE FROM)\s+\w+\s*WHERE\s*\(\s*(\w+\s*\=\s*((("[^"]*")|(\d+\.\d+)|\d+))\s*and\s*)*(\w+\s*\=\s*(("[^"]*")|(\d+\.\d+)|\d+)\s*\));$',
                sentence, re.IGNORECASE):
            return False
        elif re.search(
                '^SELECT\s(DISTINCT\s)?\s*(\w+|\w+\.\w+)(\s*,\s*(\w+|\w+\.\w+))*\sFROM\s(\w+(\s(AS)\s\w+)?)((\s*,\s(\w+(\s(AS)\s\w+)?))*)(\s(((INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN))\s\w+(\s(AS)\s\w+)(\sON\s\w+\.\w+\s=\s\w+\.\w+))((\s*,\s*(((INNER JOIN)|(OUTER JOIN)|(LEFT JOIN)|(RIGHT JOIN))\s\w+(\s(AS)\s\w+)(\sON\s\w+\.\w+\s=\s\w+\.\w+)))*))?\sWHERE\s((\w+|\w+\.\w+)\s((=|>|<|<=|>=)\s*(("[^"]*")|(\d+\.\d+)|\d+|(\w+|\w+\.\w+))|(LIKE\s("[^"]*"))|(IN\s\[(("[^"]*")|(\d+\.\d+)|\d+|null)((\s*,\s*(("[^"]*")|(\d+\.\d+)|\d+|null))*)\])|(BETWEEN\s\(((("[^"]*")|\d+\.\d+)|\d+)\s*,\s*((("[^"]*")|\d+\.\d+)|\d+)\))))((\sAND\s((\w+|\w+\.\w+)\s((=|>|<|<=|>=)\s*(("[^"]*")|(\d+\.\d+)|\d+|(\w+|\w+\.\w+))|(LIKE\s("[^"]*"))|(IN\s\[(("[^"]*")|(\d+\.\d+)|\d+|null)((\s*,\s*(("[^"]*")|(\d+\.\d+)|\d+|null))*)\])|(BETWEEN\s\(((("[^"]*")|\d+\.\d+)|\d+)\s*,\s*((("[^"]*")|\d+\.\d+)|\d+)\)))))*);$',
                sentence, re.IGNORECASE):
            return False
        else:
            return "Unknown Syntax"
