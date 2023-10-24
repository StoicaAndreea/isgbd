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
        elif re.search("^(CREATE INDEX)\s\w+\sON\s\w+\s*\(\s*\w+\s*(\s*,\s*\w+\s*)*\s*\);$",sentence,re.IGNORECASE):
            return False
        elif re.search("^(DROP INDEX)\s\w+\s*;$", sentence, re.IGNORECASE):
            return False
        else:
            return "Unknown Syntax"
