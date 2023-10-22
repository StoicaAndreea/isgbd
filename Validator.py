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
        print(sentence.strip()[-1])
        if not sentence.strip()[-1] == ';':
            return "Missing ';'"
        if re.search("^CREATE DATABASE\s.*;$", sentence, re.IGNORECASE):
            return True
        elif re.search("^DROP DATABASE\s.*;$", sentence, re.IGNORECASE):
            return True
        elif re.search("^CREATE TABLE\s.*((.|\n*?));$", sentence, re.IGNORECASE):
            #TODO nu merge perfect ,gen daca scriu column-nametype legat sau doar column name
            return True
        elif re.search("^DROP TABLE\s.*;$", sentence, re.IGNORECASE):
            return True
        elif re.search("CREATE INDEX\s.*ON\s.*((.|\n*?));", sentence, re.IGNORECASE):
            return True
        else:
            return "Unknown Syntax"
