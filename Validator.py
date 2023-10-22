import re


class Validator:
    '''
    CREATE DB       -1
    DROP DB         -2
    CREATE TB       -3
    DROP TB         -4
    CREATE INDEX    -5
    '''
    def __init__(self):
        pass

    def validate(self,sentence):
        if re.search("^CREATE DATABASE\s.*;$", sentence,re.IGNORECASE):
            return 1
        elif re.search("^DROP DATABASE\s.*;$", sentence,re.IGNORECASE):
            return 2
        elif re.search("^CREATE TABLE\s.*((.|\n*?));$", sentence, re.IGNORECASE):
            #TODO nu merge perfect ,gen daca scriu column-nametype legat sau doar column name
            return 3
        elif re.search("^DROP TABLE\s.*;$", sentence, re.IGNORECASE):
            return 4
        elif re.search("CREATE INDEX\s.*ON\s.*((.|\n*?));",sentence ,re.IGNORECASE):
            return 5
        else:return False




# if __name__ == '__main__':
#     v=Validator
#     sentence=";"
#     print(v.validate(sentence))