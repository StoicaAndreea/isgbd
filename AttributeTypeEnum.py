from enum import Enum


class AttributeTypeEnum(Enum):
    # string
    CHAR = "CHAR"  # CHAR(size)
    VARCHAR = "VARCHAR"  # VARCHAR(SIZE)
    BINARY = "BINARY"  # size
    TEXT = "TEXT"  # size
    BLOB = "BLOB"
    LONGTEXT = "LONGTEXT"

    # numeric
    BIT = "BIT"  # size
    BOOL = "BOOL" # 0 or 1
    BOOLEAN = "BOOLEAN"
    TINYINT = "TINYINT"  # size - 0-255
    SMALLINT = "SMALLINT"  # size - +-32768
    INT = "INT"  # size
    INTEGER = "INTEGER"  # size
    BIGINT =  "BIGINT"  # size
    FLOAT = "FLOAT"  # size, d - number of digits
    DOUBLE = "DOUBLE"  # size, d - number of digits

    #date and time
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIMESTAMP = "TIMESTAMP"
    TIME = "TIME"
    YEAR = "YEAR"

