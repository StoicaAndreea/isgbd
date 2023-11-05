from enum import Enum


class AttributeTypeEnum(Enum):
    # string
    CHAR = "CHAR"  # CHAR(size)
    VARCHAR = "VARCHAR"  # VARCHAR(SIZE)

    # numeric
    BIT = "BIT"  # size
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"  # size
    DOUBLE = "DOUBLE"  # size, d - number of digits

    # date and time
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIME = "TIME"
