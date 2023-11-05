from enum import Enum


class AttributeWithSizeTypeEnum(Enum):
    # string
    CHAR = "CHAR"  # CHAR(size)
    VARCHAR = "VARCHAR"  # VARCHAR(SIZE)

    # numeric
    BIT = "BIT"  # size
    INTEGER = "INTEGER"  # size
    DOUBLE = "DOUBLE"  # size, d - number of digits
