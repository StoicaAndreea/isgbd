from enum import Enum


class AttributeWithSizeTypeEnum(Enum):
    # string
    CHAR = "CHAR"  # CHAR(size)
    VARCHAR = "VARCHAR"  # VARCHAR(SIZE)
    BINARY = "BINARY"  # size
    TEXT = "TEXT"  # size
    # numeric
    BIT = "BIT"  # size
    TINYINT = "TINYINT"  # size - 0-255
    SMALLINT = "SMALLINT"  # size - +-32768
    INT = "INT"  # size
    INTEGER = "INTEGER"  # size
    BIGINT = "BIGINT"  # size
    FLOAT = "FLOAT"  # size, d - number of digits
    DOUBLE = "DOUBLE"  # size, d - number of digits
