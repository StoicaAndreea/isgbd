from enum import Enum


class ConditionEnum(Enum):
    EQ = "eq"
    LT = "lt"
    GT = "gt"
    LTE = "lte"
    GTE = "gte"
    LIKE = "like"
    IN = "in"
    BETWEEN = "between"
