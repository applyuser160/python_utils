from enum import Enum
from sqlmodel import and_, or_
from sqlalchemy import *


class ConditionType(Enum):
    EQUAL = 0
    NOT_EQUAL = 1
    GREATER_THAN = 2
    GREATER = 3
    LESS_THAN = 4
    LESS = 5
    CONTAINS = 6
    LIKE = 7


class ConditionRelationType(Enum):
    AND = 0
    OR = 1


class Condition:
    target: Any
    type: ConditionType
    value: Any
    isnot: bool

    def __init__(self, target: Any, type: ConditionType, value: Any, isnot: bool):
        self.target = target
        self.type = type
        self.value = value
        self.isnot = isnot

    def to_sqlachemy(self):
        result: bool
        if type == ConditionType.EQUAL:
            result = self.target == self.value
        elif type == ConditionType.NOT_EQUAL:
            result = self.target != self.value
        elif type == ConditionType.GREATER_THAN:
            result = self.target >= self.value
        elif type == ConditionType.GREATER:
            result = self.target > self.value
        elif type == ConditionType.LESS_THAN:
            result = self.target <= self.value
        elif type == ConditionType.LESS:
            result = self.target < self.value
        elif type == ConditionType.CONTAINS:
            result = self.target.contains(self.value)
        else:
            result = self.target.like(f"%{self.value}%")
        if self.isnot:
            result = not result
        return result


class ConditionGroup:
    condition1: Condition
    condition2: Condition = None
    relation: ConditionRelationType = None

    def to_sqlalchemy(self):
        if self.relation == ConditionRelationType.AND:
            return and_(self.condition1.to_sqlachemy(), self.condition2.to_sqlachemy())
        else:
            return or_(self.condition1.to_sqlachemy(), self.condition2.to_sqlachemy())
