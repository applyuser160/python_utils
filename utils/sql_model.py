import os
from typing import Any, Dict, List, Optional, Self, Union
from sqlmodel import Field, SQLModel
from datetime import date, datetime
from uuid import uuid4
from sqlmodel import Session, create_engine, select, and_, or_
from sqlalchemy.engine import URL
from .env import get
from .condition import Condition, ConditionType
from .logg import Logg


class Base(SQLModel, table=False):
    id: Optional[str] = Field(default=None, primary_key=True)
    createDate: Optional[datetime] = None
    createObjectId: Optional[str] = None
    updateDate: Optional[datetime] = None
    updateObjectId: Optional[str] = None

    def add_or_update(self, objectId: str):
        setattr(self, "updateDate", datetime.now())
        setattr(self, "updateObjectId", objectId)
        if self.createDate == None:
            setattr(self, "createDate", datetime.now())
            setattr(self, "createObjectId", objectId)
        if self.id == None:
            setattr(self, "id", str(uuid4()))

    def copy_only_id(self):
        staticProperty = ["_sa_instance_state", "id"]
        return Base(id=self.id)

    def copy_poperty(self: Self, copy: Self, properties: List[str]):
        for k in properties:
            v = getattr(copy, k)
            setattr(self, k, v)

    def extract_valid_value(self):
        staticProperty = ["_sa_instance_state"]
        return {
            k: v for k, v in vars(self).items() if v != None and k not in staticProperty
        }

    def delete_property(self, properties: List[str]):
        for k in properties:
            setattr(self, k, None)

    def is_new(self):
        return self.id == None

    def is_empty(self):
        staticProperty = ["_sa_instance_state"]
        evv = self.extract_valid_value()
        for k in evv.keys():
            if k not in staticProperty:
                return False
        return True

    @staticmethod
    def is_none(obj: Any):
        return obj is None


class MysqlSession:
    session: Session
    logg: Logg

    def __init__(self, logg: Logg):
        uri = get("MYSQL_URI")
        user = get("MYSQL_USER")
        pin = get("MYSQL_PASSWORD")
        database = get("MYSQL_DATABASE")
        url = URL.create(
            drivername="mysql+mysqldb",
            username=user,
            password=pin,
            host=uri,
            database=database,
            query={"charset": "utf8mb4", "ssl": "true"},
        )
        engine = create_engine(url=url)
        self.session = Session(engine)
        self.logg = logg

    def close(self):
        self.session.close()

    def _append_where(self, query, modelType: type, cond: Base, type: ConditionType):
        if not cond.is_empty():
            dict_cond = cond.extract_valid_value()
            for k, v in dict_cond.items():
                condition = Condition(getattr(modelType, k), type, v, False)
                query = query.where(condition.to_sqlachemy())
        return query

    def _exec_query(self, query, modelType: type, isOne: bool):
        self.logg.info("execute query.", {"query": query, "type": modelType.__name__})
        try:
            if isOne:
                result = self.session.exec(query).first()
                if Base.is_none(result):
                    return modelType()
                if result.is_empty():
                    return modelType()
                else:
                    return result
            else:
                result = self.session.exec(query).all()
                if len(result) == 0:
                    return []
                else:
                    return result
        except Exception as e:
            self.logg.error("execute query error", {"message": str(e)})

    def execute(self, query, modelType: type, isOne: bool):
        self.logg.info("execute sql", {"type": modelType.__name__})
        return self._exec_query(query, modelType, isOne)

    def _find_base(self, modelType: type, cond: Base = None, isOne: bool = True):
        query = select(modelType)
        query = self._append_where(query, modelType, cond, ConditionType.EQUAL)
        return self._exec_query(query, modelType, isOne)

    def find(
        self,
        modelType: type,
        conds: Dict[ConditionType, Base] = None,
        isOne: bool = True,
    ):
        self.logg.info("find sql", {"type": modelType.__name__})
        query = select(modelType)
        if conds is not None:
            for k, v in conds.items():
                query = self._append_where(query, modelType, v, k)
        return self._exec_query(query, modelType, isOne)

    def _save_base(
        self, modelType: type, model: Base, objectId: str, isnew: bool = None
    ):
        try:
            is_new = isnew if isnew != None else model.is_new()
            entity = (
                model if is_new else self._find_base(modelType, model.copy_only_id())
            )
            entity.add_or_update(objectId)
            entity.copy_poperty(model, model.extract_valid_value().keys())
            self.session.add(entity)
            self.session.expire_on_commit = False
            self.session.commit()
            return entity
        except Exception as e:
            self.logg.error("save sql error", {"message": str(e)})

    def save(self, modelType: type, model: Base, objectId: str):
        self.logg.info("save sql", {"type": modelType.__name__})
        return self._save_base(modelType, model, objectId)

    def bulk_save(self, models: List[Base], objectId: str):
        self.logg.info("bulk save sql")
        try:
            for model in models:
                model.add_or_update(objectId)
            self.session.bulk_save_objects(models)
            self.session.commit()
        except Exception as e:
            self.logg.error("buls save sql error", {"message": str(e)})

    def _delete_base(self, modelType: type, model: Base):
        try:
            entity = self._find_base(modelType, model.copy_only_id())
            self.session.delete(entity)
            self.session.commit()
        except Exception as e:
            self.logg.error("delete sql error", {"message": str(e)})

    def delete(self, modelType: type, model: Base):
        self.logg.info("delete sql", {"type": modelType.__name__})
        self._delete_base(modelType, model)
