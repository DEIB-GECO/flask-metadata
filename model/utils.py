import inspect

import sqlalchemy

from model import models
from model.models import db


class Column:

    def __init__(self, column_name, table_name, db_column, table_class):
        self.column_name = column_name
        self.table_name = table_name
        self.db_column = db_column
        self.table_class = table_class


def get_column_dict():
    columns = dict()
    duplicated_columns = []
    for class_name, model_class in inspect.getmembers(models):
        if inspect.isclass(model_class) and issubclass(model_class, db.Model):
            for column_name, column in list(model_class.__dict__.items()):
                if issubclass(type(column), sqlalchemy.orm.attributes.InstrumentedAttribute):
                    exp = column.expression
                    primary_key = exp.primary_key
                    foreign_key = bool(exp.foreign_keys)
                    annotated_col = issubclass(type(exp), sqlalchemy.sql.annotation.AnnotatedColumn)
                    if annotated_col and not primary_key and not foreign_key:
                        if column_name in columns:
                            # TODO log.WARN
                            # print("multiple "+column_name)
                            duplicated_columns.append(column_name)

                        columns[column_name] = Column(column_name, model_class.__table__, column, model_class)
    columns = {k: v for k, v in columns.items() if k not in duplicated_columns}
    return columns


column_dict = get_column_dict()
# print(column_dict)
