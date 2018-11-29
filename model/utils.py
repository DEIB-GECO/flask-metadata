import flask
from neo4jrestclient.client import GraphDatabase
# noinspection PyUnresolvedReferences
from neo4jrestclient.constants import DATA_GRAPH, DATA_ROWS, RAW


class Column:

    def __init__(self, table_name, column_name, column_type, has_tid=False):
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.has_tid = has_tid

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)




def run_query(cypher_query, returns=None, data_contents=None):
    gdb = GraphDatabase("http://localhost:7474", username='neo4j', password='yellow')
    flask.current_app.logger.info('connected')

    result = gdb.query(cypher_query, returns=returns, data_contents=data_contents)
    return result


# the view order definitions
biological_view_tables = ['Replicate', 'Biosample', 'Donor']
management_view_tables = ['Case', 'Project']
technological_view_tables = ['ExperimentType']
extraction_view_tables = ['Dataset']

columns = [
    Column('Biosample', 'type', str),
    Column('Biosample', 'tissue', str),
    Column('Biosample', 'cell_line', str),
    Column('Biosample', 'is_healthy', bool),
    Column('Biosample', 'disease', str),

    Column('CaseStudy', 'source_site', str),
    Column('CaseStudy', 'external_ref', str),

    Column('Dataset', 'name', str),
    Column('Dataset', 'data_type', str),
    Column('Dataset', 'format', str),
    Column('Dataset', 'assembly', str),
    Column('Dataset', 'annotation', str),

    Column('Donor', 'species', str),
    Column('Donor', 'age', int),
    Column('Donor', 'gender', str),
    Column('Donor', 'ethnicity', str),

    Column('ExperimentType', 'technique', str),
    Column('ExperimentType', 'feature', str),
    Column('ExperimentType', 'target', str),
    Column('ExperimentType', 'antibody', str),

    Column('Item', 'platform', str),
    Column('Item', 'pipeline', str),

    Column('Project', 'program_name', str),
    Column('Project', 'project_name', str),

    Column('Replicate', 'bio_replicate_num', int),
    Column('Replicate', 'tech_replicate_num', int),
]

columns_dict = {x.column_name: x for x in columns}

del columns

# print(columns_dict)

# import inspect
# 
# import sqlalchemy
# 
# from model import models
# from model.models import db
# 
# 
# class ColumnOld:
# 
#     def __init__(self, column_name, table_name, db_column, table_class, column_type):
#         self.column_name = column_name
#         self.table_name = table_name
#         self.db_column = db_column
#         self.table_class = table_class
#         self.column_type = column_type
# 
#     def __str__(self):
#         return str({
#             'table_name': self.table_name,
#             'column_type': str(self.column_type),
#             'column_name': self.column_name,
#         })
# 
#     def __repr__(self):
#         return str({
#             'column_name': self.column_name,
#             'table_name': self.table_name,
#             'column_type': str(self.column_type)
#         })
# 
# 
# def get_column_dict():
#     columns = dict()
#     duplicated_columns = []  # the columns with the same name available in different tables
#     tid_columns = []
#     exclude_columns = ['size', 'checksum', 'source_url', 'local_url', 'date', 'is_ann']
#     for class_name, model_class in inspect.getmembers(models):
#         if inspect.isclass(model_class) and issubclass(model_class, db.Model):
#             for column_name, column in list(model_class.__dict__.items()):
#                 if issubclass(type(column), sqlalchemy.orm.attributes.InstrumentedAttribute):
#                     exp = column.expression
#                     primary_key = exp.primary_key
#                     foreign_key = bool(exp.foreign_keys)
#                     annotated_col = issubclass(type(exp), sqlalchemy.sql.annotation.AnnotatedColumn)
#                     if annotated_col and not primary_key and not foreign_key:
#                         if column_name in columns:
#                             # TODO log.WARN
#                             # print("multiple "+column_name)
#                             duplicated_columns.append(column_name)
#                         if column_name.endswith('_tid'):
#                             tid_columns.append(column_name)
# 
#                         # columns[column_name] = Column(column_name, model_class.__tablename__, column, model_class)
#                         columns[column_name] = ColumnOld(column_name, class_name, column, model_class, exp.type)
# 
#     columns = {k: v for k, v in columns.items() if k not in duplicated_columns}
#     columns = {k: v for k, v in columns.items() if k not in tid_columns}
#     columns = {k: v for k, v in columns.items() if k not in exclude_columns}
#     return columns
# 
# 
# def get_column_table(column_dict):
#     columns = dict()
#     for column in column_dict.values():
#         table_name = column.table_name
#         column_type = column.column_type
# 
#         if table_name == 'CaseStudy':
#             table_name = 'Case'
# 
#         columns[column.column_name] = (table_name, column_type)
#         # TODO check
# 
#     return columns
# 
# 
# column_dict = get_column_dict()
# column_table_dict = get_column_table(column_dict)
# print(column_dict)
# print(column_table_dict)
# 
# for x in column_dict.values():
#     # print("'" + x[0] + "'" + ':' + str(x[1]) + ',')
#     # print(x)
#     print(f"'{x.column_name}':Column('{x.table_name}','{x.column_name}','{x.column_type}'),")
# 
# # for x in column_table_dict.items():
# #     print(x)
