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


def unfold_list(res):
    return [item for sublist in res for item in sublist]


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

    Column('Case', 'source_site', str),
    Column('Case', 'external_ref', str),

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
