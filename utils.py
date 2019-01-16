from neo4jrestclient.client import GraphDatabase
# noinspection PyUnresolvedReferences
from neo4jrestclient.constants import DATA_GRAPH, DATA_ROWS, RAW


class Column:

    def __init__(self, table_name, column_name, column_type, has_tid=False, description=""):
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.has_tid = has_tid
        self.description = description

    def var_table(self):
        return var_table(self.table_name)

    def var_column(self):
        return self.column_name[:2].lower()

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return str(self.__dict__)


def var_table(table_name):
    return table_name[:2].lower()


def run_query(cypher_query, returns=None, data_contents=None):
    gdb = GraphDatabase("http://localhost:7474", username='neo4j', password='yellow')

    result = gdb.query(cypher_query, returns=returns, data_contents=data_contents)
    return result


def unfold_list(res):
    return [item for sublist in res for item in sublist]


# the view order definitions
views = {
    'biological': ['Item', 'Replicate', 'Biosample', 'Donor'],
    'management': ['Item', 'Case', 'Project'],
    'technological': ['Item', 'ExperimentType'],
    'extraction': ['Item', 'Dataset'],
}


def calc_distance(view_name, pre_table, table_name):
    view = views[view_name]
    return view.index(table_name) - view.index(pre_table)


columns = [
    Column('Biosample', 'type', str, False),
    Column('Biosample', 'tissue', str, True),
    Column('Biosample', 'cell', str, True),
    Column('Biosample', 'is_healthy', bool, False),
    Column('Biosample', 'disease', str, True),

    Column('Case', 'source_site', str, False),
    Column('Case', 'external_ref', str, False),

    Column('Dataset', 'name', str, False),
    Column('Dataset', 'data_type', str, False),
    Column('Dataset', 'format', str, False),
    Column('Dataset', 'assembly', str, False),
    Column('Dataset', 'is_ann', bool, False),

    Column('Donor', 'species', str, True),
    Column('Donor', 'age', int, False),
    Column('Donor', 'gender', str, False),
    Column('Donor', 'ethnicity', str, True),

    Column('ExperimentType', 'technique', str, True),
    Column('ExperimentType', 'feature', str, True),
    Column('ExperimentType', 'target', str, True),
    Column('ExperimentType', 'antibody', str),

    Column('Item', 'platform', str, True),
    Column('Item', 'pipeline', str),
    Column('Item', 'content_type', str, True),  # TO BE ADDED

    Column('Project', 'program_name', str, False),
    Column('Project', 'project_name', str, False),

    Column('Replicate', 'bio_replicate_num', int, False),
    Column('Replicate', 'tech_replicate_num', int, False),
]

columns_dict = {x.column_name: x for x in columns}

del columns

# print([x.var_column() for x in columns_dict.values() if x.has_tid])
