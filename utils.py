from neo4jrestclient.client import GraphDatabase
# noinspection PyUnresolvedReferences
from neo4jrestclient.constants import DATA_GRAPH, DATA_ROWS, RAW

# the view order definitions
views = {
    'biological': ['Item', 'Replicate', 'Biosample', 'Donor'],
    'management': ['Item', 'CaseStudy', 'Project'],
    'technological': ['Item', 'ExperimentType'],
    'extraction': ['Item', 'Dataset'],
}


def get_view(table):
    if table == "Item":
        return "extraction"
    for view_name, view_tables in views.items():
        if table in view_tables:
            return view_name


class Column:

    def __init__(self, table_name, column_name, column_type, has_tid=False, description="", title=None):
        self.table_name = table_name
        self.column_name = column_name
        self.column_type = column_type
        self.has_tid = has_tid
        self.description = description
        self.title = title
        self.view = get_view(table_name)

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


def calc_distance(view_name, pre_table, table_name):
    view = views[view_name]
    return view.index(table_name) - view.index(pre_table)


columns = [
    # management
    Column('Project', 'source', str, False, "Program or consortia responsible for the production of items"),
    Column('Project', 'project_name', str, False, "Project context in which items are created"),

    Column('CaseStudy', 'external_reference', str, False, "Identifiers from alternative data sources",
           "Alternative ID"),
    Column('CaseStudy', 'source_site', str, False, "Physical site where material was analysed"),

    # extraction
    Column('Dataset', 'data_type', str, False, "Specific kind of genomic data contained in the items"),
    Column('Item', 'content_type', str, True, "Type of represented regions"),

    Column('Dataset', 'assembly', str, False, "Reference genome alignment"),

    Column('Item', 'platform', str, True, "Instrument used to sequence the raw data related to the items"),
    Column('Item', 'pipeline', str, False, "Methods used for processing phases, from raw data to processed data"),

    Column('Dataset', 'file_format', str, False, "Standard data format used in the region items"),
    Column('Dataset', 'is_annotation', bool, False, "True for annotations, False for experimental items"),

    Column('Dataset', 'dataset_name', str, False, "Directory in which items are stored for tertiary analysis"),

    # biological

    Column('Biosample', 'biosample_type', str, False, "Kind of material sample used for the experiment"),
    Column('Biosample', 'tissue', str, True,
           "Multicellular component in its natural state, or provenance tissue of cell"),
    Column('Biosample', 'cell', str, True,
           "Single cells (natural state), immortalized cell lines, or cells differentiated from specific cell types",
           "Cell/Cell line"),
    Column('Biosample', 'disease', str, True, "Illness investigated within the sample"),

    Column('Biosample', 'is_healthy', bool, False,
           "True for healthy/normal/control samples, False for non-healthy/tumoral samples", "Healthy/Control/Normal"),


    Column('Donor', 'age', int, False,
           "Age of individual from which the biological sample was derived (or cell line established)", "Donor age"),
    Column('Donor', 'gender', str, False, "Gender/sex of the individual"),
    Column('Donor', 'ethnicity', str, True, "Ethnicity/race information of the individual"),
    Column('Donor', 'species', str, True,
           "Specific organism from which the biological sample was derived (or cell line established)"),

    Column('Replicate', 'biological_replicate_number', int, False,
           "Progressive number of biosample on which the experimental protocol was performed"),
    Column('Replicate', 'technical_replicate_number', int, False,
           "Progressive number of distinct replicates from the same biosample (each treated identically)"),

    # technological

    Column('ExperimentType', 'technique', str, True, "Investigative procedure conducted to produce the items"),
    Column('ExperimentType', 'feature', str, True, "Specific genomic aspect described by the experiment"),
    Column('ExperimentType', 'target', str, True, "Gene or protein which is targeted by the experiment"),
    Column('ExperimentType', 'antibody', str, False, "Antibody protein against specific target"),

]

columns_dict = {x.column_name: x for x in columns}

del columns

# print([x.var_column() for x in columns_dict.values() if x.has_tid])
