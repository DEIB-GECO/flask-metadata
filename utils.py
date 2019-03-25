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

    # Column('CaseStudy', 'external_reference', str, False, "Identifiers from alternative data sources",
    #        "Alternative ID"),
    Column('CaseStudy', 'source_site', str, False, "Physical site where material was analysed"),

    # extraction
    Column('Dataset', 'data_type', str, False, "Specific kind of genomic data contained in the items"),
    Column('Dataset', 'assembly', str, False, "Reference genome alignment"),
    Column('Dataset', 'file_format', str, False, "Standard data format used in the region items"),
    Column('Dataset', 'is_annotation', bool, False, "True for annotations, False for experimental items"),
    Column('Dataset', 'dataset_name', str, False, "Directory in which items are stored for tertiary analysis"),

    Column('Item', 'content_type', str, True, "Type of represented regions"),
    Column('Item', 'platform', str, True, "Instrument used to sequence the raw data related to the items"),
    Column('Item', 'pipeline', str, False, "Methods used for processing phases, from raw data to processed data"),

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
columns_item = list(columns)

columns_item.extend((
    Column('Item', 'item_source_id', str, False, ""),
    Column('Item', 'size', str, False, ""),
    Column('Item', 'date', str, False, ""),
    Column('Item', 'source_url', str, False, ""),
    Column('Item', 'local_url', str, False, "")
))

columns_dict = {x.column_name: x for x in columns}

columns_dict_item = {x.column_name: x for x in columns_item}

# TODO uncomment if there are replications on the management view,
#  and create a query that takes care for different views
agg_tables = views['biological'][1:]  # +views['management'][1:]

del columns


# print([x.var_column() for x in columns_dict.values() if x.has_tid])

def sql_query_generator(gcm_query, search_type, pairs_query, return_type, agg=False, field_selected=""):
    select_part = ""
    from_part = " FROM item it " \
                "join dataset da on it.dataset_id = da.dataset_id " \
                " join experiment_type ex on it.experiment_type_id= ex.experiment_type_id" \
                " join replicate2item r2i on it.item_id = r2i.item_id" \
                " join replicate rep on r2i.replicate_id = rep.replicate_id" \
                " join biosample bi on rep.biosample_id = bi.biosample_id" \
                " join donor don on bi.donor_id = don.donor_id" \
                " join case2item c2i on it.item_id = c2i.item_id" \
                " join case_study cs on c2i.case_study_id = cs.case_study_id" \
                " join project pr on cs.project_id = pr.project_id"
    where_part = generate_where_sql(gcm_query, search_type)

    sub_where_part = ""
    group_by_part = ""
    limit = ""
    if return_type == 'table':
        if agg:
            select_part = "SELECT " + ",".join(
                x.column_name for x in columns_dict_item.values() if x.table_name not in agg_tables) + " "

            select_part += "," + ','.join(
                "STRING_AGG(COALESCE(" + x.column_name + "::VARCHAR,'N/D'),' | ' ORDER BY item_source_id) as "
                + x.column_name for x in columns_dict_item.values() if x.table_name in agg_tables)
            group_by_part = " GROUP BY " + ",".join(
                x.column_name for x in columns_dict_item.values() if x.table_name not in agg_tables)

        else:
            select_part = "SELECT " + ','.join(columns_dict_item.keys()) + " "
        limit = " LIMIT 1000 "
    elif return_type == 'count-dataset':
        select_part = "SELECT da.dataset_name as name, count(distinct it.item_id) as count "
        group_by_part = " GROUP BY da.dataset_name"

    elif return_type == 'count-source':
        select_part = "SELECT pr.source as name, count(distinct it.item_id) as count "
        group_by_part = " GROUP BY pr.source"

    elif return_type == 'download-links':
        select_part = "SELECT distinct it.local_url "
        if where_part:
            sub_where_part = " AND local_url IS NOT NULL "
        else:
            sub_where_part = " WHERE local_url IS NOT NULL "

    elif return_type == 'gmql':
        select_part = "SELECT dataset_name, array_agg(file_name) "
        if where_part:
            sub_where_part = " AND local_url IS NOT NULL "
        else:
            sub_where_part = " WHERE local_url IS NOT NULL "
        group_by_part = "GROUP BY dataset_name"

    elif return_type == 'field_value':
        select_part = f"SELECT {field_selected} as label, it.item_id as item "

    elif return_type == 'field_value_syn':
        select_part = f"SELECT label, it.item_id as item "
        from_part += f" join synonym syn on {field_selected}_tid = syn.tid "
        if where_part:
            sub_where_part = " AND type <> 'RELATED' "
        else:
            sub_where_part = " WHERE type <> 'RELATED' "

    return select_part + from_part + where_part + sub_where_part + group_by_part + limit


def generate_where_sql(gcm_query, search_type):
    sub_where = []
    where_part = ""
    if gcm_query:
        where_part = " WHERE ("

    for (column, values) in gcm_query.items():
        col = columns_dict_item[column]
        column_type = col.column_type
        lower_pre = 'LOWER(' if column_type == str else ''
        lower_post = ')' if column_type == str else ''
        syn_sub_where = []
        if search_type == 'synonym' and col.has_tid:
            syn_sub_where = [f"{col.column_name}_tid in (SELECT tid FROM synonym WHERE LOWER(label) = '{value}')" for
                             value in values
                             if value is not None]
        elif search_type == 'expanded' and col.has_tid:
            syn_sub_where = [f"{col.column_name}_tid in (SELECT tid_descendant "
                             f"FROM relationship_unfolded WHERE tid_ancestor in "
                             f"(SELECT tid FROM synonym WHERE LOWER(label) = '{value}'))" for
                             value in values
                             if value is not None]

        sub_sub_where = [f"{lower_pre}{column}{lower_post} = '{value}'" for value in values if value is not None]
        sub_sub_where_none = [f"{column} IS NULL" for value in values if value is None]
        sub_sub_where.extend(sub_sub_where_none)
        sub_sub_where.extend(syn_sub_where)
        sub_where.append(" OR ".join(sub_sub_where))

    if gcm_query:
        where_part += ") AND (".join(sub_where) + ")"
    return where_part
