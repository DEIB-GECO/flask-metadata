from neo4jrestclient.client import GraphDatabase
# noinspection PyUnresolvedReferences
from neo4jrestclient.constants import DATA_GRAPH, DATA_ROWS, RAW
from collections import OrderedDict

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
    gdb = GraphDatabase("http://localhost:27474", username='neo4j', password='yellow')

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
    Column('Item', 'content_type', str, True, "Type of represented regions"),
    Column('Item', 'platform', str, True, "Instrument used to sequence the raw data related to the items"),
    Column('Item', 'pipeline', str, False, "Methods used for processing phases, from raw data to processed data"),

    Column('Dataset', 'data_type', str, False, "Specific kind of genomic data contained in the items"),
    Column('Dataset', 'assembly', str, False, "Reference genome alignment"),
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
           "Interval of ages including the individual from which the biological sample was derived (or cell line established).",
           "Donor age"),
    Column('Donor', 'gender', str, False, "Gender/sex of the individual"),
    Column('Donor', 'ethnicity', str, True, "Ethnicity/race information of the individual"),
    Column('Donor', 'species', str, True,
           "Specific organism from which the biological sample was derived (or cell line established)"),

    Column('Replicate', 'biological_replicate_count', int, False,
           "Number of biosamples on which the experimental protocol was performed"),
    Column('Replicate', 'technical_replicate_count', int, False,
           "Number of distinct replicates from the same biosample (each treated identically)"),

    # technological
    Column('ExperimentType', 'technique', str, True, "Investigative procedure conducted to produce the items"),
    Column('ExperimentType', 'feature', str, True, "Specific genomic aspect described by the experiment"),
    Column('ExperimentType', 'target', str, True, "Gene or protein targeted by the experiment"),
    Column('ExperimentType', 'antibody', str, False, "Antibody protein against specific target"),

]
columns_item = list(columns)

columns_item.extend((
    Column('Item', 'item_source_id', str, False, ""),
    Column('Item', 'size', str, False, ""),
    Column('Item', 'date', str, False, ""),
    Column('Item', 'source_url', str, False, ""),
    Column('Item', 'local_url', str, False, ""),
    Column('Item', 'source_page', str, False, ""),
    Column('Replicate', 'biological_replicate_number', int, False,
           "Progressive number of biosample on which the experimental protocol was performed"),
    Column('Replicate', 'technical_replicate_number', int, False,
           "Progressive number of distinct replicates from the same biosample (each treated identically)"),
    Column('Donor', 'donor_source_id', str, False, ""),
    Column('Biosample', 'biosample_source_id', str, False, ""),
))

columns_dict = {x.column_name: x for x in columns}

columns_dict_item = {x.column_name: x for x in columns_item}

# TODO uncomment if there are replications on the management view,
#  and create a query that takes care for different views
agg_tables = views['biological'][1:]  # +views['management'][1:]

del columns


# print([x.var_column() for x in columns_dict.values() if x.has_tid])

def sql_query_generator(gcm_query, search_type, pairs_query, return_type, agg=False, field_selected="", limit=1000,
                        offset=0, order_col="item_source_id", order_dir="ASC", rel_distance=3):
    select_part = ""
    from_part = ""
    item = " FROM dw.item it "
    dataset_join = " join dataset da on it.dataset_id = da.dataset_id "

    pairs = generate_where_pairs(pairs_query)

    pair_join = pairs['join']
    pair_where = pairs['where']

    experiment_type_join = " join experiment_type ex on it.experiment_type_id= ex.experiment_type_id"

    replicate_join = " join replicate2item r2i on it.item_id = r2i.item_id" \
                     " join replicate rep on r2i.replicate_id = rep.replicate_id"

    biosample_join = " join biosample bi on rep.biosample_id = bi.biosample_id"

    donor_join = " join donor don on bi.donor_id = don.donor_id"

    case_join = " join case2item c2i on it.item_id = c2i.item_id" \
                " join case_study cs on c2i.case_study_id = cs.case_study_id"

    project_join = " join project pr on cs.project_id = pr.project_id"

    # joins = [dataset_join, experiment_type_join, replicate_join, biosample_join, donor_join, case_join, project_join]

    view_join = {
        'biological': [replicate_join, biosample_join, donor_join],
        'management': [case_join, project_join],
        'technological': [experiment_type_join],
        'extraction': [dataset_join],
    }
    if field_selected != "":
        columns = [x for x in gcm_query.keys()]
        columns.append(field_selected)
        tables = [columns_dict_item[x].table_name for x in columns]
        joins = []
        for table in tables:
            # table = columns_dict_item[field_selected].table_name
            view = get_view(table)
            index = calc_distance(view, 'Item', table)
            joins += view_join[view][:index]
        joins = list(OrderedDict.fromkeys(joins))
        from_part = item + " ".join(joins) + pair_join
    else:
        from_part = item + dataset_join + experiment_type_join + replicate_join + biosample_join + donor_join + case_join + project_join + pair_join

    gcm_where = generate_where_sql(gcm_query, search_type, rel_distance=rel_distance)

    where_part = ""

    if gcm_query and pair_where:
        where_part = gcm_where + " AND " + pair_where
    elif pair_where and not gcm_where:
        where_part = 'WHERE ' + pair_where
    elif gcm_where and not pair_where:
        where_part = gcm_where

    sub_where_part = ""
    group_by_part = ""
    limit_part = ""
    offset_part = ""
    order_by = ""
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
        if limit:
            limit_part = f" LIMIT {limit} "
        if offset:
            offset_part = f"OFFSET {offset} "
        order_by = f" ORDER BY {order_col} {order_dir} "
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
        col = columns_dict_item[field_selected]
        column_type = col.column_type
        lower_pre = 'LOWER(' if column_type == str else ''
        lower_post = ')' if column_type == str else ''
        distinct = ""
        if search_type == 'original':
            distinct = "distinct"
        select_part = f"SELECT {distinct} {lower_pre}{field_selected}{lower_post} as label, it.item_id as item "

    elif return_type == 'field_value_tid':
        select_part = f"SELECT LOWER(label), it.item_id as item "

        if search_type == 'synonym':
            from_part += f" join synonym syn on {field_selected}_tid = syn.tid "
        elif search_type == 'expanded':
            from_part += f" join relationship_unfolded rel on {field_selected}_tid = rel.tid_descendant "
            from_part += f" join synonym syn on rel.tid_ancestor = syn.tid "
        if where_part:
            sub_where_part = " AND type <> 'RELATED' "
            if search_type == 'expanded':
                sub_where_part += f" AND rel.distance <= {rel_distance} "
        else:
            sub_where_part = " WHERE type <> 'RELATED' "
            if search_type == 'expanded':
                sub_where_part += f" AND rel.distance <= {rel_distance} "
    elif return_type == 'item_id':
        select_part = f"SELECT it.item_id "

    return select_part + from_part + where_part + sub_where_part + group_by_part + order_by + limit_part + offset_part


def generate_where_sql(gcm_query, search_type, rel_distance=3):
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
            syn_sub_where = [f"{col.column_name}_tid in (SELECT tid FROM synonym WHERE LOWER(label) = LOWER('{value}'))"
                             for
                             value in values
                             if value is not None]
        elif search_type == 'expanded' and col.has_tid:
            syn_sub_where = [f"{col.column_name}_tid in (SELECT tid_descendant "
                             f"FROM relationship_unfolded WHERE distance <= {rel_distance} and tid_ancestor in "
                             f"(SELECT tid FROM synonym WHERE LOWER(label) = LOWER('{value}')))" for
                             value in values
                             if value is not None]
        if col.column_name == 'age':
            min = values['min_age']
            max = values['max_age']
            isNull = values['null']
            a = f" age >= {min} and age <= {max} "
            if isNull:
                a += "or age is null "
            sub_where.append(a)
        else:
            sub_sub_where = [f"{lower_pre}{column}{lower_post} = '{value}'" for value in values if value is not None]
            sub_sub_where_none = [f"{column} IS NULL" for value in values if value is None]
            sub_sub_where.extend(sub_sub_where_none)
            sub_sub_where.extend(syn_sub_where)
            sub_where.append(" OR ".join(sub_sub_where))

    if gcm_query:
        where_part += ") AND (".join(sub_where) + ")"
    return where_part


def generate_where_pairs(pair_query):
    searched = pair_query.keys()

    pair_join = []

    where = []
    i = 0
    for x in searched:
        kv = "kv_" + str(i)
        i += 1
        join = f" join unified_pair {kv} on it.item_id = {kv}.item_id "
        pair_join.append(join)
        items = pair_query[x]['query']
        gcm = items['gcm']
        pair = items['pairs']

        sub_where = []

        for k in gcm.keys():
            a = ""
            a += f" lower({kv}.key) = lower('{k}') and {kv}.is_gcm = true and "
            values = gcm[k]
            sub_sub_where = []
            for value in values:
                v = value.replace("'", "''")
                sub_sub_where.append(f"lower({kv}.value) = lower('{v}')")
            a += ("(" + " OR ".join(sub_sub_where) + ")")

            # print(a)
            sub_where.append(a)

        for k in pair.keys():
            a = ""
            a += f" lower({kv}.key) = lower('{k}') and {kv}.is_gcm = false and "
            values = pair[k]
            sub_sub_where = []
            for value in values:
                v = value.replace("'", "''")
                sub_sub_where.append(f"lower({kv}.value) = lower('{v}')")
            a += ("(" + " OR ".join(sub_sub_where) + ")")

            # print(a)
            sub_where.append(a)

        where.append("(" + ") OR (".join(sub_where) + ")")

    where_part = ""
    if pair_query:
        where_part = "(" + ") AND (".join(where) + ")"

    return {'where': where_part, 'join': " ".join(pair_join)}
