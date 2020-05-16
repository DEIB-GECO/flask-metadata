import datetime
import os
import time
from collections import OrderedDict

from flask import request

center_table = 'Sequence'
center_table_id = 'sequence_id'

# the view order definitions
views = {
    'biological_v': [center_table, 'Virus'],
    'biological_h': [center_table, 'HostSample'],
    'technological': [center_table, 'ExperimentType'],
    'organizational': [center_table, 'SequencingProject'],
    # 'analytical_a': [center_table, 'Annotation', 'AminoAcidVariant'],
    # 'analytical_v': [center_table, 'Variant'],

}


def get_view(table):
    if table == "Sequence":
        return "technological"
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



def unfold_list(res):
    return [item for sublist in res for item in sublist]


def calc_distance(view_name, pre_table, table_name):
    view = views[view_name]
    return view.index(table_name) - view.index(pre_table)


columns = [
#    def __init__(self, table_name, column_name, column_type, has_tid=False, description="", title=None):

    # technological
    Column('Sequence', 'strain_name', str, False, "Sequence-strain_name description"),
    Column('Sequence', 'is_reference', bool, False, "Sequence-is_reference description"),
    Column('Sequence', 'is_complete', bool, False, "Sequence-is_complete description"),
    Column('Sequence', 'strand', str, False, "Sequence-strand description"),
    Column('Sequence', 'length', int, False, "Sequence-length description"),
    Column('Sequence', 'gc_percentage', float, False, "Sequence-gc_percentage description"),

    Column('ExperimentType', 'sequencing_technology', str, False, "ExperimentType-sequencing_technology description"),
    Column('ExperimentType', 'assembly_method', str, False, "ExperimentType-assembly_method description"),
    Column('ExperimentType', 'coverage', str, False, "ExperimentType-coverage description"),

    # organizational
    Column('SequencingProject', 'sequencing_lab', str, False, "ExperimentType-sequencing_lab description"),
    Column('SequencingProject', 'submission_date', datetime, False, "ExperimentType-submission_date description"),
    Column('SequencingProject', 'bioproject_id', str, False, "ExperimentType-bioproject_id description"),
    Column('SequencingProject', 'database_source', str, False, "ExperimentType-database_source description"),

    # biological
    Column('Virus', 'taxon_id', int, False, "Virus-virus_taxonomy_id description", "Virus taxon id"),
    Column('Virus', 'taxon_name', str, False, "Virus-virus_taxonomy_id description", "Virus taxon name"),
    Column('Virus', 'species', str, False, "Virus-species_name description", "Virus species"),

    Column('HostSample', 'host_taxon_name', str, False, "HostSample-host_taxon_name description"),
    Column('HostSample', 'collection_date', str, False, "HostSample-collection_date description"),
    Column('HostSample', 'isolation_source', str, False, "HostSample-isolation_source description"),
    Column('HostSample', 'country', str, False, "HostSample-country description"),
    Column('HostSample', 'region', str, False, "HostSample-region description"),
    Column('HostSample', 'gender', str, False, "HostSample-gender description"),
    Column('HostSample', 'age', int, False, "HostSample-age description"),
]

columns_item = list(columns)

columns_item.extend((
    Column('Sequence', 'accession_id', str, False, ""),
    Column('Sequence','nucleotide_sequence',str, False, ""),
    Column('HostSample', 'host_taxon_id', int, False, "HostSample-host_taxon_id description"),
    Column('HostSample', 'originating_lab', str, False, "HostSample-originating_lab description"),
    Column('HostSample', 'geo_group', str, False, "HostSample-geo_group description"),
    Column('Virus', 'genus', str, False, "Virus-genus description"),
    Column('Virus', 'sub_family', str, False, "Virus-sub_family description"),
    Column('Virus', 'family', str, False, "Virus-family description"),
    Column('Virus', 'equivalent_list', str, False, "Virus-equivalent_list description"),
    Column('Virus', 'molecule_type', str, False, "Virus-molecule_type description"),
    Column('Virus', 'is_single_stranded', str, False, "Virus-is_single_stranded description"),
    Column('Virus', 'is_positive_stranded', str, False, "Virus-is_positive_stranded description")
))

columns_dict = {x.column_name: x for x in columns}

columns_dict_item = {x.column_name: x for x in columns_item}

# TODO uncomment if there are replications on the management view,
#  and create a query that takes care for different views
# TODO VIRUS
agg_tables = [] #views['biological'][1:]  # +views['management'][1:]

del columns



def sql_query_generator(gcm_query, search_type, pairs_query, return_type, agg=False, field_selected="", limit=1000,
                        offset=0, order_col="accession_id", order_dir="ASC", rel_distance=3):

    # TODO VIRUS PAIRS
    # pairs = generate_where_pairs(pairs_query)
    pairs = generate_where_pairs({})
    pair_join = pairs['join']
    pair_where = pairs['where']

    select_part = ""
    from_part = ""
    item = " FROM sequence it "
    # dataset_join = " join dataset da on it.dataset_id = da.dataset_id "

    experiment_type_join = " join experiment_type ex on it.experiment_type_id = ex.experiment_type_id"

    sequencing_project_join = " join sequencing_project sp on it.sequencing_project_id = sp.sequencing_project_id"

    host_sample_join = " join host_sample hs on it.host_sample_id = hs.host_sample_id"

    virus_join = " join virus v on it.virus_id = v.virus_id"

    # replicate_join = " join replicate2item r2i on it.item_id = r2i.item_id" \
    #                  " join dw.replicate rep on r2i.replicate_id = rep.replicate_id"
    #
    # biosample_join = " join biosample bi on rep.biosample_id = bi.biosample_id"
    #
    # donor_join = " join donor don on bi.donor_id = don.donor_id"
    #
    # case_join = " join case2item c2i on it.item_id = c2i.item_id" \
    #             " join case_study cs on c2i.case_study_id = cs.case_study_id"
    #
    # project_join = " join project pr on cs.project_id = pr.project_id"

    view_join = {
        # TODO VIRUS
        'biological_h': [host_sample_join],
        'biological_v': [virus_join],
        'organizational': [sequencing_project_join],
        'technological': [experiment_type_join],
    }
    if field_selected != "":
        columns = [x for x in gcm_query.keys()]
        columns.append(field_selected)
        tables = [columns_dict_item[x].table_name for x in columns]
        joins = []
        for table in tables:
            # table = columns_dict_item[field_selected].table_name
            view = get_view(table)
            index = calc_distance(view, center_table, table)
            joins += view_join[view][:index]
        joins = list(OrderedDict.fromkeys(joins))
        from_part = item + " ".join(joins) + pair_join
    else:
        # TODO VIRUS add all tables
        from_part = item + experiment_type_join + sequencing_project_join + host_sample_join + virus_join + pair_join



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
                "STRING_AGG(DISTINCT COALESCE(" + x.column_name + "::VARCHAR,'N/D'),' | ' ) as "
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
    # elif return_type == 'count-dataset':
    #     select_part = "SELECT da.dataset_name as name, count(distinct it.item_id) as count "
    #     group_by_part = " GROUP BY da.dataset_name"
    #
    # elif return_type == 'count-source':
    #     select_part = "SELECT pr.source as name, count(distinct it.item_id) as count "
    #     group_by_part = " GROUP BY pr.source"

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
        # if search_type == 'original':
        distinct = "distinct"
        select_part = f"SELECT {distinct} {lower_pre}{field_selected}{lower_post} as label, it.{center_table_id} as item "

    elif return_type == 'field_value_tid':
        select_part = f"SELECT distinct LOWER(label), it.{center_table_id} as item "

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
        select_part = f"SELECT it.{center_table_id} "

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
            if min is None:
                min = -1

            max = values['max_age']
            if max is None:
                max = 500 * 365

            isNull = values['is_null']
            a = f" (age >= {min} and age <= {max}) "
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
        join = f" join unified_pair {kv} on it.{center_table_id} = {kv}.{center_table_id} "
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





#IP ADDRESS AND QUERY LOGGING

ROOT_DIR = os.path.dirname(os.getcwd())
if not os.path.exists(ROOT_DIR + "/logs"):
    os.makedirs(ROOT_DIR + "/logs")
fn = ROOT_DIR + "/logs/count.log"
f = open(fn, 'a+')
header = "timestamp\tIP_address\tendpoint\tquery\tpayload\n"

f.seek(0)
firstline = f.read()

if firstline == '':
    f.write(header)

f.close()


def log_query(endpoint,q,payload):
    if 'HTTP_X_REAL_IP' in request.environ:
        addr = request.environ['HTTP_X_REAL_IP']
    else:
        addr = request.remote_addr
    ts = time.time()
    timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    data = timestamp + "\t" + addr + "\t" + endpoint + "\t" + q + "\t" + str(payload) + "\n"
    fi = open(fn, 'a+')
    fi.write(data)
    fi.close()



def ip_info(addr=''):
    from urllib.request import urlopen
    from json import load
    if addr == '':
        url = 'https://ipinfo.io/json'
    else:
        url = 'https://ipinfo.io/' + addr + '/json'
    res = urlopen(url)
    #response from url(if res==None then check connection)
    data = load(res)
    #will load the json response into data
    for attr in data.keys():
        #will print the data line by line
        print(attr,' '*13+'\t->\t',data[attr])
    print(data)

