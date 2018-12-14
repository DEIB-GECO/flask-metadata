import flask
from flask_restplus import Namespace, Resource, fields, inputs
from neo4jrestclient import constants

from utils import columns_dict, \
    run_query, views, calc_distance, var_table

api = Namespace('query', description='Query related operations')

query_result = api.model('QueryResult', {
    'source_id': fields.String,
    'size': fields.String,
    'date': fields.String,
    'pipeline': fields.String,
    'platform': fields.String,
    'source_url': fields.String,
    'local_url': fields.String,

    'name': fields.String,
    'data_type': fields.String,
    'format': fields.String,
    'assembly': fields.String,
    'annotation': fields.String,

    'technique': fields.String,
    'feature': fields.String,
    'target': fields.String,
    'antibody': fields.String
})

query = api.model('Query', {
    # 'values': fields.Nested(value, required=True, description='Values'),
    # 'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean, help='Has vocabulary (true/false)', default=False)
parser.add_argument('body', type="json", help='json ', location='json', )


@api.route('/table')
@api.response(404, 'Field not found')  # TODO correct
class Query(Resource):
    @api.doc('return_query_result')
    @api.marshal_with(query_result)
    @api.expect(parser)  # TODO correct this one
    def post(self):
        '''List all values'''

        args = parser.parse_args()
        voc = args['voc']

        filter_in = api.payload

        cypher_query = query_generator(filter_in, voc)
        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_ROWS)

        flask.current_app.logger.info('got results')

        # result_columns = results.columns
        results = results.rows

        if results:
            results = [merge_dicts(x) for x in results]
        else:
            results = []

        # print(results)

        return results


def query_generator(filter_in, voc):
    # set of distinct tables in the query, the result must have always ...
    filter_tables = set()
    filter_tables.add('Dataset')
    filter_tables.add('ExperimentType')
    for (column, values) in filter_in.items():
        table_name = columns_dict[column].table_name
        filter_tables.add(table_name)

    filter_all_view_tables = {}
    for (view_name, view_tables) in views.items():
        # exclude Item
        tables = [x for x in view_tables[1:] if x in filter_tables]
        if len(tables):
            filter_all_view_tables[view_name] = tables

    # filter_bio_tables = [x for x in view_tables['biological'] if x in filter_tables]
    # filter_mngm_tables = [x for x in view_tables['management'] if x in filter_tables]
    # filter_tech_tables = view_tables['technological']  # [x for x in view_tables['technological'] if x in filter_tables]
    # filter_extract_tables = view_tables['extraction']  # [x for x in view_tables['extraction'] if x in filter_tables]
    # filter_all_view_tables = (filter_bio_tables, filter_mngm_tables, filter_tech_tables, filter_extract_tables)
    # filter_all_view_tables = [x for x in filter_all_view_tables if len(x) > 0]

    # list of sub_queries
    sub_matches = []
    for (i, (view_name, tables)) in enumerate(filter_all_view_tables.items()):
        sub_query = f'p{i} = (it)'
        pre_table = 'Item'
        for table_name in tables:
            distance = calc_distance(view_name, pre_table, table_name)
            if distance > 1:
                dist = f"[*..{distance}]"
            else:
                dist = ""
            var_table_par = var_table(table_name)
            sub_query = sub_query + f'-{dist}->({var_table_par}:{table_name})'
            pre_table = table_name

        sub_matches.append(sub_query)

    # list of sub_where, if the column is
    sub_where = []
    sub_optional_match = []
    for (column, values) in filter_in.items():
        col = columns_dict[column]

        if voc and col.has_tid:
            var_table_par = col.var_table()
            var_column_par = col.var_column()

            where_part1 = create_where_part(column, values, False)
            where_part2 = create_where_part(column, values, True)

            optional = f"OPTIONAL MATCH ({var_table_par})-->(:Vocabulary)-->(s_{var_column_par}:Synonym) " \
                "WITH * " \
                f"WHERE ({where_part1} OR {where_part2}) "

            sub_optional_match.append(optional)
        else:
            where_part = create_where_part(column, values, False)
            sub_where.append(where_part)

    cypher_query = 'MATCH (it:Item), '
    cypher_query += ', '.join(sub_matches)
    if sub_where:
        cypher_query += 'WHERE ' + ' AND '.join(sub_where)
    if sub_optional_match:
        cypher_query += ' ' +  ''.join(sub_optional_match)

    cypher_query += ' RETURN it, ex, da'

    cypher_query += ' LIMIT 100 '
    return cypher_query


def create_where_part(column, values, is_syn):
    col = columns_dict[column]
    column_type = col.column_type
    var_name = col.var_table()
    var_col_name = col.var_column()
    if is_syn:
        var_name = "s_" + var_col_name
        column = 'label'
        sub_or = ''
    else:
        sub_or = f' OR {var_name}.{column} IS NULL' if None in values else ''
    values_wo_none = [x for x in values if x is not None]

    to_lower_pre = 'TOLOWER(' if column_type == str else ''
    to_lower_post = ')' if column_type == str else ''
    return f' ({to_lower_pre}{var_name}.{column}{to_lower_post} IN {values_wo_none}{sub_or})'


def merge_dicts(dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
