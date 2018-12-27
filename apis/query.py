import flask
from flask import Response
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
    'content_type': fields.String,

    'name': fields.String,
    'data_type': fields.String,
    'format': fields.String,
    'assembly': fields.String,
    'is_ann': fields.String,

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
    @api.doc('return_query_result1')
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


count_result = api.model('QueryResult', {
    'name': fields.String,
    'count': fields.Integer,
})


# TODO check code repetition
@api.route('/count/dataset')
@api.response(404, 'Field not found')  # TODO correct
class QueryCountDataset(Resource):
    @api.doc('return_query_result2')
    @api.marshal_with(count_result)
    @api.expect(parser)  # TODO correct this one
    def post(self):
        '''Count all values'''

        args = parser.parse_args()
        voc = args['voc']

        filter_in = api.payload

        cypher_query = query_generator(filter_in, voc, 'count-dataset')
        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_ROWS)

        flask.current_app.logger.info('got results')

        # result_columns = results.columns
        results = results.rows

        if results:
            results = [{'name': x[0], 'count': x[1]} for x in results]
        else:
            results = []

        # print(results)

        return results


# TODO check code repetition
@api.route('/count/source')
@api.response(404, 'Field not found')  # TODO correct
class QueryCountSource(Resource):
    @api.doc('return_query_result3')
    @api.marshal_with(count_result)
    @api.expect(parser)  # TODO correct this one
    def post(self):
        '''Count all values'''

        args = parser.parse_args()
        voc = args['voc']

        filter_in = api.payload

        cypher_query = query_generator(filter_in, voc, 'count-source')
        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_ROWS)

        flask.current_app.logger.info('got results')

        # result_columns = results.columns
        results = results.rows

        if results:
            results = [{'name': x[0], 'count': x[1]} for x in results]
        else:
            results = []

        # print(results)

        return results


# TODO check code repetition
@api.route('/download')
@api.response(404, 'Field not found')  # TODO correct
class QueryCountSource(Resource):
    @api.doc('return_query_result4')
    @api.expect(parser)  # TODO correct this one
    def post(self):
        '''Items local url'''

        args = parser.parse_args()
        voc = args['voc']

        filter_in = api.payload

        cypher_query = query_generator(filter_in, voc, 'download-links')
        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_ROWS)

        flask.current_app.logger.info('got results')

        # result_columns = results.columns
        results = results.rows

        results = [x[0] for x in results]

        results = [x.replace("www.gmql.eu", "genomic.deib.polimi.it") for x in results]
        results = [x + "?authToken=DOWNLOAD-TOKEN" for x in results]

        results = '\n'.join(results)
        # if results:
        #     results = [{'name': x[0], 'count': x[1]} for x in results]
        # else:
        #     results = []

        # print(results)

        return Response(results, mimetype='text/plain')


def query_generator(filter_in, voc, return_type='table'):
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

    # list of sub_queries
    sub_matches = []
    for (i, (view_name, tables)) in enumerate(filter_all_view_tables.items()):
        sub_query = ''
        # sub_query += f'p{i} = '
        sub_query += '(it)'
        pre_table = 'Item'
        for table_name in tables:
            distance = calc_distance(view_name, pre_table, table_name)
            if distance > 1:
                dist = f'[*..{distance}]'
            else:
                dist = ''
            var_table_par = var_table(table_name)
            sub_query += f'-{dist}->({var_table_par}:{table_name})'
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

            # TODO
            # OPTIONAL MATCH (do)-->(:Vocabulary)-->(s_sp:Synonym)
            # ******* WHERE (TOLOWER(s_sp.label) IN ['homo sapiens', 'man', 'human']) ***********
            # WITH *
            # WHERE ( (TOLOWER(do.species) IN ['homo sapiens', 'man', 'human']) OR  (TOLOWER(s_sp.label) IN ['homo sapiens', 'man', 'human']))

            sub_optional_match.append(optional)
        else:
            where_part = create_where_part(column, values, False)
            sub_where.append(where_part)

    cypher_query = 'MATCH (it:Item), '
    cypher_query += ', '.join(sub_matches)
    if sub_where:
        cypher_query += 'WHERE ' + ' AND '.join(sub_where)
    if sub_optional_match:
        cypher_query += ' ' + ''.join(sub_optional_match)

    cypher_query += ' WITH DISTINCT it, ex, da'

    if return_type == 'table':
        cypher_query += ' RETURN *'
        cypher_query += ' LIMIT 100 '
    elif return_type == 'count-dataset':
        cypher_query += ' RETURN da.name, count(*) '
        cypher_query += ' ORDER BY da.name '
    elif return_type == 'count-source':
        cypher_query += ' RETURN da.source, count(*) '
        cypher_query += ' ORDER BY da.source '
    elif return_type == 'download-links':
        cypher_query += ' WHERE it.local_url is not null '
        cypher_query += ' RETURN it.local_url '

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
