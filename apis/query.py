import flask
from flask import Response
from flask_restplus import Namespace, Resource, fields, inputs
from neo4jrestclient import constants

from utils import columns_dict, \
    run_query, views, calc_distance, var_table

api = Namespace('query', description='Operations to perform queries using metadata')

query_result = api.model('QueryResult', {
    'source_id': fields.String,
    'size': fields.String,
    'date': fields.String,
    'pipeline': fields.String,
    'platform': fields.String,
    'source_url': fields.String,
    'local_url': fields.String,
    'content_type': fields.String,

    'dataset_name': fields.String,
    'data_type': fields.String,
    'file_format': fields.String,
    'assembly': fields.String,
    'is_annotation': fields.String,

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
parser.add_argument('voc', type=inputs.boolean, help='Enable enriched search over controlled vocabulary terms and synonyms (true/false)', default=False)
parser.add_argument('body', type="json", help='json ', location='json')


parser_graph = api.parser()
parser_graph.add_argument('limit', type=int, default=5)
parser_graph.add_argument('biological_view',type=inputs.boolean)
parser_graph.add_argument('management_view',type=inputs.boolean)
parser_graph.add_argument('technological_view',type=inputs.boolean)
parser_graph.add_argument('extraction_view',type=inputs.boolean)
parser_graph.add_argument('body', type="json", help='json ', location='json')

query_results = []


@api.route('/graph')
@api.response(404, 'Field not found')  # TODO correct
class QueryGraph(Resource):
    @api.doc('return_query_graph')
    @api.expect(parser_graph)  # TODO correct this one
    def post(self):
        '''Generate graph'''

        args = parser_graph.parse_args()
        limit = args['limit']
        bioView = args['biological_view']
        mgmtView = args['management_view']
        techView = args['technological_view']
        extrView = args['extraction_view']
        include_views = []

        if bioView:
            include_views.append('biological')
        if mgmtView:
            include_views.append('management')
        if techView:
            include_views.append('technological')
        if extrView:
            include_views.append('extraction')

        filter_in = api.payload

        cypher_query = query_generator(filter_in, voc=False, return_type='graph',include_views=include_views,limit=limit)
        flask.current_app.logger.info(cypher_query)
        results = run_query(cypher_query, data_contents=constants.DATA_GRAPH)

        if len(results):
            return results.graph
        else:
            return api.abort(404, f'Not found')



@api.route('/table')
@api.response(404, 'Field not found')  # TODO correct
class Query(Resource):
    @api.doc('return_query_result1')
    @api.marshal_with(query_result)
    @api.expect(parser)  # TODO correct this one
    def post(self):
        '''For the posted query, it retrieves a list of items with selected characteristics'''

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
        '''For the posted query, it retrieves number of items aggregated by dataset'''

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
        '''For the posted query, it retrieves number of items aggregated by source'''

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
        '''For the items selected by the posted query, it retrieves URIs for download from our system'''

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


def query_generator(filter_in, voc, return_type='table', include_views=[], limit=1000):
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

    if return_type == 'table':
        cypher_query += ' WITH DISTINCT it, ex, da'
        cypher_query += ' RETURN *'
        cypher_query += f' LIMIT {limit} '
    elif return_type == 'count-dataset':
        cypher_query +=' WITH DISTINCT it, da'
        cypher_query += ' RETURN da.dataset_name, count(*) '
        cypher_query += ' ORDER BY da.dataset_name '
    elif return_type == 'count-source':
        cypher_query +=' WITH DISTINCT it, da'
        cypher_query += ' RETURN da.source, count(*) '
        cypher_query += ' ORDER BY da.source '
    elif return_type == 'download-links':
        cypher_query += ' WITH DISTINCT it'
        cypher_query += ' WHERE it.local_url is not null '
        cypher_query += ' RETURN it.local_url '
    elif return_type == 'graph':
        cypher_query += ' WITH DISTINCT it'
        cypher_query += f' LIMIT {limit}'
        pre_table = 'Item'
        cypher_query += ' MATCH (it: Item)'
        match_view_pre = ' (it)'
        return_part = ' return it'
        if len(include_views):
            for view in include_views:
                p_name = f'p_{view}'
                cypher_query += f', {p_name} ='
                last = views.get(view)[-1]
                cypher_query += match_view_pre
                distance = calc_distance(view, pre_table, last)
                if distance > 1:
                    dist = f'[*..{distance}]'
                else:
                    dist = ''
                cypher_query += f'-{dist}->({last[:2].lower()}:{last})'
                return_part += f', {p_name}'
        cypher_query += return_part

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
