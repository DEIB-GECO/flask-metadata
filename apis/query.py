from flask_restplus import Namespace, Resource, fields
from neo4jrestclient import constants
from neo4jrestclient.client import GraphDatabase
from sqlalchemy import String

from model.utils import column_table_dict, \
    biological_view_tables, \
    management_view_tables, \
    technological_view_tables, \
    extraction_view_tables

api = Namespace('query', description='Value related operations')

query = api.model('Query', {
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


#
# queries = api.model('Values', {
#     'values': fields.Nested(value, required=True, description='Values'),
#     'info': fields.Nested(info, required=False, description='Info', skip_none=True),
# })
#



@api.route('/')
@api.response(404, 'Field not found')  # TODO correct
class Query(Resource):
    @api.doc('return_query_result')
    @api.marshal_with(query)
    @api.expect(query)  # TODO correct this one
    def post(self):
        '''List all values'''

        filter_in = api.payload

        cypher_query = query_generator(filter_in)
        print(cypher_query)

        gdb = GraphDatabase("http://localhost:17474", username='neo4j', password='yellow')
        print('connected')

        results = gdb.query(cypher_query, data_contents=constants.DATA_ROWS)

        print('got results')

        if results.rows:
            results = [merge_dicts(x) for x in results.rows]
        else:
            results = []

        # print(results)

        return results


def query_generator(filter_in):
    # set of distinct tables in the query
    filter_tables = set()
    for (column, values) in filter_in.items():
        (table_name, _) = column_table_dict[column]
        filter_tables.add(table_name)

    filter_bio_tables = [x for x in biological_view_tables if x in filter_tables]
    filter_mngm_tables = [x for x in management_view_tables if x in filter_tables]
    filter_tech_tables = technological_view_tables  # [x for x in tech_tables if x in filter_tables]
    filter_extract_tables = extraction_view_tables  # [x for x in extract_tables if x in filter_tables]
    filter_all_view_tables = (filter_bio_tables, filter_mngm_tables, filter_tech_tables, filter_extract_tables)

    filter_all_view_tables = [x for x in filter_all_view_tables if len(x) > 0]

    # list of sub_queries
    sub_queries = []
    for (i, l) in enumerate(filter_all_view_tables):
        sub_query = 'p%d = (it:Item)' % (i)
        for table_name in l:
            var_name = table_name[:2].lower()
            sub_query = sub_query + '-[*..3]->({var_name}:{table_name})'.format(var_name=var_name,
                                                                                table_name=table_name)
        sub_queries.append(sub_query)

    sub_where = []
    for (column, values) in filter_in.items():
        (table_name, column_type) = column_table_dict[column]

        var_name = table_name[:2].lower()
        sub_or = 'OR %s.%s IS NULL' % (var_name, column) if None in values else ''
        values_wo_none = [x for x in values if x is not None]
        to_lower = 'TOLOWER' if type(column_type) == String else ''
        sub_where.append(' ({to_lower}({var_name}.{column}) IN {values_wo_none} {sub_or})'
                         .format(to_lower=to_lower,
                                 var_name=var_name,
                                 column=column,
                                 values_wo_none=values_wo_none,
                                 sub_or=sub_or))

    cypher_query = ' MATCH '
    cypher_query += ', '.join(sub_queries)
    if sub_where:
        cypher_query += 'WHERE ' + ' AND '.join(sub_where)
    cypher_query += ' RETURN it, ex, da '

    cypher_query += ' LIMIT 100 '
    return cypher_query






def merge_dicts(dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
