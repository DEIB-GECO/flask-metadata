from flask_restplus import Namespace, Resource, fields
from neo4jrestclient.client import GraphDatabase

from model.utils import column_table_dict, \
    biological_view_tables, \
    management_view_tables, \
    technological_view_tables, \
    extraction_view_tables

api = Namespace('query', description='Value related operations')
#



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

gdb = GraphDatabase("http://geco:17474", username='neo4j', password='yellow')


@api.route('/')
@api.response(404, 'Field not found')  # TODO correct
class Query(Resource):
    @api.doc('return_query_result')
    @api.marshal_with(query)
    @api.expect(query)  # TODO correct this one
    def post(self):
        '''List all values'''

        if True:
            filter_in = api.payload

            # set of distinct tables in the query
            filter_tables = set()
            for (column, values) in filter_in.items():
                table_name = column_table_dict[column]
                filter_tables.add(table_name)

            filter_bio_tables = [x for x in biological_view_tables if x in filter_tables]
            filter_mngm_tables = [x for x in management_view_tables if x in filter_tables]
            filter_tech_tables = technological_view_tables  # [x for x in tech_tables if x in filter_tables]
            filter_extract_tables = extraction_view_tables  # [x for x in extract_tables if x in filter_tables]
            filter_all_view_tables = (filter_bio_tables, filter_mngm_tables, filter_tech_tables, filter_extract_tables)

            # list of sub_queries
            sub_queries = []
            for (i, l) in enumerate(filter_all_view_tables):
                if len(l) > 0:
                    sub_query = 'p%d = (i:Item)' % (i)
                    for table_name in l:
                        var_name = table_name[:2].lower()
                        sub_query = sub_query + '-[*..3]->(%s:%s)' % (var_name, table_name)
                    sub_queries.append(sub_query)

            sub_where = []
            for (column, values) in filter_in.items():
                table_name = column_table_dict[column]
                var_name = table_name[:2].lower()
                sub_or = 'OR %s.%s IS NULL' % (var_name, column) if None in values else ''
                values_wo_none = [x for x in values if x is not None]
                sub_where.append('AND (lower(%s.%s) IN %s %s)' % (var_name, column, str(values_wo_none), sub_or))

            cypher_query = ' MATCH ' + \
                           ', '.join(sub_queries) + \
                           ' WHERE TRUE ' + ' '.join(sub_where) + \
                           ' RETURN i,ex,da ' + \
                           ' LIMIT 100 '
            print(cypher_query)

            results = gdb.query(cypher_query, data_contents=True)
            # columns = ['source_id', 'size', 'date', 'pipeline', 'platform', 'source_url', 'local_url',
            #            'name', 'data_type', 'format', 'assembly', 'annotation',
            #            'technique', 'feature', 'target', 'antibody',
            #            ]
            print(results.rows)

            if results.rows:
                results = [merge_dicts(x) for x in results.rows]
            else:
                results = []

            print(results)

            return results

        else:
            api.abort(404)


def merge_dicts(dict_args):
    """
    Given any number of dicts, shallow copy and merge into a new dict,
    precedence goes to key value pairs in latter dicts.
    """
    result = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result
