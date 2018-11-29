from flask_restplus import Namespace, Resource
from neo4jrestclient import constants

from model.utils import \
    run_query

api = Namespace('item', description='Item related operations')


@api.route('/item/<source_id>/graph')
@api.response(404, 'Item not found')  # TODO correct
class ItemGraph(Resource):
    @api.doc('get_item_graph')
    def get(self, source_id):
        cypher_query = "MATCH p1=((i:Item)-[*..3]->(x)) " \
                       "WHERE NOT 'PairsOfItem' IN labels(x)  " \
            f"AND i.source_id='{source_id}' " \
                       "RETURN *"
        print(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_GRAPH)

        print('got results')

        print(results)

        # resp = Response('hello', mimetype='text/html')
        # return resp
        return results.graph
