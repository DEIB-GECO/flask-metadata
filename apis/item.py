from flask_restplus import Namespace, Resource
from flask_restplus import fields
from neo4jrestclient import constants

from model.utils import \
    run_query, unfold_list
from .flask_models import info_field, Info

api = Namespace('item', description='Item related operations')


@api.route('/<source_id>/graph')
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
        if len(results):
            return results.graph
        else:
            item_na_error(source_id)


extra = api.model('Extra', {
    'key': fields.String(attribute='key', required=True, description='Extra key '),
    'value': fields.String(attribute='value', description='Extra value '),
})

extras = api.model('Fields', {
    'extras': fields.List(fields.Nested(extra, required=True, description='Extras', skip_none=True)),
    'info': info_field,
})


@api.route('/<source_id>/extra')
@api.response(404, 'Item not found')  # TODO correct
class ItemExtra(Resource):
    @api.doc('get_item_extra')
    @api.marshal_with(extras)
    def get(self, source_id):
        # TODO correct with pairs
        cypher_query = "MATCH (i:Item) " \
            f"WHERE i.source_id='{source_id}' " \
                       "RETURN i"
        print(cypher_query)

        results = run_query(cypher_query)

        print('got results')

        res = results.elements

        # res has only one element in inner list, however I prefer to use general one
        res = unfold_list(res)

        if len(res) > 0:
            res = [{'key': key, 'value': value} for (key, value) in res[0]['data'].items()]

            info = Info(len(res), None)

            res = {'extras': res,
                   'info': info
                   }

            return res
        else:
            item_na_error(source_id)


def item_na_error(source_id):
    return api.abort(404, f'Item with source_id ({source_id}) is not available ')
