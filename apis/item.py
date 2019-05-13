import flask
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
from neo4jrestclient import constants

import flask
from flask import Response
from flask_restplus import Namespace, Resource, fields, inputs
from model.models import db

import requests

from utils import \
    run_query, unfold_list
from .flask_models import info_field, Info

api = Namespace('item', description='Operations applicable on single items')

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean,
                    help='Enable inclusion of controlled vocabulary terms, synonyms and external references (true/false)',
                    default=False)


# parser.add_argument('onto', type=bool, help='Ontological ', default=False)

def count(id):
    query_count = f"match p=((n)--(x)) where ID(n) = {id} AND NOT 'Pair' IN labels(x) return count(x)"

    count = run_query(query_count)

    if len(count):
        return count.elements[0][0]
    else:
        item_na_error(id)


@api.route('/<id>/count')
@api.response(404, 'Item not found')  # TODO correct
class NodeCount(Resource):
    @api.doc('get_node_relations_count')
    # @api.expect(parser)
    def get(self, id):
        return count(id)


@api.route('/<label>/<id>/relations')
@api.response(404, 'Item not found')  # TODO correct
class NodeRel(Resource):
    @api.doc('get_node_relations')
    # @api.expect(parser)
    def get(self, label, id):

        # url = f'/repo-viewer/api/item/{id}/count'
        # r = requests.get(url)
        #
        # count = r.content

        cypher_query_header = f'match p=((n)--(x)) where ID(n) = {id}'

        cypher_query_where = ''

        if label.lower() == "item":
            cypher_query_where = ' AND '
            cypher_query_where += "not 'Pair' in labels(x)"

        cypher_query_return = ' return *'

        cypher_query_limit = ''

        threshold = 30

        flask.current_app.logger.debug(count(id))

        if count(id) > threshold:
            cypher_query_limit = f' limit {threshold}'

        cypher_query = cypher_query_header + cypher_query_where + cypher_query_return + cypher_query_limit
        flask.current_app.logger.debug(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_GRAPH)

        flask.current_app.logger.debug('got results')

        flask.current_app.logger.debug(results)

        if len(results):
            return results.graph
        else:
            item_na_error(id)


@api.route('/<source_id>/graph')
@api.param('source_id', 'The requested object identifier (as appearing in the source)')
@api.response(404, 'Item not found')  # TODO correct
class ItemGraph(Resource):
    @api.doc('get_item_graph')
    @api.expect(parser)
    def get(self, source_id):
        '''For the specified item identifier, it retrieves the corresponding sub-graph in JSON'''

        args = parser.parse_args()
        voc = args['voc']

        if voc:
            max_voc_count = 1
        else:
            max_voc_count = 0

        cypher_query = "MATCH p=((i:Item)-[*]->(x)) " \
            f"WHERE size([n in nodes(p) WHERE 'Vocabulary' in labels(n) | n]) <= {max_voc_count} " \
                       "AND NOT 'Pair' IN labels(x) " \
            f"AND i.source_id='{source_id}' " \
                       "RETURN *"

        # MATCH p1=((i:Item)-[*]->(x))
        # WHERE NOT 'Vocabulary' IN labels(x)
        # AND NOT 'Synonym' IN labels(x)
        # AND NOT 'Ontology' IN labels(x)
        # AND NOT 'XRef' IN labels(x)
        # AND i.source_id='08fbbee6-0780-4d19-bbab-346dda361e08-msm' RETURN *

        # if it is only 0 then without any vocabulary
        # MATCH p=((i:Item)-[*]->(x))
        # WHERE TRUE
        # AND size([n in nodes(p) WHERE 'Vocabulary' in labels(n) | n]) < 2
        # AND i.source_id='08fbbee6-0780-4d19-bbab-346dda361e08-msm'
        # RETURN p, size([n in nodes(p1) WHERE 'Vocabulary' in labels(n) | n])

        flask.current_app.logger.debug(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_GRAPH)

        flask.current_app.logger.debug('got results')

        flask.current_app.logger.debug(results)

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
@api.response(404, 'Extra information not found for input id')  # TODO correct
class ItemExtra(Resource):
    @api.doc('get_item_extra', params={'source_id': 'The requested object identifier (as appearing in the source)'})
    @api.marshal_with(extras)
    def get(self, source_id):
        '''For the specified item identifier, it retrieves a list of key-value metadata pairs'''
        # TODO correct with pairs

        # cypher_query = "MATCH (it:Item)--(pa:Pair) " \
        #     f"WHERE it.source_id='{source_id}' " \
        #                "RETURN pa "

        # flask.current_app.logger.info(cypher_query)

        # results = run_query(cypher_query)

        # flask.current_app.logger.info('got results')

        query = f"""select key, value 
                    from item it join unified_pair kv on it.item_id = kv.item_id
                    where it.item_source_id = '{source_id}'"""

        flask.current_app.logger.debug(query)
        res = db.engine.execute(query).fetchall()

        # res has only one element in inner list, however I prefer to use general one

        if len(res) > 0:
            res = [{'key': pa[0], 'value': pa[1]} for pa in res]

            info = Info(len(res), None)

            res = {'extras': res,
                   'info': info
                   }

            return res
        else:
            item_na_error(source_id)


def item_na_error(source_id):
    return api.abort(404, f'Item with source_id ({source_id}) is not available ')
