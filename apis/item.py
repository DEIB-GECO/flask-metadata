import flask
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
from neo4jrestclient import constants

from model.utils import \
    run_query, unfold_list
from .flask_models import info_field, Info

api = Namespace('item', description='Item related operations')

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean, help='Has vocabulary (true/false)', default=False)


# parser.add_argument('onto', type=bool, help='Ontological ', default=False)


@api.route('/<source_id>/graph')
@api.response(404, 'Item not found')  # TODO correct
class ItemGraph(Resource):
    @api.doc('get_item_graph')
    @api.expect(parser)
    def get(self, source_id):

        args = parser.parse_args()
        voc = args['voc']

        if voc:
            max_voc_count = 1
        else:
            max_voc_count = 0

        cypher_query = "MATCH p=((i:Item)-[*]->(x)) " \
            f"WHERE size([n in nodes(p) WHERE 'Vocabulary' in labels(n) | n]) <= {max_voc_count} " \
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

        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query, data_contents=constants.DATA_GRAPH)

        flask.current_app.logger.info('got results')

        flask.current_app.logger.info(results)

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
        flask.current_app.logger.info(cypher_query)

        results = run_query(cypher_query)

        flask.current_app.logger.info('got results')

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
