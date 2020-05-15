import flask
from flask_restplus import Namespace, Resource, fields, inputs

from model.models import db
from .flask_models import info_field, Info

api = Namespace('item', description='Operations applicable on single items')

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean,
                    help='Enable inclusion of controlled vocabulary terms, synonyms and external references (true/false)',
                    default=False)



extra = api.model('Extra', {
    'key': fields.String(attribute='key', required=True, description='Extra key '),
    'value': fields.String(attribute='value', description='Extra value '),
})

extras = api.model('Fields', {
    'extras': fields.List(fields.Nested(extra, required=True, description='Extras', skip_none=True)),
    'info': info_field,
})


@api.route('/<source_id>/extra')
@api.response(404, 'Extra information not found for input id')
class ItemExtra(Resource):
    @api.doc('get_item_extra', params={'source_id': 'The requested object identifier (as appearing in the source)'})
    @api.marshal_with(extras)
    def get(self, source_id):
        '''For the specified item identifier, it retrieves a list of key-value metadata pairs'''
        query = f"""select key, value 
                    from item it join unified_pair kv on it.item_id = kv.item_id
                    where it.item_source_id = '{source_id}'"""

        flask.current_app.logger.debug(query)
        res = db.engine.execute(query).fetchall()

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
