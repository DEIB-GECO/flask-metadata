from flask_restplus import Namespace, Resource
from flask_restplus import fields
import sqlalchemy

from model.models import db

api = Namespace('pair', description='TODO')

query = api.model('Pair', {
    'key': fields.String(attribute='column_name', required=True, description='Field name '),
    # 'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})

parser = api.parser()
parser.add_argument('key', type=str)


@api.route('/keys')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys')
    @api.expect(parser)
    def post(self):
        args = parser.parse_args()
        key = args['key']

        query_gcm = f"select distinct key " \
            f"from unified_pair " \
            f"where key ilike '%{key}%' " \
            f"and is_gcm = true"

        query_pairs = f"select distinct key " \
            f"from unified_pair " \
            f"where key ilike '%{key}%' " \
            f"and is_gcm = false"

        res_gcm = db.engine.execute(sqlalchemy.text(query_gcm)).fetchall()
        res_pairs = db.engine.execute(sqlalchemy.text(query_pairs)).fetchall()
        results_gcm = []
        results_pairs = []
        for r in res_gcm:
            results_gcm.append(r[0])

        for r in res_pairs:
            results_pairs.append(r[0])

        results = {'gcm': results_gcm, 'pairs': results_pairs}

        return results
