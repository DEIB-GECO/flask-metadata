from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
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

        print("Query start")
        res_gcm = db.engine.execute(sqlalchemy.text(query_gcm)).fetchall()
        res_pairs = db.engine.execute(sqlalchemy.text(query_pairs)).fetchall()
        results_gcm = []
        results_pairs = []
        for r in res_gcm:
            results_gcm.append({'key': r[0]})
            # results_gcm.append(r[0])

        for r in res_pairs:
            results_pairs.append({'key': r[0]})
            # results_pairs.append(r[0])

        results = {'gcm': results_gcm, 'pairs': results_pairs}
        print(results)
        return results


value_parser = api.parser()
value_parser.add_argument('is_gcm', type=inputs.boolean, default=True)


@api.route('/<key>/values')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_values_for_key')
    @api.expect(value_parser)
    def get(self, key):
        args = value_parser.parse_args()
        is_gcm = args['is_gcm']

        query = f"select value, count(item_id) as count from unified_pair where key ilike '{key}' and is_gcm = {is_gcm} group by value"

        print(query)
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()

        result = []

        for r in res:
            result.append({'value': r['value'], 'count': r['count']})

        return result
