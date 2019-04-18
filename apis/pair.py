from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
import sqlalchemy
from utils import sql_query_generator
from model.models import db

api = Namespace('pair', description='TODO')

query = api.model('Pair', {
    'key': fields.String(attribute='column_name', required=True, description='Field name '),
    # 'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})

parser = api.parser()
parser.add_argument('body', type="json", help='json ', location='json')
parser.add_argument('key', type=str)


@api.route('/keys')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys')
    @api.expect(parser)
    def post(self):
        args = parser.parse_args()
        key = args['key']

        payload = api.payload

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        sub_query = sql_query_generator(filter_in,type,pairs,'item_id', limit=None, offset=None)

        query = f"select key, is_gcm, count(distinct value) as count " \
            f"from unified_pair " \
            f"where lower(key) like '%{key}%' " \
            f"AND item_id in ({sub_query})"\
            f" group by key, is_gcm"

        print("Query start")
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()
        print(query)
        results_gcm = []
        results_pairs = []
        for r in res:
            if r['is_gcm']:
                results_gcm.append({'key': r['key'], 'count_values': r['count']})
            else:
                results_pairs.append({'key': r['key'], 'count_values': r['count']})
        results = {'gcm': results_gcm, 'pairs': results_pairs}
        return results


value_parser = api.parser()
value_parser.add_argument('body', type="json", help='json ', location='json')
value_parser.add_argument('is_gcm', type=inputs.boolean, default=True)


@api.route('/<key>/values')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_values_for_key')
    @api.expect(value_parser)
    def post(self, key):
        args = value_parser.parse_args()
        is_gcm = args['is_gcm']

        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        sub_query = sql_query_generator(filter_in, type, pairs, 'item_id', limit=None, offset=None)

        query = f"select value, count(item_id) as count from unified_pair where key = '{key}' and is_gcm = {is_gcm} and item_id in ({sub_query}) group by value"

        print(query)
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()

        result = []

        for r in res:
            result.append({'value': r['value'], 'count': r['count']})

        return result
