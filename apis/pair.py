from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
import sqlalchemy
from utils import sql_query_generator
from model.models import db

api = Namespace('pair', description='Operations to perform queries on key-value metadata pairs')

query = api.model('Pair', {
    'key': fields.String(attribute='column_name', required=True, description='Field name '),
    # 'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})

parser = api.parser()
parser.add_argument('body', type="json", help='json ', location='json')
parser.add_argument('q', type=str)


@api.route('/keys')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys')
    @api.expect(parser)
    def post(self):
        '''Retrieves all keys based on a input keyword'''
        args = parser.parse_args()
        key = args['q']

        payload = api.payload

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        if filter_in:
            sub_query = "AND item_id in (" + sql_query_generator(filter_in, type, pairs, 'item_id', limit=None,
                                                                 offset=None) + ")"
        else:
            sub_query = ""

        query = f"select key, is_gcm, value " \
                    f"from unified_pair " \
                    f"where lower(key) like '%{key.lower()}%' " + sub_query

        print("Query start")
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()
        print(query)
        results_gcm = []
        results_pairs = []

        keys_gcm = []
        keys_pairs = []
        for r in res:
            if r['is_gcm']:
                keys_gcm.append(r['key'])
            else:
                keys_pairs.append(r['key'])

        for k in keys_gcm:
            values = []
            for r in res:
                if r['key'] == k and r['is_gcm']:
                    values.append(r['value'])
            values = list(set(values))
            results_gcm.append({'key': k, 'count_values': len(values), 'values': values})

        for k in keys_pairs:
            values = []
            for r in res:
                if r['key'] == k and not r['is_gcm']:
                    values.append(r['value'])
            values = list(set(values))
            results_pairs.append({'key': k, 'count_values': len(values), 'values': values})

        results = {'gcm': results_gcm, 'pairs': results_pairs}

        # for r in res:
        #     if r['is_gcm']:
        #         results_gcm.append({'key': r['key'], 'count_values': r['count']})
        #     else:
        #         results_pairs.append({'key': r['key'], 'count_values': r['count']})
        # results = {'gcm': results_gcm, 'pairs': results_pairs}

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
        '''For a specific key, it lists all possible values'''
        args = value_parser.parse_args()
        is_gcm = args['is_gcm']

        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        if filter_in:
            sub_query = "AND item_id in (" + sql_query_generator(filter_in, type, pairs, 'item_id', limit=None,
                                                                 offset=None) + ")"
        else:
            sub_query = ""

        limit = ""
        if is_gcm:
            limit = "limit 10"

        query = f"select value, count(item_id) as count from unified_pair where key = '{key}' and is_gcm = {is_gcm} " \
                + sub_query + \
                " group by value " \
                + limit

        print(query)
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()

        result = []

        for r in res:
            result.append({'value': r['value'], 'count': r['count']})

        return result


@api.route('/values')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys')
    @api.expect(parser)
    def post(self):
        '''Retrieves all values based on a input keyword'''
        args = parser.parse_args()
        value = args['q']

        payload = api.payload

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        if filter_in:
            sub_query = "AND item_id in (" + sql_query_generator(filter_in, type, pairs, 'item_id', limit=None,
                                                                 offset=None) + ")"
        else:
            sub_query = ""

        query = f"select key, value, is_gcm, count(item_id) as count " \
                    f"from unified_pair " \
                    f"where lower(value) like '%{value.lower()}%' " + sub_query + \
                " group by key, value, is_gcm"

        print("Query start")
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()
        print(query)
        results_gcm = []
        results_pairs = []
        for r in res:
            if r['is_gcm']:
                results_gcm.append({'key': r['key'], 'value': r['value'], 'count': r['count']})
            else:
                results_pairs.append({'key': r['key'], 'value': r['value'], 'count': r['count']})
        results = {'gcm': results_gcm, 'pairs': results_pairs}
        return results
