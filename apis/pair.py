from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs
import sqlalchemy
import flask
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
parser.add_argument('exact', type=inputs.boolean, default=False)


@api.route('/keys')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_keys')
    @api.expect(parser)
    def post(self):
        '''Retrieves all keys based on a input keyword'''
        args = parser.parse_args()
        key = args['q']
        exact = args['exact']

        if not exact:
            q = f"%{key}%"
            equals = ' like '
        else:
            q = key
            equals = ' = '

        payload = api.payload

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")
        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None)
        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        # print(from_sub)
        # print(where_sub)
        query_gcm = f"select up.key as key, " \
                    f" count(distinct up.value) as count, " \
                    f" array(select unnest(array_agg(distinct up.value)) limit 10) as ex_values " + from_sub + \
                    f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                    f" and lower(up.key) {equals} lower('{q}') and up.is_gcm = true " \
                    f" group by up.key"

        query_pair = f"select up.key as key, count(distinct up.value) as count " + from_sub + \
                     f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                     f" and lower(up.key) {equals} lower('{q}') and up.is_gcm = false " \
                     f" group by up.key"

        flask.current_app.logger.debug(query_gcm)
        flask.current_app.logger.debug(query_pair)
        res_gcm = db.engine.execute(sqlalchemy.text(query_gcm)).fetchall()
        res_pair = db.engine.execute(sqlalchemy.text(query_pair)).fetchall()
        results_gcm = []
        results_pairs = []

        for r in res_gcm:
            results_gcm.append({'key': r.key, 'count_values': r.count, 'values': r.ex_values})

        for r in res_pair:
            results_pairs.append({'key': r.key, 'count_values': r.count})

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
        '''For a specific key, it lists all possible values'''
        args = value_parser.parse_args()
        is_gcm = args['is_gcm']

        payload = api.payload
        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None)

        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        # print(from_sub)
        # print(where_sub)
        query = f"select distinct up.value as value, count(up.item_id) as count " + from_sub + \
                " join unified_pair up on it.item_id = up.item_id " + where_sub + \
                f" and lower(up.key) = lower('{key}') and up.is_gcm = {is_gcm}" \
                    f" group by up.value"

        flask.current_app.logger.debug(query)
        res = db.engine.execute(sqlalchemy.text(query)).fetchall()

        result = []

        for r in res:
            result.append({'value': r.value, 'count': r.count})

        return result


@api.route('/values')
@api.response(404, 'Item not found')  # TODO correct
class Key(Resource):
    @api.doc('get_values')
    @api.expect(parser)
    def post(self):
        '''Retrieves all values based on a input keyword'''
        args = parser.parse_args()
        value = args['q']

        exact = args['exact']

        if not exact:
            q = f"%{value}%"
            equals = ' like '
        else:
            q = value
            equals = ' = '

        payload = api.payload

        filter_in = payload.get("gcm")
        type = payload.get("type")
        pairs = payload.get("kv")

        sub_query = sql_query_generator(filter_in, type, pairs, field_selected='platform', return_type='item_id',
                                        limit=None, offset=None)

        where_start = sub_query.find("WHERE")
        from_start = sub_query.find("FROM")

        from_sub = sub_query[from_start:where_start]
        where_sub = sub_query[where_start:]

        query = f"select up.key, up.value, up.is_gcm, count(distinct up.item_id) as count " + from_sub + \
                f" join unified_pair up on it.item_id = up.item_id " + where_sub + \
                f" and lower(up.value) {equals} lower('{q}') " \
                f" group by up.key, up.value, up.is_gcm"

        res = db.engine.execute(sqlalchemy.text(query)).fetchall()
        flask.current_app.logger.debug(query)
        results_gcm = []
        results_pairs = []
        i=0
        j=0
        for r in res:
            if r['is_gcm']:
                results_gcm.append({'key': r.key, 'value': r.value, 'count': r.count, 'id':i})
                i+=1
            else:
                results_pairs.append({'key': r.key, 'value': r.value, 'count': r.count, 'id':j})
                j+=1
        results = {'gcm': results_gcm, 'pairs': results_pairs}
        return results
