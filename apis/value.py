from flask_restplus import Namespace, Resource, fields

from model.utils import column_dict

api = Namespace('value', description='Value related operations')

value = api.model('Value', {
    'value': fields.String(required=True, description='Value '),
})


@api.route('/')
class ValueList(Resource):
    @api.doc('get_value_list')
    @api.marshal_with(value, )
    def get(self):
        '''List all values'''
        res = column_dict.keys()
        res = list(res)
        res = res[:3]
        res = [{'value': x} for x in res]
        return res


@api.route('/<field>')
@api.param('field', 'The field')
@api.response(404, 'Field not found')
class ValueList(Resource):
    @api.doc('get_value_list')
    @api.marshal_with(value, )
    def get(self, field):
        '''List all values'''

        if field in column_dict:
            column = column_dict[field]

            column_name = column.column_name

            table = column.table_class
            column = column.db_column

            res = table.query
            res = res.filter(column != None)
            res = res.distinct(column)
            res = res.order_by(column)
            res = res.limit(100000000)
            res = res.offset(2)
            res = res.all()

            res = sorted(set([str(x.__dict__[column_name]).lower() for x in res]))
            res = [{'value': x} for x in res]
            # res = {'result': res}

            return res
        else:
            api.abort(404)
