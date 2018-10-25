from flask_restplus import Namespace, Resource, fields

from model.utils import column_dict
from .flask_models import info, Info

api = Namespace('value', description='Value related operations')

value = api.model('Value', {
    'value': fields.Raw(required=True, description='Value '),
})

values = api.model('Values', {
    'values': fields.Nested(value, required=True, description='Values'),
    'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})


@api.route('/<field>')
@api.param('field', 'The field')
@api.response(404, 'Field not found')
class ValueList(Resource):
    @api.doc('get_value_list')
    @api.marshal_with(values)
    def get(self, field):
        '''List all values'''

        if field in column_dict:
            column = column_dict[field]

            column_name = column.column_name

            table = column.table_class
            column = column.db_column

            res = table.query
            res = res.filter(column is not None)
            res = res.distinct(column)
            res = res.order_by(column)
            res = res.limit(100)
            res = res.offset(0)
            res = res.all()

            # extract value
            res = map(lambda x: x.__dict__[column_name], res)

            # lowercase
            res = set(map(lambda x: x.lower() if type(x) == str else x, res))

            res = [{'value': x} for x in res]

            info = Info(len(res), None)
            res = {'values': res,
                   'info': info
                   }

            return res
        else:
            api.abort(404)
