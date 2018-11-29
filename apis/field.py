from flask_restplus import Namespace, Resource
from flask_restplus import fields

from model.utils import column_dict
from .flask_models import info, Info

api = Namespace('field', description='Field related operations')

field = api.model('Field', {
    'name': fields.String(attribute='column_name', required=True, description='Field name '),
    'group': fields.String(attribute='table_name', description='Field group '),
})

field_list = api.model('Fields', {
    'fields': fields.List(fields.Nested(field, required=True, description='Fields', skip_none=True)),
    'info': fields.Nested(info, required=True, description='Info', skip_none=True),
})


@api.route('')
class FieldList(Resource):
    @api.doc('get_field_list')
    @api.marshal_with(field_list, skip_none=True)
    def get(self):
        '''List all fields'''
        res = column_dict.values()
        res = list(res)
        res_len = len(res)
        info = Info(res_len, res_len)
        res = {'fields': res, 'info': info}

        # add group
        # {
        #     "field": "bio_replicate_num",
        #     "group": "biosample"
        # },

        return res  # [4:6]




value = api.model('Value', {
    'value': fields.Raw(required=True, description='Value '),
})

values = api.model('Values', {
    'values': fields.Nested(value, required=True, description='Values'),
    'info': fields.Nested(info, required=False, description='Info', skip_none=True),
})


@api.route('/<field_name>')
@api.param('field_name', 'The field')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    @api.doc('get_value_list')
    @api.marshal_with(values)
    def get(self, field_name):
        '''List all values'''

        if field_name in column_dict:
            column = column_dict[field_name]

            column_name = column.column_name

            table = column.table_class
            column = column.db_column

            res = table.query
            res = res.filter(column is not None)
            res = res.distinct(column)
            res = res.order_by(column)
            # res = res.limit(100)
            res = res.offset(0)
            res = res.all()

            # extract value
            res = map(lambda x: x.__dict__[column_name], res)

            # lowercase
            res = set(map(lambda x: x.lower() if type(x) == str else x, res))

            has_none = None in res

            res = sorted([x for x in res if x is not None])

            if has_none:
                res.append(None)

            res = [{'value': x} for x in res]

            info = Info(len(res), None)

            res = {'values': res,
                   'info': info
                   }

            return res
        else:
            api.abort(404)
