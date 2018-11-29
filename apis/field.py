import flask
from flask_restplus import Namespace, Resource
from flask_restplus import fields

from model.utils import columns_dict, run_query
from .flask_models import Info, info

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
        """List all fields"""
        res = columns_dict.values()
        res = list(res)
        res_len = len(res)
        info = Info(res_len, res_len)
        res = {'fields': res, 'info': info}

        return res


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
        """List all values"""

        if field_name in columns_dict:
            column = columns_dict[field_name]

            column_name = column.column_name
            table_name = column.table_name
            column_type = column.column_type

            to_lower = 'TOLOWER' if type(column_type) == str else ''
            cypher_query = f"MATCH (n: {table_name}) " \
                f"RETURN DISTINCT {to_lower}(n.{column_name}) " \
                "ORDER BY 1"

            flask.current_app.logger.info(cypher_query)

            results = run_query(cypher_query)
            flask.current_app.logger.info('got results')

            res = results.elements

            # res has only one element in inner list, however I prefer to use general one
            res = [item for sublist in res for item in sublist]

            res = [{'value': x} for x in res]

            info = Info(len(res), None)

            res = {'values': res,
                   'info': info
                   }

            return res

        else:
            api.abort(404)
