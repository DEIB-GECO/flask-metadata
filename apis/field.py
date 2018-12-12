import flask
from flask_restplus import Namespace, Resource
from flask_restplus import fields
from flask_restplus import inputs

from model.utils import columns_dict, run_query, unfold_list
from .flask_models import Info, info_field

api = Namespace('field', description='Field related operations')

field = api.model('Field', {
    'name': fields.String(attribute='column_name', required=True, description='Field name '),
    'group': fields.String(attribute='table_name', description='Field group '),
})

field_list = api.model('Fields', {
    'fields': fields.List(fields.Nested(field, required=True, description='Fields', skip_none=True)),
    'info': info_field,
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
    'info': info_field,
})

parser = api.parser()
parser.add_argument('voc', type=inputs.boolean, help='Has vocabulary (true/false)', default=False)


@api.route('/<field_name>')
@api.param('field_name', 'The field')
@api.response(404, 'Field not found')
class FieldValue(Resource):
    @api.doc('get_value_list')
    @api.marshal_with(values)
    @api.expect(parser)
    def get(self, field_name):
        """List all values"""

        args = parser.parse_args()
        voc = args['voc']

        if field_name in columns_dict:
            column = columns_dict[field_name]

            column_name = column.column_name
            table_name = column.table_name
            column_type = column.column_type

            # MATCH (n: Donor)
            # OPTIONAL MATCH(n)-[:HasTid{onto_attribute:'species'}]->(:Vocabulary)-->(s:Synonym)
            # UNWIND s.label + n.species as value
            # RETURN DISTINCT TOLOWER(value) as value
            # ORDER BY value

            to_lower = 'TOLOWER' if column_type == str else ''
            cypher_query = f"MATCH (n: {table_name}) "
            if voc:
                cypher_query += f"OPTIONAL MATCH(n)-[:HasTid{{onto_attribute:'{column_name}'}}]->(:Vocabulary)-->(s:Synonym) "
                cypher_query += f"UNWIND [s.label] + [n.{column_name}] as value "
            else:
                cypher_query += "WITH n.species as value "
            cypher_query += f"RETURN DISTINCT {to_lower}(value) as value "
            cypher_query += "ORDER BY value"

            flask.current_app.logger.info(cypher_query)

            results = run_query(cypher_query)
            flask.current_app.logger.info('got results')

            res = results.elements

            # res has only one element in inner list, however I prefer to use general one
            res = unfold_list(res)

            res = [{'value': x} for x in res]

            info = Info(len(res), None)

            res = {'values': res,
                   'info': info
                   }

            return res

        else:
            api.abort(404)
