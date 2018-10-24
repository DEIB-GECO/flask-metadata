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


@api.route('/')
class ValueList(Resource):
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
