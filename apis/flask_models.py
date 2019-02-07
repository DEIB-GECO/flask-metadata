from flask_restplus import Namespace, fields


class Info:
    def __init__(self, count, total=None, item=None):
        self.count = count
        self.total = total
        self.item = item


api = Namespace('models')

info = api.model('Info', {
    'shown_count': fields.Integer(attribute='count', description='TODO'),
    'total_count': fields.Integer(attribute='total', description='TODO'),
    'item_count': fields.Integer(attribute='item', description='TODO'),

})
info_field = fields.Nested(info, required=True, description='Info', skip_none=True)
