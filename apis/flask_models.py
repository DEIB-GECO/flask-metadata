from flask_restplus import Namespace, fields


class Info:
    def __init__(self, count, total):
        self.count = count
        self.total = total


api = Namespace('models')

info = api.model('Info', {
    'shown_count': fields.String(attribute='count', description='TODO'),
    'total_count': fields.String(attribute='total', description='TODO'),

})
