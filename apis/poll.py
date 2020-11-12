from flask import Response, json
from flask_restplus import Namespace, Resource

from utils import poll_dict

api = Namespace('poll', description='Operations to perform polling')


@api.route('/<poll_id>')
class Poll(Resource):
    def get(self, poll_id):
        if poll_id in poll_dict:
            return Response(json.dumps(poll_dict[poll_id]), mimetype='application/json')
        else:
            api.abort(404)
