from flask import Blueprint
from flask_restplus import Api

from .field import api as field_api
from .flask_models import api as models_api
from .item import api as item_api
from .pair import api as pair_api
from .query import api as query_api

enable_doc = False

api_blueprint = Blueprint('api', __name__)

if enable_doc:
    api = Api(title='ViruSurf API', version='1.0', description='TODO', )
else:
    api = Api(title='ViruSurf API', version='1.0', description='TODO', doc=False)


api.init_app(api_blueprint, add_specs=enable_doc)

api.add_namespace(models_api)

api.add_namespace(field_api)
api.add_namespace(query_api)
# TODO removed item and pair
# api.add_namespace(item_api)
# api.add_namespace(pair_api)
