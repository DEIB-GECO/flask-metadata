from flask import Blueprint
from flask_restplus import Api

from .field import api as field_api
from .flask_models import api as models_api
from .item import api as item_api
from .pair import api as pair_api
from .query import api as query_api
from .db_info import api as info_api
from .poll import api as poll_api
from .viz import api as viz_api
from .epitope import api as epitope_api

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
api.add_namespace(info_api)
api.add_namespace(poll_api)
api.add_namespace(viz_api)
api.add_namespace(viz_api)

api.add_namespace(epitope_api)
