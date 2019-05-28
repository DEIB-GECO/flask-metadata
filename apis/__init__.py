from flask import Blueprint
from flask_restplus import Api

from .field import api as field_api
from .flask_models import api as models_api
from .query import api as query_api
from .item import api as item_api
from .pair import api as pair_api

# TODO change URL prefix before deploy
api_blueprint = Blueprint('api', __name__)

api = Api(api_blueprint,
          title='GenoSurf API',
          version='1.0',
          description='GenoSurf API is a set of RESTful endpoints -- programmable interfaces over the Web -- '
                      'that allows third-party developers to build automation scripts and apps. '
                      'This documentation describes the API in detail, including data model information of both inputs '
                      '(parameters) and outputs (response records). It also allows you to interact with and test out '
                      'each API directly on this page, which shall provide clear insights into how the API responds '
                      'to different parameters.'
                      '\nThe API allow to retrieve information about genomic experiments metadata, '
                      'as they are described by the [Genomic Conceptual Model](http://www.bioinformatics.deib.polimi.it/geco/publications/conceptual_modelling.pdf)'
                      'and also as they are retrieved in their original form from the data sources.'
                      '\nThey are divided in four groups:'
                      '\n* \'field\' allows to retrieve information about the fields used for querying the Genomic Conceptual Model; '
                      '\n* \'query\' allows to perform queries over the repository to retrieve list of items, eventually '
                      'aggregated by dataset or source, downloadable links or [GMQL](http://gmql.eu/gmql-rest/) queries. '
                      'The the body of the POST request contains the context filters of the query;'
                      '\n* \'item\' allows to retrieve information of a specific item.'
                      '\n* \'pair\' allows to search keys or values based on user input strings, to be matched in exact or non-exact modes.',
          doc='/',
          )

api.add_namespace(models_api)

api.add_namespace(field_api)
api.add_namespace(query_api)
api.add_namespace(item_api)
api.add_namespace(pair_api)
