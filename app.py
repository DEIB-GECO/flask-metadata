from flask import Flask, render_template, redirect, Blueprint, url_for

from apis import api_blueprint
from model.models import db
from utils import get_db_uri

base_url = '/repo-viewer/'
api_url = base_url + 'api'

app = Flask(__name__)

simple_page = Blueprint('root_pages', __name__,
                        static_folder='../vue-metadata/dist/static',
                        template_folder='../vue-metadata/dist')


graph_pages = Blueprint('graph_pages', __name__,
                        static_url_path='static',
                        static_folder='./static/graph',
                        # template_folder='../vue-metadata/dist'
                        )



# base url defined in apis init
@simple_page.route('/')
def index():
    print("serve index")
    return render_template('index.html')


# Make a "catch all route" so all requests match our index.html file. This lets us use the new history APIs in the browser.
# @simple_page.route('/', defaults={'path': ''})
# @simple_page.route('/<path:path>')
# def redirect_all(path):
#     return redirect(url_for('.index'))


# if __name__ == '__main__':
#     app.run()

# register blueprints
app.register_blueprint(api_blueprint, url_prefix=api_url)
app.register_blueprint(graph_pages, url_prefix=base_url + "graph")

app.register_blueprint(simple_page, url_prefix=base_url)

app.config['SQLALCHEMY_DATABASE_URI'] = get_db_uri()
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
app.app_context().push()


# redirect all to base url
# @app.route('/', defaults={'path': ''})
# @app.route('/<path:path>')
def index_all(path):
    return redirect(base_url)


