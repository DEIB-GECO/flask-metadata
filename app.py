from flask import Flask, render_template, redirect, Blueprint, url_for

from apis import api_blueprint

base_url = '/repo-viewer/'
api_url = base_url + 'api'
graph_static_url = base_url + 'graph_static'

my_app = Flask(__name__)

simple_page = Blueprint('root_pages', __name__,
                        static_folder='../vue-metadata/dist/static',
                        template_folder='../vue-metadata/dist')

graph_pages = Blueprint('static', __name__,
                        # static_url_path='/',
                        static_folder='./graph_static/',
                        # template_folder='../vue-metadata/dist'
                        )


# base url defined in apis init
@simple_page.route('/')
def index():
    print("serve index")
    return render_template('index.html')


# Make a "catch all route" so all requests match our index.html file.
# This lets us use the new history APIs in the browser.
@simple_page.route('/', defaults={'path': ''})
@simple_page.route('/<path:path>')
def redirect_all(path):
    return redirect(url_for('.index'))


# register blueprints
my_app.register_blueprint(api_blueprint, url_prefix=api_url)
my_app.register_blueprint(graph_pages, url_prefix=graph_static_url)
my_app.register_blueprint(simple_page, url_prefix=base_url)
my_app.app_context().push()


# redirect all to base url
@my_app.route('/', defaults={'path': ''})
@my_app.route('/<path:path>')
def index_all(path):
    return redirect(base_url)

# if __name__ == '__main__':
#     my_app.run()
