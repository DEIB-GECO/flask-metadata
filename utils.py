import os


def get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        message = "Expected environment variable '{}' not set.".format(name)
        raise Exception(message)


def get_db_uri():
    postgres_url = get_env_variable("POSTGRES_URL")
    postgres_user = get_env_variable("POSTGRES_USER")
    postgres_pw = get_env_variable("POSTGRES_PW")
    postgres_db = get_env_variable("POSTGRES_DB")
    return 'postgresql+psycopg2://{user}:{pw}@{url}/{db}'.format(user=postgres_user,
                                                                 pw=postgres_pw,
                                                                 url=postgres_url,
                                                                 db=postgres_db)


def str2bool(v):
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise Exception('Boolean value expected.')
