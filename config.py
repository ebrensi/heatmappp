import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    APP_VERSION = "(alpha)"
    APP_NAME = "Heatflask {}".format(APP_VERSION)
    ADMIN = [15972102]
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SQLALCHEMY_DATABASE_URI = os.environ["DATABASE_URL"]
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Defaults to using PostgreSQL for Celery
    CELERY_BROKER_URL = "sqla+" + SQLALCHEMY_DATABASE_URI
    CELERY_RESULT_BACKEND = "db+" + SQLALCHEMY_DATABASE_URI

    # Settings for database (long) cache
    DB_CACHE_TIMEOUT = 30 * 24 * 60 * 60  # 30 days

    # Settings for fast-cache
    CACHE_REDIS_URL = os.environ.get('REDIS_URL')
    CACHE_SUMMARIES_TIMEOUT = 480   # 8 minutes
    CACHE_ACTIVITIES_TIMEOUT = 960  # 16 minutes

    SECRET_KEY = "pB\xeax\x9cJ\xd6\x81\xed\xd7\xf9\xd0\x99o\xad\rM\x92\xb1\x8b{7\x02r"

    # Flask-Cache settings
    CACHE_TYPE = "null"

    DATE_RANGE_PRESETS = ["2", "7", "30", "60", "180", "365"]

    # Strava stuff
    STRAVA_CLIENT_ID = os.environ["STRAVA_CLIENT_ID"]
    STRAVA_CLIENT_SECRET = os.environ["STRAVA_CLIENT_SECRET"]

    # Leaflet stuff
    HEATMAP_DEFAULT_OPTIONS = {
        "radius": 9,
        "blur": 15,
        "gradient": {0.4: 'blue', 0.65: 'lime', 1: 'red'}
    }

    ANTPATH_DEFAULT_OPTIONS = {
        "weight": 3,
        "opacity": 0.5,
        "color": 'yellow',
        "pulseColor": 'white',
        "delay": 2000,
        "dashArray": [3, 10]
    }

    ANTPATH_ACTIVITY_COLORS = {
        'red': ['run', 'walk', 'hike'],
        'blue': ['ride']
    }

    MAP_CENTER = [27.53, 1.58]
    MAP_ZOOM = 3


class ProductionConfig(Config):
    """
    These are settings specific to the production environment
    (the main app running on Heroku)
    """
    ANALYTICS = {
        'GOOGLE_UNIVERSAL_ANALYTICS': {
            'ACCOUNT': "UA-85621398-1"
        }
    }
    DEBUG = False

    # For Flask-Cache
    CACHE_TYPE = 'redis'

    # For Celery
    CELERY_BROKER_URL = os.environ['REDIS_URL']
    CELERY_RESULT_BACKEND = os.environ.get(['REDIS_URL'])


class StagingConfig(Config):
    """
    These are settings specific to the staging environment
     (hosted test app)
    """
    CACHE_TYPE = 'redis'

    DEVELOPMENT = True
    DEBUG = True


class DevelopmentConfig(Config):
    """
    These are settings specific to the development environment
    (Developer's personal computer)
    """
    # OFFLINE = True
    DEVELOPMENT = True
    DEBUG = True

    # For Flask-Cache
    CACHE_TYPE = "simple"
