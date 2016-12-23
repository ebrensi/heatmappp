import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    APP_VERSION = "(alpha)"
    APP_NAME = "Heatflask {}".format(APP_VERSION)
    ADMIN = [15972102]
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL")
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    MONGODB_URI = os.environ.get("MONGODB_URI")
    REDIS_URL = os.environ.get('REDIS_URL')
    CACHE_REDIS_URL = REDIS_URL

    # Settings for fast-cache
    # How long (seconds) we hold activity index before rebuilding it
    CACHE_INDEX_TIMEOUT = 7 * 24 * 60 * 60   # 7 days

    # How long before a user's index is outated and needs an update
    CACHE_INDEX_UPDATE_TIMEOUT = 10 * 60  # 10 minutes

    # We daily purge activities older than this from MongoDB
    STORE_ACTIVITIES_TIMEOUT = 7  # days

    # How long we memory-cache hires activities
    CACHE_ACTIVITIES_TIMEOUT = 1 * 60 * 60  # 1 hour

    # How long we hold a User object in memory
    CACHE_USERS_TIMEOUT = 2 * 60 * 60  # 2 hours

    SECRET_KEY = "pB\xeax\x9cJ\xd6\x81\xed\xd7\xf9\xd0\x99o\xad\rM\x92\xb1\x8b{7\x02r"

    # Flask-Cache (defaul) settings
    CACHE_TYPE = "null"

    # Strava stuff
    STRAVA_CLIENT_ID = os.environ.get("STRAVA_CLIENT_ID")
    STRAVA_CLIENT_SECRET = os.environ.get("STRAVA_CLIENT_SECRET")

    # Leaflet stuff
    HEATMAP_DEFAULT_OPTIONS = {
        "radius": 9,
        "blur": 15,
        "gradient": {0.4: 'blue', 0.65: 'lime', 1: 'red'}
    }

    ANTPATH_DEFAULT_OPTIONS = {
        "weight": 3,
        "opacity": 0.5,
        "color": 'black',
        "pulseColor": 'white',
        "delay": 2000,
        "dashArray": [3, 10],
        # "noClip": False
    }
    FLOWPATH_VARIATION_CONSTANTS = {
        "K": 12000,
        "T": 5
    }

    ANTPATH_ACTIVITY_COLORS = {
        'red': ['run', 'walk', 'hike'],
        'blue': ['ride'],
        'aqua': ['virtualride'],
        'yellow': ['swim']
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
    OFFLINE = True
    DEVELOPMENT = True
    DEBUG = True
    # For Flask-Cache
    CACHE_TYPE = "redis" if os.environ.get("REDIS_URL") else "simple"
    CACHE_ACTIVITIES_TIMEOUT = 10
