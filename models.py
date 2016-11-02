from flask_login import UserMixin
from sqlalchemy.dialects import postgresql as pg
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import polyline
import stravalib


from heatmapp import app, cache

db = SQLAlchemy(app)


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    strava_id = db.Column(db.Integer, primary_key=True, autoincrement=False)

    # These fields get refreshed every time the user logs in.
    #  They are only stored in the database to enable persistent login
    username = db.Column(db.String())
    firstname = db.Column(db.String())
    lastname = db.Column(db.String())
    profile = db.Column(db.String())
    strava_access_token = db.Column(db.String())

    dt_last_active = db.Column(pg.TIMESTAMP)
    app_activity_count = db.Column(db.Integer, default=0)

    # This is set up so that if a user gets deleted, all of the associated
    #  activities are also deleted.
    activities = db.relationship("Activity",
                                 backref="user",
                                 cascade="all, delete, delete-orphan",
                                 lazy="dynamic")

    strava_client = None

    def describe(self):
        attrs = ["username", "firstname", "lastname",
                 "profile", "strava_access_token", "dt_last_active",
                 "app_activity_count"]
        return {attr: getattr(self, attr) for attr in attrs}

    def client(self):
        if not self.strava_client:
            self.strava_client = stravalib.Client(
                access_token=self.strava_access_token)
        return self.strava_client

    def __repr__(self):
        return "<User %r>" % (self.strava_id)

    def get_id(self):
        return unicode(self.strava_id)

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
        return self

    @classmethod
    def from_access_token(cls, token):
        client = stravalib.Client(access_token=token)
        strava_user = client.get_athlete()

        user = cls.get(strava_user.id)
        if not user:
            user = cls(strava_id=strava_user.id,
                       app_activity_count=0)

        user.update(
            username=strava_user.username,
            strava_access_token=token,
            firstname=strava_user.firstname,
            lastname=strava_user.lastname,
            profile=strava_user.profile,
            dt_last_active=datetime.utcnow(),
            client=client
        )
        return user

    def update_db(self):
        if not User.get(self.strava_id):
            db.session.add(self)
        return db.session.commit()

    def delete(self):
        if User.get(self.strava_id):
            db.session.delete(self)
            return db.session.commit()

    @classmethod
    def get(cls, user_identifier):
        # Get user by id or username
        try:
            # try casting identifier to int
            user_id = int(user_identifier)
        except ValueError:
            # if that doesn't work then assume it's a string username
            user = cls.query.filter_by(username=user_identifier).first()
        else:
            user = cls.query.get(user_id)

        return user if user else None

    def activity_summaries(self, activity_ids=None, **kwargs):
        cache_timeout = 60
        unique = "{},{},{}".format(self.strava_id, activity_ids, kwargs)
        key = str(hash(unique))

        summaries = cache.get(key)
        if summaries:
            app.logger.info("got cache key '{}'".format(unique))
            for summary in summaries:
                yield summary
        else:
            summaries = []
            if activity_ids:
                activities = (self.client().get_activity(int(id))
                              for id in activity_ids)
            else:
                activities = self.client().get_activities(**kwargs)

            try:
                for a in activities:
                    data = {
                        "id": a.id,
                        "athlete_id": a.athlete.id,
                        "name": a.name,
                        "type": a.type,
                        "summary_polyline": a.map.summary_polyline,
                        "beginTimestamp": str(a.start_date_local),
                        "total_distance": float(a.distance),
                        "elapsed_time": int(a.elapsed_time.total_seconds()),
                        "user_id": self.strava_id
                    }
                    A = Activity(**data)
                    summaries.append(A)
                    yield A
            except Exception as e:
                yield {"error": str(e)}
            else:
                cache.set(key, summaries, cache_timeout)
                app.logger.info("set cache key '{}'".format(unique))


class Activity(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=False)
    athlete_id = db.Column(db.Integer)  # owner of this activity
    name = db.Column(db.String())
    type = db.Column(db.String())
    summary_polyline = db.Column(db.String())
    beginTimestamp = db.Column(pg.TIMESTAMP)
    total_distance = db.Column(db.Float())
    elapsed_time = db.Column(db.Integer)

    # streams
    time = db.Column(pg.ARRAY(db.Integer))
    polyline = db.Column(db.String())
    distance = db.Column(pg.ARRAY(db.Float()))
    altitude = db.Column(pg.ARRAY(db.Float()))
    velocity_smooth = db.Column(pg.ARRAY(pg.REAL))
    cadence = db.Column(pg.ARRAY(db.Integer))
    watts = db.Column(pg.ARRAY(pg.REAL))
    grade_smooth = db.Column(pg.ARRAY(pg.REAL))

    dt_cached = db.Column(pg.TIMESTAMP)
    dt_last_accessed = db.Column(pg.TIMESTAMP)
    access_count = db.Column(db.Integer, default=0)

    # self.user is the user that requested this activity, and may or may not
    #  be the owner of the activity (athlete_id)
    user_id = db.Column(db.Integer, db.ForeignKey("users.strava_id"))

    def owned(self):
        return self.user_id == self.athlete_id

    def __repr__(self):
        return "<Activity %r>" % (self.id)

    def serialize(self):
        attrs = ["id", "athlete_id", "name", "type", "summary_polyline",
                 "beginTimestamp", "total_distance", "elapsed_time",
                 "time", "polyline", "distance", "altitude", "velocity_smooth",
                 "cadence", "watts", "grade_smooth", "dt_cached",
                 "dt_last_accessed", "access_count"]
        return {attr: getattr(self, attr) for attr in attrs}

    def import_streams(self, streams=[]):
        stream_names = set(['time', 'latlng'])
        stream_names.update(streams)

        client = User.get(self.user_id).client()
        try:
            streams = client.get_activity_streams(self.id, types=stream_names)
        except Exception as e:
            return {"error": str(e)}

        dict = {}
        if "latlng" in streams:
            self.polyline = polyline.encode(streams['latlng'].data)
            dict["polyline"] = self.polyline
            del streams['latlng']

        for s in streams:
            dict[s] = streams[s].data
            setattr(self, s, streams[s].data)
        return self

    def update_db(self):
        if not Activity.get(self.id):
            self.user = User.get(self.user_id)
            self.dt_cached = datetime.utcnow()
            self.access_count = 0
            db.session.add(self)
        return db.session.commit()

    def delete(self):
        db.session.delete(self)
        return db.session.commit()

    @classmethod
    def get(cls, activity_id):
        try:
            id = int(activity_id)
        except ValueError:
            return None
        else:
            A = cls.query.get(id)
            if A:
                A.dt_last_accessed = datetime.utcnow()
                A.access_count += 1
                db.session.commit()
            return A


# Create tables if they don't exist
#  These commands aren't necessary if we use flask-migrate

# db.create_all()
# db.session.commit()

# If flask-migrate is being used, we build the tables from scratch using
# flask db init

# Then run
# flask db migrate
#    note: Flask-Migrate apparently has an issue with PostgreSQL ARRAY type.
#     https://github.com/miguelgrinberg/Flask-Migrate/issues/72
#       so after migrate we must go into the created migration file in
#       migrations/versions and explicitly change
#       postgresql.ARRAY(some_type) to postgresql.ARRAY(postgresql.some_type)
#       or postgresql.ARRAY(sa.some_type)
#
# Then, run
# flask db upgrade
#
#  to actually perform th upgrade.  This last step must be done both locally
#  and on the server.
