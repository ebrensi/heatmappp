# Standard library imports
import json
import uuid
import time
from bson import ObjectId
from operator import truth
from bson.binary import Binary
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import islice, repeat, starmap, takewhile

# Third party imports
import gevent
import msgpack
import pymongo
import requests
import polyline
import stravalib
import dateutil
import dateutil.parser
from sqlalchemy import inspect
from flask import current_app as app
from flask_login import UserMixin
from geventwebsocket import WebSocketError
from sqlalchemy.dialects import postgresql as pg
from requests.exceptions import HTTPError

# Local application imports
from . import mongo, db_sql, redis  # Global database clients
from . import EPOCH
from .Activities import Index, Activities

mongodb = mongo.db
log = app.logger
OFFLINE = app.config["OFFLINE"]
CACHE_ACTIVITIES_TIMEOUT = app.config["CACHE_ACTIVITIES_TIMEOUT"]
STRAVA_CLIENT_ID = app.config["STRAVA_CLIENT_ID"]
STRAVA_CLIENT_SECRET = app.config["STRAVA_CLIENT_SECRET"]
TRIAGE_CONCURRENCY = app.config["TRIAGE_CONCURRENCY"]
ADMIN = app.config["ADMIN"]
BATCH_CHUNK_SIZE = app.config["BATCH_CHUNK_SIZE"]
IMPORT_CONCURRENCY = app.config["IMPORT_CONCURRENCY"]
STORE_ACTIVITIES_TIMEOUT = app.config["STORE_ACTIVITIES_TIMEOUT"]
CACHE_TTL = app.config["CACHE_ACTIVITIES_TIMEOUT"]
DAYS_INACTIVE_CUTOFF = app.config["DAYS_INACTIVE_CUTOFF"]
MAX_IMPORT_ERRORS = app.config["MAX_IMPORT_ERRORS"]


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = db_sql.session()
    try:
        yield session
    except Exception:
        log.exception("error creating Postgres session")
        raise
    finally:
        session.close()


class Users(UserMixin, db_sql.Model):

    Column = db_sql.Column
    String = db_sql.String
    Integer = db_sql.Integer
    Boolean = db_sql.Boolean

    id = Column(Integer, primary_key=True, autoincrement=False)

    # These fields get refreshed every time the user logs in.
    #  They are only stored in the database to enable persistent login
    username = Column(String())
    firstname = Column(String())
    lastname = Column(String())
    profile = Column(String())
    access_token = Column(String())

    measurement_preference = Column(String())
    city = Column(String())
    state = Column(String())
    country = Column(String())
    email = Column(String())

    dt_last_active = Column(pg.TIMESTAMP)
    dt_indexed = Column(pg.TIMESTAMP)

    app_activity_count = Column(Integer, default=0)
    share_profile = Column(Boolean, default=False)

    def __repr__(self):
        return "U:{}".format(self.id)

    def __init__(self):
        super()
        self.cli = None
        self.idx = None

    @property
    def index(self):
        if not self.idx:
            self.idx = Index(self)
        return self.idx
    
    def db_state(self):
        state = inspect(self)
        attrs = ["transient", "pending", "persistent", "deleted", "detached"]
        return [attr for attr in attrs if getattr(state, attr)]

    def info(self):
        profile = {}
        profile.update(vars(self))
        del profile["_sa_instance_state"]
        if "activity_index" in profile:
            del profile["activity_index"]
        return profile

    def client(self, refresh=True, session=db_sql.session):
        try:
            access_info = json.loads(self.access_token)
        except Exception:
            log.info("%s using bad access_token", self)
            return

        if OFFLINE:
            return

        if not self.cli:
            self.cli = stravalib.Client(
                access_token=access_info.get("access_token"),
                rate_limiter=(lambda x=None: None)
            )
        
        now = time.time()
        one_hour = 60 * 60
        ttl = access_info["expires_at"] - now
        
        token_expired = ttl < one_hour
        start = now
        if (token_expired and refresh) or (refresh == "force"):
            
            # The existing access_token is expired
            # Attempt to refresh the token
            try:
                new_access_info = self.cli.refresh_access_token(
                    client_id=STRAVA_CLIENT_ID,
                    client_secret=STRAVA_CLIENT_SECRET,
                    refresh_token=access_info.get("refresh_token"))
                
                self.access_token = json.dumps(new_access_info)

                try:
                    session.commit()
                except Exception:
                    log.exception("postgres error")
            
                self.cli = stravalib.Client(
                    access_token=new_access_info.get("access_token"),
                    rate_limiter=(lambda x=None: None)
                )
            except Exception:
                log.exception("%s token refresh fail", self)
                self.cli = None
            else:
                elapsed = round(time.time() - start, 2)
                log.debug("%s token refresh elapsed=%s", self, elapsed)

        return self.cli

    def get_id(self):
        return str(self.id)

    def is_admin(self):
        return self.id in ADMIN

    @staticmethod
    def strava_user_data(user=None, access_info=None, session=db_sql.session):
        # fetch user data from Strava given user object or just a token
        if OFFLINE:
            return

        if user:
            client = user.client(session=session)
            access_info_string = user.access_token

        elif access_info:
            access_token = access_info["access_token"]
            access_info_string = json.dumps(access_info)
            client = stravalib.Client(access_token=access_token)

        else:
            return
        
        if not client:
            return

        try:
            strava_user = client.get_athlete()
        except Exception:
            log.exception("error getting user '%s' data from token", user)
            return

        return {
            "id": strava_user.id,
            "username": strava_user.username,
            "firstname": strava_user.firstname,
            "lastname": strava_user.lastname,
            "profile": strava_user.profile_medium or strava_user.profile,
            "measurement_preference": strava_user.measurement_preference,
            "city": strava_user.city,
            "state": strava_user.state,
            "country": strava_user.country,
            "email": strava_user.email,
            "access_token": access_info_string
        }

    def is_public(self, setting=None):
        if setting is None:
            return self.share_profile

        if setting != self.share_profile:
            self.share_profile = setting
            try:
                db_sql.session.commit()
            except Exception:
                log.exception("error updating user %s", self)
        return self.share_profile

    def update_usage(self, session=db_sql.session):
        self.dt_last_active = datetime.utcnow()
        self.app_activity_count = self.app_activity_count + 1
        session.commit()
        return self

    @classmethod
    def add_or_update(cls, session=db_sql.session, **kwargs):
        if not kwargs:
            log.info("attempted to add_or_update user with no data")
            return

        # Creates a new user or updates an existing user (with the same id)
        detached_user = cls(**kwargs)
        try:
            persistent_user = session.merge(detached_user)
            session.commit()

        except Exception:
            session.rollback()
            log.exception("error adding/updating user: %s", kwargs)
        else:
            return persistent_user

    @classmethod
    def get(cls, user_identifier, session=db_sql.session):

        # Get user from db by id or username
        try:
            # try casting identifier to int
            user_id = int(user_identifier)
        except ValueError:
            # if that doesn't work then assume it's a string username
            user = cls.query.filter_by(username=user_identifier).first()
        else:
            user = cls.query.get(user_id)
        return user if user else None

    def delete(self, deauth=True, session=db_sql.session):
        self.delete_index()
        if deauth:
            try:
                self.client().deauthorize()
            except Exception:
                pass
        try:
            session.delete(self)
            session.commit()
        except Exception:
            log.exception("error deleting %s from Postgres", self)

        log.info("%s deleted", self)

    def verify(
        self,
        days_inactive_cutoff=DAYS_INACTIVE_CUTOFF,
        update=True,
        session=db_sql.session,
        now=None
    ):

        now = now or datetime.utcnow()
        
        last_active = self.dt_last_active
        if not last_active:
            log.info("%s was never active", self)
            return

        days_inactive = (now - last_active).days

        if days_inactive >= days_inactive_cutoff:
            log.debug("%s inactive %s days > %s", self, days_inactive)
            return

        # if we got here then the user has been active recently
        #  they may have revoked our access, which we can only
        #  know if we try to get some data on their behalf
        if update and not OFFLINE:
            client = StravaClient(user=self)

            if client:
                log.debug("%s updated")
                return "updated"
            
            log.debug("%s can't create client", self)
            return
        return True

    @classmethod
    def triage(cls, days_inactive_cutoff=None, delete=False, update=False):
        with session_scope() as session:
            now = datetime.utcnow()
            stats = dict(
                count=0,
                invalid=0,
                updated=0,
                deleted=0
            )
            
            def verify_user(user):
                result = user.verify(
                    days_inactive_cutoff=days_inactive_cutoff,
                    update=update,
                    session=session,
                    now=now
                )
                return (user, result)

            def handle_verify_result(verify_user_output):
                user, result = verify_user_output
                stats["count"] += 1
                if not result:
                    if delete:
                        user.delete(session=session)
                        # log.debug("...%s deleted")
                        stats["deleted"] += 1
                    stats["invalid"] += 1
                if result == "updated":
                    stats["updated"] += 1

                if not (stats["count"] % 1000):
                    log.info("triage: %s", stats)

            def when_done(dummy):
                msg = "Users db triage: {}".format(stats)
                log.debug(msg)
                EventLogger.new_event(msg=msg)
                log.info("Triage Done: %s", stats)

            P = gevent.pool.Pool(TRIAGE_CONCURRENCY + 1)

            results = P.imap_unordered(
                verify_user, cls.query,
                maxsize=TRIAGE_CONCURRENCY + 2
            )

            def do_it():
                for result in results:
                    handle_verify_result(result)
            
            return P.spawn(do_it).link(when_done)

    @classmethod
    def dump(cls, attrs, **filter_by):
        dump = [{attr: getattr(user, attr) for attr in attrs}
                for user in cls.query.filter_by(**filter_by)]
        return dump

    def indexing(self, status=None):
        # return or set the current state of index building
        #  for this user
        key = "IDX:{}".format(self.id)
        if status is None:
            return redis.get(key)
        
        elif status is False:
            return redis.delete(key)
        else:
            return redis.setex(key, 60, status)


    

class StravaClient(object):
    # Stravalib includes a lot of unnecessary overhead
    #  so we have our own in-house client
    PAGE_REQUEST_CONCURRENCY = app.config["PAGE_REQUEST_CONCURRENCY"]
    PAGE_SIZE = app.config.get("PAGE_SIZE", 200)
    MAX_PAGE = 100

    STREAMS_TO_IMPORT = app.config["STREAMS_TO_IMPORT"]
    # MAX_PAGE = 3  # for testing

    BASE_URL = "https://www.strava.com/api/v3"
    
    GET_ACTIVITIES_ENDPOINT = "/athlete/activities?per_page={page_size}"
    GET_ACTIVITIES_URL = BASE_URL + GET_ACTIVITIES_ENDPOINT.format(
        page_size=PAGE_SIZE
    )

    GET_STREAMS_ENDPOINT = "/activities/{id}/streams?keys={keys}&key_by_type=true&series_type=time&resolution=high"
    GET_STREAMS_URL = BASE_URL + GET_STREAMS_ENDPOINT.format(
        id="{id}",
        keys=",".join(STREAMS_TO_IMPORT)
    )

    GET_ACTIVITY_ENDPOINT = "/activities/{id}?include_all_efforts=false"
    GET_ACTIVITY_URL = BASE_URL + GET_ACTIVITY_ENDPOINT.format(id="{id}")

    def __init__(self, access_token=None, user=None):
        self.user = user
        self.id = str(user)
        self.cancel_stream_import = False
        self.cancel_index_import = False

        if access_token:
            self.access_token = access_token
        elif user:
            stravalib_client = user.client()
            if not stravalib_client:
                return
            self.access_token = stravalib_client.access_token

    def __repr__(self):
        return "C:{}".format(self.id)

    @classmethod
    def strava2doc(cls, a):
        if ("id" not in a) or not a["start_latlng"]:
            return
        
        try:
            polyline = a["map"]["summary_polyline"]
            bounds = Activities.bounds(polyline)
            d = dict(
                _id=a["id"],
                user_id=a["athlete"]["id"],
                name=a["name"],
                type=a["type"],
                # group=a["athlete_count"],
                ts_UTC=a["start_date"],
                ts_local=a["start_date_local"],
                total_distance=float(a["distance"]),
                elapsed_time=int(a["elapsed_time"]),
                average_speed=float(a["average_speed"]),
                start_latlng=a["start_latlng"],
                bounds=bounds
            )
        except KeyError:
            return
        except Exception:
            log.exception("strava2doc error")
            return
        return d

    def headers(self):
        return {
            "Authorization": "Bearer {}".format(self.access_token)
        }

    def get_activity(self, _id):
        cls = self.__class__
        # get one activity summary object from strava
        url = cls.GET_ACTIVITY_URL.format(id=_id)
        # log.debug("sent request %s", url)
        try:
            response = requests.get(url, headers=self.headers())
            response.raise_for_status()

            raw = response.json()
            if "id" not in raw:
                raise UserWarning(raw)
            return cls.strava2doc(raw)
        except HTTPError as e:
            log.error(e)
            return False
        except Exception:
            log.exception("%s import-by-id %s failed", self, _id)
            return False

    def get_activities(self, ordered=False, **query):
        cls = self.__class__
        self.cancel_index_import = False

        query_base_url = cls.GET_ACTIVITIES_URL
        
        #  handle parameters
        try:
            if "limit" in query:
                limit = int(query["limit"])
            else:
                limit = None

            if "before" in query:
                before = Utility.to_epoch(query["before"])
                query_base_url += "&before={}".format(before)
        
            if "after" in query:
                after = Utility.to_epoch(query["after"])
                query_base_url += "&after={}".format(after)

        except Exception:
            log.exception("%s get_activities: parameter error", self)
            return

        page_stats = dict(pages=0, elapsed=0, empty=0)

        def page_iterator():
            page = 1
            while page <= self.final_index_page:
                yield page
                page += 1

        def request_page(pagenum):

            if pagenum > self.final_index_page:
                log.debug("%s index page %s cancelled", self, pagenum)
                return pagenum, None

            url = query_base_url + "&page={}".format(pagenum)
            
            log.debug("%s request index page %s", self, pagenum)
            page_timer = Timer()

            try:
                response = requests.get(url, headers=self.headers())
                response.raise_for_status()
                activities = response.json()

            except Exception:
                log.exception("%s failed index page request", self)
                activities = []
            
            elapsed = page_timer.elapsed()
            size = len(activities)

            #  if this page has fewer than PAGE_SIZE entries
            #  then there cannot be any further pages
            if size < cls.PAGE_SIZE:
                self.final_index_page = min(self.final_index_page, pagenum)

            # record stats
            if size:
                page_stats["elapsed"] += elapsed
                page_stats["pages"] += 1
            else:
                page_stats["empty"] += 1
            
            log.debug(
                "%s index page %s %s",
                self,
                pagenum,
                dict(elapsed=elapsed, count=size)
            )

            return pagenum, activities

        tot_timer = Timer()
        pool = gevent.pool.Pool(cls.PAGE_REQUEST_CONCURRENCY)

        num_activities_retrieved = 0
        num_pages_processed = 0

        self.final_index_page = cls.MAX_PAGE

        # imap_unordered gives a little better performance if order
        #   of results doesn't matter, which is the case if we aren't
        #   limited to the first n elements.
        mapper = pool.imap if (limit or ordered) else pool.imap_unordered

        jobs = mapper(
            request_page,
            page_iterator(),
            maxsize=cls.PAGE_REQUEST_CONCURRENCY + 2
        )

        try:
            while num_pages_processed <= self.final_index_page:

                pagenum, activities = next(jobs)

                if not activities:
                    continue

                if "errors" in activities:
                    raise UserWarning("Strava error")
                   
                num = len(activities)
                if num < cls.PAGE_SIZE:
                    total_num_activities = (pagenum - 1) * cls.PAGE_SIZE + num
                    yield {"count": total_num_activities}

                if limit and (num + num_activities_retrieved > limit):
                    # make sure no more requests are made
                    # log.debug("no more pages after this")
                    self.final_index_page = pagenum

                for a in activities:
                    doc = cls.strava2doc(a)
                    if not doc:
                        continue
                    
                    abort_signal = yield doc

                    if abort_signal:
                        log.info("%s get_activities aborted", self)
                        raise StopIteration("cancelled by user")

                    num_activities_retrieved += 1
                    if limit and (num_activities_retrieved >= limit):
                        break

                num_pages_processed += 1

        except StopIteration:
            pass
        except UserWarning:
            # TODO: find a more graceful way to do this
            log.exception("%s", activities)
            self.user.delete()
        except Exception as e:
            log.exception(e)
        
        try:
            pages = page_stats["pages"]
            if pages:
                page_stats["avg_resp"] = round(page_stats.pop("elapsed") / pages , 2)
                page_stats["rate"] = round(pages / tot_timer.elapsed(), 2)
            log.info("%s index: %s", self, page_stats)
        except Exception:
            log.exception("page stats error")

        self.final_index_page = min(pagenum, self.final_index_page)
        pool.kill()

    def get_activity_streams(self, _id):
        if self.cancel_stream_import:
            log.debug("%s import %s canceled", self, _id)
            return False

        cls = self.__class__

        url = cls.GET_STREAMS_URL.format(id=_id)

        def extract_stream(stream_dict, s):
                if s not in stream_dict:
                    raise UserWarning(
                        "{} {} not in activity {}".format(self, s, _id))
                stream = stream_dict[s]["data"]
                if len(stream) < 3:
                    raise UserWarning(
                        "{} insufficient stream {} for activity {}"
                        .format(self, s, _id)
                    )
                return stream
        
        try:
            response = requests.get(url, headers=self.headers())
            response.raise_for_status()

            stream_dict = response.json()

            if not stream_dict:
                raise UserWarning("{} no streams for {}".format(self, _id))

            streams = {
                s: extract_stream(stream_dict, s)
                for s in cls.STREAMS_TO_IMPORT
            }
        except HTTPError as e:
            code = e.response.status_code
            log.info(
                "%s http error %s for activity %s",
                self, code, _id)
            return None if code == 404 else False
        except UserWarning as e:
            log.info(e)
            return

        except Exception:
            log.exception(
                "%s failed get streams for activity %s",
                self,
                _id
            )
            return False

        return streams


class EventLogger(object):
    name = "history"
    db = mongodb.get_collection(name)

    @classmethod
    def init_db(cls, rebuild=True, size=app.config["MAX_HISTORY_BYTES"]):

        collections = mongodb.collection_names(
            include_system_collections=False)

        if (cls.name in collections) and rebuild:
            all_docs = cls.db.find()

            mongodb.create_collection("temp",
                                      capped=True,
                                      # autoIndexId=False,
                                      size=size)

            mongodb.temp.insert_many(all_docs)

            mongodb.temp.rename(cls.name, dropTarget=True)
        else:
            mongodb.create_collection(cls.name,
                                      capped=True,
                                      size=size)
            log.info("Initialized mongodb collection '%s'", cls.name)

        stats = mongodb.command("collstats", cls.name)
        cls.new_event(msg="rebuilt event log: {}".format(stats))

    @classmethod
    def get_event(cls, event_id):
        event = cls.db.find_one({"_id": ObjectId(event_id)})
        event["_id"] = str(event["_id"])
        return event

    @classmethod
    def get_log(cls, limit=0):
        events = list(
            cls.db.find(
                sort=[("$natural", pymongo.DESCENDING)]).limit(limit)
        )
        for e in events:
            e["_id"] = str(e["_id"])
            e["ts"] = Utility.to_epoch(e["ts"])
        return events

    @classmethod
    def live_updates_gen(cls, ts=None):
        def gen(ts):
            abort_signal = None
            while not abort_signal:
                cursor = cls.db.find(
                    {'ts': {'$gt': ts}},
                    cursor_type=pymongo.CursorType.TAILABLE_AWAIT
                )

                while cursor.alive and not abort_signal:
                    for doc in cursor:
                        doc["ts"] = Utility.to_epoch(doc["ts"])
                        doc["_id"] = str(doc["_id"])

                        abort_signal = yield doc
                        if abort_signal:
                            log.info("live-updates aborted")
                            return

                    # We end up here if the find() returned no
                    # documents or if the tailable cursor timed out
                    # (no new documents were added to the
                    # collection for more than 1 second)
                    gevent.sleep(2)

        if not ts:
            first = cls.db.find().sort(
                '$natural',
                pymongo.DESCENDING
            ).limit(1).next()

            ts = first['ts']

        return gen(ts)

    @classmethod
    def new_event(cls, **event):
        event["ts"] = datetime.utcnow()
        try:
            cls.db.insert_one(event)
        except Exception:
            log.exception("error inserting event %s", event)

    @classmethod
    def log_request(cls, flask_request_object, **args):
        req = flask_request_object
        args.update({
            "ip": req.access_route[-1],
            "agent": vars(req.user_agent),
        })
        cls.new_event(**args)


class Webhooks(object):
    name = "subscription"

    client = stravalib.Client()
    credentials = {
        "client_id": STRAVA_CLIENT_ID,
        "client_secret": STRAVA_CLIENT_SECRET
    }

    @classmethod
    def create(cls, callback_url):
        try:
            subs = cls.client.create_subscription(
                callback_url=callback_url,
                **cls.credentials
            )
        except Exception as e:
            log.exception("error creating subscription")
            return dict(error=str(e))

        if "updates" not in mongodb.collection_names():
            mongodb.create_collection(
                "updates",
                capped=True,
                size=1 * 1024 * 1024
            )
        log.info("create_subscription: %s", subs)
        return dict(created=subs)

    @classmethod
    def handle_subscription_callback(cls, args):
        return cls.client.handle_subscription_callback(args)

    @classmethod
    def delete(cls, subscription_id=None, delete_collection=False):
        if not subscription_id:
            subs_list = cls.list()
            if subs_list:
                subscription_id = subs_list.pop()

        if subscription_id:
            try:
                cls.client.delete_subscription(
                    subscription_id,
                    **cls.credentials
                )
            except Exception as e:
                log.exception("error deleting webhook subscription")
                return dict(error=str(e))

            if delete_collection:
                mongodb.updates.drop()

            result = dict(
                success="deleted subscription {}".format(subscription_id)
            )
        else:
            result = dict(
                error="non-existent/incorrect subscription id"
            )
        log.info(result)
        return result

    @classmethod
    def list(cls):
        subs = cls.client.list_subscriptions(**cls.credentials)
        return [sub.id for sub in subs]

    @classmethod
    def handle_update_callback(cls, update_raw):

        update = cls.client.handle_subscription_update(update_raw)
        user_id = update.owner_id
        try:
            user = Users.get(user_id)
        except Exception:
            log.exception("problem fetching user for update %s", update_raw)
            return
            
        if (not user) or (not user.index_count()):
            return

        record = dict(
            dt=datetime.utcnow(),
            subscription_id=update.subscription_id,
            owner_id=update.owner_id,
            object_id=update.object_id,
            object_type=update.object_type,
            aspect_type=update.aspect_type,
            updates=update_raw.get("updates")
        )

        _id = update.object_id

        try:
            mongodb.updates.insert_one(record)
        except Exception:
            log.exception("mongodb error")
        
        if update.object_type == "athlete":
            return

        if update.aspect_type == "update":
            if update.updates:
                # update the activity if it exists
                result = Index.update(_id, update.updates)
                if not result:
                    log.info(
                        "webhook: %s index update failed for update %s",
                        user,
                        update.updates
                    )
                    return

        #  If we got here then we know there are index entries 
        #  for this user
        if update.aspect_type == "create":
            # fetch activity and add it to the index
            result = Index.import_by_id(user, [_id])
            if result:
                log.debug("webhook: %s create %s %s", user, _id, result)
            else:
                log.info("webhook: %s create %s failed", user, _id)

        elif update.aspect_type == "delete":
            # delete the activity from the index
            Index.delete(_id)

    @staticmethod
    def iter_updates(limit=0):
        updates = mongodb.updates.find(
            sort=[("$natural", pymongo.DESCENDING)]
        ).limit(limit)

        for u in updates:
            u["_id"] = str(u["_id"])
            yield u


class Payments(object):
    name = "payments"
    db = mongodb.get_collection(name)

    @classmethod
    def init_db(cls):
        try:
            mongodb.drop_collection(cls.name)

            # create new indexes collection
            mongodb.create_collection(cls.name)
            cls.db.create_index([("ts", pymongo.DESCENDING)])
            cls.db.create_index([("user", pymongo.ASCENDING)])
        except Exception:
            log.exception("mongodb error for %s collection", cls.name)

        log.info("initialized '%s' collection", cls.name)

    @staticmethod
    def get(user=None, before=None, after=None):
        query = {}

        tsfltr = {}
        if before:
            tsfltr["$gte"] = before
        if after:
            tsfltr["$lt"] = after
        if tsfltr:
            query["ts"] = tsfltr

        field_selector = {"_id": False}
        if user:
            query["user"] = user.id
            field_selector["user"] = False

        docs = list(mongodb.payments.find(query, field_selector))

        return docs

    @staticmethod
    def add(user, amount):
        mongodb.payments.insert_one({
            "user": user.id,
            "amount": amount,
            "ts": datetime.utcnow()
        })


