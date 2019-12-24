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

    cli = None

    def __repr__(self):
        return "U:{}".format(self.id)

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
        # cls = self.__class__

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

    def index_count(self):
        return Index.user_index_size(self)

    def delete_index(self):
        return Index.delete_user_entries(self)

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

    def build_index(self, **args):
        if OFFLINE:
            return

        return Index.import_user_index(
            user=self,
            out_query=args,
            yielding=False
        )

    def query_activities(self,
                         activity_ids=None,
                         exclude_ids=[],
                         limit=None,
                         after=None, before=None,
                         streams=False,
                         owner_id=False,
                         update_index_ts=True,
                         cache_timeout=CACHE_ACTIVITIES_TIMEOUT,
                         **kwargs):

        # convert date strings to datetimes, if applicable
        if before or after:
            try:
                if after:
                    after = Utility.to_datetime(after)
                if before:
                    before = Utility.to_datetime(before)
                if before and after:
                    assert(before > after)
            except AssertionError:
                yield {"error": "Invalid Dates"}
                return

        client_query = Utility.cleandict(dict(
            limit=limit,
            after=after,
            before=before,
            activity_ids=activity_ids
        ))
        # log.debug("received query %s", client_query)

        self.strava_client = None
        if not OFFLINE:
            self.strava_client = StravaClient(user=self)
            if not self.strava_client:
                yield {"error": "bad StravaClient. cannot import"}

        # exit if this query is empty
        if not any([limit, activity_ids, before, after]):
            log.debug("%s empty query", self)
            return

        while self.indexing():
            yield {"idx": self.indexing()}
            gevent.sleep(0.5)

        if self.index_count():
            summaries_generator = Index.query(
                user=self,
                exclude_ids=exclude_ids,
                update_ts=update_index_ts,
                **client_query
            )

        else:
            # There is no activity index and we are to build one
            if OFFLINE:
                yield {"error": "cannot build index OFFLINE MODE"}
                return

            if not self.strava_client:
                yield {"error": "could not create StravaClient. authenticate?"}
                return

            summaries_generator = Index.import_user_index(
                out_query=client_query,
                client=self.strava_client
            )

            if not summaries_generator:
                log.info("Could not build index for %s", self)
                return
        
        # Here we introduce a mapper that readys an activity summary
        #  (with or without streams) to be yielded to the client
        now = datetime.utcnow()
        timer = Timer()
        self.abort_signal = False

        def export(A):
            if self.abort_signal:
                return
            if not A:
                return A

            # get an actvity object ready to send to client
            if "_id" not in A:
                # This is not an activity. It is
                #  an error message or something
                #  so pass it on.
                return A

            ttl = (A["ts"] - now).total_seconds() + STORE_INDEX_TIMEOUT
            A["ttl"] = max(0, int(ttl))

            try:
                ts_local = A.pop("ts_local")
                ts_UTC = A.pop("ts_UTC")

                ts_local = int(Utility.to_datetime(ts_local).timestamp())
                ts_UTC = int(Utility.to_datetime(ts_UTC).timestamp())
            except Exception:
                log.exception("%s, %s", ts_local, ts_UTC)
            
            # A["ts"] received by the client will be a tuple (UTC, diff)
            #  where UTC is the time of activity (GMT), and diff is
            #  hours offset so that
            #   ts_local= UTC + 3600 * diff
            A["ts"] = (ts_UTC, (ts_local - ts_UTC) / 3600)

            if owner_id:
                A.update(dict(owner=self.id, profile=self.profile))
            return A
        
        # if we are only sending summaries to client,
        #  get them ready to export and yield them
        count = 0
        if not streams:
            for A in map(export, summaries_generator):
                if A and "_id" in A:
                    count += 1
                abort_signal = yield A
                if abort_signal:
                    summaries_generator.send(abort_signal)
                    break
            log.debug(
                "%s exported %s summaries in %s", self, count, timer.elapsed()
            )
            return

        #  summaries_generator yields activity summaries without streams
        #  We want to attach a stream to each one, get it ready to export,
        #  and yield it.
        
        to_export = gevent.queue.Queue(maxsize=512)
        if self.strava_client:
            to_import = gevent.queue.Queue(maxsize=512)
        else:
            to_import = FakeQueue()

        def import_activity_streams(A):
            if not (A and "_id" in A):
                return A

            if self.abort_signal:
                log.debug("%s import %s aborted", self, A["_id"])
                return
            
            start = time.time()
            _id = A["_id"]
            log.debug("%s request import %s", self, _id)

            A = Activities.import_streams(self.strava_client, A)
            
            elapsed = time.time() - start
            log.debug("%s response %s in %s", self, _id, round(elapsed, 2))
            
            if A:
                import_stats["count"] += 1
                import_stats["elapsed"] += elapsed

            elif A is False:
                import_stats["errors"] += 1
                if import_stats["errors"] >= MAX_IMPORT_ERRORS:
                    log.info("%s Too many import errors. quitting", self)
                    self.abort_signal = True
                    return
            else:
                import_stats["empty"] += 1

            return A

        # this is where the action happens
        stats = dict(fetched=0)
        import_stats = dict(count=0, errors=0, empty=0, elapsed=0)

        import_pool = gevent.pool.Pool(IMPORT_CONCURRENCY)
        aux_pool = gevent.pool.Pool(3)
       
        if self.strava_client:
            # this is a lazy iterator that pulls activites from import queue
            #  and generates activities with streams. Roughly equivalent to
            #   imported = ( import_activity_streams(A) for A in to_import )
            imported = import_pool.imap_unordered(
                import_activity_streams, to_import
            )

            def handle_imported(imported):
                for A in imported:
                    if A and not self.abort_signal:
                        to_export.put(A)

            def imported_done(result):
                if import_stats["count"]:
                    import_stats["avg_resp"] = round(
                        import_stats["elapsed"] / import_stats["count"], 2)
                log.debug("%s done importing", self)
                to_export.put(StopIteration)

            # this background job fills export queue
            # with activities from imported
            aux_pool.spawn(handle_imported, imported).link(imported_done)

        # background job filling import and export queues
        #  it will pause when either queue is full
        chunks = Utility.chunks(summaries_generator, size=BATCH_CHUNK_SIZE)

        def process_chunks(chunks):
            for chunk in chunks:
                handle_raw(chunk)

        def handle_raw(raw_summaries):
            for A in Activities.append_streams_from_db(raw_summaries):
                handle_fetched(A)

        def handle_fetched(A):
            if not A or self.abort_signal:
                return
            if "time" in A:
                to_export.put(A)
                stats["fetched"] += 1
            elif "_id" not in A:
                to_export.put(A)
            else:
                to_import.put(A)

        def raw_done(dummy):
            # The value of result will be False
            log.debug(
                "%s done with raw summaries. elapsed=%s", self, timer.elapsed()
            )
            to_import.put(StopIteration)

        aux_pool.spawn(process_chunks, chunks).link(raw_done)

        count = 0
        for A in map(export, to_export):

            self.abort_signal = yield A
            count += 1

            if self.abort_signal:
                log.info("%s received abort_signal. quitting...", self)
                summaries_generator.send(abort_signal)
                break

        elapsed = timer.elapsed()
        stats["elapsed"] = round(elapsed, 2)
        stats = Utility.cleandict(stats)
        import_stats = Utility.cleandict(import_stats)
        if import_stats:
            try:
                import_stats["t_rel"] = round(
                    import_stats.pop("elapsed") / elapsed, 2)
                import_stats["rate"] = round(
                    import_stats["count"] / elapsed, 2)
            except Exception:
                pass
            log.info("%s import done. %s", self, import_stats)
        
        log.info("%s fetch done. %s", self, stats)
        
        if import_stats:
            stats["import"] = import_stats
        EventLogger.new_event(msg="{} fetch: {}".format(self, stats))

    def make_payment(self, amount):
        success = Payments.add(self, amount)
        return success

    def payment_record(self, after=None, before=None):
        return Payments.get(self, after=after, before=before)
                   

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


#  Activities class is only a proxy to underlying data structures.
#  There are no Activity objects
class Activities(object):
    name = "activities"
    db = mongodb.get_collection(name)

    @classmethod
    def init_db(cls, clear_cache=True):
        # Create/Initialize Activity database
        result = {}
        try:
            result["mongo_drop"] = mongodb.drop_collection(cls.name)
        except Exception as e:
            log.exception(
                "error deleting '%s' collection from MongoDB",
                cls.name
            )
            result["mongod_drop"] = str(e)

        if clear_cache:
            to_delete = redis.keys(cls.cache_key("*"))
            pipe = redis.pipeline()
            for k in to_delete:
                pipe.delete(k)

            result["redis"] = pipe.execute()

        result["mongo_create"] = mongodb.create_collection(cls.name)
        
        result = cls.db.create_index(
            "ts",
            name="ts",
            expireAfterSeconds=STORE_ACTIVITIES_TIMEOUT
        )
        log.info("initialized '{}' collection".format(cls.name))
        return result
        
    @classmethod 
    def update_ttl(cls, timeout=STORE_ACTIVITIES_TIMEOUT):

        # Update the MongoDB Activities TTL if necessary 
        info = cls.db.index_information()

        if "ts" not in info:
            cls.init_db()
            return

        current_ttl = info["ts"]["expireAfterSeconds"]

        if current_ttl != timeout:
            result = mongodb.command(
                'collMod',
                cls.name,
                index={
                    'keyPattern': {'ts': 1},
                    'background': True,
                    'expireAfterSeconds': timeout
                }
            )

            log.info("%s TTL updated: %s", cls.name, result)
        else:
            pass

    @staticmethod
    def bounds(poly):
        if poly:
            latlngs = polyline.decode(poly)

            lats = [ll[0] for ll in latlngs]
            lngs = [ll[1] for ll in latlngs]

            return {
                "SW": (min(lats), min(lngs)),
                "NE": (max(lats), max(lngs))
            }
        else:
            return {}

    @staticmethod
    def stream_encode(vals):
        diffs = [b - a for a, b in zip(vals, vals[1:])]
        encoded = []
        pair = None
        for a, b in zip(diffs, diffs[1:]):
            if a == b:
                if pair:
                    pair[1] += 1
                else:
                    pair = [a, 2]
            else:
                if pair:
                    if pair[1] > 2:
                        encoded.append(pair)
                    else:
                        encoded.extend(2 * [pair[0]])
                    pair = None
                else:
                    encoded.append(a)
        if pair:
            encoded.append(pair)
        else:
            encoded.append(b)
        return encoded

    @staticmethod
    def stream_decode(rll_encoded, first_value=0):
        running_sum = first_value
        out_list = [first_value]

        for el in rll_encoded:
            if isinstance(el, list) and len(el) == 2:
                val, num_repeats = el
                for i in xrange(num_repeats):
                    running_sum += val
                    out_list.append(running_sum)
            else:
                running_sum += el
                out_list.append(running_sum)

        return out_list

    @staticmethod
    def cache_key(id):
        return "A:{}".format(id)

    @classmethod
    def set(cls, id, data, ttl=CACHE_TTL):
        # cache it first, in case mongo is down
        packed = msgpack.packb(data)
        redis.setex(cls.cache_key(id), ttl, packed)

        document = {
            "ts": datetime.utcnow(),
            "mpk": Binary(packed)
        }
        try:
            cls.db.update_one(
                {"_id": int(id)},
                {"$set": document},
                upsert=True)
        except Exception:
            log.exception("failed mongodb write: activity %s", id)
        return

    @classmethod
    def get_many(cls, ids, ttl=CACHE_TTL, ordered=False):
        #  for each id in the ids iterable of activity-ids, this
        #  generator yields either a dict of streams
        #  or None if the streams for that activity are not in our
        #  stores.
        #  This generator uses batch operations to process the entire
        #  iterator of ids, so call it in chunks if ids iterator is
        #  a stream.

        # note we are creating a list from the entire iterable of ids!
        keys = [cls.cache_key(id) for id in ids]
    
        # Attempt to fetch activities with ids from redis cache
        read_pipe = redis.pipeline()
        for key in keys:
            read_pipe.get(key)

        # output and Update TTL for cached actitivities
        notcached = {}
        results = read_pipe.execute()

        write_pipe = redis.pipeline()

        for id, key, cached in zip(ids, keys, results):
            if cached:
                write_pipe.expire(key, ttl)
                yield (id, msgpack.unpackb(cached, encoding="utf-8"))
            else:
                notcached[int(id)] = key
        
        # Batch update TTL for redis cached activity streams
        # write_pipe.execute()
        fetched = set()
        if notcached:
            # Attempt to fetch uncached activities from MongoDB
            try:
                query = {"_id": {"$in": list(notcached.keys())}}
                results = cls.db.find(query)
            except Exception:
                log.exception("Failed mongodb query: %s", query)
                return

            # iterate through results from MongoDB query
            for doc in results:
                id = int(doc["_id"])
                packed = doc["mpk"]

                # Store in redis cache
                write_pipe.setex(notcached[id], ttl, packed)
                fetched.add(id)

                yield (id, msgpack.unpackb(packed, encoding="utf-8"))

        # All fetched streams have been sent to the client
        # now we update the data-stores
        write_pipe.execute()

        if fetched:
            # now update TTL for mongoDB records if there were any
            now = datetime.utcnow()
            try:
                cls.db.update_many(
                    {"_id": {"$in": list(fetched)}},
                    {"$set": {"ts": now}}
                )
            except Exception:
                log.exception("Failed mongoDB update_many")

    @classmethod
    def get(cls, _id, ttl=CACHE_TTL):
        packed = None
        key = cls.cache_key(id)
        cached = redis.get(key)

        if cached:
            redis.expire(key, ttl)  # reset expiration timeout
            packed = cached
        else:
            try:
                document = cls.db.find_one_and_update(
                    {"_id": int(_id)},
                    {"$set": {"ts": datetime.utcnow()}}
                )

            except Exception:
                log.debug("Failed mongodb find_one_and_update %s", _id)
                return

            if document:
                packed = document["mpk"]
                redis.setex(key, ttl, packed)
        if packed:
            return msgpack.unpackb(packed, encoding="utf-8")

    @classmethod
    def import_streams(cls, client, activity, timeout=CACHE_TTL):
        if OFFLINE:
            return
        if "_id" not in activity:
            return activity

        _id = activity["_id"]

        # start = time.time()
        # log.debug("%s request import %s", client, _id)

        result = client.get_activity_streams(_id)
        
        if not result:
            if result is None:
                # a result of None means this activity has no streams
                Index.delete(_id)
                log.debug("%s activity %s EMPTY", client, _id)

            # a result of False means there was an error
            return result

        encoded_streams = {}

        try:
            # Encode/compress latlng data into polyline format
            encoded_streams["polyline"] = polyline.encode(
                result.pop("latlng")
            )
        except Exception:
            log.exception("failed polyline encode for activity %s", _id)
            return False

        # encoded_streams = {
        #     name: encoded_stream(stream) for name, stream in result.items
        # }

        for name, stream in result.items():
            # Encode/compress these streams
            try:
                encoded_streams[name] = cls.stream_encode(stream)
            except Exception:
                log.exception(
                    "failed RLE encoding stream '%s' for activity %s",
                    name, _id)
                return False
     
        cls.set(_id, encoded_streams, timeout)
        
        activity.update(encoded_streams)

        # elapsed = round(time.time() - start, 2)
        # log.debug("%s imported %s: elapsed=%s", client, _id, elapsed)
        return activity

    @classmethod
    def append_streams_from_db(cls, summaries):
        # adds actvity streams to an iterable of summaries
        #  summaries must be manageable by a single batch operation
        to_fetch = {}
        for A in summaries:
            if "_id" not in A:
                yield A
            else:
                to_fetch[A["_id"]] = A

        if not to_fetch:
            return

        # yield stream-appended summaries that we were able to
        #  fetch streams for
        for _id, stream_data in cls.get_many(list(to_fetch.keys())):
            if not stream_data:
                continue
            A = to_fetch.pop(_id)
            A.update(stream_data)
            yield A

        # now we yield the rest of the summaries
        for A in to_fetch.values():
            yield A

    @classmethod
    def append_streams_from_import(cls, summaries, client, pool=None):
        if pool is None:
            pool = gevent.pool.Pool(IMPORT_CONCURRENCY)

        def import_activity_stream(A):
            if not A or "_id" not in A:
                return A
            imported = cls.import_streams(client, A)
            return imported
        
        return pool.imap_unordered(
            import_activity_stream,
            summaries,
            maxsize=pool.size
        )

    @classmethod
    def query(cls, queryObj):
        for user_id in queryObj:
            user = Users.get(user_id)
            if not user:
                continue

            query = queryObj[user_id]
            activities = user.query_activities(**query)

            if activities:
                for a in activities:

                    abort_signal = yield a
                    
                    if abort_signal:
                        activities.send(abort_signal)
                        return
        
        yield ""
        

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


class Utility():

    @staticmethod
    def cleandict(d):
        return {k: v for k, v in d.items() if v}

    @staticmethod
    def href(url, text):
        return "<a href='{}' target='_blank'>{}</a>".format(url, text)

    @staticmethod
    def ip_lookup_url(ip):
        return "http://freegeoip.net/json/{}".format(ip) if ip else "#"

    @staticmethod
    def ip_address(flask_request_object):
        return flask_request_object.access_route[-1]

    @classmethod
    def ip_lookup(cls, ip_address):
        r = requests.get(cls.ip_lookup_url(ip_address))
        return r.json()

    @classmethod
    def ip_timezone(cls, ip_address):
        tz = cls.ip_lookup(ip_address)["time_zone"]
        return tz if tz else 'America/Los_Angeles'

    @staticmethod
    def utc_to_timezone(dt, timezone='America/Los_Angeles'):
        from_zone = dateutil.tz.gettz('UTC')
        to_zone = dateutil.tz.gettz(timezone)
        utc = dt.replace(tzinfo=from_zone)
        return utc.astimezone(to_zone)

    @staticmethod
    def to_datetime(obj):
        if not obj:
            return
        if isinstance(obj, datetime):
            return obj
        elif isinstance(obj, int):
            return datetime.utcfromtimestamp(obj)
        try:
            dt = dateutil.parser.parse(obj, ignoretz=True)
        except ValueError:
            return
        else:
            return dt

    @staticmethod
    def to_epoch(dt):
        return int((dt - EPOCH).total_seconds())

    @staticmethod
    def set_genID(ttl=600):
        genID = "G:{}".format(uuid.uuid4().get_hex())
        redis.setex(genID, ttl, 1)
        return genID

    @staticmethod
    def del_genID(genID):
        content = redis.get(genID)
        if content:
            redis.delete(genID)

    @staticmethod
    def chunks(iterable, size=10):
        return takewhile(
            truth,
            map(tuple, starmap(islice, repeat((iter(iterable), size))))
        )
        # chunk = []
        # for thing in iterable:
        #     chunk.append(thing)
        #     if len(chunk) == size:
        #         yield chunk
        #         chunk = []
        # if chunk:
        #     yield chunk


# FakeQueue is a a queue that does nothing.  We use this for import queue if
#  the user is offline or does not have a valid access token
class FakeQueue(object):
    def put(self, x):
        return


class Timer(object):
    
    def __init__(self):
        self.start = time.time()

    def elapsed(self):
        return round(time.time() - self.start, 2)


class BinaryWebsocketClient(object):
    # WebsocketClient is a wrapper for a websocket
    #  It attempts to gracefully handle broken connections
    def __init__(self, websocket, ttl=60 * 60 * 24):
        self.ws = websocket
        self.birthday = time.time()
        self.gen = None

        # this is a the client_id for the web-page
        # accessing this websocket
        self.client_id = None

        # loc = "{REMOTE_ADDR}:{REMOTE_PORT}".format(**websocket.environ)
        ip = websocket.environ["REMOTE_ADDR"]
        self.name = "WS:{}".format(ip)
        self.key = "{}:{}".format(self.name, int(self.birthday))
        log.debug("%s OPEN", self.key)
        self.send_key()

        self.gpool = gevent.pool.Pool(2)
        self.gpool.spawn(self._pinger)

    def __repr__(self):
        return self.key

    @property
    def closed(self):
        return self.ws.closed

    # We send and receive json objects (dictionaries) encoded as strings
    def sendobj(self, obj):
        if not self.ws:
            return

        try:
            b = msgpack.packb(obj)
            self.ws.send(b, binary=True)
        except WebSocketError:
            pass
        except Exception:
            log.exception("error in sendobj")
            self.close()
            return

        return True

    def receiveobj(self):
        try:
            s = self.ws.receive()
            obj = json.loads(s)
        except (TypeError, ValueError):
            if s:
                log.info("%s recieved non-json-object: %s", self, s)
        except Exception:
            log.exception("error in receiveobj")
            return
        else:
            return obj

    def close(self):
        opensecs = int(time.time() - self.birthday)
        elapsed = timedelta(seconds=opensecs)
        log.debug("%s CLOSED. elapsed=%s", self.key, elapsed)

        try:
            self.ws.close()
        except Exception:
            pass
        self.gpool.kill()

    def send_key(self):
        self.sendobj(dict(wskey=self.key))

    def send_from(self, gen):
        # send everything from gen, a generator of dict objects.
        watchdog = self.gpool.spawn(self._watchdog, gen)

        for obj in gen:
            if self.closed:
                break
            self.sendobj(obj)
        
        watchdog.kill()

    def _pinger(self, delay=25):
        # This method runs in a separate thread, sending a ping message
        #  periodically, to keep connection from timing out.
        while not self.closed:
            gevent.sleep(25)
            try:
                self.ws.send_frame("ping", self.ws.OPCODE_PING)
            except WebSocketError:
                log.debug("can't ping. closing...")
                self.close()
                return
            except Exception:
                log.exception("%s error sending ping", self)
            # log.debug("%s sent ping", self)

    def _watchdog(self, gen):
        # This method runs in a separate thread, monitoring socket
        #  input while we send stuff from interable gen to the
        #  client device.  This allows us to receive an abort signal
        #  among other things.
        # log.debug("%s watchdog: yo!")
        while not self.closed:
            msg = self.receiveobj()
            if not msg:
                continue
            if "close" in msg:
                abort_signal = True
                log.info("%s watchdog: abort signal", self)
                try:
                    gen.send(abort_signal)
                except Exception:
                    pass
                break
        # log.debug("%s watchdog: bye bye", self)
        self.close()

