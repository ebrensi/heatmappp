import pymongo
import gevent

from flask import current_app as app
from datetime import datetime
import dateutil.parser
import gevent
import msgpack
import polyline
from bson.binary import Binary


from .import mongo
from .models import Timer, FakeQueue, Utility, EventLogger, StravaClient
from . import mongo, db_sql, redis  # Global database clients

mongodb = mongo.db
log = app.logger

STORE_INDEX_TIMEOUT = app.config["STORE_INDEX_TIMEOUT"]
IMPORT_CONCURRENCY = app.config["IMPORT_CONCURRENCY"]
OFFLINE = app.config["OFFLINE"]
CACHE_TTL = app.config["CACHE_ACTIVITIES_TIMEOUT"]


class Index(object):
    name = "index"
    db = mongodb.get_collection(name)
    
    @classmethod
    # Initialize the database
    def init_db(cls, clear_cache=False):
        # drop the "indexes" collection
        try:
            mongodb.drop_collection(cls.name)
        
            # create new index collection
            mongodb.create_collection(cls.name)
            cls.db.create_index([
                ("user_id", pymongo.ASCENDING),
                ("ts_local", pymongo.DESCENDING)
            ])

            cls.db.create_index(
                "ts",
                name="ts",
                expireAfterSeconds=STORE_INDEX_TIMEOUT
            )
        except Exception:
            log.exception(
                "MongoDB error initializing %s collection",
                cls.name
            )

        log.info("initialized '%s' collection:", cls.name)

    @classmethod
    def update_ttl(cls, timeout=STORE_INDEX_TIMEOUT):

        # Update the MongoDB Index TTL if necessary
        info = cls.db.index_information()

        if "ts" not in info:
            cls.init_db
            return

        current_ttl = info["ts"]["expireAfterSeconds"]

        if current_ttl != timeout:
            result = mongodb.command(
                'collMod',
                cls.name,
                index={'keyPattern': {'ts': 1},
                       'background': True,
                       'expireAfterSeconds': timeout}
            )

            log.info("'%s' db TTL updated: %s", cls.name, result)
        else:
            # log.debug("no need to update TTL")
            pass

    @classmethod
    def delete_user_entries(cls, user):
        try:
            result = cls.db.delete_many({"user_id": user.id})
            log.debug("deleted index entries for %s", user)
            return result
        except Exception:
            log.exception(
                "error deleting index entries for %s from MongoDB",
                user
            )

    @classmethod
    def count_user_entries(cls, user):
        try:
            activity_count = cls.db.count_documents({"user_id": user.id})
        except Exception:
            log.exception(
                "Error retrieving activity count for %s",
                user
            )
            return
        else:
            return activity_count

    @classmethod
    def import_by_range(
        cls,
        client,
        queue=None,
        fetch_query={},
        out_query={},
    ):
    
        if not client:
            return

        if OFFLINE:
            if queue:
                queue.put(dict(error="No network connection"))
            return

        user = client.user
        
        for query in [fetch_query, out_query]:
            if "before" in query:
                query["before"] = Utility.to_datetime(query["before"])
            if "after" in query:
                query["after"] = Utility.to_datetime(query["after"])

        activity_ids = out_query.get("activity_ids")
        if activity_ids:
            activity_ids = set(int(_id) for _id in activity_ids)

        after = out_query.get("after")
        before = out_query.get("before")
        check_dates = (before or after)

        limit = out_query.get("limit")

        #  If we are getting the most recent n activities (limit) then
        #  we will need them to be in order.
        # otherwise, unordered fetching is faster
        if limit or check_dates:
            fetch_query["ordered"] = True
        
        count = 0
        in_range = False
        mongo_requests = []
        user = client.user
        user.indexing(0)

        timer = Timer()
        log.info("%s building index", user)

        def in_date_range(dt):
            # log.debug(dict(dt=dt, after=after, before=before))
            t1 = (not after) or (after <= dt)
            t2 = (not before) or (dt <= before)
            result = (t1 and t2)
            return result

        if not (queue and out_query):
            queue = FakeQueue()
        
        if out_query:
            yielding = True
        
        try:
            
            summaries = client.get_activities(
                **fetch_query
            )

            dtnow = datetime.utcnow()
            for d in summaries:
                if not d or "_id" not in d:
                    continue
                
                d["ts"] = dtnow
                ts_local = None

                count += 1
                if not (count % 10):
                    user.indexing(count)
                    queue.put({"idx": count})

                if yielding:
                    d2 = d.copy()

                    # cases for outputting this activity summary
                    try:
                        if activity_ids:
                            if d2["_id"] in activity_ids:
                                queue.put(d2)
                                activity_ids.discard(d2["_id"])
                                if not activity_ids:
                                    raise StopIteration
                        
                        elif limit:
                            if count <= limit:
                                queue.put(d2)
                            else:
                                raise StopIteration

                        elif check_dates:
                            ts_local = Utility.to_datetime(d2["ts_local"])
                            if in_date_range(ts_local):
                                queue.put(d2)

                                if not in_range:
                                    in_range = True

                            elif in_range:
                                # the current activity's date was in range
                                # but is no longer. (and are in order)
                                # so we can safely say there will not be any more
                                # activities in range
                                raise StopIteration

                        else:
                            queue.put(d2)

                    except StopIteration:
                        queue.put(StopIteration)
                        #  this iterator is done, as far as the consumer is concerned
                        log.debug("%s index build done yielding", user)
                        queue = FakeQueue()
                        yielding = False

                # put d in storage
                if ts_local:
                    d["ts_local"] = ts_local
                else:
                    d["ts_local"] = Utility.to_datetime(d["ts_local"])

                mongo_requests.append(
                    pymongo.ReplaceOne({"_id": d["_id"]}, d, upsert=True)
                )
                  
            if mongo_requests:
                cls.db.bulk_write(list(mongo_requests), ordered=False)

        except Exception as e:
            log.exception("%s index import error", user)
            queue.put(dict(error=str(e)))
            
        else:
            elapsed = timer.elapsed()
            msg = "{}: index import done. {}".format(user, dict(
                elapsed=elapsed,
                count=count,
                rate=round(count / elapsed, 2))
            )
            
            log.info(msg)
            EventLogger.new_event(msg=msg)
            queue.put(dict(
                msg="done indexing {} activities.".format(count)
            ))
        finally:
            queue.put(StopIteration)
            user.indexing(False)

    @classmethod
    def import_by_id(cls, client, activity_ids):
        if not client:
            return

        pool = gevent.pool.Pool(IMPORT_CONCURRENCY)
        dtnow = datetime.utcnow()

        import_stats = dict(errors=0, imported=0, empty=0)
        mongo_requests = []
        timer = Timer()
        for d in pool.imap_unordered(client.get_activity, activity_ids):
            if not d:
                if d is False:
                    import_stats["errors"] += 1
                else:
                    import_stats["empty"] += 1
                continue

            d["ts"] = dtnow
            d["ts_local"] = Utility.to_datetime(d["ts_local"])
            
            mongo_requests.append(
                pymongo.ReplaceOne({"_id": d["_id"]}, d, upsert=True)
            )

        if mongo_requests:
            try:
                cls.db.bulk_write(mongo_requests, ordered=False)
            except Exception:
                log.exception("mongo error")
            else:
                import_stats["imported"] += len(mongo_requests)
        
        import_stats["elapsed"] = timer.elapsed()
        
        return Utility.cleandict(import_stats)

    # Return an iterable of index-entries (activitiy summaries)
    #   matching query
    @classmethod
    def query(
        cls,
        user=None,
        activity_ids=None,
        exclude_ids=None,
        after=None, before=None,
        limit=0,
        update_ts=True
    ):

        if activity_ids:
            activity_ids = set(int(id) for id in activity_ids)

        if exclude_ids:
            exclude_ids = set(int(id) for id in exclude_ids)

        limit = int(limit) if limit else 0

        query = {}
        out_fields = None

        if user:
            query["user_id"] = user.id
            out_fields = {"user_id": False}

        tsfltr = {}
        if before:
            before = Utility.to_datetime(before)
            tsfltr["$lt"] = before

        if after:
            after = Utility.to_datetime(after)
            tsfltr["$gte"] = after
        
        if tsfltr:
            query["ts_local"] = tsfltr

        if activity_ids:
            query["_id"] = {"$in": list(activity_ids)}

        to_delete = None
        
        if exclude_ids:
            try:
                result = cls.db.find(
                    query,
                    {"_id": True}
                ).sort(
                    "ts_local",
                    pymongo.DESCENDING
                ).limit(limit)

            except Exception:
                log.exception("mongo error")
                return
            
            query_ids = set(
                int(doc["_id"]) for doc in result
            )

            to_delete = list(exclude_ids - query_ids)
            to_fetch = list(query_ids - exclude_ids)

            yield {"delete": to_delete, "count": len(to_fetch)}

            query["_id"] = {"$in": to_fetch}

        else:
            count = cls.db.count_documents(query)
            if limit:
                count = min(limit, count)
            yield {"count": count}
        
        try:
            if out_fields:
                cursor = cls.db.find(query, out_fields)
            else:
                cursor = cls.db.find(query)

            cursor = cursor.sort("ts_UTC", pymongo.DESCENDING).limit(limit)

        except Exception:
            log.exception("mongo error")
            return

        ids = set()

        for a in cursor:
            if update_ts:
                ids.add(a["_id"])
            yield a

        if update_ts:
            try:
                result = cls.db.update_many(
                    {"_id": {"$in": list(ids)}},
                    {"$set": {"ts": datetime.utcnow()}}
                )
            except Exception:
                log.exception("mongo error")


    ##  ----------------Index object methods-------------------------

    def __init__(self, user):
        self.user = user
        self.cli = None

    def __repr__(self):
        return "I:{}".format(self.user.id)

    @property
    def client(self):
        if not self.cli:
            self.cli = StravaClient(user=self.user)
        return self.cli
    
    # Delete one index entry
    def delete(self, id):
        try:
            return self.__class__.db.delete_one({"_id": id})
        except Exception:
            log.exception("error deleting index summary %s", id)
            return

    # delete all entries for this user
    def drop(self):
        self.__class__.delete_user_entries(self.user)

    def count(self):
        return self.__class__.count_user_entries(self.user)

    # update one index entry
    def update(self, id, updates, replace=False):
        if not updates:
            return

        db = self.__class__.db

        if replace:
            doc = {"_id": id}
            updates.update(doc)
            try:
                db.replace_one(doc, updates, upsert=True)
            except Exception:
                log.exception("mongodb error")
                return

        if "title" in updates:
            updates["name"] = updates["title"]
            del updates["title"]

        try:
            return db.update_one({"_id": id}, {"$set": updates})
        except Exception:
            log.exception("mongodb error")

    # import all index entries for this user
    def _import(self, fetch_query={}, out_query={}):
        if OFFLINE:
            return

        if not self.client:
            return [{"error": "invalid user client. not authenticated?"}]
        
        args = dict(
            fetch_query=fetch_query,
            out_query=out_query,
        )

        if out_query:
            # The presence of out_query means the caller wants
            #  us to output activities while building the index
            queue = gevent.queue.Queue()
            args.update(dict(queue=queue))
            gevent.spawn(
                self.__class__.import_by_range,
                self.client, self.client, **args)

            def index_gen(queue):
                abort_signal = False
                for A in queue:
                    if not abort_signal:
                        abort_signal = yield A
                    
            return index_gen(queue)

    def query(self, **args):
        if not self.client:
            return
        return self.__class__.query(user=self.user, **args)

class Streams(dict):
    CACHE_ACTIVITIES_TIMEOUT = app.config["CACHE_ACTIVITIES_TIMEOUT"]
    STORE_ACTIVITIES_TIMEOUT = app.config["STORE_ACTIVITIES_TIMEOUT"]

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
            expireAfterSeconds=cls.STORE_ACTIVITIES_TIMEOUT
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
                for i in range(num_repeats):
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

    ## ---------------------------------------------------------------------

    def __init__(self):
            super()
            self._key = None

    @property
    def key(self):
        if not self._key:
            self._key = self.__class__.cache_key(self["id"])
        return self._key

    def set(self, ttl=CACHE_TTL, defer=False):
        # cache it first, in case mongo is down
        packed = msgpack.packb(self)

        document = {
            "ts": datetime.utcnow(),
            "mpk": Binary(packed)
        }

        redis.setex(self.key, ttl, packed)
        try:
            self.__class__.db.update_one(
                {"_id": int(_id)},
                {"$set": document},
                upsert=True)
        except Exception:
            log.exception("failed mongodb write: streams %s", _id)
        return


class Activities(dict):

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
        
    def query_activities(
        self,
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
