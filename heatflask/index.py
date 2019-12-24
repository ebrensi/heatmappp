import pymongo
import gevent

from flask import current_app as app
from datetime import datetime

from .import mongo
from .models import Timer, FakeQueue, Utility, EventLogger, StravaClient

mongodb = mongo.db
log = app.logger

STORE_INDEX_TIMEOUT = app.config["STORE_INDEX_TIMEOUT"]
IMPORT_CONCURRENCY = app.config["IMPORT_CONCURRENCY"]
OFFLINE = app.config["OFFLINE"]


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
    def query(cls, user=None,
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
            self.cli = StravaClient(user=self)
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
    def create(self, fetch_query={}, out_query={}):
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
