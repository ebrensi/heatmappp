

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


