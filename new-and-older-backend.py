import random
import os
import sys
import hashlib
import base64
import uuid
import time
import requests
import json
import logging
import secrets
import threading
import collections
from flask import Flask, jsonify, request
from gevent import pywsgi
import gevent
from datetime import datetime, timezone
import ssl
import mysql.connector
_DB_CFG = {
    "host":              "www.pythonanywhere.com",
    "user":              "backend name",
    "password":          "backend password",
    "database":          "flask_app.py",
    "autocommit":        True,
    "connection_timeout": 10,
}
localusername = ""
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
dih3 = os.path.join(BASE_DIR, "wowie", "R.A.M")
dih2 = os.path.join(BASE_DIR, "wowie", "R.A.M")
ApiKey = "Basic NlVSdVRTbERLS2ZZYnVEVzo="
Serverkey = "3eRmWkY6tJqCxF4"
GENERATE_FRESH_TOKENS = True
nichepeople = ["SolarHP"]

def _db():
    return mysql.connector.connect(**_DB_CFG)


app = Flask(__name__)

# ────────────────────────────────────────────────────────────────────────────
# DASHBOARD INFRASTRUCTURE (no websockets — presence is heartbeat based)
# ────────────────────────────────────────────────────────────────────────────

DASHBOARD_KEY     = os.environ.get('MY_API_KEY') or Serverkey
BANNED_USERS_FILE = os.path.join(BASE_DIR, "banned users.json")
COLORS_FILE       = os.path.join(BASE_DIR, "iron_colors.json")

# a user with no API call for this many seconds is treated as having left
PRESENCE_TIMEOUT = 120

_log_buffer           = collections.deque(maxlen=2000)
_presence             = {}   # key -> {'name','uid','version','device','ip','last'}
_presence_lock        = threading.Lock()
_version_history      = []
_version_history_lock = threading.Lock()
_version_last         = {}
_banned_devices       = set()
_temp_bans            = {}   # uname_lower -> {until, reason}
_temp_ban_lock        = threading.Lock()
_broadcasts           = collections.deque(maxlen=100)
_room_members         = {}   # room_code -> {uid: username}
_user_room            = {}   # uid -> room_code
_room_lock            = threading.Lock()

_DASH_SKIP = ('/api/logs', '/api/versions', '/api/rooms', '/api/color',
              '/api/device-ban', '/api/temp-ban', '/api/broadcast', '/api/ban-user',
              '/dashboard', 'favicon', '/photon/webhook', '/game/create', '/game/join',
              '/game/leave', '/game/close', '/game/event', '/ws/disconnect')

def _is_dash_path():
    return any(p in request.path for p in _DASH_SKIP)

class _CapturingStream:
    def __init__(self, real):
        self._real = real
    def write(self, s):
        if s and s != '\n' and not any(p in s for p in _DASH_SKIP):
            ts = time.strftime('%H:%M:%S', time.gmtime())
            _log_buffer.append(f"[{ts}] {s.rstrip()}")
        self._real.write(s)
    def flush(self): self._real.flush()
    def __getattr__(self, a): return getattr(self._real, a)

sys.stdout = _CapturingStream(sys.stdout)
sys.stderr = _CapturingStream(sys.stderr)

def _dash_auth():
    key = request.args.get('key') or request.headers.get('X-Dashboard-Key', '')
    return bool(key and key == DASHBOARD_KEY)

def normalize_username(uname):
    return (uname or '').strip().lower().replace(' ', '')

def _load_banned_users():
    if not os.path.exists(BANNED_USERS_FILE):
        return set()
    try:
        with open(BANNED_USERS_FILE, 'r', encoding='utf-8') as bf:
            data = json.load(bf)
        if isinstance(data, list):
            return {normalize_username(u) for u in data if u}
        if isinstance(data, dict):
            for k in ('banned users', 'banned'):
                if isinstance(data.get(k), list):
                    return {normalize_username(u) for u in data[k] if u}
    except Exception as e:
        logging.warning(f"[BAN] load: {e}")
    return set()

def _save_banned_users(users):
    try:
        with open(BANNED_USERS_FILE, 'w', encoding='utf-8') as bf:
            bf.write(json.dumps(sorted({normalize_username(u) for u in users if u}),
                                indent=2, ensure_ascii=False))
        return True
    except Exception as e:
        logging.warning(f"[BAN] save: {e}")
        return False

def _load_colors():
    try:
        if os.path.exists(COLORS_FILE):
            with open(COLORS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logging.warning(f"[COLOR] load: {e}")
    return {}

def _save_colors(colors):
    try:
        with open(COLORS_FILE, 'w', encoding='utf-8') as f:
            json.dump(colors, f, indent=2)
        return True
    except Exception as e:
        logging.warning(f"[COLOR] save: {e}")
        return False

def get_display_name(username):
    cfg = _load_colors().get((username or '').strip().lower())
    if not cfg:
        return username
    if isinstance(cfg, str):
        return f"<color={cfg}>{username}</color>"
    color = cfg.get('color') or ''
    name  = cfg.get('display') or username
    return f"<color={color}>{name}</color>" if color else name

def _log_version_event(user, version, device='', ip=''):
    if not user or not version or version.lower() == 'unknown':
        return
    ukey = user.lower()
    with _version_history_lock:
        if _version_last.get(ukey) == version:
            return
        _version_last[ukey] = version
        _version_history.append({'t': int(time.time()), 'user': user, 'version': version,
                                 'device': (device or '')[:40], 'ip': ip or ''})
        if len(_version_history) > 20000:
            del _version_history[:len(_version_history) - 20000]

# ── presence: last API call wins, no websocket required ──────────────────────

def _presence_key(uid, username):
    return normalize_username(username) or (uid or '')

def _touch_presence(uid, username, version='', device='', ip=''):
    key = _presence_key(uid, username)
    if not key:
        return
    with _presence_lock:
        ent = _presence.setdefault(key, {})
        ent['name'] = username or ent.get('name') or key
        if uid:     ent['uid']     = uid
        if version: ent['version'] = version
        if device:  ent['device']  = device
        if ip:      ent['ip']      = ip
        ent['last'] = time.time()

def _drop_room_member(uid):
    if not uid:
        return
    with _room_lock:
        code = _user_room.pop(uid, None)
        if code and code in _room_members:
            _room_members[code].pop(uid, None)
            if not _room_members[code]:
                del _room_members[code]

def _prune_presence():
    cutoff = time.time() - PRESENCE_TIMEOUT
    with _presence_lock:
        gone = [_presence.pop(k) for k, e in list(_presence.items()) if e.get('last', 0) < cutoff]
    for ent in gone:
        _drop_room_member(ent.get('uid', ''))
        print(f"[PRESENCE] {ent.get('name', '?')} timed out after {PRESENCE_TIMEOUT}s of silence")

def _mark_offline(uid='', username='', reason='disconnect'):
    key = _presence_key(uid, username)
    if not key:
        return False
    with _presence_lock:
        ent = _presence.pop(key, None)
    if ent is None:
        return False
    _drop_room_member(ent.get('uid') or uid)
    print(f"[PRESENCE] {ent.get('name', key)} left ({reason})")
    return True

def _presence_reaper():
    while True:
        time.sleep(30)
        try:
            _prune_presence()
        except Exception as e:
            logging.warning(f"[PRESENCE] reaper: {e}")

threading.Thread(target=_presence_reaper, daemon=True).start()

def _token_payload():
    token = request.args.get('token', '') or request.headers.get('Authorization', '')
    if token.startswith('Bearer '):
        token = token.split(' ', 1)[1]
    if token.count('.') < 2:
        return {}
    try:
        return b64decode_json(token.split('.')[1]) or {}
    except Exception:
        return {}

def _jwt_uid(token):
    try:
        return (b64decode_json(token.split('.')[1]) or {}).get('uid', '')
    except Exception:
        return ''

def _client_ip():
    fwd = request.headers.get('X-Forwarded-For', '')
    return (fwd.split(',')[0].strip() if fwd else request.remote_addr) or ''

@app.before_request
def _track_presence():
    if _is_dash_path():
        return
    payload = _token_payload()
    usn = payload.get('usn') or ''
    uid = payload.get('uid') or ''
    if not (usn or uid):
        return
    version = (payload.get('vrs') or {}).get('clientUserAgent') \
        or request.headers.get('User-Agent', '') or 'unknown'
    device = request.headers.get('X-Device-Id', '')
    ip = _client_ip()
    _touch_presence(uid, usn, version, device, ip)
    _log_version_event(usn, version, device, ip)

@app.route('/ws/disconnect', methods=['POST', 'GET'])
@app.route('/v2/ws/disconnect', methods=['POST', 'GET'])
def ws_disconnect():
    """Optional: the client may report a websocket close so the user drops
    immediately instead of waiting out PRESENCE_TIMEOUT."""
    body = request.get_json(silent=True) or {}
    payload = _token_payload()
    uid = body.get('uid') or request.args.get('uid', '') or payload.get('uid', '')
    usn = body.get('username') or request.args.get('username', '') or payload.get('usn', '')
    return jsonify({'ok': _mark_offline(uid, usn, 'ws disconnect')})


def init_db():
    try:
        conn = _db()
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id           VARCHAR(36)  PRIMARY KEY,
                username     VARCHAR(255) UNIQUE NOT NULL,
                display_name VARCHAR(255),
                lang_tag     VARCHAR(10)  DEFAULT 'en',
                metadata     TEXT         DEFAULT '{}',
                facebook_id  VARCHAR(64)  DEFAULT '',
                online       TINYINT(1)   DEFAULT 0,
                edge_count   INT          DEFAULT 0,
                create_time  DATETIME     DEFAULT CURRENT_TIMESTAMP,
                update_time  DATETIME     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_storage (
                user_id         VARCHAR(36)  NOT NULL,
                collection      VARCHAR(255) NOT NULL,
                `key`           VARCHAR(255) NOT NULL,
                value           LONGTEXT,
                version         VARCHAR(64),
                permission_read  INT DEFAULT 1,
                permission_write INT DEFAULT 1,
                create_time     DATETIME DEFAULT CURRENT_TIMESTAMP,
                update_time     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, collection, `key`)
            )
        """)
        c.close(); conn.close()
        logging.info("[DB] tables ready")
    except Exception as e:
        logging.warning(f"[DB] init_db: {e}")

def db_upsert_user(user_id, username, display_name=None, facebook_id="", edge_count=0):
    try:
        conn = _db(); c = conn.cursor()
        c.execute("""
            INSERT INTO users (id, username, display_name, facebook_id, edge_count)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                username=VALUES(username),
                display_name=VALUES(display_name),
                update_time=CURRENT_TIMESTAMP
        """, (user_id, username, display_name or username, facebook_id, edge_count))
        c.close(); conn.close()
    except Exception as e:
        logging.warning(f"[DB] db_upsert_user error: {e}")

def db_get_user_by_username(username):
    try:
        conn = _db()
        c = conn.cursor(dictionary=True)
        c.execute("SELECT * FROM users WHERE username=%s LIMIT 1", (username,))
        row = c.fetchone()
        c.close()
        conn.close()
        return row
    except Exception as e:
        return None

def db_get_user(user_id):
    try:
        conn = _db(); c = conn.cursor(dictionary=True)
        c.execute("SELECT * FROM users WHERE id=%s", (user_id,))
        row = c.fetchone(); c.close(); conn.close()
        return row
    except Exception as e:
        return None

def db_get_storage(user_id):
    try:
        conn = _db(); c = conn.cursor(dictionary=True)
        c.execute("SELECT * FROM user_storage WHERE user_id=%s", (user_id,))
        rows = c.fetchall(); c.close(); conn.close()
        out = []
        for r in rows:
            ct = r["create_time"]
            ut = r["update_time"]
            out.append({
                "collection":      r["collection"],
                "key":             r["key"],
                "user_id":         r["user_id"],
                "value":           r["value"],
                "version":         r["version"] or secrets.token_hex(16),
                "permission_read":  r["permission_read"],
                "permission_write": r["permission_write"],
                "create_time": ct.strftime("%Y-%m-%dT%H:%M:%SZ") if ct else "2024-01-01T00:00:00Z",
                "update_time": ut.strftime("%Y-%m-%dT%H:%M:%SZ") if ut else "2024-01-01T00:00:00Z",
            })
        return out
    except Exception as e:
        logging.warning(f"[DB] db_get_storage: {e}")
        return []

def db_upsert_storage(user_id, collection, key, value, permission_read=1, permission_write=1):
    try:
        version = hashlib.md5(value.encode("utf-8", errors="replace")).hexdigest()
        conn = _db(); c = conn.cursor()
        c.execute("""
            INSERT INTO user_storage
                (user_id, collection, `key`, value, version, permission_read, permission_write)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value=VALUES(value),
                version=VALUES(version),
                permission_read=VALUES(permission_read),
                permission_write=VALUES(permission_write),
                update_time=CURRENT_TIMESTAMP
        """, (user_id, collection, key, value, version, permission_read, permission_write))
        c.close(); conn.close()
    except Exception as e:
        logging.warning(f"[DB] db_upsert_storage: {e}")

try:
    init_db()
except Exception:
    pass

def b64decode_json(obj):
    return json.loads(base64.urlsafe_b64decode(obj + '=' * (-len(obj) % 4)).decode())

def b64encode_json(obj):
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip('=')

def noncevalidation(nonce, oculus_id):
    response = requests.post(
        url=f'https://graph.oculus.com/user_nonce_validate?nonce={nonce}&user_id={oculus_id}&access_token={""}',
        headers={"content-type": "application/json"}
    )
    return response.json().get("is_valid")

def SessionRefresh(token):
    changetoken = b64decode_json(token)
    now = int(time.time())
    changetoken['exp'] = now + 3600
    header = {'alg': 'HS256', 'typ': 'JWT'}
    signature = secrets.token_urlsafe(32)
    Bearer = f"{b64encode_json(header)}.{b64encode_json(changetoken)}.{signature}"
    return jsonify({
        "token": Bearer
    }), 200

def skidatoken(platformUserID, DeviceID, UserAgent):
    data = f"{platformUserID}|{DeviceID}|{UserAgent}"
    salt = os.urandom(16)
    digest = hashlib.sha256(salt + data.encode()).digest()
    token = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    mid = len(token) // 2
    token = token[:mid] + "-" + token[mid:]

    return token

def ilowkeydontknowwhy():
    efsfdfsdsdf = int(time.time())
    gfshrtfhfghjfgd = efsfdfsdsdf + 86400
    return "999999999999999999999999999"

def generate_jwt(ussername):
    header = {'alg': 'HS256', 'typ': 'JWT'}
    now = int(time.time())
    id = uuid.uuid4().hex
    id2 = uuid.uuid4().hex
    payload = {
        'tid': id2,
        'uid': id,
        'usn': ussername,
        'vrs': {
            'authID': secrets.token_hex(16),
            'clientUserAgent': "MetaQuest",
            'loginType': "meta_quest"
        },
        'exp': now + 3600,
        'iat': now
    }

    def b64encode(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip('=')

    signature = secrets.token_urlsafe(32)
    return f"{b64encode(header)}.{b64encode(payload)}.{signature}"


def generate_token_pair(username):
    user_id = secrets.token_hex(16)
    return {
        'token': generate_jwt(username),
        'refresh_token': generate_jwt(username)
    }

discord = "https://discord.com/api/webhooks/1553439076009639996/C6Y1yBVKWIh4DXHHFAP_58PBGbuiyVElhhtjYwsdnQFAtAEnrf6ZQDOT-5GZfaARTt6Z"

def log_to_discord(message: str):
    try:
        requests.post(discord, json={"content": message})
    except Exception as e:
        print(f"[Webhook Error] {e}")

@app.before_request
def log_request():
    if _is_dash_path():
        return
    try:
        headers = dict(request.headers)
        path = request.path
        queries = request.args.to_dict()
        body = request.get_json(silent=True)
        if body is None and request.form:
            body = request.form.to_dict()
        if body is None:
            body = request.data.decode(errors="ignore") or "(empty)"
        else:
            body = json.dumps(body, indent=2)

        message = (
            f"📨 **{request.method} {path}**\n"
            f"🔍 Query: ```json\n{json.dumps(queries, indent=2)}\n```\n"
            f"🧾 Headers: ```json\n{json.dumps(headers, indent=2)}\n```\n"
            f"📦 Body: ```json\n{body}\n```"
        )

        log_to_discord(message)

    except Exception as e:
        print(f"[Log Error] {e}")

@app.after_request
def log_response(response):
    if _is_dash_path():
        return response
    try:
        resp_data = response.get_data(as_text=True)
        headers = dict(response.headers)

        message = (
            f"📤 **Response {response.status}**\n"
            f"🧾 Headers: ```json\n{json.dumps(headers, indent=2)}\n```\n"
            f"📦 Body: ```json\n{resp_data[:1500]}\n```"  # limit length
        )

        log_to_discord(message)

    except Exception as e:
        print(f"[Response Log Error] {e}")

    return response

@app.route("/3/v2/rpc/research.unlock", methods=["POST"])
@app.route("/v2/rpc/research.unlock", methods=["POST"])
def cavedataavatarpurchase():
    return {
        "payload": "{\"succeeded\":true,\"wallet\":{\"softCurrency\":418291,\"hardCurrency\":418291,\"researchPoints\":418291}}"
    }

@app.route("/", methods=["GET", "POST"])
def fawhjfajkfhkj():
    return jsonify({"message": "Authenticated successfully"}), 200

@app.route("/Halloween/authenticEate/Redo/Sigma", methods=['GET', 'POST'])
def tesst():
    return jsonify({
        "ResultCode": 1,
        "Message": "Authenticated successfully"
    })

@app.route("/3/v2/account/authenticate/custom", methods=["POST", "GET"])
@app.route("/v2/account/authenticate/custom", methods=["POST", "GET"])
def authenticatecustom():
    username = request.args.get("username", "")
    if username == "M6Astraeus":
        return jsonify ({"error": "YOU BEEN BANNED FROM SEM COMPANY SKID"}), 403

    device_id = request.headers.get("X-Device-Id", "")
    if device_id and device_id in _banned_devices:
        return jsonify({"error": "device_banned", "message": "This device has been banned."}), 403
    if normalize_username(username) in _load_banned_users():
        return jsonify({"error": "username_banned", "message": "You have been banned."}), 403
    with _temp_ban_lock:
        tb = _temp_bans.get(username.strip().lower())
    if tb and tb['until'] > time.time():
        rem = int(tb['until'] - time.time())
        rsn = f" Reason: {tb['reason']}" if tb.get('reason') else ''
        return jsonify({"error": "temp_banned",
                        "message": f"You are temporarily banned for {rem//60}m {rem%60}s.{rsn}"}), 403

    body = request.get_json(silent=True) or {}
    agent = (body.get("vars") or {}).get("clientUserAgent") \
        or request.headers.get("User-Agent", "") or "MetaQuest"
    pair = generate_token_pair(username)
    _touch_presence(_jwt_uid(pair['token']), username, agent, device_id, _client_ip())
    _log_version_event(username, agent, device_id, _client_ip())
    #skid = BearerGeneration(username)
    #return skid
    return pair

@app.route("/v2/rpc/mining.balance", methods=["POST", "GET"])
def CaveDataminiangbalance():
    return jsonify ({"payload": ({
            "hardCurrency": 30000,
            "researchPoints": 40000
        })})

@app.route("/3/v2/rpc/updateWalletSoftCurrency", methods=["POST", "GET"])
@app.route("/v2/rpc/updateWalletSoftCurrency", methods=["POST", "GET"])
def updateWalletSoftCurrency():
    return jsonify({
        "Payload": "{\"ok\"}"
    })



@app.route("/v2/storage/user_loadout_templates", defaults={"user_id": None}, methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/user_loadout_templates/<user_id>", methods=["GET", "POST", "PUT"])
def loadouttemplte(user_id):
    return jsonify({

    })

@app.route("/v2/storage/user_blueprints", defaults={"user_id": None}, methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/user_blueprints/<user_id>", methods=["GET", "POST", "PUT"])
def userblueprints(user_id):
    return jsonify({

    })


@app.route("/3/v2/rpc/fishing.getWallet", methods=["POST", "GET"])
@app.route("/v2/rpc/fishing.getWallet", methods=["POST", "GET"])
def fishingwallet():
    return {
        "payload": "{\"success\":True,\"balance\":9999999}"
    }

@app.route("/3/v2/rpc/nuts.getWallet", methods=["POST", "GET"])
@app.route("/v2/rpc/nuts.getWallet", methods=["POST", "GET"])
def nutsgetWallet():
    return {
        "payload": "{\"success\":True,\"balance\":9999999}"
    }



@app.route("/v3/v2/rpc/user.getFeatureFlags", methods=["POST", "GET"])
@app.route("/v2/rpc/user.getFeatureFlags", methods=["POST", "GET"])
def getFeatureFlags():
    payloadddd = {
        "objects": [
            {
                "enableDailyMissions": True,
                "uniqueObjects": True,
                "voiceModService": "GGWP"
            }
        ]
    }
    return json.dumps({"payload": json.dumps(payloadddd)}), 200, {'Content-Type': 'application/json'}


@app.route("/v3/v2/rpc/goopPrize.getAll", methods=["POST", "GET"])
@app.route("/v2/rpc/goopPrize.getAll", methods=["POST", "GET"])
def goopgng():
    return jsonify({
        {"succeeded":true,"prizes":[{"prizeRank":1,"playerIDs":["a0fe503e-ffb5-4a95-805d-ac32bbd57806","72dc4415-fbde-467d-9ce7-13bde2e24615","03e436b7-50b5-40c9-bb87-c2e9971e4455","3379e56b-3188-43a6-8e26-54d9fcd4815d","72800349-4349-4ef7-b0e1-255e173ed7d3"],"industryName":"Mole Industries","serializedItemJson":"{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.02978515625,0.0283203125,0.484375],\"rot\":[0.204732567071915,-0.815232932567596,0.503597974777222,0.199672728776932],\"posePos\":[-0.0028076171875,0.0681610107421875,0.498504638671875],\"poseRot\":[0.118885017931461,0.82599276304245,-0.403357714414597,-0.375373065471649],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0133056640625,-0.02581787109375,0.192138671875],\"rot\":[-0.339910417795181,0.16098664700985,0.924182057380676,-0.0665733963251114],\"posePos\":[0.000732421875,-0.03125,0.177169799804688],\"poseRot\":[-0.210137531161308,0.299941003322601,0.826166272163391,0.428167104721069],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.02978515625,0.0283203125,0.484375],\"rot\":[0.204732567071915,-0.815232932567596,0.503597974777222,0.199672728776932],\"posePos\":[-0.0028076171875,0.0681610107421875,0.498504638671875],\"poseRot\":[0.118885017931461,0.82599276304245,-0.403357714414597,-0.375373065471649],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0294399261474609,0.0189666748046875,0.39892578125],\"rot\":[0.14125894010067,0.483813941478729,-0.118914544582367,-0.855470299720764],\"posePos\":[-0.020751953125,-0.001953125,0.403884887695313],\"poseRot\":[-0.0201506968587637,-0.514437973499298,0.313274472951889,0.798001706600189],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0574951171875,-0.726806640625,0.047576904296875],\"rot\":[-0.647874712944031,0.350822955369949,0.612874448299408,-0.285598397254944],\"posePos\":[0.0159912109375,0.0546875,0.587890625],\"poseRot\":[-0.442825466394424,-0.510688364505768,0.703218817710876,-0.220422983169556],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.02978515625,0.0283203125,0.484375],\"rot\":[0.204732567071915,-0.815232932567596,0.503597974777222,0.199672728776932],\"posePos\":[-0.0028076171875,0.0681610107421875,0.498504638671875],\"poseRot\":[0.118885025382042,0.82599276304245,-0.403357684612274,-0.375373065471649],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.127197265625,-0.7486572265625,0.03546142578125],\"rot\":[-0.242810606956482,0.666622221469879,0.178952634334564,-0.681640684604645],\"posePos\":[-0.00042724609375,-0.01171875,0.5235595703125],\"poseRot\":[0.61403214931488,-0.0863751694560051,-0.515110194683075,0.591747760772705],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_broom\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.02978515625,0.0283203125,0.484375],\"rot\":[0.204732567071915,-0.815232932567596,0.503597974777222,0.199672728776932],\"posePos\":[-0.0028076171875,0.0681610107421875,0.498504638671875],\"poseRot\":[0.118885017931461,0.82599276304245,-0.403357714414597,-0.375373065471649],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.1134033203125,-0.73126220703125,0.0273590087890625],\"rot\":[0.507013559341431,0.676648437976837,0.519833028316498,0.121892586350441],\"posePos\":[-0.03814697265625,-0.0084228515625,0.52630615234375],\"poseRot\":[-0.211275115609169,-0.273943990468979,-0.656709551811218,0.670112013816833],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0130615234375,-0.7481689453125,0.0273685455322266],\"rot\":[0.274230182170868,-0.443068653345108,-0.684742867946625,0.509524643421173],\"posePos\":[0.005615234375,0.7430419921875,0.024169921875],\"poseRot\":[0.274470686912537,-0.452897578477859,-0.683727860450745,0.502061545848846],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_grenade_launcher\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[{\"itemID\":\"item_cluster_grenade\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},{\"itemID\":\"item_cluster_grenade\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},{\"itemID\":\"item_cluster_grenade\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0}],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.052490234375,0.746368408203125,0.04180908203125],\"rot\":[-0.848193466663361,-0.226089641451836,-0.176952302455902,-0.445128500461578],\"posePos\":[0.140899658203125,-0.00634765625,0.22021484375],\"poseRot\":[0.252033323049545,0.82630318403244,-0.200844466686249,0.461913287639618],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_grenade_launcher\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.01397705078125,-0.7515869140625,0.0430221557617188],\"rot\":[0.519111037254333,-0.731503427028656,-0.442069172859192,-0.00121436640620232],\"posePos\":[-0.133758544921875,-0.044677734375,0.20947265625],\"poseRot\":[0.39670792222023,-0.354480296373367,-0.0802636370062828,0.842926025390625],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.01629638671875,-0.6005859375,0.0341562032699585],\"rot\":[-0.386388063430786,0.816152572631836,-0.0808482393622398,-0.421975195407867],\"posePos\":[0.0948486328125,-0.04541015625,-0.022216796875],\"poseRot\":[0.840721249580383,-0.351266592741013,0.342370361089706,0.22930808365345],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_crowbar\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.129661560058594,-0.581787109375,0.0218505859375],\"rot\":[-0.855540931224823,-0.105667740106583,0.27788433432579,-0.423868417739868],\"posePos\":[-0.019927978515625,-0.14349365234375,0.0098876953125],\"poseRot\":[-0.214356034994125,0.291348934173584,0.425039172172546,0.829764485359192],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.106369018554688,-0.7449951171875,0.03863525390625],\"rot\":[0.84938108921051,0.435015618801117,0.045527845621109,0.295365363359451],\"posePos\":[0.0875244140625,-0.752830505371094,0.01123046875],\"poseRot\":[0.43135729432106,0.373679518699646,-0.226554945111275,0.78928279876709],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0961835384368896,-0.737060546875,0.002197265625],\"rot\":[-0.700498819351196,-0.383642047643661,-0.479881644248962,0.363089740276337],\"posePos\":[0.11328125,0.759765625,-0.03863525390625],\"poseRot\":[0.74540650844574,0.611080527305603,0.0205683801323175,0.26556858420372],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_box_fan\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.0300412774085999,-0.1986083984375,-0.044677734375],\"rot\":[0.122357442975044,0.186139449477196,0.442823648452759,-0.868497550487518],\"posePos\":[-0.2845458984375,0.27496337890625,-0.0228271484375],\"poseRot\":[-0.589942991733551,-0.174053072929382,0.633815169334412,0.468989461660385],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.02349853515625,-0.7672119140625,0.0228271484375],\"rot\":[-0.655233144760132,-0.339183330535889,-0.284357905387878,0.612180471420288],\"posePos\":[0.023193359375,0.780990600585938,-0.0146484375],\"poseRot\":[-0.398695141077042,-0.205930680036545,-0.494408875703812,0.744442522525787],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.08740234375,0.7362060546875,0.0355606079101563],\"rot\":[0.903371453285217,-0.208558440208435,-0.190816313028336,0.322510182857513],\"posePos\":[-0.128173828125,0.7420654296875,0.01824951171875],\"poseRot\":[-0.488211214542389,-0.146831586956978,0.786243259906769,-0.349158853292465],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_flamethrower\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.08929443359375,-0.768798828125,-0.0196533203125],\"rot\":[-0.724011480808258,0.107675582170486,-0.401942878961563,0.550141215324402],\"posePos\":[-0.02099609375,-0.0111083984375,0.043212890625],\"poseRot\":[0.552329778671265,0.770253002643585,0.246744900941849,-0.201888754963875],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_crowbar\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.140869140625,0.5858154296875,-0.0101327896118164],\"rot\":[0.490035712718964,-0.265264481306076,-0.542060256004333,0.629023611545563],\"posePos\":[-0.1328125,-0.66961669921875,0.0013427734375],\"poseRot\":[-0.559076189994812,-0.431874245405197,0.388973951339722,0.591284811496735],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.1348876953125,0.4356689453125,-0.0461921691894531],\"rot\":[0.148564547300339,0.514655709266663,0.795278787612915,0.28388312458992],\"posePos\":[0.10748291015625,-0.50506591796875,0.047607421875],\"poseRot\":[-0.131549149751663,0.722297608852386,-0.579206585884094,0.354260891675949],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0745849609375,-0.7535400390625,-0.0298004150390625],\"rot\":[-0.139075398445129,-0.199978053569794,-0.898319065570831,0.365636110305786],\"posePos\":[0.02471923828125,0.5350341796875,0.0606689453125],\"poseRot\":[0.144017547369003,0.289498597383499,0.884766042232513,-0.335616946220398],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.02044677734375,0.5474853515625,0.03082275390625],\"rot\":[0.0155448019504547,0.864723265171051,-0.482043713331223,-0.140164241194725],\"posePos\":[0.108154296875,0.7200927734375,-0.0416259765625],\"poseRot\":[-0.14092580974102,0.669327735900879,0.725857079029083,0.0726071819663048],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.139122009277344,0.6357421875,-0.0174560546875],\"rot\":[0.877697229385376,0.279890865087509,0.00344721972942352,-0.388969480991364],\"posePos\":[0.013580322265625,-0.0998992919921875,0.01904296875],\"poseRot\":[-0.418392956256866,-0.000255768682109192,0.573780953884125,0.704075753688812],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[{\"item\":{\"itemID\":\"item_plank\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0411758422851563,0.760009765625,0.0115966796875],\"rot\":[-0.497918099164963,-0.345650106668472,0.0784777402877808,-0.79148268699646],\"posePos\":[-0.03369140625,0.74420166015625,0.02239990234375],\"poseRot\":[-0.360002130270004,0.495397925376892,0.788983523845673,0.0498438440263271],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[-0.027618408203125,-0.765869140625,-0.03533935546875],\"rot\":[0.525187909603119,-0.17417211830616,-0.617241501808167,-0.559334337711334],\"posePos\":[0.0196533203125,-0.72509765625,-0.0223388671875],\"poseRot\":[0.61744749546051,0.740744709968567,0.230191960930824,0.130642890930176],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.017730712890625,-0.7672119140625,0.0255126953125],\"rot\":[0.0826261639595032,0.081077367067337,0.677958965301514,-0.725927650928497],\"posePos\":[-0.03173828125,0.7467041015625,0.0181655883789063],\"poseRot\":[-0.492022663354874,0.518839359283447,0.534860730171204,0.450159311294556],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.00276470184326172,-0.768310546875,0.01806640625],\"rot\":[-0.643482565879822,0.291324943304062,0.675342857837677,0.212066754698753],\"posePos\":[0.0234375,0.759033203125,0.0234375],\"poseRot\":[-0.196022897958755,0.356441080570221,-0.576618254184723,0.70854526758194],\"wasDynamicStuck\":false},{\"item\":{\"itemID\":\"item_box_fan\",\"jsonData\":\"\",\"stashPos\":0,\"children\":[],\"stuckChildren\":[],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0955514907836914,-0.29248046875,-0.0250244140625],\"rot\":[0.0998826399445534,-0.307311326265335,-0.783172190189362,-0.531248211860657],\"posePos\":[0.2791748046875,0.28973388671875,-0.0228271484375],\"poseRot\":[-0.322827637195587,0.542487859725952,-0.450292736291885,0.63144725561142],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.08782958984375,0.75335693359375,0.013427734375],\"rot\":[0.549850761890411,0.374571174383163,0.205765590071678,0.717649757862091],\"posePos\":[-0.0560302734375,0.773193359375,0.03125],\"poseRot\":[-0.492069572210312,0.208050072193146,0.8436079621315,0.0539291240274906],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0},\"subParentID\":0,\"pos\":[0.0482177734375,0.7032470703125,0.0323925018310547],\"rot\":[0.708123683929443,0.26850563287735,-0.1735610216856,0.629557132720947],\"posePos\":[-0.1217041015625,-0.7203369140625,0.003173828125],\"poseRot\":[0.26366052031517,0.686940014362335,0.598314106464386,-0.317201495170593],\"wasDynamicStuck\":false}],\"grabPos\":[],\"grabRot\":[],\"state\":0,\"ammo\":0,\"isBlueprint\":true,\"scaleModifier\":0,\"colorHue\":0,\"colorSaturation\":0,\"jellyStrength\":0}"}]}
    })


@app.route("/v3/v2/rpc/advPass.getDatas", methods=["POST", "GET"])
@app.route("/v2/rpc/advPass.getData", methods=["POST", "GET"])
def advpassgetdata():
    return jsonify({
        "Payload": ""
    })


@app.route("/v3/v2/notification", methods=["POST", "GET"])
@app.route("/v2/notification", methods=["POST", "GET"])
def notification():
    return jsonify({
        "Payload": ""
    })


@app.route("/3/v2/account/link/device", methods=["POST", "GET"])
@app.route("/v2/account/link/device", methods=["POST"])
def linkdevice():
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        us1erid = payload["uid"]
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    return jsonify({
        "id": uuid.uuid4().hex,
        "user_id": us1erid,
        "linked": "true",
        "create_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    }), 200


@app.route("/3/v2/account/session/refresh", methods=["POST", "GET"])
@app.route("/v2/account/session/refresh", methods=["POST"])
def a():
    now = int(time.time())
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        userid = payload["exp"] = now + 3600
        skidload = b64decode_json(token)
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    return jsonify ({skidload})



def CaveDatarefresh():
    Authorization = request.headers.get("Authorization")
    data = request.get_json()
    token = data.get("token")
    if Authorization != "Basic NlVSdVRTbERLS2ZZYnVEVzo=":
        return jsonify({
            "Authenticated": "false",
            "message": "Authorization Incorrect",
            "error": "Authorization Header Incorrect"
        }), 401
    bearer = SessionRefresh(token)
    return jsonify(bearer), 200

@app.route("/api/v1/preauth", methods=["GET", "POST", "PUT"])
def preauth():
    playershit = request.get_json()
    CurrentPlayerVersion = "ZwYKrx9DVGFaSqMWP0Vg"
    AttestID = str(uuid.uuid4())
    platformUserID = playershit.get("platformUserID")
    DeviceID = request.headers.get("X-Device-Id")
    UserAgent = request.headers.get("User-Agent")
    expiration = ilowkeydontknowwhy()
    attestNonce = skidatoken(platformUserID, DeviceID, UserAgent)
    return jsonify ({"time": expiration, "updateType": CurrentPlayerVersion, "attestID": AttestID, "attestNonce": attestNonce})

@app.route("/3/v2/rpc/avatar.update", methods=["POST", "GET"])
@app.route("/v2/rpc/avatar.update", methods=["GET", "POST", "PUT"])
def avatarupdate():
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        username = payload["usn"]
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    return {
        "payload": "{\"succeeded\":true,\"errorCode\":\"\"}"
    }
# ids=5ceae17b8521cc25d57c8cde09af7d24
@app.route("/v2/user", methods=["POST", "GET"])
def v2user():
    id = request.args.get("ids")
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        username = payload["usn"]
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    return jsonify({"username": username, "id": id})

@app.route("/v2/friend", methods=["GET", "POST"])
def friends():
    return jsonify({
  "friends": [
    {
      "user": {
        "id": "8c1acc32f2454fb9a9a76fb6dfbf572f",
        "username": "<color=yellow>[OWNER]</color>",
        "display_name": "<color=yellow>[OWNER]</color>",
        "lang_tag": "en",
        "metadata": "{\"IsDeveloper\": true}",
        "create_time": "2024-10-19T10:33:56Z",
        "update_time": "2025-07-23T17:58:40Z"
      },
      "state": 1,
      "update_time": "2025-02-20T13:46:53Z",
      "metadata": "{\"IsDeveloper\": true}"
    }
  ],
  "cursor": "M_-DAwEBDmVkZ2VMaXN0Q3Vyc29yAf-EAAECAQVTdGF0ZQEEAAEIUG9zaXRpb24BBAAAAA3_hAL4MIT4gPadsnQA"
})

@app.route("/3/v2/rpc/promo.redeem", methods=["POST", "GET"])
@app.route("/v2/rpc/promo.redeem", methods=["POST", "GET"])
def promoredeem():
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        username = payload["usn"]
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    data = request.get_json()
    code = data.get("code")

    if code == "69420":
        if username in nichepeople:
            return jsonify ({
                "payload": "{\"stashCols\": 8, \"stashRows\": 8, \"succeeded\":true,\"wallet\":{\"softCurrency\":418291,\"hardCurrency\":418291,\"researchPoints\":418291},\"inventoryAvatarItems\":\"acc_head_creatorcap\"\"}"
            })


@app.route("/3/v2/account", methods=["POST", "GET"])
@app.route("/v2/account", methods=["GET", "POST", "PUT"])
def CaaveDataccount():
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        username = payload["usn"]
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    custom_id = secrets.token_hex(8)
    if username == "SolarHP":
        return jsonify({
            "user": {
                "id": "8c1acc32f2454fb9a9a76fb6dfbf572f",
                "username": "<color=white>SolarHP</color><color=yellow> [OWNER]</color>",
                "display_name": "<color=white>SolarHP</color><color=yellow> [OWNER]</color>",
                "lang_tag": "en",
                "metadata": {"isDeveloper": True},
                "edge_count": 240,
                "create_time": "2024-08-24T04:20:56Z",
                "update_time": "2025-07-25T18:41:17Z"
            },
            "wallet": {
                "stashCols": 8, "stashRows": 8,
                "hardCurrency": 10000000,
                "softCurrency": 10000000,
                "researchPoints": 10000000
            },
            "custom_id": "24968022896116226"
        })
    if username == "camdrippy31":
        return jsonify({
            "user": {
                "id": "8c1acc32f2454fb9a9a76fb6dfbf572f",
                "username": "<color=white>John Doe</color><color=orange> [CO OWNER]</color>",
                "display_name": "<color=white>John Doe</color><color=orange> [CO OWNER]</color>",
                "lang_tag": "en",
                "metadata": {"isDeveloper": True},
                "edge_count": 240,
                "create_time": "2024-08-24T04:20:56Z",
                "update_time": "2025-07-25T18:41:17Z"
            },
            "wallet": {
                "stashCols": 8, "stashRows": 8,
                "hardCurrency": 99999999999,
                "softCurrency": 99999999999,
                "researchPoints": 9999999999
            },
            "custom_id": "24968022896116226"
        })
    shown = get_display_name(username)
    return jsonify({
        "user": {
            "id": uuid.uuid4().hex,
            "username": shown,
            "display_name": shown,
            "lang_tag": "en",
            "metadata": {"isDeveloper": True},
            "edge_count": 240,
            "create_time": "2024-08-24T04:20:56Z",
            "update_time": "2025-07-25T18:41:17Z"
        },
        "wallet": {
            "stashCols": 8, "stashRows": 8,
            "hardCurrency": 1000000,
            "softCurrency": 1000000,
            "researchPoints": 1000000
        },
        "custom_id": custom_id
    })


def wip():
    return jsonify({"error": "small issue in the backend. working on a fix."})

def CaaveDataccount():
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if username == "SolarHP":
        return jsonify({
        "user": {
            "id": uuid.uuid4().hex,
            "username": "",
            "display_name": "",
            "lang_tag": "en",
            "metadata": {'isDeveloper': True},
            "edge_count": 240,
            "create_time": "2024-08-24T04:20:56Z",
            "update_time": "2025-07-25T18:41:17Z"
        },
        "wallet": {
            'stashCols': 8, 'stashRows': 8,
            'hardCurrency': 1000000,
            'softCurrency': 1000000,
            'researchPoints': 1000000
        },
        "custom_id": "4938276150923746"
    })
    else:
        return jsonify({
        "user": {
            "id": uuid.uuid4().hex,
            "username": username,
            "display_name": username,
            "lang_tag": "en",
            "metadata": {'isDeveloper': True},
            "edge_count": 240,
            "create_time": "2024-08-24T04:20:56Z",
            "update_time": "2025-07-25T18:41:17Z"
        },
        "wallet": {
            'stashCols': 8, 'stashRows': 8,
            'hardCurrency': 1000000,
            'softCurrency': 1000000,
            'researchPoints': 1000000
        },
        "custom_id": "4938276150923746"
    })


@app.route("/3/v2/storage/econ_avatar_items", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/econ_avatar_items", methods=["GET", "POST", "PUT"])
def CavennnnDataaeconavataritems():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_avatar_items.json")

    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_avatar_items",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)


@app.route('/3/v2/rpc/attest.start', methods=['POST'])
@app.route('/v2/rpc/attest.start', methods=['POST'])
def CaveaDatannnnatteststart():
    return jsonify({
        'payload': json.dumps({
            'status': 'success',
            'attestResult': 'Valid',
            'message': 'Attestation validated'
        })
    })

@app.route("/3/v2/storage/econ_gameplay_items", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/econ_gameplay_items", methods=["GET", "POST", "PUT"])
def CaveDatannnngaameplayitems():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_gameplay_items.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_gameplay_items",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

@app.route("/3/v2/storage/econ_research_nodes", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/econ_research_nodes", methods=["GET", "POST", "PUT"])
def CaveaDatannnnresearchnodes():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_research_nodes.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_research_nodes",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

@app.route("/3/v2/storage/econ_products", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/econ_products", methods=["GET", "POST", "PUT"])
def CaaveDannnntaeconproducts():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_products.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_products",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)
@app.route("/v3/v2/storage/econ_stash_upgrades", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage/econ_stash_upgrades", methods=["GET", "POST", "PUT"])
def CavaeDatnnnnaeconstashupgrades():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_loot_table_bindings.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_stash_upgrades",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

app.route("/v2/storage/econ_loot_table_bindings", methods=["GET", "POST", "PUT"])
def CaveDataeconloottablebindings():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    ballsjr = os.path.join(current_dir, "econ_loot_table_bindings.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_loot_table_bindings",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

app.route("/v2/storage/econ_loot_table", methods=["GET", "POST", "PUT"])
def CavaeDataeconloottable():
    ballsjr = os.path.join(dih2, "econ_loot_table.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_loot_table",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

app.route("/v2/storage/econ_crafting_materials", methods=["GET", "POST", "PUT"])
def CavaeDatacraftingmaterials():
    ballsjr = os.path.join(dih2, "econ_crafting_materials.json")
    with open(ballsjr, 'r', encoding='utf-8') as f:
        data = json.load(f) or []
    newdata = {"objects": []}
    for e in data:
        newdata["objects"].append({
            "collection": "econ_crafting_materials",
            "key": e["id"],
            "user_id": "00000000-0000-0000-0000-000000000000",
            "value": json.dumps(e),
            "version": "5c8518bd84cdb43a4e057cb62ca8d5b1",
            "permission_read": 2,
            "create_time": "2025-05-28T16:03:59Z",
            "update_time": "2025-06-11T16:16:56Z"
        })

    return jsonify(newdata)

@app.route("/3/v2/storage", methods=["GET", "POST", "PUT"])
@app.route("/v2/storage", methods=["GET", "POST", "PUT"])
def Storage():
    id = request.args.get("ids")
    token = request.args.get("token", "") or request.headers.get("Authorization", "")
    if token.startswith("Bearer "):
        token = token.split(" ", 1)[1]
    try:
        payload = b64decode_json(token.split(".")[1])
        username = payload.get("usn", "unknown")
    except Exception:
        return jsonify({"error": "invalid token"}), 403
    if request.method == "PUT":
        return "ok", 200
    if request.method == "POST":
        return jsonify (goon())
    if request.method == "GET":
        return jsonify (goon())


@app.route('/3/v2/rpc/clientBootstrap', methods=['GET', 'POST'])
@app.route('/v2/rpc/clientBootstrap', methods=['GET', 'POST'])
def CaveDatabootstrap():
    payload = {
        "updateType": "None",
        "attestResult": "Valid",
        "attestTokenExpiresAt": 1786139899,
        "photonAppID": "photon app id here",
        "photonVoiceAppID": "photon voice id here",
        "metadataHash": "3225b4ed43082cec01c79acd8b1c09ea335f77870663342a5dededf6f4979f66",
        "termsAcceptanceNeeded": [],
        "dailyMissionDateKey": "",
        "dailyMissions": None,
        "dailyMissionResetTime": 0,
        "serverTimeUnix": 1786139899,
        "gameDataURL": "https://github.com/SolarHP/AnimalCompany-semodding/blob/main/game-data/anyupdate.zip"
    }
    return json.dumps({"payload": json.dumps(payload)}), 200, {'Content-Type': 'application/json'}

def goon():
    return {"objects": _all_storage_objects()}

@app.route("/3/v2/rpc/user.getActiveSanctions", methods=["GET"])
@app.route("/v2/rpc/user.getActiveSanctions", methods=["GET"])
def getactivesanctions():
    return {
        "payload": "[]"
    }

def storage69():
    return {"objects": _all_storage_objects()}


def _all_storage_objects(user_id="edc4465a-a75f-45ad-bc9f-569d6cf821ce"):
    return [
        {
        "collection": "user_avatar",
        "key": "0",
        "user_id": user_id,
        "value": "{\"butt\": \"bp_butt_gorilla\", \"head\": \"bp_head_gorilla\", \"tail\": \"bp_tail_gorilla\", \"torso\": \"bp_torso_gorilla\", \"armLeft\": \"bp_arm_l_gorilla\", \"eyeLeft\": \"bp_eye_gorilla\", \"armRight\": \"bp_arm_r_gorilla\", \"eyeRight\": \"bp_eye_gorilla\", \"accessories\": [], \"primaryColor\": \"64F853\"}",
        "version": "e3025b7ac97aa40f31d890939328a11e",
        "permission_read": 2,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-08T09:22:23Z"
        },
        {
        "collection": "user_inventory",
        "key": "avatar",
        "user_id": user_id,
        "value": "{\"items\": [\"acc_ear_l_earring_banana\", \"acc_ear_l_earring_roundgold\", \"acc_ear_l_earring_samuraiwarriorearring\", \"acc_ear_l_earring_samuraiwarriorearring_dual\", \"acc_ear_l_earring_samuraiwarriorearring_orange\", \"acc_ear_r_earring_roundgold\", \"acc_ear_r_earring_samuraiwarriorearring\", \"acc_ear_r_earring_samuraiwarriorearring_dual\", \"acc_ear_r_earring_samuraiwarriorearring_orange\", \"acc_face_cybersuit_helmet\", \"acc_face_glasses_blue\", \"acc_face_glasses_coloredvisor\", \"acc_face_glasses_coloredvisor_orange\", \"acc_face_glasses_coolglasses\", \"acc_face_glasses_dinoshades\", \"acc_face_glasses_dinoshades_car\", \"acc_face_glasses_dinoshades_rugged\", \"acc_face_glasses_dinoshades_salmon\", \"acc_face_glasses_geek\", \"acc_face_glasses_geek_yellow\", \"acc_face_glasses_greensunset\", \"acc_face_glasses_greensunset_blue\", \"acc_face_glasses_greensunset_yellow\", \"acc_face_glasses_heart\", \"acc_face_glasses_holiday\", \"acc_face_glasses_lightvisor\", \"acc_face_glasses_pink\", \"acc_face_glasses_rayban\", \"acc_face_glasses_rayban_rose\", \"acc_face_glasses_redshades\", \"acc_face_glasses_round\", \"acc_face_glasses_shuttershadesdiscord\", \"acc_face_glasses_shuttershadesdiscord_gold\", \"acc_face_glasses_spiky\", \"acc_face_glasses_sunglasses\", \"acc_face_glasses_tacticalrobovisor\", \"acc_face_glasses_tacticalrobovisor_copper\", \"acc_face_glasses_tacticalvisor\", \"acc_face_glasses_tacticalvisor_emo\", \"acc_face_glasses_tacticalvisor_pale\", \"acc_face_glasses_visor\", \"acc_face_glasses_yellow\", \"acc_face_goggles\", \"acc_face_goggles_aura\", \"acc_face_goggles_green\", \"acc_face_goggles_red\", \"acc_face_sunglasses_damaged\", \"acc_fit_animesuit\", \"acc_fit_animesuit_blue\", \"acc_fit_animesuitfemale\", \"acc_fit_animesuitfemale_black\", \"acc_fit_apocalypsesurvivor\", \"acc_fit_apocalypsesurvivor_banana\", \"acc_fit_apocalypsesurvivor_bloodorange\", \"acc_fit_apocalypsesurvivor_blueberry\", \"acc_fit_aquaarmor\", \"acc_fit_aquaarmor_green\", \"acc_fit_aquaarmor_red\", \"acc_fit_arbordaydruid\", \"acc_fit_arbordayshaman\", \"acc_fit_arbordaytree\", \"acc_fit_bluehooded_jacket\", \"acc_fit_broccoli\", \"acc_fit_brown_basic_tanktop\", \"acc_fit_brown_hooded_zip_up_jacket\", \"acc_fit_bunnyoutfit\", \"acc_fit_bunnyoutfit_blackwhite\", \"acc_fit_bunnyoutfit_blue\", \"acc_fit_bunnyoutfit_yellow\", \"acc_fit_business_suit\", \"acc_fit_business_suit_heartsuit\", \"acc_fit_chinesewarriorarmor\", \"acc_fit_chinesewarriorarmor_gold\", \"acc_fit_cincodemayo\", \"acc_fit_cincodemayo_crimson\", \"acc_fit_cincodemayo_forest\", \"acc_fit_cincodemayo_saffron\", \"acc_fit_cincodemayoblanket\", \"acc_fit_cincodemayoblanket_crimson\", \"acc_fit_cincodemayoblanket_forest\", \"acc_fit_cincodemayoblanket_saffron\", \"acc_fit_clownoutfit\", \"acc_fit_coloredjacket\", \"acc_fit_coloredjacket_fire\", \"acc_fit_coloredjacket_red\", \"acc_fit_coolsuit\", \"acc_fit_coolsuit_bluewhite\", \"acc_fit_coolsuit_purplewhite\", \"acc_fit_cubes\", \"acc_fit_cubes_frog\", \"acc_fit_cubes_gorilla\", \"acc_fit_cubes_shepherd\", \"acc_fit_cupidoutfit\", \"acc_fit_cybersuit\", \"acc_fit_demolitionjumpsuit\", \"acc_fit_demolitionjumpsuit_aura\", \"acc_fit_demolitionjumpsuit_green\", \"acc_fit_demolitionjumpsuit_red\", \"acc_fit_demonboyband\", \"acc_fit_demonboyband_glowblue\", \"acc_fit_demonboyband_glowgreen\", \"acc_fit_demonboyband_glowpurple\", \"acc_fit_denimjacket\", \"acc_fit_denimjacket_hippie\", \"acc_fit_discobutt\", \"acc_fit_discobutt_phoenix\", \"acc_fit_diversuit\", \"acc_fit_diversuit_green\", \"acc_fit_diversuit_rusty\", \"acc_fit_diversuit_yellow\", \"acc_fit_dwarf\", \"acc_fit_dwarf_blue\", \"acc_fit_dwarf_tin\", \"acc_fit_dwarf_wood\", \"acc_fit_early_bird_tshirt\", \"acc_fit_eastervest\", \"acc_fit_eastervest_blue\", \"acc_fit_eastervest_yellow\", \"acc_fit_eggsuit\", \"acc_fit_eggsuit_b\", \"acc_fit_eggsuit_chocolate\", \"acc_fit_eggsuit_golden\", \"acc_fit_elfoutfit\", \"acc_fit_elfoutfit_blue\", \"acc_fit_elfoutfit_pink\", \"acc_fit_face_glasses_eyepatch\", \"acc_fit_face_glasses_eyepatch_desert\", \"acc_fit_face_glasses_eyepatch_green\", \"acc_fit_face_glasses_eyepatch_snow\", \"acc_fit_fallleafponcho\", \"acc_fit_ghostbuster_jacket\", \"acc_fit_ghostcloth\", \"acc_fit_gladiatorarmor\", \"acc_fit_gladiatorarmor_cat\", \"acc_fit_gladiatorarmor_frog\", \"acc_fit_gladiatorarmor_gorilla\", \"acc_fit_gladiatorarmor_pug\", \"acc_fit_gladiatorarmor_skeleton\", \"acc_fit_glowgrilla\", \"acc_fit_glowgrilla_purple\", \"acc_fit_glowjacket_pink\", \"acc_fit_glowjacket_red\", \"acc_fit_grapplesoldier_uniform\", \"acc_fit_grapplesoldier_uniform_black\", \"acc_fit_grapplesoldier_uniform_blue\", \"acc_fit_grapplesoldier_uniform_red\", \"acc_fit_grimreaper\", \"acc_fit_grimreaper_gold\", \"acc_fit_grimreaper_red\", \"acc_fit_grimreaper_white\", \"acc_fit_grimreaperpremium\", \"acc_fit_halloween_jacket\", \"acc_fit_halloween_shirt\", \"acc_fit_halloweenskeletonshirt\", \"acc_fit_halloweenskeletonshirt_blue\", \"acc_fit_halloweenskeletonshirt_red\", \"acc_fit_halloweenskeletonshirt_yellow\", \"acc_fit_hawaiiangirl\", \"acc_fit_hawaiiangirl_blonde\", \"acc_fit_hawaiiangirl_brown\", \"acc_fit_hawaiianshirt\", \"acc_fit_hawaiianshirt_blue\", \"acc_fit_hawaiianshirt_yellow\", \"acc_fit_hawaiianshirtgold\", \"acc_fit_hazmatsuit\", \"acc_fit_hazmatsuit_blue\", \"acc_fit_hazmatsuit_green\", \"acc_fit_hazmatsuit_orange\", \"acc_fit_hazmatsuit_pink\", \"acc_fit_hazmatsuit_purple\", \"acc_fit_hazmatsuit_red\", \"acc_fit_head_animehair_longa\", \"acc_fit_head_animehair_longb\", \"acc_fit_head_animehair_shorta\", \"acc_fit_head_animehair_shortb\", \"acc_fit_head_animehair_shortc\", \"acc_fit_head_apocalypsesurvivor\", \"acc_fit_head_apocalypsesurvivor_banana\", \"acc_fit_head_apocalypsesurvivor_bloodorange\", \"acc_fit_head_apocalypsesurvivor_blueberry\", \"acc_fit_head_aquaarmor\", \"acc_fit_head_aquaarmor_green\", \"acc_fit_head_aquaarmor_red\", \"acc_fit_head_bunnyears\", \"acc_fit_head_bunnyears_blue\", \"acc_fit_head_bunnyears_yellow\", \"acc_fit_head_cube\", \"acc_fit_head_cube_frog\", \"acc_fit_head_cube_gorilla\", \"acc_fit_head_cube_shepherd\", \"acc_fit_head_cupidhair\", \"acc_fit_head_demonboyband\", \"acc_fit_head_demonboyband_glowblue\", \"acc_fit_head_demonboyband_glowgreen\", \"acc_fit_head_demonboyband_glowpurple\", \"acc_fit_head_diversuit\", \"acc_fit_head_diversuit_green\", \"acc_fit_head_diversuit_rusty\", \"acc_fit_head_diversuit_yellow\", \"acc_fit_head_dwarfhelmet\", \"acc_fit_head_dwarfhelmet_blue\", \"acc_fit_head_dwarfhelmet_tin\", \"acc_fit_head_dwarfhelmet_wood\", \"acc_fit_head_elfhat\", \"acc_fit_head_elfhat_blue\", \"acc_fit_head_elfhat_pink\", \"acc_fit_head_glowgrilla\", \"acc_fit_head_glowgrilla_purple\", \"acc_fit_head_hair_spikey\", \"acc_fit_head_hawaiiangirl\", \"acc_fit_head_hawaiiangirl_blonde\", \"acc_fit_head_hawaiiangirl_brown\", \"acc_fit_head_headband\", \"acc_fit_head_headband_desert\", \"acc_fit_head_headband_red\", \"acc_fit_head_headband_snow\", \"acc_fit_head_kingofhearts\", \"acc_fit_head_kingofhearts_blue\", \"acc_fit_head_kpopvisor\", \"acc_fit_head_kpopvisor_black\", \"acc_fit_head_kpopvisor_blue\", \"acc_fit_head_kpopvisor_darkpuple\", \"acc_fit_head_mohawk\", \"acc_fit_head_piratebandana\", \"acc_fit_head_queenofheartscrown\", \"acc_fit_head_racerhelmet\", \"acc_fit_head_racerhelmet_black\", \"acc_fit_head_racerhelmet_white\", \"acc_fit_head_racerhelmet_yellow\", \"acc_fit_head_redknight\", \"acc_fit_head_redknight_summon\", \"acc_fit_head_rockhair\", \"acc_fit_head_rockhair_redblonde\", \"acc_fit_head_romanhelmet\", \"acc_fit_head_romanhelmet_cat\", \"acc_fit_head_romanhelmet_frog\", \"acc_fit_head_romanhelmet_gorilla\", \"acc_fit_head_romanhelmet_pug\", \"acc_fit_head_romanhelmet_skeleton\", \"acc_fit_head_santa\", \"acc_fit_head_santa_blue\", \"acc_fit_head_santa_purple\", \"acc_fit_head_spacehelmet\", \"acc_fit_head_spacehelmet_blue\", \"acc_fit_head_spacehelmet_orange\", \"acc_fit_head_spacehelmet_rainbow\", \"acc_fit_head_squidgamedollhair\", \"acc_fit_head_squidgamedollhair_brunette\", \"acc_fit_head_steampunkmask\", \"acc_fit_head_steampunkmask_boat\", \"acc_fit_head_steampunkmask_fireengine\", \"acc_fit_head_steampunkmask_tinman\", \"acc_fit_head_sungear\", \"acc_fit_head_supermask\", \"acc_fit_head_supermask_atom\", \"acc_fit_head_supermask_leaf\", \"acc_fit_head_supermask_recycle\", \"acc_fit_head_tacticalmininghelmet\", \"acc_fit_head_tacticalmininghelmet_almandine\", \"acc_fit_head_tacticalmininghelmet_diamond\", \"acc_fit_head_tacticalmininghelmet_emerald\", \"acc_fit_head_tie\", \"acc_fit_head_tie_blue\", \"acc_fit_head_warrior_ascendant\", \"acc_fit_head_warrior_ascendant_dark\", \"acc_fit_head_warrior_engineer\", \"acc_fit_head_warrior_scholar\", \"acc_fit_holidaysuit\", \"acc_fit_itemployee\", \"acc_fit_itemployee_blue\", \"acc_fit_jacketleatherfuture\", \"acc_fit_kilt\", \"acc_fit_kilt_blue\", \"acc_fit_kilt_red\", \"acc_fit_kilt_yellow\", \"acc_fit_kingofhearts\", \"acc_fit_kingofhearts_red\", \"acc_fit_knightarmorscarf\", \"acc_fit_kpop\", \"acc_fit_kpop_black\", \"acc_fit_kpop_purple\", \"acc_fit_kpop_white\", \"acc_fit_kungfucoat\", \"acc_fit_kungfucoat_black\", \"acc_fit_kungfucoat_blue\", \"acc_fit_leatherjacket\", \"acc_fit_lifejacket\", \"acc_fit_lifejacket_blue\", \"acc_fit_lifejacket_green\", \"acc_fit_lifejacket_orange\", \"acc_fit_mask_samuraiwarriormouthlock\", \"acc_fit_necromancer\", \"acc_fit_nfljersey\", \"acc_fit_nfljersey_bear\", \"acc_fit_nfljersey_cat\", \"acc_fit_nfljersey_frog\", \"acc_fit_nfljersey_pug\", \"acc_fit_nfljersey_skeleton\", \"acc_fit_ogretop\", \"acc_fit_orcmage\", \"acc_fit_orcmage_summon\", \"acc_fit_parkranger\", \"acc_fit_parkranger_car\", \"acc_fit_parkranger_rugged\", \"acc_fit_parkranger_salmon\", \"acc_fit_pilgrim\", \"acc_fit_piratecoat\", \"acc_fit_piratecoat_red\", \"acc_fit_piratevest\", \"acc_fit_policeman\", \"acc_fit_policeman_brown\", \"acc_fit_potofgold\", \"acc_fit_potofgold_flames\", \"acc_fit_potofgold_gold\", \"acc_fit_potofgold_green\", \"acc_fit_princessdress\", \"acc_fit_princessdress_green\", \"acc_fit_queenofheartsdress\", \"acc_fit_racerjacket\", \"acc_fit_racerjacket_black\", \"acc_fit_racerjacket_white\", \"acc_fit_racerjacket_yellow\", \"acc_fit_redknight\", \"acc_fit_redknight_summon\", \"acc_fit_rustic_brown_winter_coat\", \"acc_fit_samurai\", \"acc_fit_samurai_purple\", \"acc_fit_samurai_red\", \"acc_fit_samurai_white\", \"acc_fit_samuraiwarrior\", \"acc_fit_samuraiwarrior_dual\", \"acc_fit_samuraiwarrior_orange\", \"acc_fit_samuraiwarrior_pink\", \"acc_fit_santa\", \"acc_fit_santa_blue\", \"acc_fit_santa_purple\", \"acc_fit_securityguard\", \"acc_fit_securityguard_green\", \"acc_fit_shoegloves\", \"acc_fit_sneakingsuit\", \"acc_fit_sneakingsuit_desert\", \"acc_fit_sneakingsuit_green\", \"acc_fit_sneakingsuit_snow\", \"acc_fit_spacesuit\", \"acc_fit_spacesuit_blue\", \"acc_fit_spacesuit_orange\", \"acc_fit_spacesuit_rainbow\", \"acc_fit_squidgamedoll\", \"acc_fit_squidgamedoll_brunette\", \"acc_fit_squidgamefrontman\", \"acc_fit_squidgamefrontman_white\", \"acc_fit_squidgamejacket\", \"acc_fit_squidgamejacket_purple\", \"acc_fit_squidgamejacket_red\", \"acc_fit_squidgamejacketparticipant\", \"acc_fit_steampunkrobot\", \"acc_fit_steampunkrobot_boat\", \"acc_fit_steampunkrobot_fireengine\", \"acc_fit_steampunkrobot_tinman\", \"acc_fit_supermansuit\", \"acc_fit_supermansuit_atom\", \"acc_fit_supermansuit_leaf\", \"acc_fit_supermansuit_recycle\", \"acc_fit_sweater_turkey\", \"acc_fit_tacticalarmor\", \"acc_fit_tacticalarmor_emo\", \"acc_fit_tacticalarmor_pale\", \"acc_fit_tacticalmining\", \"acc_fit_tacticalmining_almandine\", \"acc_fit_tacticalmining_diamond\", \"acc_fit_tacticalmining_emerald\", \"acc_fit_tacticalroboarmor\", \"acc_fit_tacticalroboarmor_copper\", \"acc_fit_tight_fit_blue_tshirt\", \"acc_fit_tight_fit_blue_tshirt_heartshirt\", \"acc_fit_turkeyhunter\", \"acc_fit_tuxleprechaun\", \"acc_fit_varsityjacket\", \"acc_fit_varsityjacket_black\", \"acc_fit_varsityjacket_gold\", \"acc_fit_varsityjacket_gold2\", \"acc_fit_varsityjacket_toasty\", \"acc_fit_viking\", \"acc_fit_viking_firestorm\", \"acc_fit_viking_flaxen\", \"acc_fit_viking_twilight\", \"acc_fit_warrior_ascendant\", \"acc_fit_warrior_ascendant_dark\", \"acc_fit_warrior_engineer\", \"acc_fit_warrior_scholar\", \"acc_fit_winterscarf\", \"acc_fit_worndownemployee\", \"acc_fit_worndownemployee_green\", \"acc_head_alienears\", \"acc_head_arbordaycrown\", \"acc_head_arbordaydruidhoodie\", \"acc_head_artisthat\", \"acc_head_banana_hat\", \"acc_head_beach_hat\", \"acc_head_beanie\", \"acc_head_beerhat\", \"acc_head_beret\", \"acc_head_beret_blue\", \"acc_head_beret_red\", \"acc_head_beret_yellow\", \"acc_head_black_1984_headphones\", \"acc_head_cap\", \"acc_head_catearscap\", \"acc_head_cathelmet\", \"acc_head_cathelmet_phoenix\", \"acc_head_chinesewarriorhelmet\", \"acc_head_chinesewarriorhelmet_gold\", \"acc_head_clownhat\", \"acc_head_coloredcap\", \"acc_head_coloredcap_fire\", \"acc_head_coloredcap_red\", \"acc_head_cone\", \"acc_head_cop\", \"acc_head_cowboy_hat\", \"acc_head_creatorcap\", \"acc_head_crochethat\", \"acc_head_crown\", \"acc_head_egghat\", \"acc_head_egghat_b\", \"acc_head_egghat_chocolate\", \"acc_head_egghat_golden\", \"acc_head_fedora_hat\", \"acc_head_frogeyes\", \"acc_head_goldenhalo\", \"acc_head_gopro\", \"acc_head_gopro_easter\", \"acc_head_goprojune\", \"acc_head_grapplesoldier_beret\", \"acc_head_grapplesoldier_beret_black\", \"acc_head_grapplesoldier_beret_blue\", \"acc_head_grapplesoldier_beret_red\", \"acc_head_grimreapercrown\", \"acc_head_hardhat\", \"acc_head_hardhat_canopy\", \"acc_head_hazmathelmet\", \"acc_head_hazmathelmet_blue\", \"acc_head_hazmathelmet_green\", \"acc_head_hazmathelmet_orange\", \"acc_head_hazmathelmet_pink\", \"acc_head_hazmathelmet_purple\", \"acc_head_hazmathelmet_red\", \"acc_head_horns\", \"acc_head_jesterhat\", \"acc_head_knifehat\", \"acc_head_kungfuhat\", \"acc_head_mage_hat\", \"acc_head_mexicanhat_redblack\", \"acc_head_mimic_hat\", \"acc_head_minerhat\", \"acc_head_minerhat_aura\", \"acc_head_minerhat_green\", \"acc_head_minerhat_red\", \"acc_head_mop\", \"acc_head_nflhelmet\", \"acc_head_nflhelmet_bear\", \"acc_head_nflhelmet_cat\", \"acc_head_nflhelmet_dog\", \"acc_head_nflhelmet_frog\", \"acc_head_nflhelmet_skeleton\", \"acc_head_parkranger\", \"acc_head_parkranger_car\", \"acc_head_parkranger_rugged\", \"acc_head_parkranger_salmon\", \"acc_head_partyhat\", \"acc_head_patriothat\", \"acc_head_pilgrimhat\", \"acc_head_pimp_hat\", \"acc_head_piratehat\", \"acc_head_piratehat_red\", \"acc_head_plunger\", \"acc_head_policehat\", \"acc_head_policehat_brown\", \"acc_head_propeller_cap\", \"acc_head_rainbow\", \"acc_head_rainbow_gold\", \"acc_head_rainbow_green\", \"acc_head_rainbow_ofdarkness\", \"acc_head_ricepattyhat\", \"acc_head_ricepattyhat_blue\", \"acc_head_securityguard\", \"acc_head_securityguard_green\", \"acc_head_sombrero\", \"acc_head_sombrero_crimson\", \"acc_head_sombrero_forest\", \"acc_head_sombrero_saffron\", \"acc_head_summerhat\", \"acc_head_summerhat_blue\", \"acc_head_summerhat_golden\", \"acc_head_summerhat_yellow\", \"acc_head_sweatband\", \"acc_head_tallcowboy_hat\", \"acc_head_tiara\", \"acc_head_tiara_gold\", \"acc_head_toilet_hat\", \"acc_head_top_hat\", \"acc_head_tophatclover\", \"acc_head_turkeyhat\", \"acc_head_turkeyhunter\", \"acc_head_vikinghelmet\", \"acc_head_vikinghelmet_firestorm\", \"acc_head_vikinghelmet_flaxen\", \"acc_head_vikinghelmet_twilight\", \"acc_head_winterglasses\", \"acc_head_winterhat\", \"acc_mask_arbordayshaman\", \"acc_mask_diademuertos\", \"acc_mask_hazmat\", \"acc_mask_hazmat_blue\", \"acc_mask_hazmat_green\", \"acc_mask_hazmat_orange\", \"acc_mask_hazmat_pink\", \"acc_mask_hazmat_purple\", \"acc_mask_hazmat_red\", \"acc_mask_jason\", \"acc_mask_medicalmask\", \"acc_mask_samuraimaskdemon\", \"acc_mask_samuraimaskdemon_purple\", \"acc_mask_samuraimaskdemon_red\", \"acc_mask_samuraimaskdemon_white\", \"acc_mask_squidgame\", \"acc_mask_squidgame_nut\", \"acc_mask_squidgame_star\", \"acc_mask_squidgamefrontman\", \"acc_mask_squidgamefrontman_gold\", \"acc_mouthcorner_lolipop\", \"acc_mouthcorner_lolipop_green\", \"acc_mouthcorner_rose\", \"acc_mouthcorner_tusks\", \"acc_mouthcorner_tusks_summon\", \"acc_nosetip_bunny\", \"acc_nosetip_bunny_blackwhite\", \"acc_nosetip_bunny_blue\", \"acc_nosetip_bunny_yellow\", \"acc_nosetip_clownnose\", \"acc_nosetip_steampunkmask\", \"acc_nosetip_steampunkmask_boat\", \"acc_nosetip_steampunkmask_fireengine\", \"acc_nosetip_steampunkmask_tinman\", \"animal_cat\", \"animal_chameleon\", \"animal_crab\", \"animal_cyborg_duck\", \"animal_duck\", \"animal_frog\", \"animal_germanshep\", \"animal_goat\", \"animal_gorilla\", \"animal_kitten\", \"animal_mole\", \"animal_polarbear\", \"animal_pug\", \"animal_rabbit\", \"animal_raccoon\", \"animal_reindeer\", \"animal_shark\", \"animal_shark_goblin\", \"animal_shark_hammer\", \"animal_skeletongorilla\", \"animal_tiger\", \"animal_trex\", \"animal_trex_shorthands\", \"animal_trex_winged\", \"animal_turkey\", \"animal_turtle\", \"bp_arm_l_cat\", \"bp_arm_l_chameleon\", \"bp_arm_l_crab\", \"bp_arm_l_demonarms\", \"bp_arm_l_duck\", \"bp_arm_l_duck_metal\", \"bp_arm_l_frog\", \"bp_arm_l_germanshep\", \"bp_arm_l_goat\", \"bp_arm_l_goldarms\", \"bp_arm_l_gorilla\", \"bp_arm_l_gorilla_og\", \"bp_arm_l_hookarms\", \"bp_arm_l_iceyarms\", \"bp_arm_l_kitten\", \"bp_arm_l_mole\", \"bp_arm_l_polarbear\", \"bp_arm_l_pug\", \"bp_arm_l_rabbit\", \"bp_arm_l_raccoon\", \"bp_arm_l_reindeer\", \"bp_arm_l_shark\", \"bp_arm_l_skeletongorilla\", \"bp_arm_l_slinkyarms\", \"bp_arm_l_tiger\", \"bp_arm_l_trex\", \"bp_arm_l_trex_short\", \"bp_arm_l_trex_wing\", \"bp_arm_l_turkey\", \"bp_arm_l_turtle\", \"bp_arm_r_cat\", \"bp_arm_r_chameleon\", \"bp_arm_r_crab\", \"bp_arm_r_demonarms\", \"bp_arm_r_duck\", \"bp_arm_r_duck_metal\", \"bp_arm_r_frog\", \"bp_arm_r_germanshep\", \"bp_arm_r_goat\", \"bp_arm_r_goldarms\", \"bp_arm_r_gorilla\", \"bp_arm_r_gorilla_og\", \"bp_arm_r_hookarms\", \"bp_arm_r_iceyarms\", \"bp_arm_r_kitten\", \"bp_arm_r_mole\", \"bp_arm_r_polarbear\", \"bp_arm_r_pug\", \"bp_arm_r_rabbit\", \"bp_arm_r_raccoon\", \"bp_arm_r_reindeer\", \"bp_arm_r_shark\", \"bp_arm_r_skeletongorilla\", \"bp_arm_r_slinkyarms\", \"bp_arm_r_tiger\", \"bp_arm_r_trex\", \"bp_arm_r_trex_short\", \"bp_arm_r_trex_wing\", \"bp_arm_r_turkey\", \"bp_arm_r_turtle\", \"bp_butt_bigbutt\", \"bp_butt_bigbutt_animals\", \"bp_butt_bigbutt_ducky\", \"bp_butt_bigbutt_galaxy\", \"bp_butt_bigbutt_golden\", \"bp_butt_bigbutt_hearts\", \"bp_butt_bigbutt_leaves\", \"bp_butt_cat\", \"bp_butt_chameleon\", \"bp_butt_crab\", \"bp_butt_duck\", \"bp_butt_frog\", \"bp_butt_germanshep\", \"bp_butt_goat\", \"bp_butt_gorilla\", \"bp_butt_kitten\", \"bp_butt_mole\", \"bp_butt_polarbear\", \"bp_butt_pug\", \"bp_butt_rabbit\", \"bp_butt_raccoon\", \"bp_butt_reindeer\", \"bp_butt_shark\", \"bp_butt_skeletongorilla\", \"bp_butt_tiger\", \"bp_butt_trex\", \"bp_butt_turkey\", \"bp_butt_turtle\", \"bp_eye_alieneyes\", \"bp_eye_buttoneyes\", \"bp_eye_cat\", \"bp_eye_chameleon\", \"bp_eye_crab\", \"bp_eye_demoneyes\", \"bp_eye_duck\", \"bp_eye_duck_cyborg\", \"bp_eye_frog\", \"bp_eye_frogeyes\", \"bp_eye_germanshep\", \"bp_eye_gloweyes\", \"bp_eye_goat\", \"bp_eye_goldeyes\", \"bp_eye_gorilla\", \"bp_eye_hearteyes\", \"bp_eye_kitten\", \"bp_eye_lenseyes\", \"bp_eye_lizardeyes\", \"bp_eye_mole\", \"bp_eye_ninjaeyes\", \"bp_eye_polarbear\", \"bp_eye_pug\", \"bp_eye_rabbit\", \"bp_eye_raccoon\", \"bp_eye_reindeer\", \"bp_eye_roboeyes\", \"bp_eye_shark\", \"bp_eye_skeletongorilla\", \"bp_eye_tiger\", \"bp_eye_trex\", \"bp_eye_turkey\", \"bp_eye_turtle\", \"bp_head_cat\", \"bp_head_chameleon\", \"bp_head_chameleon_crest\", \"bp_head_chameleon_horns\", \"bp_head_crab\", \"bp_head_duck\", \"bp_head_duck_cyborg\", \"bp_head_frog\", \"bp_head_germanshep\", \"bp_head_goat\", \"bp_head_goat_ramhorns\", \"bp_head_goat_shorthorns\", \"bp_head_gorilla\", \"bp_head_kitten\", \"bp_head_mole\", \"bp_head_polarbear\", \"bp_head_pug\", \"bp_head_rabbit\", \"bp_head_rabbit_foldedear\", \"bp_head_rabbit_lopear\", \"bp_head_raccoon\", \"bp_head_reindeer\", \"bp_head_shark\", \"bp_head_shark_goblin\", \"bp_head_shark_hammer\", \"bp_head_skeletongorilla\", \"bp_head_tiger\", \"bp_head_trex\", \"bp_head_turkey\", \"bp_head_turtle\", \"bp_tail_ankytail\", \"bp_tail_bananapeeltail\", \"bp_tail_cat\", \"bp_tail_chameleon\", \"bp_tail_donkeypintail\", \"bp_tail_duck\", \"bp_tail_electricalcordtail\", \"bp_tail_germanshep\", \"bp_tail_goat\", \"bp_tail_kitten\", \"bp_tail_mole\", \"bp_tail_polarbear\", \"bp_tail_pug\", \"bp_tail_rabbit\", \"bp_tail_raccoon\", \"bp_tail_reindeer\", \"bp_tail_shark\", \"bp_tail_tiger\", \"bp_tail_trex\", \"bp_tail_turkey\", \"bp_torso_cat\", \"bp_torso_chameleon\", \"bp_torso_crab\", \"bp_torso_duck\", \"bp_torso_frog\", \"bp_torso_germanshep\", \"bp_torso_goat\", \"bp_torso_gorilla\", \"bp_torso_kitten\", \"bp_torso_mole\", \"bp_torso_polarbear\", \"bp_torso_pug\", \"bp_torso_rabbit\", \"bp_torso_raccoon\", \"bp_torso_reindeer\", \"bp_torso_shark\", \"bp_torso_skeletongorilla\", \"bp_torso_tiger\", \"bp_torso_trex\", \"bp_torso_turkey\", \"bp_torso_turtle\", \"bp_torso_turtle_shell2\", \"bp_torso_turtle_shell3\", \"bp_torso_turtle_shell4\", \"character_battle_shark\", \"character_chame_leo\", \"character_delta_hare\", \"character_goat\", \"character_goat_ram\", \"character_goat_smallhorns\", \"character_grim_gorilla\", \"character_metal_duck\", \"character_mole_a_tov\", \"character_polar_paws\", \"character_shelllong\", \"character_sigma_frog\", \"character_swag_stag\", \"character_trex_pirate\", \"character_trex_pirate_crew\", \"character_turkey_hunter\", \"outfit_anime_fem_black\", \"outfit_anime_fem_pink\", \"outfit_anime_mas_blue\", \"outfit_anime_mas_white\", \"outfit_apocalypsesurvivor\", \"outfit_apocalypsesurvivor_banana\", \"outfit_apocalypsesurvivor_bloodorange\", \"outfit_apocalypsesurvivor_blueberry\", \"outfit_aquaarmor\", \"outfit_aquaarmor_green\", \"outfit_aquaarmor_red\", \"outfit_arborday_druid\", \"outfit_arborday_shaman\", \"outfit_arborday_tree\", \"outfit_armor_king\", \"outfit_bunny\", \"outfit_bunny_blackwhite\", \"outfit_bunny_blue\", \"outfit_bunny_yellow\", \"outfit_bunnydrip_blue\", \"outfit_bunnydrip_pink\", \"outfit_bunnydrip_yellow\", \"outfit_chinese_warrior\", \"outfit_chinese_warrior_gold\", \"outfit_cincodemayo\", \"outfit_cincodemayo_crimson\", \"outfit_cincodemayo_forest\", \"outfit_cincodemayo_saffron\", \"outfit_cincodemayoblanket\", \"outfit_cincodemayoblanket_crimson\", \"outfit_cincodemayoblanket_forest\", \"outfit_cincodemayoblanket_saffron\", \"outfit_clown\", \"outfit_cube\", \"outfit_cube_frog\", \"outfit_cube_gorilla\", \"outfit_cube_shepherd\", \"outfit_cupid\", \"outfit_cybersuit\", \"outfit_cyborg_punk\", \"outfit_deltahare_blue\", \"outfit_deltahare_desert\", \"outfit_deltahare_green\", \"outfit_deltahare_snow\", \"outfit_demolitionjumpsuit\", \"outfit_demolitionjumpsuit_green\", \"outfit_demolitionjumpsuit_red\", \"outfit_demonboyband\", \"outfit_demonboyband_glowblue\", \"outfit_demonboyband_glowgreen\", \"outfit_demonboyband_glowpurple\", \"outfit_discobutt\", \"outfit_discobutt_phoenix\", \"outfit_diversuit\", \"outfit_diversuit_green\", \"outfit_diversuit_rusty\", \"outfit_diversuit_yellow\", \"outfit_dwarf\", \"outfit_dwarf_blue\", \"outfit_dwarf_tin\", \"outfit_dwarf_wood\", \"outfit_eggsuit_chocolate\", \"outfit_eggsuit_colorful_green\", \"outfit_eggsuit_colorful_purple\", \"outfit_eggsuit_golden\", \"outfit_elf_blue\", \"outfit_elf_green\", \"outfit_elf_pink\", \"outfit_employee_suit_blue\", \"outfit_employee_suit_gold\", \"outfit_employee_suit_purple\", \"outfit_fallponcho\", \"outfit_fur_future\", \"outfit_gladiator\", \"outfit_gladiator_cat\", \"outfit_gladiator_frog\", \"outfit_gladiator_gorilla\", \"outfit_gladiator_pug\", \"outfit_gladiator_skeletongorilla\", \"outfit_glowgrilla\", \"outfit_glowgrilla_purple\", \"outfit_grapplesoldier\", \"outfit_grapplesoldier_black\", \"outfit_grapplesoldier_blue\", \"outfit_grapplesoldier_red\", \"outfit_grimreaper_premium\", \"outfit_hawaiian\", \"outfit_hawaiian_blue\", \"outfit_hawaiian_yellow\", \"outfit_hawaiiangirl\", \"outfit_hawaiiangirl_blonde\", \"outfit_hawaiiangirl_brown\", \"outfit_hazmat_blue\", \"outfit_hazmat_green\", \"outfit_hazmat_orange\", \"outfit_hazmat_pink\", \"outfit_hazmat_purple\", \"outfit_hazmat_red\", \"outfit_hazmat_yellow\", \"outfit_hippie\", \"outfit_irish_kilt\", \"outfit_irish_kilt_blue\", \"outfit_irish_kilt_red\", \"outfit_irish_kilt_yellow\", \"outfit_it_employee_blue\", \"outfit_it_employee_brown\", \"outfit_kingofhearts_blue\", \"outfit_kingofhearts_red\", \"outfit_kpop\", \"outfit_kpop_black\", \"outfit_kpop_purple\", \"outfit_kpop_white\", \"outfit_kungfu_black\", \"outfit_kungfu_blue\", \"outfit_kungfu_orange\", \"outfit_leprachaun\", \"outfit_liona\", \"outfit_necromancer\", \"outfit_neon_miner\", \"outfit_nfljersey_bear\", \"outfit_nfljersey_cat\", \"outfit_nfljersey_frog\", \"outfit_nfljersey_gorilla\", \"outfit_nfljersey_pug\", \"outfit_nfljersey_skeleton\", \"outfit_og_fit1\", \"outfit_og_fit2\", \"outfit_orcmage\", \"outfit_orcmage_summon\", \"outfit_parkranger\", \"outfit_parkranger_car\", \"outfit_parkranger_rugged\", \"outfit_parkranger_salmon\", \"outfit_pilgrim\", \"outfit_pirate_blue\", \"outfit_pirate_crew\", \"outfit_pirate_red\", \"outfit_policeman_blue\", \"outfit_policeman_brown\", \"outfit_potofgold\", \"outfit_potofgold_flames\", \"outfit_potofgold_gold\", \"outfit_potofgold_green\", \"outfit_punkrock\", \"outfit_queenofhearts\", \"outfit_racerjacket_black\", \"outfit_racerjacket_red\", \"outfit_racerjacket_white\", \"outfit_racerjacket_yellow\", \"outfit_redknight\", \"outfit_redknight_summon\", \"outfit_rocker\", \"outfit_samurai\", \"outfit_samurai_purple\", \"outfit_samurai_red\", \"outfit_samurai_white\", \"outfit_samuraiwarrior\", \"outfit_samuraiwarrior_dual\", \"outfit_samuraiwarrior_orange\", \"outfit_samuraiwarrior_pink\", \"outfit_santa\", \"outfit_santa_blue\", \"outfit_santa_purple\", \"outfit_securityguard_green\", \"outfit_securityguard_white\", \"outfit_shiny_swim_set\", \"outfit_spacesuit\", \"outfit_spacesuit_blue\", \"outfit_spacesuit_orange\", \"outfit_spacesuit_rainbow\", \"outfit_squidgame_pink\", \"outfit_squidgame_purple\", \"outfit_squidgame_red\", \"outfit_squidgamedoll\", \"outfit_squidgamedoll_brunette\", \"outfit_squidgamefrontman\", \"outfit_squidgamefrontman_white\", \"outfit_steampunkrobot\", \"outfit_steampunkrobot_boat\", \"outfit_steampunkrobot_fireengine\", \"outfit_steampunkrobot_tinman\", \"outfit_supermansuit\", \"outfit_supermansuit_atom\", \"outfit_supermansuit_leaf\", \"outfit_supermansuit_recycle\", \"outfit_tacticalarmor\", \"outfit_tacticalarmor_emo\", \"outfit_tacticalarmor_pale\", \"outfit_tacticalarmor_robo\", \"outfit_tacticalarmor_robo_copper\", \"outfit_tacticalmining\", \"outfit_tacticalmining_almandine\", \"outfit_tacticalmining_diamond\", \"outfit_tacticalmining_emerald\", \"outfit_teamblue\", \"outfit_teamblue_fire\", \"outfit_teamred\", \"outfit_thanksgiving_turkey\", \"outfit_trek\", \"outfit_valentines_suit\", \"outfit_valentines_youme\", \"outfit_viking\", \"outfit_viking_firestorm\", \"outfit_viking_flaxen\", \"outfit_viking_twilight\", \"outfit_warrior_ascendant\", \"outfit_warrior_ascendant_dark\", \"outfit_warrior_engineer\", \"outfit_warrior_scholar\", \"outfit_worndownemployee_blue\", \"outfit_worndownemployee_green\"]}",
        "version": "277a87beb4905dbe2333d5cd55a7e5be",
        "permission_read": 1,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-07T01:22:02Z"
        },
        {
        "collection": "user_inventory",
        "key": "research",
        "user_id": user_id,
        "value": "{\"nodes\": [\"node_dynamite\", \"node_teleport_grenade\", \"node_glowsticks\", \"node_skill_backpack_cap_1\", \"node_skill_health_1\", \"node_crowbar\", \"node_flaregun\", \"node_ogre_hands\", \"node_revolver\", \"node_skill_gundamage_1\", \"node_skill_explosive_1\", \"node_rpg\", \"node_skill_selling_1\", \"node_revolver_ammo\", \"node_rpg_ammo\", \"node_flashbang\", \"node_impact_grenade\", \"node_cluster_grenade\", \"node_jetpack\", \"node_shotgun\", \"node_tripwire_explosive\", \"node_crossbow\", \"node_tablet\", \"node_plunger\", \"node_umbrella\", \"node_backpack\", \"node_flashlight_mega\", \"node_lance\", \"node_balloon\", \"node_saddle\", \"node_skill_right_hip_attachment\", \"node_skill_left_hip_attachment\", \"node_sticky_dynamite\", \"node_rpg_cny\", \"node_zipline_gun\", \"node_zipline_rope\", \"node_company_ration\", \"node_balloon_heart\", \"node_crossbow_heart\", \"node_arrow\", \"node_arrow_heart\", \"node_hoverpad\", \"node_quiver\", \"node_backpack_large\", \"node_shield\", \"node_shield_police\", \"node_hookshot\", \"node_baseball_bat\", \"node_police_baton\", \"node_heart_gun\", \"node_pogostick\", \"node_boxfan\", \"node_mega_broccoli\", \"node_mini_broccoli\", \"node_dynamite_cube\", \"node_skill_backpack_cap_2\", \"node_whoopie\", \"node_disposable_camera\", \"node_sticker_dispenser\", \"node_impulse_grenade\", \"node_stash_grenade\", \"node_cardboardbox\", \"node_rpg_easter\", \"node_rpg_ammo_egg\", \"node_pinata_bat\", \"node_hawaiian_drum\", \"node_ukulele\", \"node_anti_gravity_grenade\", \"node_antigrav_grenade\", \"node_football\", \"node_skill_backpack_cap_3\", \"node_item_nut_shredder\", \"node_hookshot_sword\", \"node_rpg_spear\", \"node_rpg_ammo_spear\", \"node_skill_health_2\", \"node_skill_selling_2\", \"node_skill_selling_3\", \"node_frying_pan\", \"node_skill_melee_1\", \"node_skill_melee_2\", \"node_skill_melee_3\", \"node_viking_hammer\", \"node_viking_hammer_twilight\", \"node_mega_broccoli_bomb\", \"node_micro_broccoli_bomb\", \"node_teleport_gun\", \"node_arrow_bomb\", \"node_robo_monke\", \"node_friend_launcher\", \"node_grenade_launcher\"]}",
        "version": "58c1d1c4ade0e8e205939be8a07ce49b",
        "permission_read": 1,
        "create_time": "2024-11-25T17:55:33Z",
        "update_time": "2025-07-14T21:34:27Z"
        },
        {
        "collection": "user_inventory",
        "key": "stash",
        "user_id": user_id,
        "value": "{\"items\": [{\"itemID\": \"item_backpack_large_base\"}]}",
        "version": "d1315c03b540bef68ce4742d46e77cc0",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-10-30T01:21:34Z",
        "update_time": "2025-08-08T20:34:56Z"
        },
        {
        "collection": "user_inventory",
        "key": "gameplay_loadout",
        "user_id": user_id,
        "value": "{\"version\": 1}",
        "version": "3846efa925d304495efbfed41eaafe74",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-10-30T01:21:34Z",
        "update_time": "2025-08-08T20:34:56Z"
        },
        {
        "collection": "user_preferences",
        "key": "gameplay_items",
        "user_id": user_id,
        "value": "{\"recents\": []}",
        "version": "fe9acf47fd31aeb3ea1aa209e6485ce3",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-12-10T20:44:21Z",
        "update_time": "2025-08-09T07:39:43Z"
        },
        {
        "collection": "user_preferences",
        "key": "common",
        "user_id": user_id,
        "value": "{\"appearOffline\": false}",
        "version": "d56295314bb7a4c43e13da9c446a77a8",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2025-06-11T03:57:33Z",
        "update_time": "2025-08-07T18:09:40Z"
        },
        {
        "collection": "user_inventory",
        "key": "upgrades",
        "user_id": user_id,
        "value": "{\"upgrades\": [\"col_1\", \"col_2\", \"row_1\", \"row_2\", \"mtl_1\", \"mtl_2\", \"bp_1\", \"col_3\", \"row_3\", \"loadout_slot_1\", \"col_4\", \"col_5\", \"col_6\", \"col_7\", \"col_8\", \"row_4\", \"row_5\"]}",
        "version": "8a1cb259727a3a8091e45f7b1897814c",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2026-05-21T17:38:32Z",
        "update_time": "2026-05-24T11:57:27Z"
        },
        {
        "collection": "user_inventory",
        "key": "fishing",
        "user_id": user_id,
        "value": "{\"items\": []}",
        "version": "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-08T09:22:23Z"
        },
        {
        "collection": "user_quest_system",
        "key": "progress",
        "user_id": user_id,
        "value": "{\"quests\": {}}",
        "version": "c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-08T09:22:23Z"
        },
        {
        "collection": "user_preferences",
        "key": "skills",
        "user_id": user_id,
        "value": "{\"skills\": []}",
        "version": "d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-08T09:22:23Z"
        },
        {
        "collection": "user_discord_accounts",
        "key": "-",
        "user_id": user_id,
        "value": "{}",
        "version": "e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
        "permission_read": 1,
        "permission_write": 1,
        "create_time": "2024-09-20T21:26:18Z",
        "update_time": "2025-08-08T09:22:23Z"
        }
    ]

@app.route('/3/v2/rpc/purchase.list', methods=['GET'])
@app.route('/v2/rpc/purchase.list', methods=['GET'])
def purchaselist():
    return {
        "payload": "{\"purchases\":[{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"KPOP_GOLD\",\"transaction_id\":\"716802304855579\",\"store\":3,\"purchase_time\":{\"seconds\":1754458259},\"create_time\":{\"seconds\":1754458305,\"nanos\":154543000},\"update_time\":{\"seconds\":1754458305,\"nanos\":154543000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true, \\\"grant_time\\\": 1754458259}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"CHAMELEON_BUNDLE\",\"transaction_id\":\"642819158920561\",\"store\":3,\"purchase_time\":{\"seconds\":1748315321},\"create_time\":{\"seconds\":1748315350,\"nanos\":615822000},\"update_time\":{\"seconds\":1748315350,\"nanos\":615822000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true, \\\"grant_time\\\": 1748315321}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"SHELLLONG_BUNDLE\",\"transaction_id\":\"583001341569010\",\"store\":3,\"purchase_time\":{\"seconds\":1742932724},\"create_time\":{\"seconds\":1742932880,\"nanos\":773282000},\"update_time\":{\"seconds\":1742932880,\"nanos\":773282000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"G.O.A.T_BUNDLE\",\"transaction_id\":\"556928314176313\",\"store\":3,\"purchase_time\":{\"seconds\":1740523561},\"create_time\":{\"seconds\":1740523626,\"nanos\":858485000},\"update_time\":{\"seconds\":1741616276,\"nanos\":636221000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"CURRENCY_SMALL\",\"transaction_id\":\"520077871194691\",\"store\":3,\"purchase_time\":{\"seconds\":1737165591},\"create_time\":{\"seconds\":1737165616,\"nanos\":219758000},\"update_time\":{\"seconds\":1737165616,\"nanos\":219758000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"POLAR_PAWS_BUNDLE\",\"transaction_id\":\"498846086651203\",\"store\":3,\"purchase_time\":{\"seconds\":1735139449},\"create_time\":{\"seconds\":1735155215,\"nanos\":988535000},\"update_time\":{\"seconds\":1735155215,\"nanos\":988535000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true}\",\"environment\":2},{\"user_id\":\"3560fe2e-015c-4d2b-b2a6-6eb9f8d6a236\",\"product_id\":\"FROG_BUNDLE\",\"transaction_id\":\"411070858762060\",\"store\":3,\"purchase_time\":{\"seconds\":1726952768},\"create_time\":{\"seconds\":1726953016,\"nanos\":937191000},\"update_time\":{\"seconds\":1726953016,\"nanos\":937191000},\"refund_time\":{},\"provider_response\":\"{\\\"success\\\": true}\",\"environment\":2}]}"
    }


# ────────────────────────────────────────────────────────────────────────────
# DASHBOARD API ROUTES
# ────────────────────────────────────────────────────────────────────────────

@app.route('/api/logs')
def api_logs():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    lines = list(_log_buffer)
    return jsonify({'lines': lines, 'total': len(lines)})

@app.route('/api/logs/download')
def api_logs_download():
    from flask import Response
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    return Response('\n'.join(_log_buffer), mimetype='text/plain',
                    headers={'Content-Disposition': 'attachment; filename=iron_logs.txt'})

@app.route('/api/versions')
def api_versions():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    _prune_presence()
    with _room_lock:
        user_room_snap = dict(_user_room)
    with _presence_lock:
        snap = [dict(e) for e in _presence.values()]
    counts = {}
    for ent in snap:
        uid = ent.get('uid', '')
        name = ent.get('name') or (uid[:8] if uid else '?')
        counts.setdefault(ent.get('version') or 'unknown', []).append(
            {'name': name, 'room': user_room_snap.get(uid, '')})
    total = sum(len(v) for v in counts.values())
    result = []
    for ver, users in sorted(counts.items(), key=lambda x: -len(x[1])):
        pct = round(len(users) * 100 / total) if total else 0
        result.append({'version': ver, 'count': len(users), 'pct': pct, 'users': users})
    return jsonify({'versions': result, 'online': total})

@app.route('/api/ban-user', methods=['POST'])
def api_ban_user():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get('username') or '').strip().lower()
    if not username: return jsonify({'error': 'username required'}), 400
    users = _load_banned_users()
    users.add(normalize_username(username))
    _save_banned_users(users)
    _mark_offline('', username, 'banned')
    print(f"[BAN] {username} banned via dashboard")
    return jsonify({'ok': True})

@app.route('/api/device-ban', methods=['GET', 'POST', 'DELETE'])
def api_device_ban():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    if request.method == 'GET':
        return jsonify({'devices': list(_banned_devices)})
    body = request.get_json(force=True, silent=True) or {}
    dev = (body.get('device_id') or '').strip()
    if not dev: return jsonify({'error': 'device_id required'}), 400
    if request.method == 'POST':
        _banned_devices.add(dev)
        print(f"[DEVICE_BAN] banned {dev[:16]}")
        return jsonify({'ok': True})
    _banned_devices.discard(dev)
    return jsonify({'ok': True})

@app.route('/api/temp-ban', methods=['GET', 'POST', 'DELETE'])
def api_temp_ban():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    now = time.time()
    if request.method == 'GET':
        with _temp_ban_lock:
            active = [{'username': k, 'remaining': int(v['until'] - now), 'reason': v.get('reason', '')}
                      for k, v in _temp_bans.items() if v['until'] > now]
        return jsonify({'bans': active})
    body = request.get_json(force=True, silent=True) or {}
    uname = (body.get('username') or '').strip().lower()
    if not uname: return jsonify({'error': 'username required'}), 400
    if request.method == 'DELETE':
        with _temp_ban_lock:
            _temp_bans.pop(uname, None)
        return jsonify({'ok': True})
    dur = float(body.get('duration', 0) or 0)
    unit = body.get('unit', 'm')
    seconds = dur * {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}.get(unit, 60)
    if seconds <= 0: return jsonify({'error': 'duration required'}), 400
    reason = (body.get('reason') or '').strip()[:200]
    with _temp_ban_lock:
        _temp_bans[uname] = {'until': now + seconds, 'reason': reason}
    _mark_offline('', uname, 'temp banned')
    print(f"[TEMP_BAN] {uname} banned for {seconds}s reason={reason!r}")
    return jsonify({'ok': True})

@app.route('/api/broadcasts', methods=['GET'])
def api_broadcasts_list():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    return jsonify({'broadcasts': list(_broadcasts)})

@app.route('/api/broadcast', methods=['POST'])
def api_broadcast():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    body = request.get_json(force=True, silent=True) or {}
    subject = body.get('subject', '').strip()
    content = body.get('content', '').strip()
    if not subject: return jsonify({'error': 'subject required'}), 400
    bc = {
        'id': str(uuid.uuid4()),
        'subject': subject,
        'content': content or '{}',
        'code': 1,
        'sender_id': '00000000-0000-0000-0000-000000000000',
        'persistent': True,
        'create_time': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    }
    _broadcasts.appendleft(bc)
    print(f"[BROADCAST] queued: {subject!r}")
    return jsonify({'ok': True, 'online_pushed': 0})

@app.route('/api/broadcast/<bc_id>', methods=['DELETE'])
def api_broadcast_delete(bc_id):
    global _broadcasts
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    before = len(_broadcasts)
    _broadcasts = collections.deque(
        (bc for bc in _broadcasts if bc['id'] != bc_id), maxlen=100)
    return jsonify({'ok': True, 'removed': before - len(_broadcasts)})

def _photon_track(event_type, room_code, user_id, nickname=''):
    """Shared room-tracking logic for Photon webhook paths."""
    if not room_code:
        return
    display = nickname
    if not display:
        with _presence_lock:
            display = next((e.get('name') for e in _presence.values()
                            if e.get('uid') == user_id), '')
    display = display or (user_id[:8] if user_id else '?')
    print(f"[PHOTON] {event_type} room={room_code} uid={user_id[:8] if user_id else '?'} nick={display}")
    with _room_lock:
        if event_type in ('create', 'join'):
            _room_members.setdefault(room_code, {})[user_id] = display
            if user_id:
                _user_room[user_id] = room_code
        elif event_type == 'leave':
            if room_code in _room_members:
                _room_members[room_code].pop(user_id, None)
                if not _room_members[room_code]:
                    del _room_members[room_code]
            _user_room.pop(user_id, None)
        elif event_type == 'close':
            for uid in list(_room_members.get(room_code, {}).keys()):
                _user_room.pop(uid, None)
            _room_members.pop(room_code, None)

@app.route('/photon/webhook', methods=['POST'])
def photon_room_webhook():
    data = request.get_json(silent=True) or {}
    event_type = {'GameCreated': 'create', 'GameJoined': 'join',
                  'GameLeft': 'leave', 'GameClosed': 'close'}.get(data.get('Type', ''), '')
    if event_type:
        _photon_track(event_type, data.get('GameId', ''), data.get('UserId', ''),
                      data.get('Nickname', '') or data.get('username', ''))
    return jsonify({'ResultCode': 1}), 200

@app.route('/game/create', methods=['POST'])
def photon_game_create():
    data = request.get_json(silent=True) or {}
    room_code = data.get('RoomName') or data.get('GameId', '').split(':')[-1]
    _photon_track('create', room_code, data.get('UserId', ''))
    return jsonify({'ResultCode': 1}), 200

@app.route('/game/join', methods=['POST'])
def photon_game_join():
    data = request.get_json(silent=True) or {}
    room_code = data.get('RoomName') or data.get('GameId', '').split(':')[-1]
    _photon_track('join', room_code, data.get('UserId', ''))
    return jsonify({'ResultCode': 1}), 200

@app.route('/game/leave', methods=['POST'])
def photon_game_leave():
    data = request.get_json(silent=True) or {}
    room_code = data.get('RoomName') or data.get('GameId', '').split(':')[-1]
    _photon_track('leave', room_code, data.get('UserId', ''))
    return jsonify({'ResultCode': 1}), 200

@app.route('/game/close', methods=['POST'])
def photon_game_close():
    data = request.get_json(silent=True) or {}
    room_code = data.get('RoomName') or data.get('GameId', '').split(':')[-1]
    _photon_track('close', room_code, '')
    return jsonify({'ResultCode': 1}), 200

@app.route('/game/event', methods=['POST'])
def photon_game_event():
    return jsonify({'ResultCode': 1}), 200

@app.route('/api/rooms')
def api_rooms():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    with _room_lock:
        rooms = [{'code': code, 'players': list(members.values()), 'count': len(members)}
                 for code, members in _room_members.items()]
    return jsonify({'rooms': rooms, 'total': sum(r['count'] for r in rooms)})

@app.route('/api/color', methods=['GET', 'POST', 'DELETE'])
def api_color_dash():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    if request.method == 'GET':
        return jsonify({'colors': _load_colors()})
    body = request.get_json(force=True, silent=True) or {}
    username = (body.get('username') or '').strip().lower()
    if not username: return jsonify({'error': 'username required'}), 400
    colors = _load_colors()
    if request.method == 'DELETE':
        colors.pop(username, None)
        _save_colors(colors)
        return jsonify({'ok': True})
    color = (body.get('color') or '').strip()
    display = (body.get('display') or '').strip()
    if not color: return jsonify({'error': 'color required'}), 400
    colors[username] = {'color': color, 'display': display or username}
    _save_colors(colors)
    return jsonify({'ok': True})

@app.route('/dashboard/version-history')
def dashboard_version_history():
    if not _dash_auth(): return jsonify({'error': 'auth'}), 403
    with _version_history_lock:
        evs = list(reversed(_version_history[-3000:]))
    return jsonify({'events': evs, 'count': len(evs)})

@app.route('/dashboard')
def dashboard():
    if not _dash_auth():
        return '<style>body{font-family:monospace;background:#111;color:#f66;padding:2rem}</style><p>Wrong key. Add ?key=YOUR_KEY</p>', 403
    html = r"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Sem Company Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:monospace;background:#0d0d0d;color:#ccc;padding:1rem;font-size:13px}
html,body{height:100%;overflow:hidden}
.grid{display:grid;grid-template-columns:320px minmax(0,1fr) 300px;grid-template-rows:minmax(0,1fr) 220px;gap:1rem;height:calc(100vh - 2rem);min-height:0}
.panel.full-right{grid-column:3/4;grid-row:1/3}
.panel{min-height:0;min-width:0;background:#161616;border:1px solid #2a2a2a;border-radius:6px;padding:.75rem;overflow:hidden;display:flex;flex-direction:column}
.panel h2{font-size:14px;color:#aaa;text-transform:uppercase;letter-spacing:.05em;margin-bottom:.5rem}
#ver-list{flex:1;overflow-y:auto}
.ver-row{display:flex;align-items:center;gap:.5rem;padding:3px 0;border-bottom:1px solid #1e1e1e}
.ver-name{flex:1;color:#fff}
.ver-bar-wrap{width:80px;background:#222;height:8px;border-radius:4px;overflow:hidden}
.ver-bar{height:8px;background:#3a7bd5;border-radius:4px}
.ver-count{width:60px;text-align:right;color:#888}
#log-wrap{flex:1;overflow-y:auto;background:#111;border-radius:4px;padding:.5rem}
#log-box{white-space:pre-wrap;word-break:break-all;font-size:12px;line-height:1.5}
.controls{display:flex;gap:.5rem;margin-bottom:.5rem}
input,textarea,select{background:#222;border:1px solid #333;color:#ccc;padding:4px 8px;border-radius:4px;font-family:monospace;font-size:12px}
input{flex:1}
textarea{width:100%;resize:vertical;min-height:52px}
button{background:#1e3a5f;border:1px solid #2a5a8f;color:#5b9bd5;padding:4px 12px;border-radius:4px;cursor:pointer;font-family:monospace;font-size:12px}
button:hover{background:#2a5a8f}
button.danger{background:#3a1a1a;border-color:#8f2a2a;color:#e05252}
.badge{display:inline-block;background:#1e3a5f;color:#5b9bd5;padding:2px 8px;border-radius:10px;font-size:11px;margin-left:.5rem}
.ts{color:#555}.warn{color:#e6a817}.err{color:#e05252}.ok{color:#4caf50}
#status{font-size:11px;color:#555;margin-top:.25rem}
#bc-result{font-size:11px;color:#4caf50;margin-top:.4rem;min-height:16px}
</style></head><body>
<div class="grid">
  <div class="panel" style="grid-row:1/2">
    <h2>Versions <span id="online-badge" class="badge">0 online</span></h2>
    <div id="ver-list"></div>
  </div>
  <div class="panel" style="grid-row:1/2">
    <h2>Logs</h2>
    <div class="controls">
      <input id="filter" placeholder="filter..." oninput="applyFilter()">
      <button onclick="copyLogs()" id="copy-btn">Copy</button>
      <button onclick="downloadLogs()">Download</button>
    </div>
    <div id="log-wrap"><div id="log-box"></div></div>
    <div id="status"></div>
  </div>
  <div class="panel" style="grid-row:2/3">
    <h2>Broadcast Notification</h2>
    <div style="display:flex;flex-direction:column;gap:.4rem;flex:1;overflow:hidden">
      <input id="bc-subject" placeholder="Subject">
      <textarea id="bc-content" placeholder="Content (optional)" style="height:42px"></textarea>
      <button onclick="sendBroadcast()">Queue Broadcast</button>
      <div id="bc-result"></div>
      <div style="font-size:10px;color:#555;text-transform:uppercase;margin-top:4px">Active Broadcasts</div>
      <div id="bc-list" style="flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:4px"></div>
    </div>
  </div>
  <div class="panel" style="grid-row:2/3">
    <h2>Ban User</h2>
    <div style="display:flex;gap:.5rem;margin-bottom:.5rem">
      <input id="ban-uname" placeholder="username">
      <button onclick="banUser()">Ban</button>
    </div>
    <div id="ban-result" style="font-size:11px;color:#888;min-height:14px"></div>
  </div>
  <div class="panel full-right" style="overflow:hidden">
    <h2>Live Rooms <span id="rooms-badge" class="badge">0 rooms</span></h2>
    <div id="rooms-list" style="overflow-y:auto;display:flex;flex-direction:column;gap:3px;font-size:12px;max-height:110px;margin-bottom:.6rem"></div>
    <h2>Colors</h2>
    <div style="display:flex;flex-direction:column;gap:.4rem;margin-bottom:.6rem">
      <input id="col-uname" placeholder="username">
      <input id="col-color" placeholder="#RRGGBB">
      <input id="col-display" placeholder="display name (optional)">
      <div style="display:flex;gap:.5rem">
        <button onclick="colorApply()" style="flex:1">Apply</button>
        <button class="danger" onclick="colorClear()" style="flex:1">Clear</button>
      </div>
      <div id="col-result" style="font-size:11px;color:#888;min-height:14px"></div>
    </div>
    <div style="font-size:10px;color:#555;text-transform:uppercase;margin-bottom:4px">Active colors</div>
    <div id="col-list" style="flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:3px;font-size:12px;max-height:120px;margin-bottom:.6rem"></div>

    <h2 style="margin-top:.4rem">Device Bans</h2>
    <div style="display:flex;flex-direction:column;gap:.4rem;margin-bottom:.6rem">
      <input id="dev-id" placeholder="device ID (32-char hash)">
      <div style="display:flex;gap:.5rem">
        <button onclick="deviceBan()" style="flex:1">Ban</button>
        <button class="danger" onclick="deviceUnban()" style="flex:1">Unban</button>
      </div>
      <div id="dev-result" style="font-size:11px;color:#888;min-height:14px"></div>
    </div>
    <div style="font-size:10px;color:#555;text-transform:uppercase;margin-bottom:4px">Banned devices</div>
    <div id="dev-list" style="overflow-y:auto;display:flex;flex-direction:column;gap:3px;font-size:11px;max-height:80px;margin-bottom:.6rem"></div>

    <h2 style="margin-top:.4rem">Temp Bans</h2>
    <div style="display:flex;flex-direction:column;gap:.4rem;margin-bottom:.6rem">
      <input id="tb-uname" placeholder="username">
      <div style="display:flex;gap:.3rem">
        <input id="tb-duration" type="number" min="1" placeholder="duration" style="flex:1">
        <select id="tb-unit" style="flex:0 0 60px">
          <option value="s">sec</option>
          <option value="m" selected>min</option>
          <option value="h">hour</option>
          <option value="d">day</option>
        </select>
      </div>
      <input id="tb-reason" placeholder="reason (optional)" maxlength="200">
      <div style="display:flex;gap:.5rem">
        <button onclick="tempBanApply()" style="flex:1">Ban</button>
        <button class="danger" onclick="tempBanClear()" style="flex:1">Lift</button>
      </div>
      <div id="tb-result" style="font-size:11px;color:#888;min-height:14px"></div>
    </div>
    <div style="font-size:10px;color:#555;text-transform:uppercase;margin-bottom:4px">Active temp bans</div>
    <div id="tb-list" style="flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:3px;font-size:12px"></div>
  </div>
</div>
<button id="analytics-btn" onclick="toggleAnalytics()" style="position:fixed;top:12px;right:16px;z-index:60">&#128202; Analytics</button>
<div id="analytics-overlay" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.82);z-index:100;padding:2.5rem" onclick="if(event.target===this)toggleAnalytics()">
  <div style="background:#161616;border:1px solid #2a2a2a;border-radius:8px;max-width:960px;margin:0 auto;height:100%;display:flex;flex-direction:column;padding:1rem">
    <h2 style="font-size:14px;color:#aaa;text-transform:uppercase;letter-spacing:.05em;margin-bottom:.5rem;display:flex;align-items:center;gap:.5rem">
      <span style="flex:1">Version analytics</span>
      <input id="an-filter" placeholder="filter user / version..." oninput="renderAnalytics()" style="flex:0 0 200px;font-size:12px">
      <button onclick="fetchAnalytics()">Refresh</button>
      <button class="danger" onclick="toggleAnalytics()">Close</button>
    </h2>
    <div id="an-status" style="font-size:11px;color:#666;margin-bottom:.4rem">click Refresh to load</div>
    <div style="flex:1;overflow-y:auto;background:#111;border-radius:4px;min-height:0">
      <table style="width:100%;border-collapse:collapse;font-size:12px">
        <thead><tr style="position:sticky;top:0;background:#1a1a1a;color:#888;text-align:left;cursor:pointer;user-select:none">
          <th style="padding:5px 8px" onclick="anSortClick('t')">Time <span id="an-arr-t"></span></th>
          <th style="padding:5px 8px" onclick="anSortClick('user')">User <span id="an-arr-user"></span></th>
          <th style="padding:5px 8px" onclick="anSortClick('version')">Version <span id="an-arr-version"></span></th>
          <th style="padding:5px 8px" onclick="anSortClick('device')">Device <span id="an-arr-device"></span></th></tr></thead>
        <tbody id="an-body"></tbody>
      </table>
    </div>
  </div>
</div>
<script>
const KEY = new URLSearchParams(location.search).get('key') || '';
let allLines = [], pinned = true;

function colorLine(l) {
  const e = document.createElement('span');
  const m = l.match(/"[A-Z]+ \S+ HTTP\/[\d.]+" (\d{3})/);
  const status = m ? parseInt(m[1]) : 0;
  if (status >= 500 || /error|traceback|exception/i.test(l)) e.className = 'err';
  else if (status >= 400) e.className = 'err';
  else if (status >= 300) e.className = 'warn';
  else if (status >= 200) e.className = 'ok';
  else if (/connected|authed|accepted/i.test(l)) e.className = 'ok';
  else e.className = 'ts';
  e.textContent = l + '\n';
  return e;
}

function applyFilter() {
  const q = document.getElementById('filter').value.toLowerCase();
  const box = document.getElementById('log-box');
  box.innerHTML = '';
  const shown = q ? allLines.filter(l => l.toLowerCase().includes(q)) : allLines;
  shown.forEach(l => box.appendChild(colorLine(l)));
  if (pinned) box.parentElement.scrollTop = box.parentElement.scrollHeight;
}

let selPaused = false;
document.addEventListener('selectionchange', () => {
  selPaused = !!document.getSelection().toString();
  if (!selPaused) document.getElementById('status').textContent = lastStatus;
  else document.getElementById('status').textContent = 'paused — text selected';
});

let lastStatus = '';
async function fetchLogs() {
  if (selPaused) return;
  try {
    const r = await fetch(`/api/logs?key=${KEY}`);
    const d = await r.json();
    allLines = d.lines || [];
    applyFilter();
    lastStatus = `${d.total} lines · refresh 3s`;
    document.getElementById('status').textContent = lastStatus;
  } catch(e) {}
}

function copyLogs() {
  navigator.clipboard.writeText(allLines.join('\n')).then(() => {
    const btn = document.getElementById('copy-btn');
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 1500);
  });
}

function downloadLogs() { window.open(`/api/logs/download?key=${KEY}`, '_blank'); }

async function fetchVersions() {
  try {
    const r = await fetch(`/api/versions?key=${KEY}`);
    const d = await r.json();
    document.getElementById('online-badge').textContent = `${d.online} online`;
    const list = document.getElementById('ver-list');
    list.innerHTML = '';
    (d.versions || []).forEach(v => {
      const row = document.createElement('div');
      row.className = 'ver-row';
      const head = document.createElement('div');
      head.style.flex = '1';
      head.innerHTML = `<span class="ver-name">${v.version}</span>`;
      const tags = document.createElement('div');
      tags.style.cssText = 'color:#666;font-size:11px;margin-top:1px;display:flex;flex-wrap:wrap;gap:4px';
      (v.users || []).forEach(u => {
        const name = (typeof u === 'string') ? u : u.name;
        const room = (typeof u === 'object' && u.room) ? u.room : '';
        const chip = document.createElement('span');
        chip.style.cssText = 'background:#1a1a1a;border-radius:3px;padding:1px 6px;color:#ccc;display:inline-flex;align-items:center;gap:4px';
        const nameEl = document.createElement('span');
        nameEl.textContent = name;
        chip.appendChild(nameEl);
        if (room) {
          const roomEl = document.createElement('span');
          roomEl.style.cssText = 'background:#1e3a5f;color:#5b9bd5;border-radius:3px;padding:0 4px;font-size:10px;line-height:16px';
          roomEl.textContent = room;
          chip.appendChild(roomEl);
        }
        tags.appendChild(chip);
      });
      head.appendChild(tags);
      row.appendChild(head);
      const barWrap = document.createElement('div');
      barWrap.className = 'ver-bar-wrap';
      barWrap.innerHTML = `<div class="ver-bar" style="width:${v.pct}%"></div>`;
      row.appendChild(barWrap);
      const count = document.createElement('span');
      count.className = 'ver-count';
      count.textContent = `${v.count} (${v.pct}%)`;
      row.appendChild(count);
      list.appendChild(row);
    });
  } catch(e) {}
}

async function banUser() {
  const uname = document.getElementById('ban-uname').value.trim();
  const el = document.getElementById('ban-result');
  if (!uname || !confirm(`Ban "${uname}"?`)) return;
  try {
    const r = await fetch(`/api/ban-user?key=${KEY}`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname})});
    const d = await r.json();
    el.style.color = d.ok ? '#4caf50' : '#e05252';
    el.textContent = d.ok ? `Banned ${uname}` : d.error || 'failed';
    if (d.ok) document.getElementById('ban-uname').value = '';
  } catch(e) { el.style.color='#e05252'; el.textContent='Request failed'; }
}

async function fetchBroadcasts() {
  try {
    const r = await fetch(`/api/broadcasts?key=${KEY}`);
    const d = await r.json();
    const list = document.getElementById('bc-list');
    const bcs = d.broadcasts || [];
    if (!bcs.length) { list.innerHTML = '<div style="color:#444;font-size:11px">No active broadcasts</div>'; return; }
    list.innerHTML = '';
    bcs.forEach(bc => {
      const row = document.createElement('div');
      row.style.cssText = 'display:flex;align-items:center;gap:6px;background:#1a1a1a;border-radius:3px;padding:5px 8px;font-size:12px';
      const txt = document.createElement('span');
      txt.style.cssText = 'flex:1;color:#ccc;overflow:hidden;text-overflow:ellipsis;white-space:nowrap';
      txt.textContent = bc.subject;
      const del = document.createElement('button');
      del.textContent = '✕';
      del.style.cssText = 'background:#3a1a1a;border:none;color:#e05252;cursor:pointer;border-radius:3px;padding:2px 6px;font-size:11px;flex-shrink:0';
      del.onclick = async () => {
        del.disabled = true;
        await fetch(`/api/broadcast/${bc.id}?key=${KEY}`, {method:'DELETE'});
        fetchBroadcasts();
      };
      row.appendChild(txt); row.appendChild(del);
      list.appendChild(row);
    });
  } catch(e) {}
}

async function sendBroadcast() {
  const subject = document.getElementById('bc-subject').value.trim();
  const content = document.getElementById('bc-content').value.trim();
  const el = document.getElementById('bc-result');
  if (!subject) { el.textContent = 'Subject required.'; return; }
  el.style.color = '#888'; el.textContent = 'Sending...';
  try {
    const r = await fetch(`/api/broadcast?key=${KEY}`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject,content})});
    const d = await r.json();
    if (d.ok) {
      el.style.color = '#4caf50'; el.textContent = 'Queued! Users will see it on next login.';
      document.getElementById('bc-subject').value = '';
      document.getElementById('bc-content').value = '';
      fetchBroadcasts();
    } else { el.style.color = '#e05252'; el.textContent = d.error || 'Failed'; }
  } catch(e) { el.style.color='#e05252'; el.textContent='Request failed'; }
}

const wrap = document.getElementById('log-wrap');
wrap.addEventListener('scroll', () => { pinned = wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight < 40; });

function colorResult(msg, ok) {
  const el = document.getElementById('col-result');
  el.textContent = msg; el.style.color = ok ? '#4caf50' : '#e05252';
  setTimeout(() => { el.textContent = ''; }, 2500);
}

async function fetchColors() {
  try {
    const r = await fetch(`/api/color?key=${KEY}`);
    const d = await r.json();
    const list = document.getElementById('col-list');
    const entries = Object.entries(d.colors || {});
    if (!entries.length) { list.innerHTML = '<div style="color:#444">No color entries</div>'; return; }
    list.innerHTML = '';
    entries.sort(([a],[b]) => a.localeCompare(b)).forEach(([uname, cfg]) => {
      const color = (cfg && (cfg.color || cfg.colour || cfg.value)) || '';
      const dn    = (cfg && (cfg.display || cfg.display_name)) || uname;
      const row = document.createElement('div');
      row.style.cssText = 'display:flex;align-items:center;gap:8px;background:#1a1a1a;border-radius:3px;padding:4px 8px;cursor:pointer';
      row.ondblclick = () => {
        document.getElementById('col-uname').value = uname;
        document.getElementById('col-color').value = color;
        document.getElementById('col-display').value = dn === uname ? '' : dn;
      };
      const swatch = document.createElement('span');
      swatch.style.cssText = `display:inline-block;width:14px;height:14px;border:1px solid #555;background:${color||'transparent'};border-radius:3px;flex-shrink:0`;
      const left = document.createElement('div');
      left.style.flex = '1';
      left.innerHTML = `<b style="color:#fff">${uname}</b> → <span style="color:${color||'#5af'}">${dn}</span>`;
      const del = document.createElement('button');
      del.textContent = '✕'; del.className = 'danger';
      del.style.cssText = 'padding:1px 6px;font-size:10px;line-height:1.2';
      del.onclick = async () => {
        del.disabled = true;
        await fetch(`/api/color?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname})});
        fetchColors();
      };
      row.appendChild(swatch); row.appendChild(left); row.appendChild(del);
      list.appendChild(row);
    });
  } catch(e) {}
}

async function colorApply() {
  const uname = document.getElementById('col-uname').value.trim();
  const color = document.getElementById('col-color').value.trim();
  const display = document.getElementById('col-display').value.trim();
  if (!uname) { colorResult('username required', false); return; }
  if (!color) { colorResult('color required', false); return; }
  try {
    const r = await fetch(`/api/color?key=${KEY}`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname,color,display:display||null})});
    const d = await r.json();
    if (d.ok) { colorResult(`set ${uname}`, true); fetchColors(); }
    else colorResult(d.error||'failed', false);
  } catch(e) { colorResult('request failed', false); }
}

async function colorClear() {
  const uname = document.getElementById('col-uname').value.trim();
  if (!uname) { colorResult('enter username to clear', false); return; }
  try {
    const r = await fetch(`/api/color?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname})});
    const d = await r.json();
    if (d.ok) { colorResult(`cleared ${uname}`, true); fetchColors(); }
    else colorResult(d.error||'failed', false);
  } catch(e) { colorResult('request failed', false); }
}

function deviceResult(msg, ok) {
  const el = document.getElementById('dev-result');
  el.textContent = msg; el.style.color = ok ? '#4caf50' : '#e05252';
  setTimeout(() => { el.textContent = ''; }, 2500);
}

async function fetchDevices() {
  try {
    const r = await fetch(`/api/device-ban?key=${KEY}`);
    const d = await r.json();
    const list = document.getElementById('dev-list');
    const arr = d.devices || [];
    if (!arr.length) { list.innerHTML = '<div style="color:#444">No banned devices</div>'; return; }
    list.innerHTML = '';
    arr.forEach(devId => {
      const row = document.createElement('div');
      row.style.cssText = 'display:flex;align-items:center;gap:8px;background:#1a1a1a;border-radius:3px;padding:3px 8px';
      const left = document.createElement('div');
      left.style.cssText = 'flex:1;word-break:break-all;color:#ccc';
      left.textContent = devId;
      const del = document.createElement('button');
      del.textContent = '✕'; del.className = 'danger';
      del.style.cssText = 'padding:1px 6px;font-size:10px;line-height:1.2';
      del.onclick = async () => {
        del.disabled = true;
        await fetch(`/api/device-ban?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:devId})});
        fetchDevices();
      };
      row.appendChild(left); row.appendChild(del);
      list.appendChild(row);
    });
  } catch(e) {}
}

async function deviceBan() {
  const id = document.getElementById('dev-id').value.trim();
  if (!id) { deviceResult('deviceID required', false); return; }
  try {
    const r = await fetch(`/api/device-ban?key=${KEY}`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:id})});
    const d = await r.json();
    if (d.ok) { deviceResult(`banned ${id.slice(0,12)}…`, true); document.getElementById('dev-id').value=''; fetchDevices(); }
    else deviceResult(d.error||'failed', false);
  } catch(e) { deviceResult('request failed', false); }
}

async function deviceUnban() {
  const id = document.getElementById('dev-id').value.trim();
  if (!id) { deviceResult('deviceID required', false); return; }
  try {
    const r = await fetch(`/api/device-ban?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:id})});
    const d = await r.json();
    if (d.ok) { deviceResult(`unbanned`, true); document.getElementById('dev-id').value=''; fetchDevices(); }
    else deviceResult(d.error||'failed', false);
  } catch(e) { deviceResult('request failed', false); }
}

function tempBanResult(msg, ok) {
  const el = document.getElementById('tb-result');
  el.textContent = msg; el.style.color = ok ? '#4caf50' : '#e05252';
  setTimeout(() => { el.textContent = ''; }, 2500);
}

function fmtRemaining(s) {
  s = Math.max(0, parseInt(s)||0);
  const d = Math.floor(s/86400); s -= d*86400;
  const h = Math.floor(s/3600);  s -= h*3600;
  const m = Math.floor(s/60);    const sec = s-m*60;
  if (d) return `${d}d ${h}h`;
  if (h) return `${h}h ${m}m`;
  if (m) return `${m}m ${sec}s`;
  return `${sec}s`;
}

async function fetchTempBans() {
  try {
    const r = await fetch(`/api/temp-ban?key=${KEY}`);
    const d = await r.json();
    const list = document.getElementById('tb-list');
    const arr = d.bans || [];
    if (!arr.length) { list.innerHTML = '<div style="color:#444">No active temp bans</div>'; return; }
    list.innerHTML = '';
    arr.forEach(b => {
      const row = document.createElement('div');
      row.style.cssText = 'display:flex;align-items:center;gap:8px;background:#1a1a1a;border-radius:3px;padding:3px 8px';
      const left = document.createElement('div');
      left.style.flex = '1';
      const name = document.createElement('div');
      name.style.cssText = 'color:#ccc;word-break:break-all';
      name.textContent = `${b.username} · ${fmtRemaining(b.remaining)}`;
      left.appendChild(name);
      if (b.reason) {
        const rsn = document.createElement('div');
        rsn.style.cssText = 'color:#888;font-size:10px';
        rsn.textContent = b.reason;
        left.appendChild(rsn);
      }
      const del = document.createElement('button');
      del.textContent = '✕'; del.className = 'danger';
      del.style.cssText = 'padding:1px 6px;font-size:10px;line-height:1.2';
      del.onclick = async () => {
        del.disabled = true;
        await fetch(`/api/temp-ban?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:b.username})});
        fetchTempBans();
      };
      row.appendChild(left); row.appendChild(del);
      list.appendChild(row);
    });
  } catch(e) {}
}

async function tempBanApply() {
  const uname = document.getElementById('tb-uname').value.trim();
  const duration = document.getElementById('tb-duration').value.trim();
  const unit = document.getElementById('tb-unit').value;
  const reason = document.getElementById('tb-reason').value.trim();
  if (!uname) { tempBanResult('username required', false); return; }
  if (!duration || parseFloat(duration) <= 0) { tempBanResult('duration required', false); return; }
  try {
    const r = await fetch(`/api/temp-ban?key=${KEY}`, {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname,duration,unit,reason})});
    const d = await r.json();
    if (d.ok) { tempBanResult(`banned ${uname}`, true); document.getElementById('tb-duration').value=''; document.getElementById('tb-reason').value=''; fetchTempBans(); }
    else tempBanResult(d.error||'failed', false);
  } catch(e) { tempBanResult('request failed', false); }
}

async function tempBanClear() {
  const uname = document.getElementById('tb-uname').value.trim();
  if (!uname) { tempBanResult('username required', false); return; }
  try {
    const r = await fetch(`/api/temp-ban?key=${KEY}`, {method:'DELETE',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:uname})});
    const d = await r.json();
    if (d.ok) { tempBanResult(`lifted ${uname}`, true); fetchTempBans(); }
    else tempBanResult(d.error||'not banned', false);
  } catch(e) { tempBanResult('request failed', false); }
}

// ── analytics overlay ──
let _anData = [], anSort = {col:'t', dir:-1};
function anVerKey(v){ const m=String(v||'').match(/(\d+)\.(\d+)/); return m?parseInt(m[1])*100000+parseInt(m[2]):-1; }
function anBaseCmp(a,b,col){
  if(col==='t')       return (b.t||0)-(a.t||0);
  if(col==='version') return anVerKey(b.version)-anVerKey(a.version);
  if(col==='user')    return String(a.user||'').toLowerCase().localeCompare(String(b.user||'').toLowerCase());
  if(col==='device')  return String(a.device||'').toLowerCase().localeCompare(String(b.device||'').toLowerCase());
  return 0;
}
function anSortClick(col){
  if(anSort.col===col) anSort.dir=-anSort.dir; else{anSort.col=col;anSort.dir=-1;}
  renderAnalytics();
}
function anUpdateArrows(){
  ['t','user','version','device'].forEach(c=>{const el=document.getElementById('an-arr-'+c);if(el)el.textContent=(anSort.col===c)?(anSort.dir===-1?'▼':'▲'):'';});
}
function anEsc(s){return(s==null?'':String(s)).replace(/[&<>]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));}
function renderAnalytics(){
  const q=(document.getElementById('an-filter').value||'').toLowerCase();
  let rows=_anData.filter(r=>!q||Object.values(r).some(v=>String(v).toLowerCase().includes(q)));
  rows.sort((a,b)=>anSort.dir*anBaseCmp(a,b,anSort.col));
  anUpdateArrows();
  const tbody=document.getElementById('an-body');
  tbody.innerHTML='';
  rows.forEach(ev=>{
    const d=new Date((ev.t||0)*1000);
    const ts=d.toLocaleDateString()+' '+d.toLocaleTimeString();
    tbody.innerHTML+=`<tr style="border-bottom:1px solid #1a1a1a"><td style="padding:4px 8px;color:#666">${anEsc(ts)}</td><td style="padding:4px 8px;color:#ccc">${anEsc(ev.user)}</td><td style="padding:4px 8px;color:#5b9bd5">${anEsc(ev.version)}</td><td style="padding:4px 8px;color:#888;font-size:11px">${anEsc((ev.device||'').slice(0,16))}</td></tr>`;
  });
}
function toggleAnalytics(){
  const o=document.getElementById('analytics-overlay');
  const show=o.style.display==='none';
  o.style.display=show?'block':'none';
  if(show&&!_anData.length)fetchAnalytics();
}
async function fetchAnalytics(){
  const s=document.getElementById('an-status');s.textContent='loading...';
  try{
    const r=await fetch('/dashboard/version-history?key='+encodeURIComponent(KEY));
    const j=await r.json();
    _anData=j.events||[];
    s.textContent=_anData.length+' logins (newest first)';
    renderAnalytics();
  }catch(e){s.textContent='error: '+e;}
}

async function fetchRooms() {
  try {
    const r = await fetch(`/api/rooms?key=${KEY}`);
    const d = await r.json();
    const rooms = d.rooms || [];
    document.getElementById('rooms-badge').textContent = `${rooms.length} room${rooms.length!==1?'s':''}`;
    const list = document.getElementById('rooms-list');
    if (!rooms.length) { list.innerHTML = '<div style="color:#444">No active rooms</div>'; return; }
    list.innerHTML = '';
    rooms.forEach(room => {
      const row = document.createElement('div');
      row.style.cssText = 'background:#1a1a1a;border-radius:3px;padding:4px 8px';
      const header = document.createElement('div');
      header.style.cssText = 'display:flex;align-items:center;gap:8px';
      const code = document.createElement('b');
      code.style.cssText = 'color:#5b9bd5;font-size:13px;letter-spacing:.04em';
      code.textContent = room.code;
      const cnt = document.createElement('span');
      cnt.style.cssText = 'color:#888;font-size:11px';
      cnt.textContent = `${room.count} player${room.count!==1?'s':''}`;
      header.appendChild(code); header.appendChild(cnt);
      const players = document.createElement('div');
      players.style.cssText = 'color:#aaa;font-size:11px;margin-top:1px';
      players.textContent = room.players.join(', ');
      row.appendChild(header); row.appendChild(players);
      list.appendChild(row);
    });
  } catch(e) {}
}

fetchLogs(); fetchVersions(); fetchBroadcasts(); fetchColors(); fetchDevices(); fetchTempBans(); fetchRooms();
setInterval(fetchLogs, 3000);
setInterval(fetchVersions, 10000);
setInterval(fetchBroadcasts, 15000);
setInterval(fetchColors, 20000);
setInterval(fetchDevices, 20000);
setInterval(fetchTempBans, 5000);
setInterval(fetchRooms, 5000);
</script></body></html>"""
    return html

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=9080)
