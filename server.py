# Library yang digunakan
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple
from dotenv import load_dotenv
from functools import wraps
from flask import Flask, jsonify, request, Response
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from google.auth.exceptions import DefaultCredentialsError
import midtransclient
from werkzeug.security import check_password_hash, generate_password_hash
from apscheduler.schedulers.background import BackgroundScheduler
import threading
import paho.mqtt.client as mqtt
import json

""" 
Format respon API:
{
    "query_status": "success" or "error",
    "return_code": status_code,
    "payload": payload,
    "message": message (opsional)
}
"""
def api_response(
    payload: Any,
    status_code: int = 200,
    message: Optional[str] = None,
) -> Tuple[Response, int]:
    query_status = "success" if 200 <= status_code < 400 else "error"

    body: Dict[str, Any] = {
        "query_status": query_status,
        "return_code": status_code,
        "payload": payload,
    }

    if message:
        body["message"] = message

    return jsonify(body), status_code

# Konversi ke format waktu UTC
def _normalize_to_utc_aware(value: Any) -> Optional[datetime]:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    try:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# Pengecekan kedaluwarsa session
def _is_session_expired(expires_at_value: Any) -> Tuple[bool, Optional[str]]:
    dt = _normalize_to_utc_aware(expires_at_value)
    if dt is None:
        return True, None

    now = datetime.now(timezone.utc)
    expired = dt <= now
    return expired, dt.isoformat().replace("+00:00", "Z")

# Inisialisasi koneksi ke Firestore
def init_firestore_client(project_id: Optional[str]) -> Optional[firestore.Client]:
    if not project_id:
        return None

    return firestore.Client(project=project_id)

# Inisialisasi koneksi ke Midtrans
def init_midtrans_core_api(config: Dict[str, Any]) -> Optional[midtransclient.CoreApi]:
    if not config:
        return None

    server_key = config.get("MIDTRANS_SERVER_KEY")
    client_key = config.get("MIDTRANS_CLIENT_KEY")
    is_production_raw = str(config.get("MIDTRANS_IS_PRODUCTION", "false")).lower()

    if not server_key or not client_key:
        return None

    is_production = is_production_raw in {"1", "true", "yes"}

    return midtransclient.CoreApi(
        is_production=is_production,
        server_key=server_key,
        client_key=client_key,
    )

# Inisialisasi koneksi ke MQTT Broker
def init_mqtt_client(config: Dict[str, Any]) -> Optional[mqtt.Client]:
    if not config:
        return None

    mqtt_broker = config.get("MQTT_BROKER_URL")
    mqtt_port_raw = config.get("MQTT_BROKER_PORT")
    mqtt_username = config.get("MQTT_USERNAME")
    mqtt_password = config.get("MQTT_PASSWORD")

    if not mqtt_broker or not mqtt_port_raw:
        return None

    try:
        mqtt_port = int(mqtt_port_raw)
        # Gunakan transport dari config jika tersedia, fallback ke auto-detect berdasarkan port
        if config.get("MQTT_TRANSPORT"):
            transport = config.get("MQTT_TRANSPORT")
        else:
            transport = "websockets" if mqtt_port in [443, 80, 8884] else "tcp"
            
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"GS-SERVER-{uuid.uuid4().hex[:6]}", transport=transport)
        
        # Callback saat berhasil terhubung atau reconnect
        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                print(f"[MQTT] ✅ Terhubung ke broker {mqtt_broker}:{mqtt_port} via {transport}")
            else:
                print(f"[MQTT ERROR] ❌ Gagal terhubung dengan kode: {reason_code}")

        # Callback saat koneksi terputus
        def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
            print(f"[MQTT WARNING] ⚠️ Koneksi terputus (Reason: {reason_code}). Mencoba menyambung kembali secara otomatis...")

        mqtt_client.on_connect = on_connect
        mqtt_client.on_disconnect = on_disconnect
        
        if mqtt_username and mqtt_password:
            mqtt_client.username_pw_set(mqtt_username, mqtt_password)
            
        if mqtt_port in [443, 8883, 8884]:
            import ssl
            mqtt_client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
            
        mqtt_client.connect(mqtt_broker, mqtt_port, 60)
        mqtt_client.loop_start()
        return mqtt_client
    except Exception as e:
        print(f"[MQTT ERROR] ❌ Gagal koneksi: {e}")
        return None

# Mengambil setting dari Firestore settings/global
def fetch_dynamic_settings(db: firestore.Client) -> Dict[str, Any]:
    try:
        doc_ref = db.collection("settings").document("global")
        doc = doc_ref.get()
        if doc.exists:
            raw_data = doc.to_dict() or {}
            # Normalisasi semua key menjadi Uppercase
            data = {k.upper(): v for k, v in raw_data.items()}
            return data
    except Exception as e:
        print(f"[FIREBASE ERROR] ❌ Gagal memuat settings: {e}")
    return {}

# Ekstrak QR URL
def extract_qr_url(actions: Any) -> Optional[str]:
    if not isinstance(actions, list):
        return None

    for a in actions:
        if not isinstance(a, dict):
            continue
        name = str(a.get("name") or "").lower()
        url = a.get("url")
        if not url:
            continue
        if "qr" in name:
            return str(url)

    for a in actions:
        if isinstance(a, dict) and a.get("url"):
            return str(a["url"])

    return None

# Mapping status Midtrans
def map_midtrans_status(
    transaction_status: Optional[str],
    fraud_status: Optional[str],
) -> str:
    if transaction_status is None:
        return "unknown"

    if transaction_status in {"capture", "settlement"}:
        if fraud_status and fraud_status.lower() == "challenge":
            return "on_review"
        return "settlement"

    if transaction_status == "pending":
        return "pending"

    if transaction_status in {"deny", "cancel"}:
        return "failed"

    if transaction_status == "expired":
        return "expired"

    return transaction_status

# Generate order_id dengan format: "TRX-YYYYMMDDHHMMSS-XXXXXX"
def generate_order_id(prefix: str = "TRX") -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = uuid.uuid4().hex[:6].upper()
    return f"{prefix}-{now}-{random_suffix}"

# MAIN FLASK APP
def create_app() -> Flask:
    load_dotenv()

    app = Flask(__name__)

    app.config["PROJECT_ID"] = os.getenv("GCP_PROJECT_ID")
    db = init_firestore_client(app.config["PROJECT_ID"])
    app.config["FIRESTORE_DB"] = db

    # Load Dynamic Settings from Firestore
    dynamic_settings = fetch_dynamic_settings(db)
    app.config["DYNAMIC_SETTINGS"] = dynamic_settings

    # Load Midtrans Core API & MQTT Broker Connection Dynamic Settings
    core_api = init_midtrans_core_api(config=dynamic_settings)
    app.config["MIDTRANS_CORE_API"] = core_api
    mqtt_client = init_mqtt_client(config=dynamic_settings)
    app.config["MQTT_CLIENT"] = mqtt_client

    # Inisialisasi APScheduler
    scheduler = BackgroundScheduler(timezone=timezone(timedelta(hours=7)))
    scheduler.start()
    app.config["SCHEDULER"] = scheduler

    # Fungsi untuk memulai event rental
    def start_rental_event(order_id: str):
        db_client = app.config.get("FIRESTORE_DB")
        if not db_client: return
        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", order_id)).limit(1).stream()
        tx_docs = list(tx_query)
        if tx_docs:
            tx_doc = tx_docs[0]
            tx_ref = tx_doc.reference
            tx_data = tx_doc.to_dict() or {}
            if tx_data.get("status") == "upcoming": # Cek jika status transaksi adalah upcoming pada pemesanan booking
                tx_ref.set({"status": "ongoing"}, merge=True)
                loc_id = tx_data.get("location_id")
                sta_id = tx_data.get("station_id")
                if loc_id and sta_id: 
                    booking_ref = db_client.collection("locations").document(loc_id)\
                        .collection("stations").document(sta_id)\
                        .collection("bookings").document(order_id)
                    booking_ref.set({"status": "ongoing"}, merge=True)
                    
                    # Tembak perintah MQTT "ON" dan set status station menjadi "occupied"
                    publish_mqtt_command(loc_id, sta_id, "ON")
                    db_client.collection("locations").document(loc_id).collection("stations").document(sta_id).set({"status": "occupied"}, merge=True)
                
                now_str = datetime.now(timezone(timedelta(hours=7))).strftime('%H:%M:%S')
                print(f"[SCHEDULER {now_str}] 🟢 Rental Started: {order_id} is now 'ongoing'")

    # Fungsi untuk mengakhiri event rental                
    def finish_rental_event(order_id: str):
        db_client = app.config.get("FIRESTORE_DB")
        if not db_client: return
        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", order_id)).limit(1).stream()
        tx_docs = list(tx_query)
        if tx_docs:
            tx_doc = tx_docs[0]
            tx_ref = tx_doc.reference
            tx_data = tx_doc.to_dict() or {}
            if tx_data.get("status") in ["ongoing", "upcoming"]:
                tx_ref.set({"status": "completed"}, merge=True)
                loc_id = tx_data.get("location_id")
                sta_id = tx_data.get("station_id")
                
                # Update status semua perpanjangan sesi yang berkaitan (tanpa collection_group agar tidak kena error missing index)
                if loc_id:
                    ext_query = db_client.collection("locations").document(loc_id).collection("transactions").where(filter=FieldFilter("parent_order_id", "==", order_id)).stream()
                    for ext_doc in ext_query:
                        ext_doc.reference.set({"status": "completed"}, merge=True)
                
                if loc_id and sta_id:
                    booking_ref = db_client.collection("locations").document(loc_id)\
                        .collection("stations").document(sta_id)\
                        .collection("bookings").document(order_id)
                    booking_ref.set({"status": "completed"}, merge=True)
                    
                    # Tembak perintah MQTT (OFF) dan set status station menjadi "available"
                    publish_mqtt_command(loc_id, sta_id, "OFF")
                    db_client.collection("locations").document(loc_id).collection("stations").document(sta_id).set({"status": "available"}, merge=True)
                
                now_str = datetime.now(timezone(timedelta(hours=7))).strftime('%H:%M:%S')
                print(f"[SCHEDULER {now_str}] 🔴 Rental Finished: {order_id} is now 'completed'")

    # Fungsi untuk mengirim perintah ke MQTT Broker (ON/OFF)
    def publish_mqtt_command(loc_id: str, sta_id: str, action: str):
        mqtt_client = app.config.get("MQTT_CLIENT")
        if not mqtt_client: return
        
        topic_action = f"{loc_id}/{sta_id}/action"
        
        try:
            mqtt_client.publish(topic_action, action, qos=1)
            print(f"[MQTT] 📡 Berhasil publish ke {topic_action} -> {action}")
        except Exception as e:
            print(f"[MQTT ERROR] Gagal publish untuk {loc_id}/{sta_id}: {e}")

    # Fungsi untuk cek role (permission)
    def _require_roles(allowed_roles: list[str]):
        def decorator(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                db_client = app.config.get("FIRESTORE_DB")
                if db_client is None:
                    return api_response(None, 500, message="Firestore client is not initialized.")
                
                username = request.headers.get("X-Username")
                session_token = request.headers.get("X-Session-Token")
                
                if not session_token:
                    auth = request.headers.get("Authorization") or ""
                    if auth.lower().startswith("bearer "):
                        session_token = auth.split(" ", 1)[1].strip()

                if not username or not session_token:
                    return api_response(None, 401, message="Unauthorized: Missing X-Username or session token headers")

                username = str(username).strip()
                session_token = str(session_token).strip()

                user_doc_ref = db_client.collection("users").document(username)
                user_snapshot = user_doc_ref.get()
                if not user_snapshot.exists:
                    return api_response(None, 401, message="Unauthorized: User not found")

                user_data = user_snapshot.to_dict() or {}
                
                if user_data.get("role") not in allowed_roles:
                    return api_response(None, 403, message=f"Forbidden: Requires one of roles {allowed_roles}")
                    
                stored_token = user_data.get("session_token")
                expires_at_value = user_data.get("session_expires_at")

                if not stored_token or stored_token != session_token:
                    return api_response(None, 401, message="Unauthorized: Invalid session token")

                expired, _ = _is_session_expired(expires_at_value)
                if expired:
                    user_doc_ref.update({"session_token": None, "session_expires_at": None})
                    return api_response(None, 401, message="Unauthorized: Session expired")

                return f(*args, **kwargs)
            return decorated_function
        return decorator

    # Aliases
    require_customer = _require_roles(["customer"])
    require_manager = _require_roles(["manager"])
    require_auth = _require_roles(["customer", "manager"])

    # Route GET / (pesan status server)
    @app.get("/")
    def index():
        return api_response({"message": "REST API server is up!"})

    # Route POST /auth/register (membuat akun customer baru)
    @app.post("/auth/register")
    def register_customer():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        required_fields = ["email", "full_name", "username", "password"]
        missing = [f for f in required_fields if not body.get(f)]
        if missing:
            return api_response(
                payload={"missing_fields": missing},
                status_code=400,
                message="Missing required fields",
            )

        email = str(body["email"]).strip().lower()
        username = str(body["username"]).strip()
        full_name = str(body["full_name"]).strip()
        raw_password = str(body["password"])

        if not username:
            return api_response(
                payload={"field": "username"},
                status_code=400,
                message="username cannot be empty",
            )

        users_ref = db_client.collection("users")
        user_doc_ref = users_ref.document(username)
        if user_doc_ref.get().exists:
            return api_response(
                payload={"username": username},
                status_code=409,
                message="Username already exists",
            )

        existing_email = list(
            users_ref.where(filter=FieldFilter("email", "==", email)).limit(1).stream()
        )
        if existing_email:
            return api_response(
                payload={"email": email},
                status_code=409,
                message="Email already registered",
            )

        password_hash = generate_password_hash(raw_password)

        user_data = {
            "email": email,
            "full_name": full_name,
            "username": username,
            "password": password_hash,
            "role": "customer",
            "last_login": None,
            "created_at": firestore.SERVER_TIMESTAMP,
        }

        user_doc_ref.set(user_data)

        response_user = {
            "email": email,
            "full_name": full_name,
            "username": username,
            "role": "customer",
        }

        return api_response(response_user, status_code=201)

    # Route POST /auth/login (login customer atau manager)
    @app.post("/auth/login")
    def login_user():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        required_fields = ["username", "password"]
        missing = [f for f in required_fields if not body.get(f)]
        if missing:
            return api_response(
                payload={"missing_fields": missing},
                status_code=400,
                message="Missing required fields",
            )

        username = str(body["username"]).strip()
        raw_password = str(body["password"])

        users_ref = db_client.collection("users")
        user_doc_ref = users_ref.document(username)
        user_snapshot = user_doc_ref.get()
        if not user_snapshot.exists:
            return api_response(
                payload=None,
                status_code=401,
                message="Invalid username or password",
            )

        user_data = user_snapshot.to_dict() or {}
        stored_hash = user_data.get("password")
        if not stored_hash or not check_password_hash(stored_hash, raw_password):
            return api_response(
                payload=None,
                status_code=401,
                message="Invalid username or password",
            )

        if user_data.get("role") not in ["customer", "manager"]:
            return api_response(
                payload={"role": user_data.get("role")},
                status_code=401,
                message="Invalid username or password",
            )

        # Buat session token berlaku 7 hari
        session_token = uuid.uuid4().hex
        expires_at = datetime.now(timezone.utc) + timedelta(days=7)

        user_doc_ref.update(
            {
                "last_login": firestore.SERVER_TIMESTAMP,
                "session_token": session_token,
                "session_expires_at": expires_at,
            }
        )

        response_user = {
            "email": user_data.get("email"),
            "full_name": user_data.get("full_name"),
            "username": user_data.get("username"),
            "role": user_data.get("role"),
            "session_token": session_token,
            "session_expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        }

        return api_response(response_user)

    # Route POST /auth/session/validate (validasi session customer dan manager)
    @app.post("/auth/session/validate")
    def validate_session():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        required_fields = ["username", "session_token"]
        missing = [f for f in required_fields if not body.get(f)]
        if missing:
            return api_response(
                payload={"missing_fields": missing},
                status_code=400,
                message="Missing required fields",
            )

        username = str(body["username"]).strip()
        session_token = str(body["session_token"]).strip()

        users_ref = db_client.collection("users")
        user_doc_ref = users_ref.document(username)
        user_snapshot = user_doc_ref.get()
        if not user_snapshot.exists:
            return api_response(
                payload=None,
                status_code=401,
                message="Invalid session",
            )

        user_data = user_snapshot.to_dict() or {}
        if user_data.get("role") not in ["customer", "manager"]:
            return api_response(
                payload=None,
                status_code=401,
                message="Invalid role for this session",
            )
        stored_token = user_data.get("session_token")
        expires_at_value = user_data.get("session_expires_at")

        if not stored_token or stored_token != session_token:
            return api_response(
                payload=None,
                status_code=401,
                message="Invalid session",
            )

        expired, expires_str = _is_session_expired(expires_at_value)

        if expired:
            # hapus token lama supaya benar‑benar logout
            user_doc_ref.update(
                {
                    "session_token": None,
                    "session_expires_at": None,
                }
            )
            return api_response(
                payload=None,
                status_code=401,
                message="Session expired",
            )

        response_user = {
            "email": user_data.get("email"),
            "full_name": user_data.get("full_name"),
            "username": user_data.get("username"),
            "role": user_data.get("role"),
            "session_token": stored_token,
            "session_expires_at": expires_str,
        }

        return api_response(response_user)

    # Route POST /auth/me (mengambil profil user yang sedang login)
    @app.post("/auth/me")
    def auth_me():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        username = body.get("username") or request.headers.get("X-Username")
        session_token = body.get("session_token") or request.headers.get("X-Session-Token")
        if not session_token:
            auth = request.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                session_token = auth.split(" ", 1)[1].strip()

        if not username or not session_token:
            return api_response(
                payload={"missing_fields": ["username", "session_token"]},
                status_code=400,
                message="Missing required fields",
            )

        username = str(username).strip()
        session_token = str(session_token).strip()

        user_doc_ref = db_client.collection("users").document(username)
        user_snapshot = user_doc_ref.get()
        if not user_snapshot.exists:
            return api_response(payload=None, status_code=401, message="Invalid session")

        user_data = user_snapshot.to_dict() or {}
        stored_token = user_data.get("session_token")
        expires_at_value = user_data.get("session_expires_at")
        if not stored_token or stored_token != session_token:
            return api_response(payload=None, status_code=401, message="Invalid session")

        expired, _ = _is_session_expired(expires_at_value)

        if expired:
            user_doc_ref.update({"session_token": None, "session_expires_at": None})
            return api_response(payload=None, status_code=401, message="Session expired")

        payload = {
            "email": user_data.get("email"),
            "full_name": user_data.get("full_name"),
            "username": user_data.get("username"),
            "role": user_data.get("role"),
            "last_login": user_data.get("last_login"),
        }
        return api_response(payload)

    # Route POST /auth/logout (logout user berdasarkan session)
    @app.post("/auth/logout")
    def auth_logout():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        username = body.get("username") or request.headers.get("X-Username")
        session_token = body.get("session_token") or request.headers.get("X-Session-Token")
        if not session_token:
            auth = request.headers.get("Authorization") or ""
            if auth.lower().startswith("bearer "):
                session_token = auth.split(" ", 1)[1].strip()

        if not username or not session_token:
            return api_response(
                payload={"missing_fields": ["username", "session_token"]},
                status_code=400,
                message="Missing required fields",
            )

        username = str(username).strip()
        session_token = str(session_token).strip()

        user_doc_ref = db_client.collection("users").document(username)
        user_snapshot = user_doc_ref.get()
        if not user_snapshot.exists:
            return api_response(payload=None, status_code=401, message="Invalid session")

        user_data = user_snapshot.to_dict() or {}
        stored_token = user_data.get("session_token")
        if not stored_token or stored_token != session_token:
            return api_response(payload=None, status_code=401, message="Invalid session")

        user_doc_ref.update({"session_token": None, "session_expires_at": None})
        return api_response({"logged_out": True})

    # Route POST /locations (menambahkan lokasi rental baru)
    @app.post("/locations")
    @require_manager
    def create_location():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")

        body = request.get_json(silent=True) or {}
        required_fields = ["id", "location_name", "address", "open_time", "close_time", "phone", "active"]
        missing = [f for f in required_fields if not body.get(f)]
        if missing:
            return api_response({"missing_fields": missing}, 400, "Missing required fields")

        loc_id = str(body["id"]).strip()
        location_ref = db_client.collection("locations").document(loc_id)
        
        if location_ref.get().exists:
            return api_response(None, 409, f"Location ID '{loc_id}' already exists")

        loc_data = {
            "location_name": str(body["location_name"]),
            "address": str(body["address"]),
            "open_time": str(body["open_time"]),
            "close_time": str(body["close_time"]),
            "phone": str(body.get("phone", "")),
            "active": body.get("active", True)
        }
        
        location_ref.set(loc_data)
        
        return api_response({
            "id": loc_id,
            **loc_data
        }, 201)

    # Route GET /locations (mengambil daftar lokasi rental yang aktif)
    @app.get("/locations")
    @require_auth
    def list_locations():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        locations: list[Dict[str, Any]] = []

        for doc in db_client.collection("locations").stream():
            data = doc.to_dict() or {}
            locations.append(
                {
                    "id": doc.id,
                    "address": data.get("address"),
                    "location_name": data.get("location_name"),
                    "active": data.get("active"),
                    "open_time": data.get("open_time"),
                    "close_time": data.get("close_time"),
                    "phone": data.get("phone"),
                }
            )

        return api_response({"locations": locations})

    # Route POST /locations/<location_id>/stations (menambahkan station baru ke suatu lokasi)
    @app.post("/locations/<location_id>/stations")
    @require_manager
    def create_station(location_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")

        body = request.get_json(silent=True) or {}
        required_fields = ["id", "station_name", "console_type", "price_per_hour"]
        missing = [f for f in required_fields if body.get(f) is None]
        if missing:
            return api_response({"missing_fields": missing}, 400, "Missing required fields")

        location_ref = db_client.collection("locations").document(location_id)
        if not location_ref.get().exists:
            return api_response(None, 404, f"Location '{location_id}' not found")

        sta_id = str(body["id"]).strip()
        station_ref = location_ref.collection("stations").document(sta_id)
        
        if station_ref.get().exists:
            return api_response(None, 409, f"Station ID '{sta_id}' already exists in this location")

        try:
            price_per_hour = int(body["price_per_hour"])
        except (ValueError, TypeError):
            return api_response({"field": "price_per_hour"}, 400, "price_per_hour must be an integer")

        sta_data = {
            "station_name": str(body["station_name"]),
            "console_type": str(body["console_type"]),
            "price_per_hour": price_per_hour,
            "active": body.get("active", True),
            "status": "available"
        }
        
        station_ref.set(sta_data)
        
        return api_response({
            "id": sta_id,
            "location_id": location_id,
            **sta_data
        }, 201)

    # Route PUT /locations/<location_id> (mengedit data lokasi yang sudah ada)
    @app.put("/locations/<location_id>")
    @require_manager
    def update_location(location_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")

        body = request.get_json(silent=True) or {}
        if not body:
            return api_response(None, 400, message="Request body is empty")

        location_ref = db_client.collection("locations").document(location_id)
        if not location_ref.get().exists:
            return api_response(None, 404, f"Location '{location_id}' not found")

        allowed_fields = ["location_name", "address", "open_time", "close_time", "phone", "active"]
        update_data = {k: v for k, v in body.items() if k in allowed_fields}
        if not update_data:
            return api_response(None, 400, message=f"No valid fields to update. Allowed: {allowed_fields}")

        location_ref.update(update_data)
        return api_response({"id": location_id, **update_data})

    # Route PUT /locations/<location_id>/stations/<station_id> (mengedit data station yang sudah ada)
    @app.put("/locations/<location_id>/stations/<station_id>")
    @require_manager
    def update_station(location_id: str, station_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")

        body = request.get_json(silent=True) or {}
        if not body:
            return api_response(None, 400, message="Request body is empty")

        station_ref = db_client.collection("locations").document(location_id).collection("stations").document(station_id)
        if not station_ref.get().exists:
            return api_response(None, 404, f"Station '{station_id}' not found in location '{location_id}'")

        allowed_fields = ["station_name", "console_type", "price_per_hour", "active", "status"]
        update_data = {k: v for k, v in body.items() if k in allowed_fields}
        if not update_data:
            return api_response(None, 400, message=f"No valid fields to update. Allowed: {allowed_fields}")

        if "price_per_hour" in update_data:
            try:
                update_data["price_per_hour"] = int(update_data["price_per_hour"])
            except (ValueError, TypeError):
                return api_response({"field": "price_per_hour"}, 400, "price_per_hour must be an integer")

        if "status" in update_data and update_data["status"] not in ["available", "occupied"]:
            return api_response({"field": "status"}, 400, "status must be 'available' or 'occupied'")

        station_ref.update(update_data)
        return api_response({"id": station_id, "location_id": location_id, **update_data})

    # Route GET /admin/settings (mengambil data konfigurasi global dari Firestore)
    @app.get("/admin/settings")
    @require_manager
    def get_admin_settings():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")
        
        settings = fetch_dynamic_settings(db_client)
        return api_response(settings)

    # Route POST /admin/settings (memperbarui data konfigurasi global di Firestore)
    @app.post("/admin/settings")
    @require_manager
    def update_admin_settings():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")
            
        body = request.get_json(silent=True) or {}
        if not body:
            return api_response(None, 400, message="Request body is empty")
            
        normalized_body = {k.lower(): v for k, v in body.items()}
            
        db_client.collection("settings").document("global").set(normalized_body, merge=True)
        return api_response(normalized_body, message="Settings updated in Firestore. Use /admin/settings/reload to apply.")

    # Route POST /admin/settings/reload (me-reload data konfigurasi global dari Firestore)
    @app.post("/admin/settings/reload")
    @require_manager
    def reload_admin_settings():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, message="Firestore client is not initialized.")

        # 1. Fetch data terbaru
        new_settings = fetch_dynamic_settings(db_client)
        app.config["DYNAMIC_SETTINGS"] = new_settings

        # 2. Re-init Midtrans
        new_midtrans = init_midtrans_core_api(config=new_settings)
        app.config["MIDTRANS_CORE_API"] = new_midtrans

        # 3. Re-init MQTT Broker(Matikan yang lama jika ada)
        old_mqtt = app.config.get("MQTT_CLIENT")
        if old_mqtt:
            try:
                old_mqtt.loop_stop()
                old_mqtt.disconnect()
            except:
                pass
        
        new_mqtt = init_mqtt_client(config=new_settings)
        app.config["MQTT_CLIENT"] = new_mqtt

        return api_response({
            "midtrans_status": "re-initialized" if new_midtrans else "failed",
            "mqtt_status": "re-initialized" if new_mqtt else "failed"
        }, message="System configuration reloaded successfully.")

    # Route GET /locations/<location_id>/stations (mengambil daftar station yang aktif untuk satu lokasi)
    @app.get("/locations/<location_id>/stations")
    @require_auth
    def list_stations_by_location(location_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        location_ref = db_client.collection("locations").document(location_id)
        location_snapshot = location_ref.get()
        if not location_snapshot.exists:
            return api_response(
                payload=None,
                status_code=404,
                message=f"Location '{location_id}' not found",
            )

        stations: list[Dict[str, Any]] = []
        for station_doc in location_ref.collection("stations").stream():
            station_data = station_doc.to_dict() or {}
            stations.append(
                {
                    "id": station_doc.id,
                    **station_data,
                }
            )

        return api_response(
            {
                "location_id": location_id,
                "stations": stations,
            }
        )

    # Route GET /locations/<location_id>/stations/<station_id>/schedule (mengambil jadwal yang tersedia selama 7 hari kedepan untuk station tertentu)
    @app.get("/locations/<location_id>/stations/<station_id>/schedule")
    @require_customer
    def get_station_schedule(location_id: str, station_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        location_ref = db_client.collection("locations").document(location_id)
        location_snap = location_ref.get()
        if not location_snap.exists:
            return api_response(None, 404, message="Location not found")
        
        loc_data = location_snap.to_dict() or {}
        open_time_str = loc_data.get("open_time", "09:00")
        close_time_str = loc_data.get("close_time", "22:00")
        
        station_ref = location_ref.collection("stations").document(station_id)
        if not station_ref.get().exists:
            return api_response(None, 404, message="Station not found")
            
        wib = timezone(timedelta(hours=7))
        now_wib = datetime.now(wib)
        current_date_str = now_wib.strftime("%Y-%m-%d")
        current_time_str = now_wib.strftime("%H:%M")
        
        target_dates = [(now_wib + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(7)]
        
        bookings_ref = station_ref.collection("bookings")
        bookings_stream = bookings_ref.where(filter=FieldFilter("booking_date", "in", target_dates)).stream()
        
        booked_ranges_by_date = {d: [] for d in target_dates}
        
        for b_doc in bookings_stream:
            b_data = b_doc.to_dict() or {}
            
            b_date = b_data.get("booking_date")
            b_time = b_data.get("booking_time")
            b_duration_raw = b_data.get("booking_duration")
            
            if not b_date or not b_time or b_duration_raw is None:
                continue
                
            try:
                duration_mins = int(float(b_duration_raw))
                b_time_clean = str(b_time).strip()[:5]
                b_start = datetime.strptime(b_time_clean, "%H:%M")
                b_end = b_start + timedelta(minutes=duration_mins)
                
                if b_date in booked_ranges_by_date:
                    booked_ranges_by_date[b_date].append((b_start, b_end))
            except (ValueError, TypeError):
                continue
                
        try:
            start_open = datetime.strptime(open_time_str, "%H:%M")
            end_close = datetime.strptime(close_time_str, "%H:%M")
            if end_close < start_open:
                end_close += timedelta(days=1)
        except (ValueError, TypeError):
            start_open = datetime.strptime("09:00", "%H:%M")
            end_close = datetime.strptime("22:00", "%H:%M")
            
        schedule = []
        for d in target_dates:
            daily_available = []
            
            curr_slot = start_open
            while curr_slot < end_close:
                slot_str = curr_slot.strftime("%H:%M")
                slot_end = curr_slot + timedelta(minutes=30)
                
                if d == current_date_str and slot_str <= current_time_str:
                    curr_slot += timedelta(minutes=30)
                    continue

                is_available = True
                for b_start, b_end in booked_ranges_by_date[d]:
                    if b_start < slot_end and b_end > curr_slot:
                        is_available = False
                        break
                        
                if is_available:
                    daily_available.append(slot_str)
                    
                curr_slot += timedelta(minutes=30)
            
            schedule.append({
                "date": d,
                "available_times": daily_available
            })
            
        return api_response({
            "location_id": location_id,
            "station_id": station_id,
            "schedule": schedule
        })

    # Route GET /locations/<location_id>/transactions (mengambil daftar transaksi yang sudah selesai untuk satu lokasi)
    @app.get("/locations/<location_id>/transactions")
    @require_manager
    def list_transactions_by_location(location_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        location_ref = db_client.collection("locations").document(location_id)
        location_snapshot = location_ref.get()
        if not location_snapshot.exists:
            return api_response(
                payload=None,
                status_code=404,
                message=f"Location '{location_id}' not found",
            )

        transactions: list[Dict[str, Any]] = []
        for tx_doc in location_ref.collection("transactions").stream():
            tx_data = tx_doc.to_dict() or {}
            transactions.append(
                {
                    "id": tx_doc.id,
                    **tx_data,
                }
            )

        return api_response(
            {
                "location_id": location_id,
                "transactions": transactions,
            }
        )

    # Route POST /payments/qris (membuat pembayaran QR langsung via Midtrans Core API /v2/charge)
    @app.post("/payments/qris")
    @require_customer
    def charge_qris_and_get_qr():
        core_api_client: Optional[midtransclient.CoreApi] = app.config.get(
            "MIDTRANS_CORE_API"
        )
        if core_api_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Midtrans Core API is not configured. Check your env vars.",
            )

        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        body = request.get_json(silent=True) or {}
        is_walkin = (body.get("type") == "walk-in")
        
        required_fields = [
            "payment_amount", "console_type", "customer_name", "username",
            "booking_duration", "location_id", "station_id"
        ]
        if not is_walkin:
            required_fields.extend(["booking_date", "booking_time"])
            
        missing = [f for f in required_fields if f not in body]
        if missing:
            return api_response(
                payload={"missing_fields": missing},
                status_code=400,
                message="Missing required fields",
            )

        try:
            payment_amount = int(body["payment_amount"])
            booking_duration = int(float(body["booking_duration"]))
        except (TypeError, ValueError):
            return api_response(
                payload={"field": "payment_amount/booking_duration"},
                status_code=400,
                message="payment_amount and booking_duration must be numbers",
            )

        location_id = str(body["location_id"])
        station_id = str(body["station_id"])
        location_snap = db_client.collection("locations").document(location_id).get()
        if not location_snap.exists:
            return api_response(None, 404, "Location not found")
            
        station_snap = db_client.collection("locations").document(location_id).collection("stations").document(station_id).get()
        if not station_snap.exists:
            return api_response(None, 404, "Station not found")
            
        loc_data = location_snap.to_dict() or {}
        open_time_str = loc_data.get("open_time", "09:00")
        close_time_str = loc_data.get("close_time", "22:00")
        
        try:
            if is_walkin:
                now_str = datetime.now(timezone(timedelta(hours=7))).strftime("%H:%M")
                start_dt = datetime.strptime(now_str, "%H:%M")
            else:
                start_dt = datetime.strptime(str(body.get("booking_time", "")).strip()[:5], "%H:%M")
                
            end_dt = start_dt + timedelta(minutes=booking_duration)
            
            open_dt = datetime.strptime(open_time_str, "%H:%M")
            close_dt = datetime.strptime(close_time_str, "%H:%M")
            if close_dt < open_dt:
                close_dt += timedelta(days=1)
                if start_dt < open_dt:
                    start_dt += timedelta(days=1)
                    end_dt += timedelta(days=1)
                    
            if start_dt < open_dt or end_dt > close_dt:
                return api_response(
                    payload=None, 
                    status_code=400, 
                    message=f"Booking time out of operational hours ({open_time_str} - {close_time_str})"
                )
        except Exception:
            return api_response(payload=None, status_code=400, message="Invalid booking time format")

        if payment_amount <= 0:
            return api_response(
                payload={"field": "payment_amount"},
                status_code=400,
                message="Amount must be greater than 0",
            )

        order_id = str(body.get("order_id") or generate_order_id())

        charge_params: Dict[str, Any] = {
            "payment_type": "qris",
            "transaction_details": {
                "order_id": order_id,
                "gross_amount": payment_amount,
            },
            "custom_expiry": {
                "expiry_duration": 5,
                "unit": "minute"
            }
        }

        if body.get("acquirer"):
            charge_params["qris"] = {"acquirer": body["acquirer"]}

        try:
            charge_response = core_api_client.charge(charge_params)
        except Exception as exc:  # pragma: no cover
            return api_response(
                payload={"detail": str(exc)},
                status_code=502,
                message="Failed to create Midtrans charge",
            )

        actions = charge_response.get("actions") or []
        qr_url = extract_qr_url(actions)

        payment_type = charge_response.get("payment_type", "qris")
        expiry_time = charge_response.get("expiry_time", "")

        tx_data: Dict[str, Any] = {
            "order_id": order_id,
            "payment_amount": payment_amount,
            "location_id": location_id,
            "station_id": station_id,
            "console_type": str(body["console_type"]),
            "customer_name": str(body["customer_name"]),
            "username": str(body["username"]),
            "booking_date": str(body.get("booking_date", "")),
            "booking_duration": booking_duration,
            "booking_time": str(body.get("booking_time", "")),
            "payment_status": "pending",
            "status": "ongoing",
            "type": "walk-in" if is_walkin else "booking",
            "qr_url": qr_url,
            "payment_type": payment_type,
            "expiry_time": expiry_time,
        }

        db_client.collection("locations").document(location_id).collection("transactions").document(order_id).set(tx_data, merge=True)

        return api_response(
            {
                "order_id": order_id,
                "payment_type": payment_type,
                "qr_url": qr_url,
                "expiry_time": expiry_time
            }
        )

    # Route POST /payments/qris/extend (memperpanjang waktu main untuk walk-in)
    @app.post("/payments/qris/extend")
    @require_customer
    def extend_playtime():
        core_api_client: Optional[midtransclient.CoreApi] = app.config.get("MIDTRANS_CORE_API")
        if core_api_client is None:
            return api_response(None, 500, "Midtrans Core API is not configured.")

        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        body = request.get_json(silent=True) or {}
        
        required_fields = ["parent_order_id", "payment_amount", "extension_duration"]
        missing = [f for f in required_fields if f not in body]
        if missing:
            return api_response({"missing_fields": missing}, 400, "Missing required fields")

        parent_order_id = str(body["parent_order_id"])
        
        # Validasi Transaksi Induk
        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", parent_order_id)).limit(1).stream()
        tx_docs = list(tx_query)
        if not tx_docs:
            return api_response(None, 404, "Parent Order ID not found")
            
        parent_data = tx_docs[0].to_dict() or {}
        if parent_data.get("status") not in ["ongoing", "upcoming"]:
            return api_response(None, 400, "Cannot extend closed or completed booking")
            
        try:
            payment_amount = int(body["payment_amount"])
            extension_duration = int(float(body["extension_duration"]))
        except (TypeError, ValueError):
            return api_response(None, 400, "payment_amount and extension_duration must be numbers")

        # Validasi batas jam buka tutup untuk perpanjangan sesi
        try:
            loc_id = parent_data.get("location_id")
            if loc_id:
                location_snap = db_client.collection("locations").document(loc_id).get()
                loc_data = location_snap.to_dict() or {}
                open_time_str = loc_data.get("open_time", "09:00")
                close_time_str = loc_data.get("close_time", "22:00")
    
                p_book_time = parent_data.get("booking_time", "")
                p_book_duration = int(float(parent_data.get("booking_duration", 0)))
                
                p_time_str = str(p_book_time).strip()
                if len(p_time_str) >= 8:
                    start_dt = datetime.strptime(f"{p_time_str[:8]}", "%H:%M:%S")
                else:
                    start_dt = datetime.strptime(f"{p_time_str[:5]}", "%H:%M")
                    
                end_dt = start_dt + timedelta(minutes=p_book_duration + extension_duration)
                
                open_dt = datetime.strptime(open_time_str, "%H:%M")
                close_dt = datetime.strptime(close_time_str, "%H:%M")
                
                if close_dt < open_dt:
                    close_dt += timedelta(days=1)
                    if start_dt < open_dt:
                        start_dt += timedelta(days=1)
                        end_dt += timedelta(days=1)
                        
                if end_dt > close_dt:
                    return api_response(None, 400, f"Cannot extend beyond operational hours ({close_time_str})")
        except Exception as e:
            return api_response(None, 400, f"Error validating operational hours: {str(e)}")

        # Generate Order ID dengan prefix EXT
        order_id = generate_order_id("EXT")

        charge_params: Dict[str, Any] = {
            "payment_type": "qris",
            "transaction_details": {
                "order_id": order_id,
                "gross_amount": payment_amount,
            },
            "custom_expiry": {
                "expiry_duration": 5,
                "unit": "minute"
            }
        }

        try:
            charge_response = core_api_client.charge(charge_params)
        except Exception as exc:
            return api_response({"detail": str(exc)}, 502, "Failed to create Midtrans extend charge")

        actions = charge_response.get("actions") or []
        qr_url = extract_qr_url(actions)
        payment_type = charge_response.get("payment_type", "qris")
        expiry_time = charge_response.get("expiry_time", "")

        tx_data: Dict[str, Any] = {
            "order_id": order_id,
            "parent_order_id": parent_order_id,
            "payment_amount": payment_amount,
            "booking_duration": extension_duration,
            "location_id": parent_data.get("location_id"),
            "station_id": parent_data.get("station_id"),
            "customer_name": parent_data.get("customer_name"),
            "username": parent_data.get("username"),
            "payment_status": "pending",
            "status": "ongoing", # Inherit active status state
            "type": "extend",
            "qr_url": qr_url,
            "payment_type": payment_type,
            "expiry_time": expiry_time,
        }

        db_client.collection("locations").document(parent_data.get("location_id")).collection("transactions").document(order_id).set(tx_data)

        return api_response({
            "order_id": order_id,
            "parent_order_id": parent_order_id,
            "payment_type": payment_type,
            "qr_url": qr_url,
            "expiry_time": expiry_time
        })

    # Route POST /payments/midtrans/notification (menerima HTTP notification dari Midtrans)
    @app.post("/payments/midtrans/notification")
    def midtrans_notification():
        notification = request.get_json(silent=True) or {}

        order_id = notification.get("order_id")
        transaction_status = notification.get("transaction_status")
        payment_type = notification.get("payment_type")
        fraud_status = notification.get("fraud_status")

        if not order_id:
            return api_response(
                payload=None,
                status_code=400,
                message="Missing order_id in notification payload",
            )

        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(
                payload=None,
                status_code=500,
                message="Firestore client is not initialized.",
            )

        payment_status = map_midtrans_status(transaction_status, fraud_status)

        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", order_id)).limit(1).stream()
        tx_docs = list(tx_query)
        if not tx_docs:
            return api_response(None, 404, "Transaction not found")
            
        tx_doc = tx_docs[0]
        tx_ref = tx_doc.reference
        tx_data = tx_doc.to_dict() or {}
        
        old_payment_status = tx_data.get("payment_status")

        update_data: Dict[str, Any] = {
            "payment_status": payment_status,
        }
        
        # Jika pembayaran kedaluwarsa atau gagal, tandai booking status menjadi cancelled
        if payment_status in ["expire", "failed", "cancel"]:
            update_data["status"] = "cancelled"

        tx_ref.set(update_data, merge=True)
        
        if payment_status in ["paid", "settlement"] and old_payment_status not in ["paid", "settlement"]:
            loc_id = tx_data.get("location_id")
            sta_id = tx_data.get("station_id")
            if loc_id and sta_id:
                tx_type = tx_data.get("type", "booking")
                wib = timezone(timedelta(hours=7))
                
                if tx_type == "extend":
                    parent_order_id = tx_data.get("parent_order_id")
                    if parent_order_id:
                        p_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", parent_order_id)).limit(1).stream()
                        p_docs = list(p_query)
                        if p_docs:
                            p_doc = p_docs[0]
                            p_data = p_doc.to_dict() or {}
                            p_ref = p_doc.reference
                            
                            old_duration_val = int(float(p_data.get("booking_duration", 0)))
                            ext_duration_val = int(float(tx_data.get("booking_duration", 0)))
                            new_duration_val = old_duration_val + ext_duration_val
                            
                            old_payment_val = int(float(p_data.get("payment_amount", 0)))
                            ext_payment_val = int(float(tx_data.get("payment_amount", 0)))
                            new_payment_val = old_payment_val + ext_payment_val
                            
                            # Update parent transaction document
                            p_ref.set({
                                "booking_duration": new_duration_val,
                                "payment_amount": new_payment_val
                            }, merge=True)
                            
                            p_book_date = p_data.get("booking_date")
                            p_book_time = p_data.get("booking_time")
                            
                            p_time_str = str(p_book_time).strip()
                            try:
                                if len(p_time_str) >= 8:
                                    start_dt = datetime.strptime(f"{p_book_date} {p_time_str[:8]}", "%Y-%m-%d %H:%M:%S")
                                else:
                                    start_dt = datetime.strptime(f"{p_book_date} {p_time_str[:5]}", "%Y-%m-%d %H:%M")
                                start_dt = start_dt.replace(tzinfo=wib)
                            except ValueError:
                                start_dt = datetime.now(wib)
                                
                            # TODO: Revert 'seconds' to 'minutes' after testing
                            new_end_dt = start_dt + timedelta(minutes=new_duration_val)
                            
                            # Update parent booking document
                            booking_ref = db_client.collection("locations").document(loc_id)\
                                .collection("stations").document(sta_id)\
                                .collection("bookings").document(parent_order_id)
                            booking_ref.set({"booking_duration": new_duration_val}, merge=True)
                            
                            # Proses perpanjangan sesi penyewaan melalui scheduler
                            scheduler = app.config.get("SCHEDULER")
                            if scheduler:
                                scheduler.add_job(finish_rental_event, 'date', run_date=new_end_dt, args=[parent_order_id], id=f"end_{parent_order_id}", replace_existing=True)
                                now_str = datetime.now(timezone(timedelta(hours=7))).strftime('%H:%M:%S')
                                print(f"[SCHEDULER {now_str}] ⏳ 🟢 Rental Extended: {parent_order_id} timer pushed back by {ext_duration_val}")
                                
                                # Edge Case if Parent was already completed magically during payment latency
                                if p_data.get("status") == "completed":
                                    p_ref.set({"status": "ongoing"}, merge=True)
                                    booking_ref.set({"status": "ongoing"}, merge=True)
                                    print(f"[SCHEDULER {now_str}] 🟢 Rental Re-Started: {parent_order_id} extended after completed")
                else:
                    if tx_type == "walk-in":
                        now_wib = datetime.now(wib)
                        book_date = now_wib.strftime("%Y-%m-%d")
                        book_time = now_wib.strftime("%H:%M:%S")
                        tx_ref.set({"booking_date": book_date, "booking_time": book_time}, merge=True)
                        start_dt = now_wib
                    else:
                        book_date = tx_data.get("booking_date")
                        book_time = tx_data.get("booking_time")
                        start_dt_str = f"{book_date} {str(book_time).strip()[:8]}"
                        try:
                            try:
                                start_dt = datetime.strptime(start_dt_str, "%Y-%m-%d %H:%M:%S")
                            except ValueError:
                                start_dt = datetime.strptime(f"{book_date} {str(book_time).strip()[:5]}", "%Y-%m-%d %H:%M")
                            start_dt = start_dt.replace(tzinfo=wib)
                        except ValueError:
                            start_dt = datetime.now(wib)
                    
                    duration_val = int(float(tx_data.get("booking_duration", 0)))
                    # TODO: Revert 'seconds' to 'minutes' after testing
                    end_dt = start_dt + timedelta(minutes=duration_val)
                    
                    now_wib = datetime.now(wib)
                    initial_status = "ongoing" if tx_type == "walk-in" or now_wib >= start_dt else "upcoming"
    
                    if tx_type != "walk-in" and initial_status == "upcoming":
                        tx_ref.set({"status": "upcoming"}, merge=True)
    
                    booking_data = {
                        "booking_date": book_date,
                        "booking_time": book_time,
                        "booking_duration": tx_data.get("booking_duration"),
                        "status": initial_status,
                        "type": tx_type,
                        "created_at": firestore.SERVER_TIMESTAMP,
                        "order_id": order_id,
                        "customer_name": tx_data.get("customer_name"),
                        "username": tx_data.get("username")
                    }
                    
                    db_client.collection("locations").document(loc_id)\
                        .collection("stations").document(sta_id)\
                        .collection("bookings").document(order_id).set(booking_data)
                    
                    # Proses penjadwalan untuk transaksi booking dan walk-in melalui scheduler
                    scheduler = app.config.get("SCHEDULER")
                    if scheduler:
                        if initial_status == "upcoming":
                            scheduler.add_job(start_rental_event, 'date', run_date=start_dt, args=[order_id], id=f"start_{order_id}", replace_existing=True)
                            print(f"[SCHEDULER {now_wib.strftime('%H:%M:%S')}] 📅 🔵 Rental Scheduled: {order_id} will start at {start_dt.strftime('%Y-%m-%d %H:%M:%S')} (Online Booking)")
                            
                        elif initial_status == "ongoing":
                            publish_mqtt_command(loc_id, sta_id, "ON")
                            db_client.collection("locations").document(loc_id).collection("stations").document(sta_id).set({"status": "occupied"}, merge=True)
                            print(f"[SCHEDULER {now_wib.strftime('%H:%M:%S')}] 🟢 Rental Started: {order_id} is now 'ongoing' (Walk-In Instant)")
                        
                        scheduler.add_job(finish_rental_event, 'date', run_date=end_dt, args=[order_id], id=f"end_{order_id}", replace_existing=True)

        gross_amount_str = notification.get("gross_amount", "0")
        try:
            payment_amount = int(float(gross_amount_str))
        except (ValueError, TypeError):
            payment_amount = 0

        return api_response(
            {
                "order_id": order_id,
                "payment_status": payment_status,
                "payment_amount": payment_amount,
            }
        )
    
    # Route GET /payments/status/<order_id> (untuk pengecekan status pembayaran)
    @app.get("/payments/status/<order_id>")
    @require_auth
    def get_payment_status(order_id: str):
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, "Firestore client is not initialized.")
            
        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("order_id", "==", order_id)).limit(1).stream()
        tx_docs = list(tx_query)
        
        if not tx_docs:
            return api_response(None, 404, "Transaction not found")
            
        tx_data = tx_docs[0].to_dict() or {}
        
        return api_response({
            "order_id": order_id,
            "payment_status": tx_data.get("payment_status", "unknown"),
            "status": tx_data.get("status", "unknown"),
            "payment_amount": tx_data.get("payment_amount", 0)
        })

    # Route GET /user/bookings (untuk mengambil riwayat transaksi customer)
    @app.get("/user/bookings")
    @require_auth
    def get_user_bookings():
        db_client: Optional[firestore.Client] = app.config.get("FIRESTORE_DB")
        if db_client is None:
            return api_response(None, 500, "Firestore client is not initialized.")
            
        username = str(request.headers.get("X-Username")).strip()
        
        # Kita mencari transaksi dimana username sama persis dengan yang meminta API
        tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("username", "==", username)).stream()
        
        results = []
        for tx in tx_query:
            t_data = tx.to_dict() or {}
            
            # Kalkulasi price_per_hour dari payment_amount dan booking_duration
            try:
                payment_amount = float(t_data.get("payment_amount", 0))
                booking_duration = float(t_data.get("booking_duration", 0))
                if booking_duration > 0:
                    price_per_hour = round((payment_amount / booking_duration) * 60)
                else:
                    price_per_hour = 0
            except (TypeError, ValueError):
                price_per_hour = 0
            
            t_data["price_per_hour"] = price_per_hour
            results.append(t_data)
            
        # Mengurutkan dari yang terbaru (bisa diurutkan dari booking_date jika butuh)
        # Pada python kita urutkan secara terbalik (karena kita tidak pakai Firestore index descending agar tidak error)
        results.reverse()
        
        return api_response(results)
        
    # Fungsi recover_schedules untuk recovery scheduler saat server restart
    def recover_schedules():
        import time
        # Tunggu sedikit agar proses startup Flask sempurna sebelum nge-query Firestore
        time.sleep(3)
        db_client = app.config.get("FIRESTORE_DB")
        scheduler = app.config.get("SCHEDULER")
        if not db_client or not scheduler: return
        
        wib = timezone(timedelta(hours=7))
        now_wib = datetime.now(wib)
        
        try:
            tx_query = db_client.collection_group("transactions").where(filter=FieldFilter("payment_status", "in", ["paid", "settlement"])).stream()
            for tx_doc in tx_query:
                tx_data = tx_doc.to_dict() or {}
                status = tx_data.get("status")
                if status == "completed":
                    continue
                    
                order_id = tx_data.get("order_id")
                book_date = tx_data.get("booking_date")
                book_time = tx_data.get("booking_time")
                duration_raw = tx_data.get("booking_duration")
                
                if not order_id or not book_date or not book_time or duration_raw is None:
                    continue
                    
                try:
                    duration_val = int(float(duration_raw))
                    
                    p_time_str = str(book_time).strip()
                    try:
                        if len(p_time_str) >= 8:
                            start_dt = datetime.strptime(f"{book_date} {p_time_str[:8]}", "%Y-%m-%d %H:%M:%S")
                        else:
                            start_dt = datetime.strptime(f"{book_date} {p_time_str[:5]}", "%Y-%m-%d %H:%M")
                        start_dt = start_dt.replace(tzinfo=wib)
                    except ValueError:
                        start_dt = datetime.now(wib)
                        
                    # TODO: Revert 'seconds' to 'minutes' after testing
                    end_dt = start_dt + timedelta(minutes=duration_val)
                    
                    if now_wib >= end_dt:
                        finish_rental_event(order_id)
                    elif now_wib >= start_dt:
                        if status != "ongoing":
                            start_rental_event(order_id)
                        scheduler.add_job(finish_rental_event, 'date', run_date=end_dt, args=[order_id], id=f"end_{order_id}", replace_existing=True)
                    else:
                        if status != "upcoming":
                            tx_doc.reference.set({"status": "upcoming"}, merge=True)
                        scheduler.add_job(start_rental_event, 'date', run_date=start_dt, args=[order_id], id=f"start_{order_id}", replace_existing=True)
                        print(f"[RECOVERY {now_wib.strftime('%H:%M:%S')}] 📅 🔵 Scheduled: {order_id} loaded into memory. Starts at {start_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                        scheduler.add_job(finish_rental_event, 'date', run_date=end_dt, args=[order_id], id=f"end_{order_id}", replace_existing=True)
                except Exception as e:
                    print(f"[RECOVERY ERROR] Order {order_id}: {e}")
        except Exception as e:
            print(f"[RECOVERY CRASH] {e}")

    threading.Thread(target=recover_schedules, daemon=True).start()

    return app

# MAIN PROGRAM
if __name__ == "__main__":
    flask_app = create_app()
    if "PORT" not in os.environ:
        raise RuntimeError('Missing required env var PORT. Set it in your ".env" file.')

    flask_app.run(host="0.0.0.0", port=int(os.environ["PORT"]), debug=True)