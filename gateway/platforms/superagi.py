"""SuperAGI gateway adapter.

Connects to the SuperAGI messaging system via MQTT (paho-mqtt over WebSockets)
for receiving messages and the SuperAGI REST API for sending messages, typing
indicators, and fetching message content.

Environment variables:
    SUPERAGI_API_KEY            X-API-KEY header for REST API (primary workspace)
    SUPERAGI_API_BASE_URL       REST API base (e.g. https://api.superagi.com)
    SUPERAGI_BOT_USER_ID        Numeric bot user ID
    SUPERAGI_WORKSPACE_ID       Numeric workspace ID (primary)
    SUPERAGI_MQTT_URL           MQTT broker URL (ws://emqx.superagi.com:8083/mqtt)
    SUPERAGI_MQTT_USERNAME      MQTT username (default: supersales)
    SUPERAGI_MQTT_PASSWORD      MQTT password
    SUPERAGI_ENV                Environment name (default: production)
    SUPERAGI_HOME_CHANNEL       Default group_id for cron delivery
    SUPERAGI_ALLOWED_USERS      Comma-separated allowed user IDs
    SUPERAGI_ALLOW_ALL_USERS    Allow all users (true/false)

    Cross-workspace twin sharing (new):
    SUPERAGI_SUBSCRIPTIONS      JSON array of {workspace_id, group_id, api_key,
                                api_base_url} tuples — one per (workspace, group)
                                pair this sandbox should handle. Replaces the old
                                comma-separated SUPERAGI_ENABLED_GROUPS format.
                                On startup, subscriptions are also loaded from
                                /workspace/.subscribed-groups.json (persisted
                                across pause/resume). New subscriptions received
                                via POST /admin/subscribe-group are merged in
                                and persisted immediately.
    SUPERAGI_ADMIN_PORT         Port for the admin HTTP server (default: 8001).
                                Exposes POST /admin/subscribe-group for runtime
                                subscription management.
    SANDBOX_ID                  Sandbox identifier (injected by claw-service),
                                used for sandbox-service heartbeat calls.
    SANDBOX_SERVICE_URL         Internal URL of the sandbox service
                                (e.g. http://sandbox-service:8111), used for
                                heartbeat calls so the lifecycle worker doesn't
                                auto-pause actively-used sandboxes.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, Optional

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 4000

_RECONNECT_BASE_DELAY = 2.0
_RECONNECT_MAX_DELAY = 60.0
_RECONNECT_JITTER = 0.3


def check_superagi_requirements() -> bool:
    """Return True if the SuperAGI adapter can be used.

    Only API_KEY, BOT_USER_ID, WORKSPACE_ID, and MQTT_PASSWORD are strictly
    required.  API_BASE_URL and MQTT_URL have sensible defaults matching the
    standard SuperAGI deployment.
    """
    required = {
        "SUPERAGI_API_KEY": os.getenv("SUPERAGI_API_KEY", ""),
        "SUPERAGI_BOT_USER_ID": os.getenv("SUPERAGI_BOT_USER_ID", ""),
        "SUPERAGI_WORKSPACE_ID": os.getenv("SUPERAGI_WORKSPACE_ID", ""),
        "SUPERAGI_MQTT_PASSWORD": os.getenv("SUPERAGI_MQTT_PASSWORD", ""),
    }
    for var, val in required.items():
        if not val:
            logger.debug("SuperAGI: %s not set", var)
            return False

    try:
        import paho.mqtt.client  # noqa: F401
        return True
    except ImportError:
        logger.warning("SuperAGI: paho-mqtt not installed. Run: pip install paho-mqtt")
        return False


def _redact_api_key(key: str) -> str:
    if not key or len(key) < 10:
        return "***"
    return key[:4] + "..." + key[-4:]


def _to_int(val: Any) -> int:
    """Coerce a value to int, returning 0 on failure."""
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


class SuperAGIAdapter(BasePlatformAdapter):
    """Gateway adapter for SuperAGI messaging (MQTT + REST API)."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.SUPERAGI)

        self._api_key: str = (
            config.extra.get("api_key", "")
            or os.getenv("SUPERAGI_API_KEY", "")
        )
        self._api_base_url: str = (
            config.extra.get("api_base_url", "")
            or os.getenv("SUPERAGI_API_BASE_URL", "https://api.superagi.com/chat")
        ).rstrip("/")
        self._bot_user_id: int = _to_int(
            config.extra.get("bot_user_id", "")
            or os.getenv("SUPERAGI_BOT_USER_ID", "")
        )
        self._workspace_id: int = _to_int(
            config.extra.get("workspace_id", "")
            or os.getenv("SUPERAGI_WORKSPACE_ID", "")
        )
        self._mqtt_url: str = (
            config.extra.get("mqtt_url", "")
            or os.getenv("SUPERAGI_MQTT_URL", "ws://emqx.superagi.com:8083/mqtt")
        )
        self._mqtt_username: str = (
            config.extra.get("mqtt_username", "")
            or os.getenv("SUPERAGI_MQTT_USERNAME", "supersales")
        )
        self._mqtt_password: str = (
            config.extra.get("mqtt_password", "")
            or os.getenv("SUPERAGI_MQTT_PASSWORD", "")
        )
        self._env: str = (
            config.extra.get("env", "")
            or os.getenv("SUPERAGI_ENV", "production")
        )

        # ── Cross-workspace subscription map ──────────────────────────────
        # Each entry: {workspace_id, group_id, api_key, api_base_url}
        # Indexed by group_id (int) for O(1) per-message lookup.
        self._subscription_map: Dict[int, Dict[str, Any]] = {}

        # 1. Parse SUPERAGI_SUBSCRIPTIONS JSON (new format)
        _subs_raw = (
            config.extra.get("subscriptions", "")
            or os.getenv("SUPERAGI_SUBSCRIPTIONS", "")
        )
        if _subs_raw:
            try:
                subs = json.loads(_subs_raw)
                for sub in subs:
                    gid = _to_int(sub.get("group_id", 0))
                    if gid:
                        self._subscription_map[gid] = sub
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("SuperAGI: failed to parse SUPERAGI_SUBSCRIPTIONS: %s", e)

        # 2. Merge persisted subscriptions from /workspace/.subscribed-groups.json
        #    (survive pause/resume; written by _persist_subscriptions())
        _persist_path = os.getenv("SUPERAGI_SUBSCRIPTIONS_PATH", "/workspace/.subscribed-groups.json")
        self._subscriptions_persist_path = _persist_path
        try:
            if os.path.exists(_persist_path):
                with open(_persist_path, encoding="utf-8") as _f:
                    _persisted = json.load(_f)
                for sub in _persisted:
                    gid = _to_int(sub.get("group_id", 0))
                    if gid and gid not in self._subscription_map:
                        self._subscription_map[gid] = sub
                if _persisted:
                    logger.info(
                        "SuperAGI: loaded %d persisted subscriptions from %s",
                        len(self._subscription_map), _persist_path,
                    )
        except Exception as e:
            logger.warning("SuperAGI: could not load persisted subscriptions: %s", e)

        # 3. Fallback: old SUPERAGI_ENABLED_GROUPS comma-separated format
        #    (only used when no SUBSCRIPTIONS were provided — single-workspace case)
        if not self._subscription_map:
            _enabled_groups_raw = (
                config.extra.get("enabled_groups", "")
                or os.getenv("SUPERAGI_ENABLED_GROUPS", "")
            )
            if _enabled_groups_raw:
                for _gid_str in str(_enabled_groups_raw).split(","):
                    _gid_str = _gid_str.strip()
                    if _gid_str:
                        _gid = _to_int(_gid_str)
                        if _gid:
                            # Legacy: use primary workspace credentials for all groups
                            self._subscription_map[_gid] = {
                                "workspace_id": self._workspace_id,
                                "group_id": _gid,
                                "api_key": self._api_key,
                                "api_base_url": self._api_base_url,
                            }

        # Enabled groups set (derived from map) — for fast membership checks
        self._enabled_groups: set[int] = set(self._subscription_map.keys())
        if self._enabled_groups:
            logger.info("SuperAGI: enabled groups filter active: %s", self._enabled_groups)

        # Admin HTTP server config
        self._admin_port: int = int(os.getenv("SUPERAGI_ADMIN_PORT", "8001"))

        # Sandbox heartbeat config (prevents auto-pause during active use)
        self._sandbox_id: str = os.getenv("SANDBOX_ID", "")
        self._sandbox_service_url: str = os.getenv("SANDBOX_SERVICE_URL", "").rstrip("/")

        self._mqtt_client: Any = None
        self._event_loop: Any = None  # captured during connect() for cross-thread dispatch
        self._closing = False

        # httpx.AsyncClient — created in connect()
        self._http_session: Any = None

        # Dedup: message_id -> monotonic timestamp
        self._seen_messages: Dict[str, float] = {}
        self._SEEN_MAX = 2000
        self._SEEN_TTL = 300  # 5 minutes

        # Track message IDs sent by this adapter (for self-message loop prevention)
        self._sent_message_ids: Dict[str, float] = {}
        self._SENT_MAX = 500
        self._SENT_TTL = 300  # 5 minutes

        # Fallback: track sent content hashes when API doesn't return message IDs
        self._sent_content_hashes: Dict[str, float] = {}
        self._SENT_HASH_TTL = 60  # 60 seconds — short window to match echoed replies

        self._subscribed_groups: set[int] = set()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _api_headers(self, api_key: Optional[str] = None, workspace_id: Optional[int] = None) -> Dict[str, str]:
        headers = {
            "X-API-KEY": api_key if api_key is not None else self._api_key,
            "Content-Type": "application/json",
        }
        ws = workspace_id if workspace_id is not None else self._workspace_id
        if ws:
            headers["X-Workspace-Id"] = str(ws)
        if self._bot_user_id:
            headers["X-USER-ID"] = str(self._bot_user_id)
        return headers

    def _credentials_for_group(self, group_id: int) -> tuple[str, str, int]:
        """Return (api_key, api_base_url, workspace_id) for the given group.

        Falls back to the primary credentials when the group has no dedicated
        subscription entry (e.g. non-twin / legacy single-workspace path).
        """
        sub = self._subscription_map.get(group_id)
        if sub:
            return (
                sub.get("api_key", self._api_key),
                sub.get("api_base_url", self._api_base_url).rstrip("/"),
                _to_int(sub.get("workspace_id", self._workspace_id)),
            )
        return self._api_key, self._api_base_url, self._workspace_id

    async def _api_get(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        url = f"{self._api_base_url}{path}"
        try:
            resp = await self._http_session.get(
                url, headers=self._api_headers(), params=params
            )
            if resp.status_code >= 400:
                text = resp.text[:200]
                logger.warning("SuperAGI API GET %s -> %d: %s", path, resp.status_code, text)
                return {"error": f"HTTP {resp.status_code}: {text}"}
            return resp.json()
        except Exception as e:
            logger.error("SuperAGI API GET %s failed: %s", path, e)
            return {"error": str(e)}

    async def _api_get_for_group(self, group_id: int, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        api_key, api_base_url, workspace_id = self._credentials_for_group(group_id)
        url = f"{api_base_url}{path}"
        try:
            resp = await self._http_session.get(
                url, headers=self._api_headers(api_key, workspace_id), params=params
            )
            if resp.status_code >= 400:
                text = resp.text[:200]
                logger.warning("SuperAGI API GET %s -> %d: %s", path, resp.status_code, text)
                return {"error": f"HTTP {resp.status_code}: {text}"}
            return resp.json()
        except Exception as e:
            logger.error("SuperAGI API GET %s failed: %s", path, e)
            return {"error": str(e)}

    async def _api_post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self._api_base_url}{path}"
        try:
            resp = await self._http_session.post(
                url, headers=self._api_headers(), json=body
            )
            if resp.status_code >= 400:
                text = resp.text[:200]
                logger.warning("SuperAGI API POST %s -> %d: %s", path, resp.status_code, text)
                return {"error": f"HTTP {resp.status_code}: {text}"}
            return resp.json()
        except Exception as e:
            logger.error("SuperAGI API POST %s failed: %s", path, e)
            return {"error": str(e)}

    async def _api_post_for_group(self, group_id: int, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        api_key, api_base_url, workspace_id = self._credentials_for_group(group_id)
        url = f"{api_base_url}{path}"
        try:
            resp = await self._http_session.post(
                url, headers=self._api_headers(api_key, workspace_id), json=body
            )
            if resp.status_code >= 400:
                text = resp.text[:200]
                logger.warning("SuperAGI API POST %s -> %d: %s", path, resp.status_code, text)
                return {"error": f"HTTP {resp.status_code}: {text}"}
            return resp.json()
        except Exception as e:
            logger.error("SuperAGI API POST %s failed: %s", path, e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # MQTT topic helpers
    # ------------------------------------------------------------------

    def _sidebar_topic(self) -> str:
        return f"{self._env}/workspace/{self._workspace_id}/user/{self._bot_user_id}/sidebar"

    def _group_topic(self, group_id: int) -> str:
        sub = self._subscription_map.get(group_id)
        ws_id = _to_int(sub["workspace_id"]) if sub and "workspace_id" in sub else self._workspace_id
        return f"{self._env}/workspace/{ws_id}/group/{group_id}/messages"

    # ------------------------------------------------------------------
    # Connect / disconnect
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        import httpx
        import paho.mqtt.client as mqtt

        logger.info(
            "SuperAGI: connecting (bot_user_id=%s, workspace=%s, api=%s)",
            self._bot_user_id,
            self._workspace_id,
            _redact_api_key(self._api_key),
        )

        self._event_loop = asyncio.get_running_loop()
        self._http_session = httpx.AsyncClient(timeout=30.0, follow_redirects=True)

        mqtt_host, mqtt_port = self._parse_mqtt_url(self._mqtt_url)
        logger.info(
            "SuperAGI: parsed MQTT URL %s -> host=%s port=%d",
            self._mqtt_url, mqtt_host, mqtt_port,
        )
        if not mqtt_host:
            logger.error("SuperAGI: cannot parse MQTT URL: %s", self._mqtt_url)
            self._set_fatal_error(
                "MQTT_URL_INVALID",
                f"Cannot parse MQTT URL: {self._mqtt_url}",
                retryable=False,
            )
            return False

        client_id = f"hermes-superagi-{self._bot_user_id}-{int(time.time())}"
        logger.info("SuperAGI: creating MQTT client id=%s transport=websockets", client_id)

        # paho-mqtt v2 requires callback_api_version; v1 does not have it
        client_kwargs: dict[str, Any] = {
            "client_id": client_id,
            "transport": "websockets",
            "protocol": mqtt.MQTTv311,
        }
        if hasattr(mqtt, "CallbackAPIVersion"):
            client_kwargs["callback_api_version"] = mqtt.CallbackAPIVersion.VERSION1
        self._mqtt_client = mqtt.Client(**client_kwargs)
        self._mqtt_client.username_pw_set(self._mqtt_username, self._mqtt_password)
        self._mqtt_client.ws_set_options(path="/mqtt")

        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_message = self._on_mqtt_message
        self._mqtt_client.on_disconnect = self._on_mqtt_disconnect

        try:
            logger.info("SuperAGI: connecting MQTT to %s:%d ...", mqtt_host, mqtt_port)
            self._mqtt_client.connect(mqtt_host, mqtt_port, keepalive=60)
            self._mqtt_client.loop_start()
        except Exception as e:
            logger.error("SuperAGI: MQTT connection failed: %s", e)
            self._set_fatal_error("MQTT_CONNECT_FAILED", str(e), retryable=True)
            return False

        for _ in range(50):  # 5 seconds max
            if self._mqtt_client.is_connected():
                break
            await asyncio.sleep(0.1)

        if not self._mqtt_client.is_connected():
            logger.error("SuperAGI: MQTT connection timed out")
            self._set_fatal_error(
                "MQTT_CONNECT_TIMEOUT",
                "MQTT connection timed out after 5 seconds",
                retryable=True,
            )
            return False

        self._mark_connected()
        logger.info("SuperAGI: connected successfully")

        # Subscribe to all pre-configured (workspace, group) topic pairs.
        # Each subscription uses its own workspace_id so cross-workspace twins
        # get the right MQTT topic (e.g. workspace/2/group/200/messages).
        for gid, sub in self._subscription_map.items():
            if gid not in self._subscribed_groups:
                ws_id = _to_int(sub.get("workspace_id", self._workspace_id))
                topic = f"{self._env}/workspace/{ws_id}/group/{gid}/messages"
                self._mqtt_client.subscribe(topic, qos=1)
                self._subscribed_groups.add(gid)
                logger.info("SuperAGI: subscribed to group topic: %s", topic)

        # Start the lightweight admin HTTP server for runtime subscription management
        # (POST /admin/subscribe-group). Uses aiohttp; runs in the current event loop.
        asyncio.ensure_future(self._start_admin_server())

        return True

    @staticmethod
    def _parse_mqtt_url(url: str) -> tuple[str, int]:
        """Parse ws://host:port/path into (host, port)."""
        import re as _re
        match = _re.match(r"wss?://([^/:]+):?(\d+)?", url)
        if not match:
            return "", 0
        host = match.group(1)
        port = int(match.group(2)) if match.group(2) else 8083
        return host, port

    async def disconnect(self) -> None:
        self._closing = True

        if self._mqtt_client:
            try:
                self._mqtt_client.loop_stop()
                self._mqtt_client.disconnect()
            except Exception as e:
                logger.debug("SuperAGI: MQTT disconnect error: %s", e)
            self._mqtt_client = None

        if self._http_session:
            try:
                await self._http_session.aclose()
            except Exception:
                pass
            self._http_session = None

        self._mark_disconnected()
        logger.info("SuperAGI: disconnected")

    # ------------------------------------------------------------------
    # MQTT callbacks (run in paho's background thread)
    # ------------------------------------------------------------------

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = self._sidebar_topic()
            logger.info("SuperAGI: MQTT connected (rc=0), subscribing to: %s", topic)
            client.subscribe(topic, qos=1)
        else:
            rc_names = {1: "bad protocol", 2: "client-id rejected", 3: "server unavailable",
                        4: "bad credentials", 5: "not authorized"}
            reason = rc_names.get(rc, "unknown")
            logger.error("SuperAGI: MQTT connect FAILED rc=%d (%s)", rc, reason)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        if self._closing:
            return
        logger.warning("SuperAGI: MQTT disconnected (rc=%d), paho will auto-reconnect", rc)

    def _on_mqtt_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.warning("SuperAGI: invalid MQTT payload on %s: %s", msg.topic, e)
            return

        event_type = payload.get("event")
        logger.info(
            "SuperAGI: MQTT message on %s event=%s sender=%s group=%s msg_id=%s",
            msg.topic,
            event_type,
            payload.get("sender_id", payload.get("user_id", "?")),
            payload.get("group_id", "?"),
            payload.get("message_id", "?"),
        )

        if event_type == "SIDEBAR_NEW_MESSAGE":
            asyncio.run_coroutine_threadsafe(
                self._handle_sidebar_event(payload),
                self._event_loop,
            )
        elif event_type == "NEW_MESSAGE":
            asyncio.run_coroutine_threadsafe(
                self._handle_group_message_event(payload),
                self._event_loop,
            )
        else:
            logger.debug("SuperAGI: ignoring MQTT event type: %s", event_type)

    # ------------------------------------------------------------------
    # Event handlers (async, dispatched from MQTT thread)
    # ------------------------------------------------------------------

    async def _handle_sidebar_event(self, payload: Dict[str, Any]) -> None:
        """Handle a SIDEBAR_NEW_MESSAGE event.

        Fields are at the top level of the MQTT payload (not nested under "data").
        """
        sender_id = _to_int(payload.get("sender_id", 0))
        group_id = _to_int(payload.get("group_id", 0))
        message_id = str(payload.get("message_id", ""))

        # Enabled groups filter — if set, only process messages from listed groups
        if self._enabled_groups and group_id not in self._enabled_groups:
            logger.debug("SuperAGI: skipping sidebar message from non-enabled group=%s", group_id)
            return

        # Self-message check: use sent message tracking if enabled groups active (twin mode)
        # Otherwise use sender_id check (regular digital employee mode)
        if self._enabled_groups:
            if self._is_sent_by_self(message_id):
                logger.debug("SuperAGI: skipping own sent message_id=%s", message_id)
                return
        else:
            if sender_id == self._bot_user_id:
                logger.debug("SuperAGI: skipping own sidebar message (sender=%s)", sender_id)
                return

        if not self._check_and_mark_seen(message_id):
            logger.debug("SuperAGI: dedup skip message_id=%s", message_id)
            return

        logger.info(
            "SuperAGI: handling SIDEBAR_NEW_MESSAGE sender=%s group=%s msg_id=%s",
            sender_id, group_id, message_id,
        )

        # Auto-subscribe to group topic for future messages
        if group_id and group_id not in self._subscribed_groups:
            topic = self._group_topic(group_id)
            self._mqtt_client.subscribe(topic, qos=1)
            self._subscribed_groups.add(group_id)
            logger.info("SuperAGI: subscribed to group topic: %s", topic)

        content = await self._fetch_message_content(group_id, message_id)
        if not content:
            logger.warning("SuperAGI: no content for message_id=%s group=%s", message_id, group_id)
            return

        text = content.get("text", content.get("content", content.get("message", ""))).strip()
        if not text:
            logger.debug("SuperAGI: empty text in message_id=%s, skipping", message_id)
            return

        # Content-hash self-message check (fallback when API doesn't return message IDs)
        chat_id_str = str(group_id) if group_id else str(sender_id)
        if self._is_content_sent_by_self(chat_id_str, text):
            logger.debug("SuperAGI: skipping own message by content hash (group=%s)", group_id)
            return

        sender_name = content.get("sender_name", content.get("user_name", ""))
        logger.info(
            "SuperAGI: received message from %s (%s): \"%s\"",
            sender_name or sender_id, f"group={group_id}" if group_id else "dm",
            text[:80] + ("..." if len(text) > 80 else ""),
        )

        source = self.build_source(
            chat_id=chat_id_str,
            chat_name=content.get("group_name", sender_name),
            chat_type="group" if group_id else "dm",
            user_id=str(sender_id),
            user_name=sender_name,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
        )
        await self.handle_message(event)

    async def _handle_group_message_event(self, payload: Dict[str, Any]) -> None:
        """Handle a NEW_MESSAGE event from a subscribed group.

        Fields are at the top level of the MQTT payload.
        """
        user_id = _to_int(payload.get("user_id", 0))
        group_id = _to_int(payload.get("group_id", 0))
        message_id = str(payload.get("message_id", ""))

        # Enabled groups filter
        if self._enabled_groups and group_id not in self._enabled_groups:
            logger.debug("SuperAGI: skipping group message from non-enabled group=%s", group_id)
            return

        # Self-message check
        if self._enabled_groups:
            if self._is_sent_by_self(message_id):
                logger.debug("SuperAGI: skipping own sent group message_id=%s", message_id)
                return
        else:
            if user_id == self._bot_user_id:
                logger.debug("SuperAGI: skipping own group message (user=%s)", user_id)
                return

        if not self._check_and_mark_seen(message_id):
            logger.debug("SuperAGI: dedup skip message_id=%s", message_id)
            return

        logger.info(
            "SuperAGI: handling NEW_MESSAGE user=%s group=%s msg_id=%s",
            user_id, group_id, message_id,
        )

        content = await self._fetch_message_content(group_id, message_id)
        if not content:
            logger.warning("SuperAGI: no content for message_id=%s group=%s", message_id, group_id)
            return

        text = content.get("text", content.get("content", content.get("message", ""))).strip()
        if not text:
            logger.debug("SuperAGI: empty text in message_id=%s, skipping", message_id)
            return

        # Content-hash self-message check (fallback when API doesn't return message IDs)
        if self._is_content_sent_by_self(str(group_id), text):
            logger.debug("SuperAGI: skipping own group message by content hash (group=%s)", group_id)
            return

        sender_name = content.get("sender_name", content.get("user_name", ""))
        logger.info(
            "SuperAGI: received group message from %s (group=%s): \"%s\"",
            sender_name or user_id, group_id,
            text[:80] + ("..." if len(text) > 80 else ""),
        )

        source = self.build_source(
            chat_id=str(group_id),
            chat_name=content.get("group_name", ""),
            chat_type="group",
            user_id=str(user_id),
            user_name=sender_name,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=message_id,
        )
        await self.handle_message(event)

    async def _fetch_message_content(
        self, group_id: int, message_id: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch full message content from REST API by ID.

        Uses per-group credentials when available so cross-workspace twins
        fetch messages from the correct workspace's API endpoint.
        """
        if not message_id:
            return None

        params: Dict[str, Any] = {}
        if group_id:
            params["group_id"] = group_id

        logger.debug("SuperAGI: fetching message content id=%s group=%s", message_id, group_id)
        result = await self._api_get_for_group(
            group_id, f"/v1/messages/{message_id}", params=params or None
        )
        if "error" in result:
            logger.warning(
                "SuperAGI: failed to fetch message %s: %s", message_id, result["error"]
            )
            return None

        # Unwrap nested response if present
        msg = result.get("message", result.get("data", result))
        logger.debug("SuperAGI: fetched message content keys=%s", list((msg if isinstance(msg, dict) else result).keys()))
        return msg if isinstance(msg, dict) else result

    # ------------------------------------------------------------------
    # Dedup helpers
    # ------------------------------------------------------------------

    def _check_and_mark_seen(self, message_id: str) -> bool:
        """Return True if message_id is new (not seen before), and mark it seen."""
        if not message_id:
            return True
        now = time.monotonic()
        if message_id in self._seen_messages:
            return False
        if len(self._seen_messages) > self._SEEN_MAX:
            cutoff = now - self._SEEN_TTL
            self._seen_messages = {
                k: v for k, v in self._seen_messages.items() if v > cutoff
            }
        self._seen_messages[message_id] = now
        return True

    # ------------------------------------------------------------------
    # Sent message tracking (for self-message loop prevention in twin mode)
    # ------------------------------------------------------------------

    def _mark_as_sent(self, message_id: str) -> None:
        """Track a message_id that we sent, so we can skip it on MQTT echo."""
        if not message_id:
            return
        now = time.monotonic()
        if len(self._sent_message_ids) > self._SENT_MAX:
            cutoff = now - self._SENT_TTL
            self._sent_message_ids = {
                k: v for k, v in self._sent_message_ids.items() if v > cutoff
            }
        self._sent_message_ids[message_id] = now

    def _is_sent_by_self(self, message_id: str) -> bool:
        """Return True if this message_id was sent by this adapter."""
        if not message_id:
            return False
        return message_id in self._sent_message_ids

    def _mark_content_sent(self, chat_id: str, content: str) -> None:
        """Track a content hash so we can detect our own messages echoed back via MQTT."""
        import hashlib
        key = f"{chat_id}:{hashlib.sha256(content.strip().encode()).hexdigest()[:16]}"
        now = time.monotonic()
        # Evict stale entries
        cutoff = now - self._SENT_HASH_TTL
        if len(self._sent_content_hashes) > self._SENT_MAX:
            self._sent_content_hashes = {
                k: v for k, v in self._sent_content_hashes.items() if v > cutoff
            }
        self._sent_content_hashes[key] = now

    def _is_content_sent_by_self(self, chat_id: str, content: str) -> bool:
        """Return True if we recently sent this exact content to this chat."""
        import hashlib
        key = f"{chat_id}:{hashlib.sha256(content.strip().encode()).hexdigest()[:16]}"
        ts = self._sent_content_hashes.get(key)
        if ts is None:
            return False
        if time.monotonic() - ts > self._SENT_HASH_TTL:
            del self._sent_content_hashes[key]
            return False
        # Remove after matching to allow the same content to be sent again later
        del self._sent_content_hashes[key]
        return True

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message via the SuperAGI REST API.

        Uses per-group credentials (api_key, api_base_url, workspace_id) when
        the group has a subscription entry — enabling cross-workspace twin
        sharing where each workspace gets its own API key on outbound calls.
        Falls back to primary credentials for non-twin / single-workspace use.
        """
        if not content or not content.strip():
            return SendResult(success=True)

        group_id = _to_int(chat_id)

        body = {
            "recipient_type": 1,
            "recipient_id": group_id,
            "text": content,
            "message_type": "twin_response",
        }

        logger.info(
            "SuperAGI: sending message to chat_id=%s (%d chars)",
            chat_id, len(content),
        )
        result = await self._api_post_for_group(group_id, "/v1/messages", body)
        if "error" in result:
            logger.error("SuperAGI: send failed to chat_id=%s: %s", chat_id, result["error"])
            is_network = any(
                kw in result["error"].lower()
                for kw in ("timeout", "connect", "network")
            )
            return SendResult(
                success=False,
                error=result["error"],
                retryable=is_network,
            )

        msg_id = result.get("id") or result.get("message_id")
        if msg_id:
            self._mark_as_sent(str(msg_id))
        else:
            # API didn't return message ID — track by content hash as fallback
            self._mark_content_sent(chat_id, content)
        logger.info("SuperAGI: message sent to chat_id=%s msg_id=%s", chat_id, msg_id)

        # Best-effort heartbeat: keep the sandbox alive while messages are flowing
        asyncio.ensure_future(self._heartbeat_sandbox())

        return SendResult(success=True, message_id=str(msg_id) if msg_id else None)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a typing indicator via the SuperAGI REST API.

        Uses per-group credentials so cross-workspace twins post typing events
        to the correct workspace's API endpoint.
        """
        try:
            group_id = _to_int(chat_id)
            await self._api_post_for_group(
                group_id, "/v1/realtime/typing", {"group_id": group_id}
            )
        except Exception as e:
            logger.debug("SuperAGI: typing indicator failed: %s", e)

    # ------------------------------------------------------------------
    # Subscription management (runtime + persistence)
    # ------------------------------------------------------------------

    def _persist_subscriptions(self) -> None:
        """Write current subscription map to /workspace/.subscribed-groups.json.

        The PVC at /workspace survives pause/resume, so subscriptions added at
        runtime (via POST /admin/subscribe-group) are available again after the
        pod is woken up.
        """
        try:
            subs = list(self._subscription_map.values())
            with open(self._subscriptions_persist_path, "w", encoding="utf-8") as f:
                json.dump(subs, f, indent=2)
            logger.debug(
                "SuperAGI: persisted %d subscriptions to %s",
                len(subs), self._subscriptions_persist_path,
            )
        except Exception as e:
            logger.warning("SuperAGI: failed to persist subscriptions: %s", e)

    async def _add_subscription(self, sub: Dict[str, Any]) -> None:
        """Add a subscription at runtime and subscribe to the MQTT topic.

        Called by the admin HTTP server when the claw-service sends a
        POST /admin/subscribe-group request (e.g. when a twin is added to a
        shared sandbox at runtime).
        """
        group_id = _to_int(sub.get("group_id", 0))
        if not group_id:
            logger.warning("SuperAGI: _add_subscription called with invalid group_id")
            return

        ws_id = _to_int(sub.get("workspace_id", self._workspace_id))
        self._subscription_map[group_id] = sub
        self._enabled_groups.add(group_id)

        if self._mqtt_client and group_id not in self._subscribed_groups:
            topic = f"{self._env}/workspace/{ws_id}/group/{group_id}/messages"
            self._mqtt_client.subscribe(topic, qos=1)
            self._subscribed_groups.add(group_id)
            logger.info("SuperAGI: runtime subscribed to group topic: %s", topic)

        self._persist_subscriptions()

    # ------------------------------------------------------------------
    # Admin HTTP server (POST /admin/subscribe-group)
    # ------------------------------------------------------------------

    async def _start_admin_server(self) -> None:
        """Start a lightweight aiohttp server for admin operations.

        The claw-service calls POST /admin/subscribe-group when a digital twin
        is assigned to share this sandbox at runtime. The payload is the full
        GroupSubscription: {workspace_id, group_id, api_key, api_base_url}.
        """
        try:
            from aiohttp import web as _web
        except ImportError:
            logger.warning("SuperAGI: aiohttp not installed, admin server disabled (pip install aiohttp)")
            return

        adapter_ref = self  # capture for closure

        async def handle_subscribe_group(request: Any) -> Any:
            try:
                body = await request.json()
            except Exception:
                return _web.Response(status=400, text="invalid JSON body")

            group_id = _to_int(body.get("group_id", 0))
            workspace_id = _to_int(body.get("workspace_id", 0))
            if not group_id or not workspace_id:
                return _web.Response(status=400, text="group_id and workspace_id are required")

            sub: Dict[str, Any] = {
                "workspace_id": workspace_id,
                "group_id": group_id,
                "api_key": body.get("api_key", ""),
                "api_base_url": body.get("api_base_url", "").rstrip("/"),
            }
            await adapter_ref._add_subscription(sub)
            logger.info(
                "SuperAGI: admin subscribe-group: workspace=%d group=%d",
                workspace_id, group_id,
            )
            return _web.json_response({"status": "ok", "group_id": group_id, "workspace_id": workspace_id})

        async def handle_health(request: Any) -> Any:
            return _web.json_response({
                "status": "ok",
                "subscriptions": len(adapter_ref._subscription_map),
            })

        app = _web.Application()
        app.router.add_post("/admin/subscribe-group", handle_subscribe_group)
        app.router.add_get("/admin/health", handle_health)

        runner = _web.AppRunner(app)
        await runner.setup()
        site = _web.TCPSite(runner, "0.0.0.0", self._admin_port)
        try:
            await site.start()
            logger.info("SuperAGI: admin server listening on 0.0.0.0:%d", self._admin_port)
        except Exception as e:
            logger.warning("SuperAGI: admin server failed to start on port %d: %s", self._admin_port, e)

    # ------------------------------------------------------------------
    # Sandbox heartbeat (MQTT wakeup prevention)
    # ------------------------------------------------------------------

    async def _heartbeat_sandbox(self) -> None:
        """Notify the sandbox service that this sandbox is actively handling messages.

        Prevents the lifecycle worker from auto-pausing an actively-used sandbox.
        Best-effort: failures are logged at debug level and never propagated.
        """
        if not self._sandbox_id or not self._sandbox_service_url:
            return
        try:
            await self._http_session.post(
                f"{self._sandbox_service_url}/api/sandboxes/{self._sandbox_id}/heartbeat",
                timeout=5.0,
            )
        except Exception as e:
            logger.debug("SuperAGI: sandbox heartbeat failed (non-fatal): %s", e)

    # ------------------------------------------------------------------
    # Image / chat info
    # ------------------------------------------------------------------

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image (URL as text -- SuperAGI renders link previews)."""
        text = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        result = await self._api_get(
            "/v1/messages/group",
            params={"group_id": chat_id},
        )
        if "error" not in result:
            return {
                "name": result.get("name", result.get("group_name", chat_id)),
                "type": "group" if result.get("is_group") else "dm",
                "chat_id": chat_id,
            }
        return {"name": chat_id, "type": "dm", "chat_id": chat_id}


# ------------------------------------------------------------------
# Standalone sender (for cron jobs / send_message tool)
# ------------------------------------------------------------------

async def send_superagi_message(
    api_key: str,
    api_base_url: str,
    chat_id: str,
    message: str,
) -> Dict[str, Any]:
    """Send a single message to SuperAGI without the full adapter.

    Used by the cron scheduler and send_message tool for out-of-band
    message delivery (no MQTT listener needed).
    """
    try:
        import httpx
    except ImportError:
        return {"error": "httpx not installed. Run: pip install httpx"}

    api_base_url = api_base_url.rstrip("/")
    url = f"{api_base_url}/v1/messages"
    headers = {
        "X-API-KEY": api_key,
        "Content-Type": "application/json",
    }
    body = {
        "recipient_type": 1,
        "recipient_id": _to_int(chat_id),
        "text": message,
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(url, headers=headers, json=body)
            if resp.status_code >= 400:
                text = resp.text[:200]
                return {"error": f"SuperAGI API error ({resp.status_code}): {text}"}
            data = resp.json()
            msg_id = data.get("id") or data.get("message_id")
            return {
                "success": True,
                "platform": "superagi",
                "chat_id": chat_id,
                "message_id": str(msg_id) if msg_id else None,
            }
    except Exception as e:
        return {"error": f"SuperAGI send failed: {e}"}
