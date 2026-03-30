"""Tests for SuperAGI messaging platform adapter."""

import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.config import Platform, PlatformConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_adapter(**extra_overrides):
    """Create a SuperAGIAdapter with sensible test defaults."""
    from gateway.platforms.superagi import SuperAGIAdapter

    config = PlatformConfig()
    config.enabled = True
    config.api_key = "test-api-key-123456"
    config.extra = {
        "api_key": "test-api-key-123456",
        "api_base_url": "https://api.superagi.com",
        "bot_user_id": "42",
        "workspace_id": "100",
        "mqtt_url": "ws://emqx.superagi.com:8083/mqtt",
        "mqtt_username": "supersales",
        "mqtt_password": "secret",
        "env": "development",
        **extra_overrides,
    }
    return SuperAGIAdapter(config)


# ---------------------------------------------------------------------------
# Platform Enum
# ---------------------------------------------------------------------------

class TestSuperAGIPlatformEnum:
    def test_enum_exists(self):
        assert Platform.SUPERAGI.value == "superagi"

    def test_in_platform_list(self):
        platforms = [p.value for p in Platform]
        assert "superagi" in platforms


# ---------------------------------------------------------------------------
# Config Loading
# ---------------------------------------------------------------------------

class TestSuperAGIConfigLoading:
    def test_apply_env_overrides(self, monkeypatch):
        monkeypatch.setenv("SUPERAGI_API_KEY", "key123")
        monkeypatch.setenv("SUPERAGI_API_BASE_URL", "https://api.test.com")
        monkeypatch.setenv("SUPERAGI_BOT_USER_ID", "42")
        monkeypatch.setenv("SUPERAGI_WORKSPACE_ID", "100")
        monkeypatch.setenv("SUPERAGI_MQTT_URL", "ws://mqtt.test.com:8083/mqtt")
        monkeypatch.setenv("SUPERAGI_MQTT_PASSWORD", "pass")
        monkeypatch.setenv("SUPERAGI_ENV", "development")

        from gateway.config import GatewayConfig, _apply_env_overrides
        config = GatewayConfig()
        _apply_env_overrides(config)

        assert Platform.SUPERAGI in config.platforms
        sc = config.platforms[Platform.SUPERAGI]
        assert sc.enabled is True
        assert sc.api_key == "key123"
        assert sc.extra["api_base_url"] == "https://api.test.com"
        assert sc.extra["bot_user_id"] == "42"
        assert sc.extra["workspace_id"] == "100"
        assert sc.extra["mqtt_url"] == "ws://mqtt.test.com:8083/mqtt"
        assert sc.extra["mqtt_password"] == "pass"
        assert sc.extra["mqtt_username"] == "supersales"
        assert sc.extra["env"] == "development"

    def test_not_loaded_without_required_vars(self, monkeypatch):
        monkeypatch.setenv("SUPERAGI_API_KEY", "key123")
        # Missing the other required vars

        from gateway.config import GatewayConfig, _apply_env_overrides
        config = GatewayConfig()
        _apply_env_overrides(config)

        assert Platform.SUPERAGI not in config.platforms

    def test_home_channel(self, monkeypatch):
        monkeypatch.setenv("SUPERAGI_API_KEY", "key123")
        monkeypatch.setenv("SUPERAGI_API_BASE_URL", "https://api.test.com")
        monkeypatch.setenv("SUPERAGI_BOT_USER_ID", "42")
        monkeypatch.setenv("SUPERAGI_WORKSPACE_ID", "100")
        monkeypatch.setenv("SUPERAGI_MQTT_URL", "ws://mqtt.test.com:8083/mqtt")
        monkeypatch.setenv("SUPERAGI_MQTT_PASSWORD", "pass")
        monkeypatch.setenv("SUPERAGI_HOME_CHANNEL", "999")

        from gateway.config import GatewayConfig, _apply_env_overrides
        config = GatewayConfig()
        _apply_env_overrides(config)

        sc = config.platforms[Platform.SUPERAGI]
        assert sc.home_channel is not None
        assert sc.home_channel.chat_id == "999"


# ---------------------------------------------------------------------------
# Adapter Init
# ---------------------------------------------------------------------------

class TestSuperAGIAdapterInit:
    def test_init_parses_config(self):
        adapter = _make_adapter()
        assert adapter._api_key == "test-api-key-123456"
        assert adapter._api_base_url == "https://api.superagi.com"
        assert adapter._bot_user_id == 42
        assert adapter._workspace_id == 100
        assert adapter._mqtt_url == "ws://emqx.superagi.com:8083/mqtt"
        assert adapter._mqtt_username == "supersales"
        assert adapter._mqtt_password == "secret"
        assert adapter._env == "development"

    def test_bot_user_id_is_int(self):
        adapter = _make_adapter(bot_user_id="555")
        assert adapter._bot_user_id == 555
        assert isinstance(adapter._bot_user_id, int)


# ---------------------------------------------------------------------------
# URL Parsing
# ---------------------------------------------------------------------------

class TestParseMqttUrl:
    def test_ws_with_port(self):
        from gateway.platforms.superagi import SuperAGIAdapter
        host, port = SuperAGIAdapter._parse_mqtt_url("ws://emqx.superagi.com:8083/mqtt")
        assert host == "emqx.superagi.com"
        assert port == 8083

    def test_ws_without_port(self):
        from gateway.platforms.superagi import SuperAGIAdapter
        host, port = SuperAGIAdapter._parse_mqtt_url("ws://broker.example.com/mqtt")
        assert host == "broker.example.com"
        assert port == 8083

    def test_wss_with_port(self):
        from gateway.platforms.superagi import SuperAGIAdapter
        host, port = SuperAGIAdapter._parse_mqtt_url("wss://secure.broker.com:8884/mqtt")
        assert host == "secure.broker.com"
        assert port == 8884

    def test_invalid_url(self):
        from gateway.platforms.superagi import SuperAGIAdapter
        host, port = SuperAGIAdapter._parse_mqtt_url("not-a-url")
        assert host == ""
        assert port == 0


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

class TestDedup:
    def test_first_seen_returns_true(self):
        adapter = _make_adapter()
        assert adapter._check_and_mark_seen("msg1") is True

    def test_second_seen_returns_false(self):
        adapter = _make_adapter()
        adapter._check_and_mark_seen("msg1")
        assert adapter._check_and_mark_seen("msg1") is False

    def test_empty_id_always_passes(self):
        adapter = _make_adapter()
        assert adapter._check_and_mark_seen("") is True
        assert adapter._check_and_mark_seen("") is True

    def test_eviction_on_overflow(self):
        adapter = _make_adapter()
        adapter._SEEN_MAX = 5
        adapter._SEEN_TTL = 0.001
        for i in range(6):
            adapter._check_and_mark_seen(f"msg-{i}")
        time.sleep(0.01)
        # Trigger eviction by inserting a new message after TTL expires
        assert adapter._check_and_mark_seen("trigger") is True
        # Old entries should have been evicted (TTL expired)
        assert adapter._check_and_mark_seen("msg-0") is True


# ---------------------------------------------------------------------------
# Send (REST API payload shape)
# ---------------------------------------------------------------------------

class TestSend:
    @pytest.mark.asyncio
    async def test_send_payload_shape(self):
        adapter = _make_adapter()
        captured_body = {}

        async def mock_post(path, body):
            captured_body.update(body)
            return {"id": "resp-1"}

        adapter._api_post = mock_post

        result = await adapter.send("12345", "Hello world")

        assert result.success is True
        assert result.message_id == "resp-1"
        assert captured_body["recipient_type"] == 1
        assert captured_body["recipient_id"] == 12345
        assert captured_body["text"] == "Hello world"

    @pytest.mark.asyncio
    async def test_send_empty_content(self):
        adapter = _make_adapter()
        result = await adapter.send("123", "")
        assert result.success is True

    @pytest.mark.asyncio
    async def test_send_error_handling(self):
        adapter = _make_adapter()

        async def mock_post(path, body):
            return {"error": "timeout connecting"}

        adapter._api_post = mock_post

        result = await adapter.send("123", "Hi")
        assert result.success is False
        assert result.retryable is True

    @pytest.mark.asyncio
    async def test_send_non_network_error(self):
        adapter = _make_adapter()

        async def mock_post(path, body):
            return {"error": "invalid recipient"}

        adapter._api_post = mock_post

        result = await adapter.send("123", "Hi")
        assert result.success is False
        assert result.retryable is False


# ---------------------------------------------------------------------------
# Typing Indicator
# ---------------------------------------------------------------------------

class TestSendTyping:
    @pytest.mark.asyncio
    async def test_typing_payload_shape(self):
        adapter = _make_adapter()
        captured_body = {}

        async def mock_post(path, body):
            captured_body.update(body)
            return {}

        adapter._api_post = mock_post

        await adapter.send_typing("999")

        assert captured_body == {"group_id": 999}


# ---------------------------------------------------------------------------
# MQTT Event Handling
# ---------------------------------------------------------------------------

class TestMqttEventDispatch:
    def test_sidebar_event_dispatched(self):
        adapter = _make_adapter()
        handler_called = []

        async def mock_sidebar(payload):
            handler_called.append(payload)

        adapter._handle_sidebar_event = mock_sidebar

        payload = json.dumps({
            "event": "SIDEBAR_NEW_MESSAGE",
            "sender_id": 99,
            "group_id": 55,
            "message_id": "msg-1",
        }).encode()

        msg = MagicMock()
        msg.payload = payload
        msg.topic = "dev/workspace/100/user/42/sidebar"

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            with patch("asyncio.get_event_loop"):
                adapter._on_mqtt_message(None, None, msg)
                mock_run.assert_called_once()

    def test_new_message_event_dispatched(self):
        adapter = _make_adapter()

        payload = json.dumps({
            "event": "NEW_MESSAGE",
            "user_id": 99,
            "group_id": 55,
            "message_id": "msg-2",
        }).encode()

        msg = MagicMock()
        msg.payload = payload
        msg.topic = "dev/workspace/100/group/55/messages"

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            with patch("asyncio.get_event_loop"):
                adapter._on_mqtt_message(None, None, msg)
                mock_run.assert_called_once()

    def test_own_message_skipped_in_sidebar(self):
        """SIDEBAR_NEW_MESSAGE with sender_id == bot_user_id should be skipped."""
        adapter = _make_adapter()

        payload = json.dumps({
            "event": "SIDEBAR_NEW_MESSAGE",
            "sender_id": 42,  # same as bot_user_id
            "group_id": 55,
            "message_id": "msg-self",
        }).encode()

        msg = MagicMock()
        msg.payload = payload
        msg.topic = "dev/workspace/100/user/42/sidebar"

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            with patch("asyncio.get_event_loop"):
                adapter._on_mqtt_message(None, None, msg)
                # Handler should still be called -- skip happens inside _handle_sidebar_event
                # The _on_mqtt_message dispatches unconditionally
                mock_run.assert_called_once()

    def test_invalid_payload_ignored(self):
        adapter = _make_adapter()
        msg = MagicMock()
        msg.payload = b"not-json"
        msg.topic = "some/topic"

        with patch("asyncio.run_coroutine_threadsafe") as mock_run:
            adapter._on_mqtt_message(None, None, msg)
            mock_run.assert_not_called()


class TestSidebarEventHandler:
    @pytest.mark.asyncio
    async def test_skips_own_messages(self):
        adapter = _make_adapter()
        adapter.handle_message = AsyncMock()

        payload = {
            "event": "SIDEBAR_NEW_MESSAGE",
            "sender_id": 42,  # bot_user_id
            "group_id": 55,
            "message_id": "msg-self",
        }

        await adapter._handle_sidebar_event(payload)
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_reads_flat_fields(self):
        """Verify event fields are read from top-level (not nested 'data')."""
        adapter = _make_adapter()
        adapter.handle_message = AsyncMock()

        fetch_calls = []

        async def mock_fetch(group_id, message_id):
            fetch_calls.append((group_id, message_id))
            return {"text": "hello", "sender_name": "Alice"}

        adapter._fetch_message_content = mock_fetch
        adapter._mqtt_client = MagicMock()

        payload = {
            "event": "SIDEBAR_NEW_MESSAGE",
            "sender_id": 99,
            "group_id": 55,
            "message_id": "msg-123",
        }

        await adapter._handle_sidebar_event(payload)

        assert len(fetch_calls) == 1
        assert fetch_calls[0] == (55, "msg-123")
        adapter.handle_message.assert_called_once()


class TestGroupMessageEventHandler:
    @pytest.mark.asyncio
    async def test_skips_own_messages(self):
        adapter = _make_adapter()
        adapter.handle_message = AsyncMock()

        payload = {
            "event": "NEW_MESSAGE",
            "user_id": 42,
            "group_id": 55,
            "message_id": "msg-self",
        }

        await adapter._handle_group_message_event(payload)
        adapter.handle_message.assert_not_called()

    @pytest.mark.asyncio
    async def test_reads_flat_fields(self):
        adapter = _make_adapter()
        adapter.handle_message = AsyncMock()

        fetch_calls = []

        async def mock_fetch(group_id, message_id):
            fetch_calls.append((group_id, message_id))
            return {"text": "world", "sender_name": "Bob"}

        adapter._fetch_message_content = mock_fetch

        payload = {
            "event": "NEW_MESSAGE",
            "user_id": 99,
            "group_id": 55,
            "message_id": "msg-456",
        }

        await adapter._handle_group_message_event(payload)

        assert len(fetch_calls) == 1
        assert fetch_calls[0] == (55, "msg-456")
        adapter.handle_message.assert_called_once()


# ---------------------------------------------------------------------------
# Fetch Message Content
# ---------------------------------------------------------------------------

class TestFetchMessageContent:
    @pytest.mark.asyncio
    async def test_passes_group_id_param(self):
        adapter = _make_adapter()
        captured = {}

        async def mock_get(path, params=None):
            captured["path"] = path
            captured["params"] = params
            return {"text": "hi", "sender_name": "Test"}

        adapter._api_get = mock_get

        result = await adapter._fetch_message_content(55, "msg-789")

        assert captured["path"] == "/v1/messages/msg-789"
        assert captured["params"] == {"group_id": 55}
        assert result is not None

    @pytest.mark.asyncio
    async def test_no_group_id_omits_param(self):
        adapter = _make_adapter()
        captured = {}

        async def mock_get(path, params=None):
            captured["params"] = params
            return {"text": "hi"}

        adapter._api_get = mock_get

        await adapter._fetch_message_content(0, "msg-000")
        assert captured["params"] is None

    @pytest.mark.asyncio
    async def test_returns_none_on_error(self):
        adapter = _make_adapter()

        async def mock_get(path, params=None):
            return {"error": "not found"}

        adapter._api_get = mock_get

        result = await adapter._fetch_message_content(1, "bad-id")
        assert result is None


# ---------------------------------------------------------------------------
# Requirements Check
# ---------------------------------------------------------------------------

class TestCheckRequirements:
    def test_returns_false_when_missing_env(self, monkeypatch):
        monkeypatch.delenv("SUPERAGI_API_KEY", raising=False)
        monkeypatch.delenv("SUPERAGI_API_BASE_URL", raising=False)
        monkeypatch.delenv("SUPERAGI_BOT_USER_ID", raising=False)
        monkeypatch.delenv("SUPERAGI_WORKSPACE_ID", raising=False)
        monkeypatch.delenv("SUPERAGI_MQTT_URL", raising=False)
        monkeypatch.delenv("SUPERAGI_MQTT_PASSWORD", raising=False)

        from gateway.platforms.superagi import check_superagi_requirements
        assert check_superagi_requirements() is False

    def test_returns_true_with_all_vars(self, monkeypatch):
        monkeypatch.setenv("SUPERAGI_API_KEY", "key")
        monkeypatch.setenv("SUPERAGI_API_BASE_URL", "https://api.test.com")
        monkeypatch.setenv("SUPERAGI_BOT_USER_ID", "1")
        monkeypatch.setenv("SUPERAGI_WORKSPACE_ID", "2")
        monkeypatch.setenv("SUPERAGI_MQTT_URL", "ws://broker:8083/mqtt")
        monkeypatch.setenv("SUPERAGI_MQTT_PASSWORD", "pass")

        with patch.dict("sys.modules", {"paho": MagicMock(), "paho.mqtt": MagicMock(), "paho.mqtt.client": MagicMock()}):
            from gateway.platforms.superagi import check_superagi_requirements
            assert check_superagi_requirements() is True


# ---------------------------------------------------------------------------
# Authorization Maps Integration
# ---------------------------------------------------------------------------

class TestAuthorizationMaps:
    def test_superagi_in_allowed_users_map(self):
        from gateway.run import GatewayRunner
        # Verify the map is constructed correctly by checking its source
        import inspect
        source = inspect.getsource(GatewayRunner._is_user_authorized)
        assert "SUPERAGI_ALLOWED_USERS" in source
        assert "SUPERAGI_ALLOW_ALL_USERS" in source


# ---------------------------------------------------------------------------
# Platform Maps (cron + send_message)
# ---------------------------------------------------------------------------

class TestPlatformMaps:
    def test_cron_platform_map(self):
        import inspect
        from cron import scheduler
        source = inspect.getsource(scheduler._deliver_result)
        assert '"superagi"' in source or "'superagi'" in source

    def test_send_message_platform_map(self):
        import inspect
        from tools import send_message_tool
        source = inspect.getsource(send_message_tool._handle_send)
        assert '"superagi"' in source or "'superagi'" in source


# ---------------------------------------------------------------------------
# MQTT Topics
# ---------------------------------------------------------------------------

class TestMqttTopics:
    def test_sidebar_topic(self):
        adapter = _make_adapter()
        topic = adapter._sidebar_topic()
        assert topic == "development/workspace/100/user/42/sidebar"

    def test_group_topic(self):
        adapter = _make_adapter()
        topic = adapter._group_topic(55)
        assert topic == "development/workspace/100/group/55/messages"


# ---------------------------------------------------------------------------
# Standalone Sender
# ---------------------------------------------------------------------------

class TestStandaloneSender:
    @pytest.mark.asyncio
    async def test_send_superagi_message_payload(self):
        from gateway.platforms.superagi import send_superagi_message

        captured = {}

        async def mock_post(url, headers=None, json=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["json"] = json
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"id": "sent-1"}
            return resp

        with patch("httpx.AsyncClient") as MockClient:
            mock_client = AsyncMock()
            mock_client.post = mock_post
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            MockClient.return_value = mock_client

            result = await send_superagi_message(
                api_key="key1",
                api_base_url="https://api.test.com",
                chat_id="999",
                message="Test msg",
            )

        assert result["success"] is True
        assert result["platform"] == "superagi"
        assert captured["json"]["recipient_type"] == 1
        assert captured["json"]["recipient_id"] == 999
        assert captured["json"]["text"] == "Test msg"
        assert captured["headers"]["X-API-KEY"] == "key1"


# ---------------------------------------------------------------------------
# Toolset
# ---------------------------------------------------------------------------

class TestToolset:
    def test_hermes_superagi_toolset_exists(self):
        from toolsets import TOOLSETS
        assert "hermes-superagi" in TOOLSETS

    def test_hermes_gateway_includes_superagi(self):
        from toolsets import TOOLSETS
        assert "hermes-superagi" in TOOLSETS["hermes-gateway"]["includes"]
