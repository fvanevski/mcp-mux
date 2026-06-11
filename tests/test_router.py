import asyncio
import os
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from starlette.testclient import TestClient

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_router.core.config_loader import EndpointConfig, RouterConfig
from mcp_router.core.process_manager import ProcessManager
from mcp_router.server import app, router

# --- Config Loader Tests ---

def test_valid_config():
    data = {
        "endpoints": [
            {
                "path": "weather",
                "mode": "remote",
                "url": "http://api.weather.com",
                "summary": "Weather API"
            },
            {
                "path": "files",
                "mode": "managed_cli",
                "command": "uvx mcp-server-filesystem",
                "url": "http://localhost:8011/mcp",
                "summary": "File tools"
            }
        ]
    }
    cfg = RouterConfig.model_validate(data)
    assert len(cfg.endpoints) == 2
    assert cfg.endpoints[0].path == "weather"
    assert cfg.endpoints[1].mode == "managed_cli"
    assert cfg.endpoints[1].url == "http://localhost:8011/mcp"

def test_config_port_collision():
    data = {
        "endpoints": [
            {
                "path": "files1",
                "mode": "managed_cli",
                "command": "uvx mcp-server-filesystem",
                "url": "http://localhost:8011/mcp",
                "summary": "Files 1"
            },
            {
                "path": "files2",
                "mode": "managed_cli",
                "command": "uvx mcp-server-filesystem",
                "url": "http://localhost:8011/mcp",
                "summary": "Files 2"
            }
        ]
    }
    with pytest.raises(ValueError, match="Duplicate port detected"):
        RouterConfig.model_validate(data)

def test_config_duplicate_path():
    data = {
        "endpoints": [
            {
                "path": "weather",
                "mode": "remote",
                "url": "http://api1.weather.com",
                "summary": "Weather 1"
            },
            {
                "path": "weather",
                "mode": "remote",
                "url": "http://api2.weather.com",
                "summary": "Weather 2"
            }
        ]
    }
    with pytest.raises(ValueError, match="Duplicate path detected"):
        RouterConfig.model_validate(data)

def test_config_missing_remote_url():
    data = {
        "endpoints": [
            {
                "path": "weather",
                "mode": "remote",
                "summary": "Missing URL"
            }
        ]
    }
    with pytest.raises(ValueError, match="url is required for remote mode"):
        RouterConfig.model_validate(data)

def test_config_missing_managed_cli_url():
    data = {
        "endpoints": [
            {
                "path": "files",
                "mode": "managed_cli",
                "command": "uvx",
                "summary": "Missing URL"
            }
        ]
    }
    with pytest.raises(ValueError, match="url is required for managed_cli mode"):
        RouterConfig.model_validate(data)

# --- Process Manager Tests ---

@pytest.mark.asyncio
async def test_process_manager_lifecycle():
    pm = ProcessManager()
    # Reset singleton internal state for testing
    pm._processes.clear()
    pm._log_tasks.clear()

    cfg = EndpointConfig(
        path="mock-mcp",
        mode="managed_cli",
        command="python -m http.server 8099",
        url="http://localhost:8099/mcp",
        summary="Mock python server"
    )

    # Mock the asyncio.create_subprocess_shell and _wait_for_port
    mock_proc = AsyncMock()
    mock_proc.pid = 99999
    mock_proc.returncode = None
    mock_proc.stdout = AsyncMock()
    mock_proc.stderr = AsyncMock()

    with patch("asyncio.create_subprocess_shell", return_value=mock_proc) as mock_shell, \
         patch.object(pm, "_wait_for_port", return_value=True) as mock_wait:
        
        target_url = await pm.start_managed_server(cfg)
        
        assert target_url == "http://localhost:8099/mcp"
        mock_shell.assert_called_once()
        mock_wait.assert_called_once_with(8099, host="localhost")
        assert "mock-mcp" in pm._processes

        # Test stop
        with patch("os.killpg") as mock_killpg, patch("os.getpgid", return_value=123):
            await pm.stop_managed_server("mock-mcp")
            mock_killpg.assert_called_once_with(123, 15)  # signal.SIGTERM is 15
            assert "mock-mcp" not in pm._processes

# --- Routes & App Tests ---

def test_summary_route():
    router._configs = {
        "weather": EndpointConfig(
            path="weather",
            mode="remote",
            url="http://weather/mcp",
            summary="Weather summary"
        )
    }

    client = TestClient(app)
    response = client.get("/summary")
    assert response.status_code == 200
    
    data = response.json()
    assert "endpoints" in data
    assert len(data["endpoints"]) == 1
    assert data["endpoints"][0]["path"] == "weather"
    assert data["endpoints"][0]["mode"] == "remote"
    assert data["endpoints"][0]["summary"] == "Weather summary"

def test_not_found_route():
    client = TestClient(app)
    response = client.get("/nonexistent")
    assert response.status_code == 404
    assert "configured" in response.json()["error"]


@pytest.mark.asyncio
async def test_streamable_http_bridge():
    router._configs = {
        "weather": EndpointConfig(
            path="weather",
            mode="remote",
            url="http://api.weather.com/mcp",
            summary="Weather summary",
            transport="streamable-http"
        )
    }
    
    import httpx
    from httpx import AsyncClient
    transport = httpx.ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Establish GET SSE connection and read the endpoint registration message
        async with client.stream("GET", "/weather", headers={"Accept": "text/event-stream"}) as sse_res:
            assert sse_res.status_code == 200
            
            lines_iter = sse_res.aiter_lines()
            l1 = await anext(lines_iter)
            l2 = await anext(lines_iter)
            
            assert "event: endpoint" in l1
            assert "data: /weather?session_id=" in l2
            session_id = l2.split("session_id=")[1]
            
            # 2. Prepare the background POST task that simulates remote backend response streaming
            async def run_post(sid):
                mock_response = MagicMock()
                mock_response.status_code = 200
                async def mock_aiter_lines():
                    yield "event: message"
                    yield "data: {\"result\":\"cloudy\"}"
                    yield ""
                mock_response.aiter_lines = mock_aiter_lines
                
                mock_stream = MagicMock()
                mock_stream.__aenter__ = AsyncMock(return_value=mock_response)
                mock_stream.__aexit__ = AsyncMock(return_value=None)
                
                with patch("httpx.AsyncClient.stream", return_value=mock_stream):
                    post_res = await client.post(f"/weather?session_id={sid}", json={"jsonrpc":"2.0","id":1,"method":"foo"})
                    assert post_res.status_code == 202
                    assert post_res.text == "Accepted"
            
            post_task = asyncio.create_task(run_post(session_id))
            
            # 3. Read the bridged response message on the persistent GET SSE stream
            l3 = await anext(lines_iter)
            l4 = await anext(lines_iter)
            
            assert "event: message" in l3
            assert "data: {\"result\":\"cloudy\"}" in l4
            
            await post_task


# --- Tool Filtering Tests ---

from mcp_router.server import filter_tools_response

def test_endpoint_config_allowed_denied_validation():
    # Both provided -> denied_tools is cleared to None (precedence to allowed)
    cfg = EndpointConfig(
        path="test",
        mode="remote",
        url="http://localhost/mcp",
        summary="Test",
        allowed_tools=["toolA"],
        denied_tools=["toolB"]
    )
    assert cfg.allowed_tools == ["toolA"]
    assert cfg.denied_tools is None

    # Only allowed provided -> preserved
    cfg_allow = EndpointConfig(
        path="test",
        mode="remote",
        url="http://localhost/mcp",
        summary="Test",
        allowed_tools=["toolA"]
    )
    assert cfg_allow.allowed_tools == ["toolA"]
    assert cfg_allow.denied_tools is None

    # Only denied provided -> preserved
    cfg_deny = EndpointConfig(
        path="test",
        mode="remote",
        url="http://localhost/mcp",
        summary="Test",
        denied_tools=["toolB"]
    )
    assert cfg_deny.allowed_tools is None
    assert cfg_deny.denied_tools == ["toolB"]

def test_filter_tools_response_sse():
    allowed = ["toolA"]
    line = 'data: {"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"toolA"},{"name":"toolB"}]}}'
    filtered = filter_tools_response(line, allowed, None)
    assert filtered.startswith("data: ")
    import json
    data = json.loads(filtered[6:])
    assert len(data["result"]["tools"]) == 1
    assert data["result"]["tools"][0]["name"] == "toolA"

def test_filter_tools_response_json():
    denied = ["toolA"]
    body = '{"jsonrpc":"2.0","id":1,"result":{"tools":[{"name":"toolA"},{"name":"toolB"}]}}'
    filtered = filter_tools_response(body, None, denied)
    import json
    data = json.loads(filtered)
    assert len(data["result"]["tools"]) == 1
    assert data["result"]["tools"][0]["name"] == "toolB"


