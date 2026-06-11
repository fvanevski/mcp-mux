import asyncio
import os
import sys
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mcp_router.core.config_loader import EndpointConfig, RouterConfig, ConfigWatcher
from mcp_router.core.process_manager import ProcessManager
from mcp_router.routes.discovery import setup_discovery
from mcp_router.main import MCPRouter

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
                "command": "uvx",
                "args": ["mcp-server-filesystem"],
                "port": 8011,
                "summary": "File tools"
            }
        ]
    }
    cfg = RouterConfig.model_validate(data)
    assert len(cfg.endpoints) == 2
    assert cfg.endpoints[0].path == "weather"
    assert cfg.endpoints[1].port == 8011

def test_config_port_collision():
    data = {
        "endpoints": [
            {
                "path": "files1",
                "mode": "managed_cli",
                "command": "uvx",
                "port": 8011,
                "summary": "Files 1"
            },
            {
                "path": "files2",
                "mode": "managed_cli",
                "command": "uvx",
                "port": 8011,
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
        command="python",
        args=["-m", "http.server", "8099"],
        port=8099,
        summary="Mock python server"
    )

    # Mock the asyncio.create_subprocess_exec and _wait_for_port
    mock_proc = AsyncMock()
    mock_proc.pid = 99999
    mock_proc.returncode = None
    mock_proc.stdout = AsyncMock()
    mock_proc.stderr = AsyncMock()

    with patch("asyncio.create_subprocess_exec", return_value=mock_proc) as mock_exec, \
         patch.object(pm, "_wait_for_port", return_value=True) as mock_wait:
        
        target_url = await pm.start_managed_server(cfg)
        
        assert target_url == "http://127.0.0.1:8099/mcp"
        mock_exec.assert_called_once()
        mock_wait.assert_called_once_with(8099)
        assert "mock-mcp" in pm._processes

        # Test stop
        with patch("os.killpg") as mock_killpg, patch("os.getpgid", return_value=123):
            await pm.stop_managed_server("mock-mcp")
            mock_killpg.assert_called_once_with(123, 15)  # signal.SIGTERM is 15
            assert "mock-mcp" not in pm._processes

# --- Routes Discovery Tests ---

@pytest.mark.asyncio
async def test_discovery_route_response():
    mcp_mock = MagicMock()
    # Capture route decorator arguments
    decorator_args = []
    
    def mock_custom_route(path, methods):
        decorator_args.append((path, methods))
        def inner(fn):
            mcp_mock._route_fn = fn
            return fn
        return inner

    mcp_mock.custom_route = mock_custom_route

    configs = {
        "weather": EndpointConfig(
            path="weather",
            mode="remote",
            url="http://weather/mcp",
            summary="Weather summary"
        )
    }

    setup_discovery(mcp_mock, configs)

    assert len(decorator_args) == 1
    assert decorator_args[0] == ("/summary", ["GET"])
    assert hasattr(mcp_mock, "_route_fn")

    # Invoke the endpoint
    mock_req = MagicMock()
    response = await mcp_mock._route_fn(mock_req)
    
    import json
    body = json.loads(response.body.decode("utf-8"))
    assert "endpoints" in body
    assert len(body["endpoints"]) == 1
    assert body["endpoints"][0]["path"] == "weather"
    assert body["endpoints"][0]["mode"] == "remote"
    assert body["endpoints"][0]["summary"] == "Weather summary"
