import os
import asyncio
import logging
import time
from uuid import uuid4
from contextlib import asynccontextmanager
from typing import List, Optional, Dict
from urllib.parse import urlparse, urlunparse
import json
import yaml
import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.middleware.cors import CORSMiddleware

from mcp_router.core.config_loader import RouterConfig, ConfigWatcher, EndpointConfig
from mcp_router.core.process_manager import ProcessManager

# Configure logging to console safely
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("mcp_router")

def get_target_url(config_url: str, request_path: str, path_prefix: str) -> str:
    parsed_cfg = urlparse(config_url)
    prefix = f"/{path_prefix}"
    if request_path.startswith(prefix):
        suffix = request_path[len(prefix):]
    else:
        suffix = request_path
    
    cfg_path = parsed_cfg.path.rstrip("/")
    joined_path = cfg_path + suffix
    if not joined_path.startswith("/"):
        joined_path = "/" + joined_path
        
    target = urlunparse((
        parsed_cfg.scheme,
        parsed_cfg.netloc,
        joined_path,
        parsed_cfg.params,
        "",  # query is handled separately
        parsed_cfg.fragment
    ))
    return target

def filter_tools_response(line_or_body: str, allowed_tools: Optional[list[str]], denied_tools: Optional[list[str]]) -> str:
    """
    Parses a string (which may be a full JSON body or a single line of an SSE data payload),
    detects if it represents a JSON-RPC tools/list response, and filters the returned
    tools list based on allowed/denied configuration.
    """
    if allowed_tools is None and denied_tools is None:
        return line_or_body

    prefix = ""
    json_str = line_or_body
    if line_or_body.startswith("data: "):
        prefix = "data: "
        json_str = line_or_body[6:].strip()
        
    try:
        data = json.loads(json_str)
        if isinstance(data, dict) and "result" in data:
            result = data["result"]
            if isinstance(result, dict) and "tools" in result and isinstance(result["tools"], list):
                original_tools = result["tools"]
                
                # Apply allowed/denied logic
                # If both allowed and denied included, default to allowed tools and ignore deny list.
                filtered_tools = []
                if allowed_tools is not None:
                    allowed_set = set(allowed_tools)
                    filtered_tools = [t for t in original_tools if t.get("name") in allowed_set]
                elif denied_tools is not None:
                    denied_set = set(denied_tools)
                    filtered_tools = [t for t in original_tools if t.get("name") not in denied_set]
                else:
                    filtered_tools = original_tools
                
                result["tools"] = filtered_tools
                data["result"] = result
                return f"{prefix}{json.dumps(data)}"
    except Exception:
        pass
    return line_or_body

class MCPRouter:
    def __init__(self, app: Starlette, config_path: str):
        self.app = app
        self.config_path = config_path
        self.process_manager = ProcessManager()
        self._configs: dict[str, EndpointConfig] = {}
        self.last_activity: dict[str, float] = {}
        self.active_connections: dict[str, int] = {}
        self.locks: dict[str, asyncio.Lock] = {}
        self.active_sessions: dict[str, asyncio.Queue] = {}
        self._running = False
        self._checker_task = None

        # Add routes to the app
        self.app.add_route("/summary", self.get_summary, methods=["GET"])
        self.app.add_route("/{path_prefix:str}", self.catch_all_proxy, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])
        self.app.add_route("/{path_prefix:str}/{subpath:path}", self.catch_all_proxy, methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"])

    def apply_configuration(self, config: RouterConfig):
        new_endpoints = {ep.path: ep for ep in config.endpoints}
        
        # Determine running processes to stop
        to_stop = []
        for path in list(self._configs.keys()):
            if path not in new_endpoints:
                to_stop.append(path)
            else:
                if self._configs[path] != new_endpoints[path]:
                    logger.info(f"Config for {path} changed, stopping it.")
                    to_stop.append(path)

        for path in to_stop:
            asyncio.create_task(self.process_manager.stop_managed_server(path))
            self._configs.pop(path, None)
            self.last_activity.pop(path, None)
            self.locks.pop(path, None)

        # Update configs and setup locks
        for path, ep_cfg in new_endpoints.items():
            self._configs[path] = ep_cfg
            if path not in self.locks:
                self.locks[path] = asyncio.Lock()
        
        logger.info(f"Applied config. Active paths: {list(self._configs.keys())}")

    async def idle_timeout_checker(self):
        while self._running:
            try:
                await asyncio.sleep(10)  # check every 10 seconds
                current_time = time.time()
                for path, ep_cfg in list(self._configs.items()):
                    if ep_cfg.mode == "managed_cli":
                        # Check if process is running
                        if self.process_manager.is_running(path):
                            # If there are active connections, keep updating the last activity
                            active_conns = self.active_connections.get(path, 0)
                            if active_conns > 0:
                                self.last_activity[path] = current_time

                            last_act = self.last_activity.get(path, 0)
                            timeout = ep_cfg.timeout
                            if current_time - last_act > timeout:
                                logger.info(f"Inactivity timeout ({timeout}s) exceeded for {path}. Stopping process.")
                                await self.process_manager.stop_managed_server(path)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in idle timeout checker: {e}")

    async def get_summary(self, request: Request):
        summary_list = []
        for path, cfg in self._configs.items():
            summary_list.append({
                "path": cfg.path,
                "mode": cfg.mode,
                "summary": cfg.summary
            })
        return JSONResponse({"endpoints": summary_list})

    async def sse_proxy_generator(self, target_url, headers, params, path_prefix):
        self.active_connections[path_prefix] = self.active_connections.get(path_prefix, 0) + 1
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", target_url, headers=headers, params=params, timeout=None) as response:
                    buffer = ""
                    async for chunk in response.aiter_text():
                        self.last_activity[path_prefix] = time.time()
                        buffer += chunk
                        while "\n" in buffer:
                            line, buffer = buffer.split("\n", 1)
                            if line.startswith("data: "):
                                data_content = line[6:].strip()
                                if data_content.startswith("/") or data_content.startswith("http"):
                                    parsed = urlparse(data_content)
                                    if parsed.netloc:
                                        new_path = f"/{path_prefix}{parsed.path}"
                                        if parsed.query:
                                            new_path += f"?{parsed.query}"
                                        line = f"data: {new_path}"
                                    else:
                                        line = f"data: /{path_prefix}{data_content}"
                            # Filter tools list response if configured
                            ep_cfg = self._configs.get(path_prefix)
                            if ep_cfg:
                                line = filter_tools_response(line, ep_cfg.allowed_tools, ep_cfg.denied_tools)
                            yield (line + "\n").encode("utf-8")
        finally:
            self.active_connections[path_prefix] = max(0, self.active_connections.get(path_prefix, 0) - 1)
            self.last_activity[path_prefix] = time.time()

    async def local_sse_generator(self, session_id, queue, path_prefix):
        self.active_connections[path_prefix] = self.active_connections.get(path_prefix, 0) + 1
        client_post_uri = f"/{path_prefix}?session_id={session_id}"
        yield f"event: endpoint\ndata: {client_post_uri}\n\n".encode("utf-8")
        try:
            while True:
                line = await queue.get()
                yield (line + "\n").encode("utf-8")
        except asyncio.CancelledError:
            pass
        finally:
            self.active_sessions.pop(session_id, None)
            self.active_connections[path_prefix] = max(0, self.active_connections.get(path_prefix, 0) - 1)
            self.last_activity[path_prefix] = time.time()

    async def catch_all_proxy(self, request: Request):
        path_prefix = request.path_params.get("path_prefix")
        if not path_prefix or path_prefix not in self._configs:
            return JSONResponse({"error": f"Endpoint '{path_prefix}' not configured"}, status_code=404)

        ep_cfg = self._configs[path_prefix]
        
        # 1. Update last activity
        self.last_activity[path_prefix] = time.time()

        # 2. Check if we need to start the process
        if ep_cfg.mode == "managed_cli":
            async with self.locks[path_prefix]:
                if not self.process_manager.is_running(path_prefix):
                    logger.info(f"On-demand activation triggered for: {path_prefix}")
                    try:
                        await self.process_manager.start_managed_server(ep_cfg)
                    except Exception as e:
                        logger.error(f"Failed to start managed server {path_prefix}: {e}")
                        return JSONResponse({"error": f"Failed to start managed server: {e}"}, status_code=500)

        # 3. Construct target URL
        request_path = request.url.path
        target_url = get_target_url(ep_cfg.url, request_path, path_prefix)

        # 4. Proxy the request
        exclude_headers = {"host", "content-length", "connection", "transfer-encoding", "keep-alive"}
        forward_headers = {k: v for k, v in request.headers.items() if k.lower() not in exclude_headers}

        is_sse_init = request.method == "GET" and "text/event-stream" in request.headers.get("accept", "").lower()
        if is_sse_init:
            if ep_cfg.transport == "streamable-http":
                session_id = uuid4().hex
                queue = asyncio.Queue()
                self.active_sessions[session_id] = queue
                return StreamingResponse(
                    self.local_sse_generator(session_id, queue, path_prefix),
                    media_type="text/event-stream"
                )
            else:
                return StreamingResponse(
                    self.sse_proxy_generator(target_url, forward_headers, dict(request.query_params), path_prefix),
                    media_type="text/event-stream"
                )
        else:
            session_id = request.query_params.get("session_id")
            if request.method == "POST" and ep_cfg.transport == "streamable-http" and session_id:
                if session_id not in self.active_sessions:
                    return JSONResponse({"error": "Session not found"}, status_code=404)
                queue = self.active_sessions[session_id]
                params = {k: v for k, v in request.query_params.items() if k != "session_id"}
                forward_headers["Mcp-Session-Id"] = session_id
                try:
                    client = httpx.AsyncClient()
                    req_body = await request.body()
                    response_stream = client.stream(
                        method="POST",
                        url=target_url,
                        headers=forward_headers,
                        params=params,
                        content=req_body,
                        timeout=60.0
                    )
                    response = await response_stream.__aenter__()
                    async def process_response():
                        try:
                            async for line in response.aiter_lines():
                                filtered_line = filter_tools_response(line, ep_cfg.allowed_tools, ep_cfg.denied_tools)
                                await queue.put(filtered_line)
                        except Exception as e:
                            logger.error(f"Error reading response from streamable-http backend: {e}")
                        finally:
                            await response_stream.__aexit__(None, None, None)
                            await client.aclose()
                    asyncio.create_task(process_response())
                    return Response("Accepted", status_code=202)
                except Exception as e:
                    logger.error(f"Failed to proxy POST request to streamable-http backend: {e}")
                    return JSONResponse({"error": f"Failed to proxy request: {e}"}, status_code=502)
            else:
                try:
                    client = httpx.AsyncClient()
                    req_body = await request.body()
                    response_stream = client.stream(
                        method=request.method,
                        url=target_url,
                        headers=forward_headers,
                        params=dict(request.query_params),
                        content=req_body,
                        timeout=60.0
                    )
                    response = await response_stream.__aenter__()
                    
                    resp_headers = {k: v for k, v in response.headers.items() if k.lower() not in exclude_headers}
                    
                    content_type = response.headers.get("content-type", "")
                    if "application/json" in content_type.lower():
                        body_bytes = await response.aread()
                        await response_stream.__aexit__(None, None, None)
                        await client.aclose()
                        body_str = body_bytes.decode("utf-8", errors="replace")
                        filtered_body = filter_tools_response(body_str, ep_cfg.allowed_tools, ep_cfg.denied_tools)
                        return Response(
                            content=filtered_body.encode("utf-8"),
                            status_code=response.status_code,
                            headers=resp_headers,
                            media_type=content_type
                        )
                    elif "event-stream" in content_type.lower():
                        async def sse_content_generator():
                            try:
                                async for line in response.aiter_lines():
                                    filtered_line = filter_tools_response(line, ep_cfg.allowed_tools, ep_cfg.denied_tools)
                                    yield (filtered_line + "\n").encode("utf-8")
                            finally:
                                await response_stream.__aexit__(None, None, None)
                                await client.aclose()
                                
                        return StreamingResponse(
                            sse_content_generator(),
                            status_code=response.status_code,
                            headers=resp_headers,
                            media_type=content_type
                        )
                    else:
                        async def content_generator():
                            try:
                                async for chunk in response.aiter_bytes():
                                    yield chunk
                            finally:
                                await response_stream.__aexit__(None, None, None)
                                await client.aclose()
                                
                        return StreamingResponse(
                            content_generator(),
                            status_code=response.status_code,
                            headers=resp_headers,
                            media_type=content_type
                        )
                except Exception as e:
                    logger.error(f"Proxy error for {path_prefix}: {e}")
                    return JSONResponse({"error": f"Proxy error: {e}"}, status_code=502)

# Determine config file path relative to server.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

@asynccontextmanager
async def lifespan(app: Starlette):
    logger.info("Initializing MCP Router Lifespan...")
    
    # Start dynamic configuration file watcher
    watcher = ConfigWatcher(CONFIG_PATH, router.apply_configuration)
    await watcher.start()
    
    # Perform initial configuration load
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                data = yaml.safe_load(f) or {}
            initial_config = RouterConfig.model_validate(data)
            router.apply_configuration(initial_config)
        except Exception as e:
            logger.error(f"Failed to apply initial configuration: {e}")
            
    # Start idle timeout checker
    router._running = True
    router._checker_task = asyncio.create_task(router.idle_timeout_checker())
    
    try:
        yield
    finally:
        logger.info("Shutting down MCP Router Lifespan...")
        router._running = False
        if router._checker_task:
            router._checker_task.cancel()
            try:
                await router._checker_task
            except asyncio.CancelledError:
                pass
        await watcher.stop()
        await router.process_manager.cleanup()

app = Starlette(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
router = MCPRouter(app, CONFIG_PATH)
