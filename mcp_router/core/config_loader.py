import asyncio
import os
import logging
from typing import List, Optional, Callable
import yaml
from pydantic import BaseModel, Field, model_validator

logger = logging.getLogger(__name__)

class EndpointConfig(BaseModel):
    path: str
    mode: str  # "remote", "managed_cli", "stdio_bridge"
    url: Optional[str] = None
    command: Optional[str] = None
    args: List[str] = Field(default_factory=list)
    port: Optional[int] = None
    summary: str

    @model_validator(mode="after")
    def validate_mode_requirements(self) -> "EndpointConfig":
        """Validate configuration requirements based on the mode."""
        if self.mode == "remote":
            if not self.url:
                raise ValueError("url is required for remote mode")
        elif self.mode in ("managed_cli", "stdio_bridge"):
            if not self.command:
                raise ValueError(f"command is required for {self.mode} mode")
            if not self.port:
                raise ValueError(f"port is required for {self.mode} mode")
        else:
            raise ValueError(f"Invalid mode: {self.mode}")
        return self

class RouterConfig(BaseModel):
    endpoints: List[EndpointConfig]

    @model_validator(mode="after")
    def validate_ports_and_paths(self) -> "RouterConfig":
        """Enforce uniqueness for paths/namespaces and local ports."""
        ports = []
        paths = []
        for ep in self.endpoints:
            if ep.path in paths:
                raise ValueError(f"Duplicate path detected: {ep.path}")
            paths.append(ep.path)
            if ep.port is not None:
                if ep.port in ports:
                    raise ValueError(f"Duplicate port detected: {ep.port}")
                ports.append(ep.port)
        return self

class ConfigWatcher:
    """
    An asynchronous file watcher that polls config.yaml modification times.
    Triggers an event-safe async or sync callback when modifications are detected.
    """
    def __init__(self, config_path: str, callback: Callable[[RouterConfig], None], poll_interval: float = 1.0):
        self.config_path = config_path
        self.callback = callback
        self.poll_interval = poll_interval
        self._last_mtime: Optional[float] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._poll_loop())
        logger.info(f"Started config polling for: {self.config_path}")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped config polling.")

    async def _poll_loop(self):
        while self._running:
            try:
                if os.path.exists(self.config_path):
                    mtime = os.path.getmtime(self.config_path)
                    if self._last_mtime is None or mtime > self._last_mtime:
                        self._last_mtime = mtime
                        logger.info(f"Detected change in configuration: {self.config_path}")
                        try:
                            with open(self.config_path, "r") as f:
                                data = yaml.safe_load(f) or {}
                            new_config = RouterConfig.model_validate(data)
                            if asyncio.iscoroutinefunction(self.callback):
                                await self.callback(new_config)
                            else:
                                self.callback(new_config)
                        except Exception as e:
                            logger.error(f"Failed to load or validate new config: {e}")
            except Exception as e:
                logger.error(f"Error in config watcher poll loop: {e}")
            await asyncio.sleep(self.poll_interval)
