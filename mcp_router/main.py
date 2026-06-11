import os
import asyncio
import logging
import yaml
from contextlib import asynccontextmanager
from fastmcp import FastMCP
from fastmcp.server import create_proxy


from mcp_router.core.config_loader import RouterConfig, ConfigWatcher, EndpointConfig
from mcp_router.core.process_manager import ProcessManager
from mcp_router.routes.discovery import setup_discovery

# Configure logging to console safely
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("mcp_router")

class MCPRouter:
    def __init__(self, mcp_server: FastMCP, config_path: str):
        self.mcp_server = mcp_server
        self.config_path = config_path
        self.process_manager = ProcessManager()
        self._mounted_configs: dict[str, EndpointConfig] = {}
        self._path_to_provider = {}

        # Set up the lightweight summary discovery route
        setup_discovery(self.mcp_server, self._mounted_configs)

    async def apply_configuration(self, config: RouterConfig):
        """
        Compares incoming configuration against currently mounted ones.
        Unmounts missing or changed endpoints, and mounts new/updated ones.
        """
        active_paths = list(self._mounted_configs.keys())
        new_endpoints = {ep.path: ep for ep in config.endpoints}

        # 1. Determine endpoints to unmount
        to_remove = []
        for path in active_paths:
            if path not in new_endpoints:
                to_remove.append(path)
            else:
                current_cfg = self._mounted_configs[path]
                new_cfg = new_endpoints[path]
                if current_cfg != new_cfg:
                    logger.info(f"Configuration change detected for path '{path}'. Will reload.")
                    to_remove.append(path)

        # Unmount and stop processes
        for path in to_remove:
            logger.info(f"Unmounting path '{path}'")
            provider = self._path_to_provider.pop(path, None)
            if provider and provider in self.mcp_server.providers:
                try:
                    self.mcp_server.providers.remove(provider)
                except Exception as e:
                    logger.error(f"Error removing provider for path '{path}': {e}")
            
            await self.process_manager.stop_managed_server(path)
            self._mounted_configs.pop(path, None)

        # 2. Mount new or updated endpoints
        for path, ep_cfg in new_endpoints.items():
            if path not in self._mounted_configs:
                logger.info(f"Setting up path '{path}' (mode: {ep_cfg.mode})")
                try:
                    if ep_cfg.mode == "remote":
                        target_url = ep_cfg.url
                    elif ep_cfg.mode in ("managed_cli", "stdio_bridge"):
                        target_url = await self.process_manager.start_managed_server(ep_cfg)
                    else:
                        logger.error(f"Unsupported mode '{ep_cfg.mode}' for path '{path}'")
                        continue

                    logger.info(f"Mounting proxy for path '{path}' to target {target_url}")
                    proxy = create_proxy(target_url)

                    # Capture the provider registered in main_mcp
                    providers_before = list(self.mcp_server.providers)
                    self.mcp_server.mount(proxy, namespace=ep_cfg.path)
                    providers_after = list(self.mcp_server.providers)

                    new_providers = [p for p in providers_after if p not in providers_before]
                    if new_providers:
                        self._path_to_provider[path] = new_providers[0]

                    self._mounted_configs[path] = ep_cfg
                    logger.info(f"Successfully mounted path '{path}'")

                except Exception as e:
                    logger.error(f"Failed to mount endpoint '{path}': {e}", exc_info=True)

# Determine config file path relative to main.py
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")

# Instantiate the main FastMCP server
main_mcp = FastMCP("Orchestrator")
router = MCPRouter(main_mcp, CONFIG_PATH)

@asynccontextmanager
async def mcp_lifespan(server: FastMCP):
    """
    Manages startup and shutdown lifespans for the orchestrator,
    including starting file watchers and stopping subprocesses.
    """
    logger.info("Initializing MCP Router Lifespan...")
    
    # 1. Perform initial configuration load & mount
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r") as f:
                data = yaml.safe_load(f) or {}
            initial_config = RouterConfig.model_validate(data)
            await router.apply_configuration(initial_config)
        except Exception as e:
            logger.error(f"Failed to apply initial configuration: {e}")
    else:
        logger.warning(f"Config file not found at startup: {CONFIG_PATH}")

    # 2. Start dynamic configuration file watcher
    watcher = ConfigWatcher(CONFIG_PATH, router.apply_configuration)
    await watcher.start()

    try:
        yield {}
    finally:
        logger.info("Shutting down MCP Router Lifespan...")
        await watcher.stop()
        await router.process_manager.cleanup()

# Bind lifespan to our main FastMCP server
main_mcp._lifespan = mcp_lifespan

if __name__ == "__main__":
    # Run the server using HTTP transport as requested
    main_mcp.run(transport="http", host="127.0.0.1", port=8000)
