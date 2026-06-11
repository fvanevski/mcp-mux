import asyncio
import os
import signal
import logging
from typing import Dict, List, Optional
from .config_loader import EndpointConfig

logger = logging.getLogger(__name__)

class ProcessManager:
    """
    Registry for managing local subprocess lifecycles.
    Spawns background processes using asyncio.create_subprocess_exec, captures logs,
    and isolates zombies safely using Unix process groups.
    """
    _instance: Optional["ProcessManager"] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(ProcessManager, cls).__new__(cls)
            cls._instance._processes = {}
            cls._instance._log_tasks = []
        return cls._instance

    def __init__(self):
        if hasattr(self, "_initialized"):
            return
        self._initialized = True

    def is_running(self, path: str) -> bool:
        """Returns True if the process for path is running and hasn't exited."""
        proc = self._processes.get(path)
        if proc is None:
            return False
        return proc.returncode is None

    async def start_managed_server(self, endpoint_cfg: EndpointConfig) -> str:
        """
        Spawns a local background server process using asyncio.create_subprocess_shell.
        Returns the streamable target HTTP url.
        """
        path = endpoint_cfg.path
        if self.is_running(path):
            logger.info(f"Process for path '{path}' is already running.")
            return endpoint_cfg.url

        command = endpoint_cfg.command
        target_url = endpoint_cfg.url

        # Parse port and host from url
        from urllib.parse import urlparse
        parsed = urlparse(target_url)
        port = parsed.port or 80
        host = parsed.hostname or "127.0.0.1"

        logger.info(f"Spawning background shell process for path '{path}': {command} on port {port}")

        try:
            # We use preexec_fn=os.setsid on Unix to isolate the process in a new process group.
            # This ensures that when we kill the shell, we cleanly kill all of its children.
            proc = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                preexec_fn=os.setsid if hasattr(os, "setsid") else None
            )
            self._processes[path] = proc

            # Start non-blocking streams to consume logs and avoid buffer overflows
            task_stdout = asyncio.create_task(self._stream_logs(proc.stdout, f"{path}:stdout"))
            task_stderr = asyncio.create_task(self._stream_logs(proc.stderr, f"{path}:stderr"))
            self._log_tasks.extend([task_stdout, task_stderr])

            # Wait for local HTTP server port readiness
            success = await self._wait_for_port(port, host=host)
            if not success:
                if proc.returncode is not None:
                    raise RuntimeError(f"Process terminated instantly with exit code {proc.returncode}")
                raise TimeoutError(f"Local HTTP service on port {port} failed to become ready in time")

            logger.info(f"Subserver for path '{path}' is ready at {target_url}")
            return target_url

        except Exception as e:
            logger.error(f"Failed to launch subserver for path '{path}': {e}")
            await self.stop_managed_server(path)
            raise

    async def stop_managed_server(self, path: str):
        """Terminates process and process groups cleanly."""
        proc = self._processes.pop(path, None)
        if proc:
            logger.info(f"Terminating subprocess group for '{path}' (PID {proc.pid})")
            try:
                if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                    try:
                        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                else:
                    proc.terminate()

                try:
                    await asyncio.wait_for(proc.wait(), timeout=3.0)
                except asyncio.TimeoutError:
                    logger.warning(f"Process for '{path}' (PID {proc.pid}) did not exit. Forcing SIGKILL.")
                    if hasattr(os, "killpg") and hasattr(os, "getpgid"):
                        try:
                            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                    else:
                        proc.kill()
                    await proc.wait()
            except Exception as e:
                logger.error(f"Error terminating process group for '{path}': {e}")

    async def _stream_logs(self, stream: asyncio.StreamReader, prefix: str):
        """Asynchronously consumes process outputs to prevent terminal logging corruption."""
        try:
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded = line.decode('utf-8', errors='replace').rstrip()
                logger.info(f"[{prefix}] {decoded}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error streaming logs for {prefix}: {e}")

    async def _wait_for_port(self, port: int, host: str = "127.0.0.1", timeout: float = 15.0) -> bool:
        """Asynchronously polls local port readiness via TCP connections."""
        start_time = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                _, writer = await asyncio.open_connection(host, port)
                writer.close()
                await writer.wait_closed()
                return True
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(0.2)
        return False

    async def cleanup(self):
        """Clean up all managed processes and streams."""
        logger.info("Initiating cleanup of all active subprocesses.")
        paths = list(self._processes.keys())
        for path in paths:
            await self.stop_managed_server(path)

        for task in self._log_tasks:
            if not task.done():
                task.cancel()
        if self._log_tasks:
            await asyncio.gather(*self._log_tasks, return_exceptions=True)
            self._log_tasks.clear()
        logger.info("Process manager cleanup complete.")
