import argparse
import uvicorn
from mcp_router.server import app

def main():
    """Parse command line arguments and launch the orchestrator server."""
    parser = argparse.ArgumentParser(description="Launch the Dynamic Multi-Endpoint Python MCP Router")
    parser.add_argument(
        "--port",
        type=int,
        default=8012,
        help="Port to run the HTTP transport server on (default: 8012)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host interface to bind to (default: 127.0.0.1)"
    )
    args = parser.parse_args()

    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main()
