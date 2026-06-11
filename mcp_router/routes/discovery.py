from starlette.requests import Request
from starlette.responses import JSONResponse

def setup_discovery(mcp_server, configs_dict):
    """
    Registers the custom /summary route directly on the FastMCP instance.
    Returns a light representation of the active subserver routes and their summaries.
    """
    @mcp_server.custom_route("/summary", methods=["GET"])
    async def get_summary_manifest(request: Request) -> JSONResponse:
        summary_list = []
        for path, cfg in configs_dict.items():
            summary_list.append({
                "path": cfg.path,
                "mode": cfg.mode,
                "summary": cfg.summary
            })
        return JSONResponse({"endpoints": summary_list})
