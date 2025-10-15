# app.py
import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from google.analytics.data_v1beta import (
    BetaAnalyticsDataClient,
    RunReportRequest,
    DateRange,
    Metric,
    Dimension,
    FilterExpression,
    Filter,
)

AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN")
app = FastAPI(title="GA4 MCP HTTP Server", version="1.1.0")

# ──────────────────────────────────────────────────────────────────────────────
# Auth helper
# ──────────────────────────────────────────────────────────────────────────────
def require_bearer(request: Request) -> None:
    if not AUTH_TOKEN:
        raise HTTPException(status_code=500, detail="Server missing MCP_AUTH_TOKEN")
    header = request.headers.get("authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = header.split(" ", 1)[1].strip()
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


# ──────────────────────────────────────────────────────────────────────────────
# Shared GA4 logic
# ──────────────────────────────────────────────────────────────────────────────
def build_dimension_filter(args: Dict[str, Any]) -> Optional[FilterExpression]:
    """Supports a simple equality filter for eventName."""
    df = args.get("dimensionFilter")
    if not df:
        return None
    if "eventName" in df and df["eventName"]:
        return FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(
                    value=df["eventName"],
                    match_type=Filter.StringFilter.MatchType.EXACT,
                ),
            )
        )
    return None


def run_ga4_report(args: Dict[str, Any]) -> Dict[str, Any]:
    # Required
    prop = args["property"]  # "properties/<NUMERIC_ID>"
    metrics_in = [Metric(name=m["name"]) for m in args["metrics"]]
    date_ranges_in = [
        DateRange(start_date=r["startDate"], end_date=r["endDate"])
        for r in args["dateRanges"]
    ]
    # Optional
    dims_in = [Dimension(name=d["name"]) for d in args.get("dimensions", [])]
    limit = int(args.get("limit", 1000))
    dim_filter = build_dimension_filter(args)

    client = BetaAnalyticsDataClient()  # uses Cloud Run SA via ADC
    req = RunReportRequest(
        property=prop,
        metrics=metrics_in,
        dimensions=dims_in,
        date_ranges=date_ranges_in,
        limit=limit,
        dimension_filter=dim_filter,
    )
    resp = client.run_report(req)

    headers = [h.name for h in resp.dimension_headers] + [
        h.name for h in resp.metric_headers
    ]
    rows: List[Dict[str, Any]] = []
    for r in resp.rows:
        vals = [v.string_value for v in r.dimension_values] + [
            m.value for m in r.metric_values
        ]
        rows.append({k: v for k, v in zip(headers, vals)})

    return {"ok": True, "rowCount": len(rows), "rows": rows}


# ──────────────────────────────────────────────────────────────────────────────
# Health
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "service": "ga4-mcp-http"}


# ──────────────────────────────────────────────────────────────────────────────
# Legacy endpoints (for curl)
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/tools")
async def legacy_tools(request: Request):
    require_bearer(request)
    return {
        "tools": [
            {
                "name": "run_report",
                "description": "Run a GA4 Core (Data API) report.",
                "input_schema": {
                    "type": "object",
                    "required": ["property", "metrics", "dateRanges"],
                    "properties": {
                        "property": {
                            "type": "string",
                            "description": "GA4 property resource: 'properties/<NUMERIC_ID>'",
                            "example": "properties/123456789",
                        },
                        "metrics": {
                            "type": "array",
                            "items": {"type": "object", "properties": {"name": {"type": "string"}}},
                            "example": [{"name": "activeUsers"}],
                        },
                        "dimensions": {
                            "type": "array",
                            "items": {"type": "object", "properties": {"name": {"type": "string"}}},
                            "example": [{"name": "date"}],
                        },
                        "dateRanges": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "startDate": {"type": "string", "example": "7daysAgo"},
                                    "endDate": {"type": "string", "example": "yesterday"},
                                },
                                "required": ["startDate", "endDate"],
                            },
                            "example": [{"startDate": "7daysAgo", "endDate": "yesterday"}],
                        },
                        "limit": {"type": "integer", "default": 1000},
                        "dimensionFilter": {
                            "type": "object",
                            "description": 'Optional simple filter. Example: {"eventName":"purchase"}',
                            "example": {"eventName": "purchase"},
                        },
                    },
                },
            }
        ]
    }


@app.post("/call")
async def legacy_call(request: Request):
    require_bearer(request)
    body = await request.json()
    tool = body.get("toolName")
    if tool != "run_report":
        raise HTTPException(status_code=400, detail=f"Unknown tool: {tool}")
    args = body.get("arguments", {})
    try:
        result = run_ga4_report(args)
        return JSONResponse(result)
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {e.args[0]}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# ──────────────────────────────────────────────────────────────────────────────
# MCP HTTP Transport — POST /
#   Accepts:
#     {"id":"1","method":"tools/list","params":{}}
#     {"id":"2","method":"tools/call","params":{"name":"run_report","arguments":{...}}}
#   Returns:
#     {"id":"1","result":{"tools":[...]}}
#     {"id":"2","result":{"content":[{"type":"json","data":{...}}]}}
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/")
async def mcp_http(request: Request):
    require_bearer(request)
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    req_id = payload.get("id")
    method = payload.get("method")
    params = payload.get("params", {}) or {}

    if not method:
        raise HTTPException(status_code=400, detail="Missing 'method'")

    # tools/list
    if method == "tools/list":
        tools = (await legacy_tools(request))["tools"]  # reuse same tool description
        return JSONResponse({"id": req_id, "result": {"tools": tools}})

    # tools/call
    if method == "tools/call":
        name = params.get("name")
        if name != "run_report":
            return JSONResponse(
                {"id": req_id, "error": {"code": 400, "message": f"Unknown tool: {name}"}},
                status_code=400,
            )
        args = params.get("arguments", {}) or {}
        try:
            result = run_ga4_report(args)
            # MCP content payload — simple JSON wrapper
            return JSONResponse(
                {"id": req_id, "result": {"content": [{"type": "json", "data": result}]}}
            )
        except KeyError as e:
            return JSONResponse(
                {"id": req_id, "error": {"code": 400, "message": f"Missing required field: {e.args[0]}"}},
                status_code=400,
            )
        except Exception as e:
            return JSONResponse(
                {"id": req_id, "error": {"code": 400, "message": str(e)}},
                status_code=400,
            )

    # Unknown method
    return JSONResponse(
        {"id": req_id, "error": {"code": 400, "message": f"Unknown method: {method}"}},
        status_code=400,
    )
