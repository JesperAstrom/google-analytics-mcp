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

# ──────────────────────────────────────────────────────────────────────────────
# Config / Auth
# ──────────────────────────────────────────────────────────────────────────────
AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN")

app = FastAPI(title="GA4 MCP HTTP Server", version="1.0.0")


def check_auth(request: Request) -> None:
    """Enforce 'Authorization: Bearer <token>' with env MCP_AUTH_TOKEN."""
    if not AUTH_TOKEN:
        # This is a server config error; surface loudly so you fix env/secret.
        raise HTTPException(status_code=500, detail="Server missing MCP_AUTH_TOKEN")
    header = request.headers.get("authorization", "")
    if not header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = header.split(" ", 1)[1].strip()
    if token != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")


# ──────────────────────────────────────────────────────────────────────────────
# Utility: build optional dimension filter (supports eventName equality)
# Extend here if you need more filters.
# ──────────────────────────────────────────────────────────────────────────────
def build_dimension_filter(args: Dict[str, Any]) -> Optional[FilterExpression]:
    """
    args may include:
      "dimensionFilter": { "eventName": "purchase" }
    This builds a FilterExpression: eventName == "purchase"
    """
    df = args.get("dimensionFilter")
    if not df:
        return None

    # Only support simple equality for eventName for now; extend as needed.
    event_name = df.get("eventName")
    if event_name:
        return FilterExpression(
            filter=Filter(
                field_name="eventName",
                string_filter=Filter.StringFilter(value=event_name, match_type=Filter.StringFilter.MatchType.EXACT),
            )
        )

    # If present but unrecognized, ignore rather than fail hard.
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Health / Root
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "service": "ga4-mcp-http"}


# ──────────────────────────────────────────────────────────────────────────────
# MCP: List Tools
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/tools")
async def list_tools(request: Request):
    check_auth(request)
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
                            "description": "GA4 property resource name: 'properties/<NUMERIC_ID>'",
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
                            "description": 'Optional simple filter. Currently supports {"eventName":"purchase"}',
                            "example": {"eventName": "purchase"},
                        },
                    },
                },
            }
        ]
    }


# ──────────────────────────────────────────────────────────────────────────────
# MCP: Call Tool
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/call")
async def call_tool(request: Request):
    check_auth(request)
    body: Dict[str, Any] = await request.json()

    tool = body.get("toolName")
    if tool != "run_report":
        raise HTTPException(status_code=400, detail=f"Unknown tool: {tool}")

    args: Dict[str, Any] = body.get("arguments", {})

    # Parse required args
    try:
        prop = args["property"]  # "properties/<NUMERIC_ID>"
        metrics_in = [Metric(name=m["name"]) for m in args["metrics"]]
        date_ranges_in = [
            DateRange(start_date=r["startDate"], end_date=r["endDate"]) for r in args["dateRanges"]
        ]
    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Missing required field: {e.args[0]}")

    # Optional args
    dims_in = [Dimension(name=d["name"]) for d in args.get("dimensions", [])]
    limit = int(args.get("limit", 1000))
    dim_filter = build_dimension_filter(args)

    # Call GA4 Data API using ADC (Cloud Run service account)
    try:
        client = BetaAnalyticsDataClient()
        req = RunReportRequest(
            property=prop,
            metrics=metrics_in,
            dimensions=dims_in,
            date_ranges=date_ranges_in,
            limit=limit,
            dimension_filter=dim_filter,
        )
        resp = client.run_report(req)

        # Flatten response rows into a list of dicts
        headers = [h.name for h in resp.dimension_headers] + [h.name for h in resp.metric_headers]
        rows: List[Dict[str, Any]] = []
        for r in resp.rows:
            vals = [v.string_value for v in r.dimension_values] + [m.value for m in r.metric_values]
            rows.append({k: v for k, v in zip(headers, vals)})

        return JSONResponse({"ok": True, "rowCount": len(rows), "rows": rows})

    except Exception as e:
        # Surface the exact error to the client for easier debugging
        # (e.g., PERMISSION_DENIED, INVALID_ARGUMENT, NOT_FOUND)
        raise HTTPException(status_code=400, detail=str(e))
