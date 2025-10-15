import os
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any, List

# GA4 Data API
from google.analytics.data_v1beta import (
    BetaAnalyticsDataClient,
    RunReportRequest,
    DateRange,
    Metric,
    Dimension,
)

AUTH_TOKEN = os.environ.get("MCP_AUTH_TOKEN")  # 👈 read from env/secret
app = FastAPI()

def check_auth(request: Request):
    if not AUTH_TOKEN:
        raise HTTPException(status_code=500, detail="Server missing MCP_AUTH_TOKEN")
    hdr = request.headers.get("authorization", "")
    if not hdr.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    if hdr.split(" ", 1)[1].strip() != AUTH_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.get("/tools")
async def list_tools(request: Request):
    check_auth(request)
    return {
        "tools": [
            {
                "name": "run_report",
                "description": "Run a GA4 Core report via the Analytics Data API.",
                "input_schema": {
                    "type": "object",
                    "required": ["property", "metrics", "dateRanges"],
                    "properties": {
                        "property": {"type": "string", "example": "properties/123456789"},
                        "metrics": {
                            "type": "array",
                            "items": {"type": "object", "properties": {"name": {"type": "string"}}},
                            "example": [{"name": "purchaseCount"}],
                        },
                        "dimensions": {
                            "type": "array",
                            "items": {"type": "object", "properties": {"name": {"type": "string"}}},
                            "example": [{"name": "date"}],
                        },
                        "dateRanges": {
                            "type": "array",
                            "items": {"type": "object", "properties": {
                                "startDate": {"type": "string"},
                                "endDate": {"type": "string"}
                            }, "required": ["startDate","endDate"]},
                            "example": [{"startDate": "7daysAgo", "endDate": "yesterday"}],
                        },
                        "limit": {"type": "integer", "default": 1000}
                    }
                }
            }
        ]
    }

@app.post("/call")
async def call_tool(request: Request):
    check_auth(request)
    body: Dict[str, Any] = await request.json()
    if body.get("toolName") != "run_report":
        raise HTTPException(status_code=400, detail="Unknown tool")
    args = body.get("arguments", {})

    client = BetaAnalyticsDataClient()  # uses Cloud Run service account (ADC)
    req = RunReportRequest(
        property=args["property"],
        metrics=[Metric(name=m["name"]) for m in args["metrics"]],
        dimensions=[Dimension(name=d["name"]) for d in args.get("dimensions", [])],
        date_ranges=[DateRange(start_date=r["startDate"], end_date=r["endDate"]) for r in args["dateRanges"]],
        limit=int(args.get("limit", 1000)),
    )
    resp = client.run_report(req)

    rows: List[Dict[str, Any]] = [
        {h.name: (v.string_value or v.int_value or v.float_value or v.double_value)
         for h, v in zip(resp.dimension_headers + resp.metric_headers,
                         r.dimension_values + r.metric_values)}
        for r in resp.rows
    ]
    return JSONResponse({"ok": True, "rows": rows})
