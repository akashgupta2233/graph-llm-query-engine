from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_summary_loads():
    response = client.get("/api/summary")
    assert response.status_code == 200
    payload = response.json()
    assert payload["raw_tables"]
    assert any(table["table"] == "sales_order_headers" for table in payload["raw_tables"])


def test_graph_slice_for_billing_document():
    response = client.get("/api/graph", params={"node_id": "billing_document:90504298", "depth": 2})
    assert response.status_code == 200
    payload = response.json()
    node_ids = {node["id"] for node in payload["nodes"]}
    assert "billing_document:90504298" in node_ids
    assert any(node_id.startswith("delivery:") for node_id in node_ids)


def test_entity_lookup_order():
    response = client.post("/api/chat", json={"message": "Show details for order 740506", "history": []})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "answered"
    assert "740506" in payload["answer"]


def test_aggregation_query():
    response = client.post(
        "/api/chat",
        json={"message": "Which products are associated with the highest number of billing documents?", "history": []},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "answered"
    assert payload["intent"] == "aggregation"
    assert payload["evidence"]


def test_trace_query():
    response = client.post(
        "/api/chat",
        json={"message": "Trace the full flow for billing document 90504298", "history": []},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "answered"
    assert payload["intent"] == "trace"
    assert "90504298" in payload["answer"]


def test_anomaly_query():
    response = client.post(
        "/api/chat",
        json={"message": "Find orders delivered but not billed", "history": []},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "answered"
    assert payload["intent"] == "anomaly"


def test_guardrail_refusal():
    response = client.post("/api/chat", json={"message": "Write me a poem about mountains", "history": []})
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "refused"
    assert "dataset only" in payload["answer"].lower()


def test_unsupported_missing_link_query_returns_grounded_proxy():
    response = client.post(
        "/api/chat",
        json={"message": "Which billing documents have no downstream payment?", "history": []},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "answered"
    assert "clearing" in payload["answer"].lower()
