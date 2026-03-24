from typing import Any, Literal

from pydantic import BaseModel, Field


class GraphNode(BaseModel):
    id: str
    type: str
    label: str
    source_table: str
    depth: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    id: str
    source: str
    target: str
    type: str
    source_table: str
    inferred: bool = False
    confidence: float = 1.0
    evidence: dict[str, Any] = Field(default_factory=dict)


class GraphResponse(BaseModel):
    nodes: list[GraphNode]
    edges: list[GraphEdge]
    focus_node_id: str | None = None


class SummaryResponse(BaseModel):
    generated_at: str
    dataset_path: str
    raw_tables: list[dict[str, Any]]
    node_counts: list[dict[str, Any]]
    edge_counts: list[dict[str, Any]]
    data_quality_notes: list[str]
    inferred_relationships: list[str]


class SearchResult(BaseModel):
    id: str
    type: str
    label: str
    source_table: str


class NodeDetail(BaseModel):
    id: str
    type: str
    label: str
    source_table: str
    metadata: dict[str, Any]
    incident_edges: list[GraphEdge]


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str
    context: dict[str, Any] | None = None


class ChatRequest(BaseModel):
    message: str
    history: list[ChatMessage] = Field(default_factory=list)


class QueryPlan(BaseModel):
    intent: str
    sql: str | None = None
    params: list[Any] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class EvidenceTable(BaseModel):
    title: str
    columns: list[str]
    rows: list[dict[str, Any]]


class ChatResponse(BaseModel):
    status: Literal["answered", "refused", "clarify", "cannot_answer", "error"]
    intent: str
    answer: str
    query_plan: QueryPlan
    evidence: list[EvidenceTable] = Field(default_factory=list)
    focus_node_id: str | None = None
    highlight_node_ids: list[str] = Field(default_factory=list)
    highlight_edge_ids: list[str] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)

