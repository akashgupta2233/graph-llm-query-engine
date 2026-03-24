from __future__ import annotations

import json
from collections import deque

from app.db import db_session
from app.schemas import GraphEdge, GraphNode, GraphResponse, NodeDetail, SearchResult


class GraphService:
    def get_summary(self) -> dict:
        with db_session() as conn:
            rows = conn.execute("SELECT key, value FROM dataset_profile").fetchall()
        return {row["key"]: json.loads(row["value"]) for row in rows}

    def search_nodes(self, query: str, node_type: str | None = None, limit: int = 15) -> list[SearchResult]:
        where_clauses = ["(LOWER(label) LIKE ? OR LOWER(entity_key) LIKE ?)"]
        params = [f"%{query.lower()}%", f"%{query.lower()}%"]
        if node_type:
            where_clauses.append("node_type = ?")
            params.append(node_type)

        sql = f"""
            SELECT node_id, node_type, label, source_table
            FROM graph_nodes
            WHERE {' AND '.join(where_clauses)}
            ORDER BY label
            LIMIT ?
        """
        params.append(limit)

        with db_session() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            SearchResult(
                id=row["node_id"],
                type=row["node_type"],
                label=row["label"],
                source_table=row["source_table"],
            )
            for row in rows
        ]

    def get_node_detail(self, node_id: str) -> NodeDetail | None:
        with db_session() as conn:
            node = conn.execute(
                "SELECT node_id, node_type, label, source_table, metadata_json FROM graph_nodes WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if not node:
                return None
            edge_rows = conn.execute(
                """
                SELECT edge_id, source_node_id, target_node_id, edge_type, source_table, inferred, confidence, evidence_json
                FROM graph_edges
                WHERE source_node_id = ? OR target_node_id = ?
                ORDER BY edge_type
                LIMIT 100
                """,
                (node_id, node_id),
            ).fetchall()
        return NodeDetail(
            id=node["node_id"],
            type=node["node_type"],
            label=node["label"],
            source_table=node["source_table"],
            metadata=json.loads(node["metadata_json"]),
            incident_edges=[self._edge_from_row(row) for row in edge_rows],
        )

    def get_graph_slice(self, node_id: str, depth: int = 1, limit: int = 80) -> GraphResponse:
        with db_session() as conn:
            seed = conn.execute(
                "SELECT node_id FROM graph_nodes WHERE node_id = ?",
                (node_id,),
            ).fetchone()
            if not seed:
                return GraphResponse(nodes=[], edges=[], focus_node_id=node_id)

            queue = deque([(node_id, 0)])
            seen_depth: dict[str, int] = {node_id: 0}
            edge_rows = []
            while queue and len(seen_depth) < limit:
                current_id, current_depth = queue.popleft()
                if current_depth >= depth:
                    continue
                neighbors = conn.execute(
                    """
                    SELECT edge_id, source_node_id, target_node_id, edge_type, source_table, inferred, confidence, evidence_json
                    FROM graph_edges
                    WHERE source_node_id = ? OR target_node_id = ?
                    LIMIT 200
                    """,
                    (current_id, current_id),
                ).fetchall()
                for row in neighbors:
                    edge_rows.append(row)
                    neighbor_id = row["target_node_id"] if row["source_node_id"] == current_id else row["source_node_id"]
                    if neighbor_id not in seen_depth and len(seen_depth) < limit:
                        seen_depth[neighbor_id] = current_depth + 1
                        queue.append((neighbor_id, current_depth + 1))

            placeholders = ", ".join("?" for _ in seen_depth)
            node_rows = conn.execute(
                f"""
                SELECT node_id, node_type, label, source_table, metadata_json
                FROM graph_nodes
                WHERE node_id IN ({placeholders})
                """,
                tuple(seen_depth.keys()),
            ).fetchall()

        nodes = [
            GraphNode(
                id=row["node_id"],
                type=row["node_type"],
                label=row["label"],
                source_table=row["source_table"],
                depth=seen_depth[row["node_id"]],
                metadata=json.loads(row["metadata_json"]),
            )
            for row in node_rows
        ]

        unique_edges = {
            row["edge_id"]: row
            for row in edge_rows
            if row["source_node_id"] in seen_depth and row["target_node_id"] in seen_depth
        }
        edges = [self._edge_from_row(row) for row in unique_edges.values()]
        return GraphResponse(nodes=nodes, edges=edges, focus_node_id=node_id)

    @staticmethod
    def _edge_from_row(row) -> GraphEdge:
        return GraphEdge(
            id=row["edge_id"],
            source=row["source_node_id"],
            target=row["target_node_id"],
            type=row["edge_type"],
            source_table=row["source_table"],
            inferred=bool(row["inferred"]),
            confidence=row["confidence"],
            evidence=json.loads(row["evidence_json"]),
        )
