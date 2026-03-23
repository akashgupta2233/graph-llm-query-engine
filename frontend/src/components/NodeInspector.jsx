export default function NodeInspector({ node, onExpand }) {
  const entries = node ? Object.entries(node.metadata || {}) : [];

  return (
    <div className="panel inspector-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Inspector</p>
          <h2>{node ? node.label : "Select a node"}</h2>
        </div>
        {node && (
          <button className="ghost-button" onClick={onExpand}>
            Expand neighbors
          </button>
        )}
      </div>

      {!node && <div className="empty-state">Click a node in the graph to inspect its metadata and incident edges.</div>}

      {node && (
        <>
          <div className="inspector-meta">
            <span>{node.type}</span>
            <code>{node.id}</code>
          </div>
          <div className="metadata-list">
            {entries.slice(0, 18).map(([key, value]) => (
              <div key={key} className="metadata-row">
                <strong>{key}</strong>
                <span>{typeof value === "string" ? value : JSON.stringify(value)}</span>
              </div>
            ))}
          </div>
          <h3>Incident edges</h3>
          <div className="edge-list">
            {node.incident_edges.slice(0, 20).map((edge) => (
              <div key={edge.id} className="edge-item">
                <strong>{edge.type}</strong>
                <span>
                  {edge.source} → {edge.target}
                </span>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}

