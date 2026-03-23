function buildLayout(nodes) {
  const byDepth = nodes.reduce((acc, node) => {
    const depth = node.depth || 0;
    acc[depth] = acc[depth] || [];
    acc[depth].push(node);
    return acc;
  }, {});

  const centerX = 460;
  const centerY = 280;
  const positions = {};

  Object.entries(byDepth).forEach(([depthText, depthNodes]) => {
    const depth = Number(depthText);
    if (depth === 0) {
      positions[depthNodes[0].id] = { x: centerX, y: centerY };
      return;
    }
    const radius = 120 + depth * 95;
    depthNodes.forEach((node, index) => {
      const angle = (Math.PI * 2 * index) / depthNodes.length;
      positions[node.id] = {
        x: centerX + Math.cos(angle) * radius,
        y: centerY + Math.sin(angle) * radius
      };
    });
  });

  return positions;
}

export default function GraphCanvas({
  graph,
  selectedNodeId,
  highlightNodeIds,
  onSelectNode,
  onExpandNode,
  palette
}) {
  const positions = buildLayout(graph.nodes || []);
  const highlights = new Set(highlightNodeIds || []);

  return (
    <div className="graph-canvas">
      <svg viewBox="0 0 920 560" className="graph-svg">
        {(graph.edges || []).map((edge) => {
          const source = positions[edge.source];
          const target = positions[edge.target];
          if (!source || !target) return null;
          return (
            <g key={edge.id}>
              <line
                x1={source.x}
                y1={source.y}
                x2={target.x}
                y2={target.y}
                stroke={highlights.has(edge.source) && highlights.has(edge.target) ? "#ffae42" : "#768699"}
                strokeWidth={highlights.has(edge.source) && highlights.has(edge.target) ? 3 : 1.6}
                opacity={0.75}
              />
              <text
                x={(source.x + target.x) / 2}
                y={(source.y + target.y) / 2 - 6}
                className="edge-label"
              >
                {edge.type}
              </text>
            </g>
          );
        })}
      </svg>
      {(graph.nodes || []).map((node) => {
        const position = positions[node.id];
        if (!position) return null;
        const color = palette[node.type] || "#4d6070";
        const isSelected = node.id === selectedNodeId;
        const isHighlighted = highlights.has(node.id);
        return (
          <button
            key={node.id}
            className={`graph-node ${isSelected ? "selected" : ""} ${isHighlighted ? "highlighted" : ""}`}
            style={{
              left: position.x,
              top: position.y,
              borderColor: color,
              background: isHighlighted ? `${color}` : "#102234"
            }}
            onClick={() => onSelectNode(node.id)}
            onDoubleClick={() => onExpandNode(node.id)}
            title={`${node.type} | ${node.id}`}
          >
            <span>{node.label}</span>
            <small>{node.type.replaceAll("_", " ")}</small>
          </button>
        );
      })}
    </div>
  );
}

