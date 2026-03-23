import { useEffect, useState } from "react";
import ChatPanel from "./components/ChatPanel";
import GraphCanvas from "./components/GraphCanvas";
import NodeInspector from "./components/NodeInspector";

const API_BASE = "http://localhost:8000/api";

const typePalette = {
  customer: "#245c73",
  sales_order: "#b35c1e",
  sales_order_item: "#e2a354",
  delivery: "#307351",
  delivery_item: "#74a97f",
  billing_document: "#7d2f4f",
  billing_item: "#bf6b89",
  accounting_document: "#3e447a",
  product: "#7d6130",
  plant: "#4a5c2f",
  storage_location: "#7b857a",
  address: "#46606f"
};

function App() {
  const [summary, setSummary] = useState(null);
  const [examples, setExamples] = useState({ supported: [], unsupported: [] });
  const [graph, setGraph] = useState({ nodes: [], edges: [], focus_node_id: null });
  const [selectedNodeId, setSelectedNodeId] = useState(null);
  const [selectedNode, setSelectedNode] = useState(null);
  const [highlightNodeIds, setHighlightNodeIds] = useState([]);
  const [messages, setMessages] = useState([]);
  const [searchText, setSearchText] = useState("billing_document:90504298");
  const [searchResults, setSearchResults] = useState([]);
  const [depth, setDepth] = useState(2);
  const [status, setStatus] = useState("Loading dataset summary...");

  useEffect(() => {
    async function bootstrap() {
      try {
        const [summaryRes, examplesRes] = await Promise.all([
          fetch(`${API_BASE}/summary`),
          fetch(`${API_BASE}/examples`)
        ]);
        const summaryJson = await summaryRes.json();
        const examplesJson = await examplesRes.json();
        setSummary(summaryJson);
        setExamples(examplesJson);
        setStatus("Dataset loaded.");
        const defaultNodeId = "billing_document:90504298";
        setSelectedNodeId(defaultNodeId);
        await loadGraph(defaultNodeId, 2, []);
      } catch (error) {
        setStatus(`Failed to load app metadata: ${error.message}`);
      }
    }
    bootstrap();
  }, []);

  async function loadGraph(nodeId, nextDepth = depth, nextHighlights = highlightNodeIds) {
    if (!nodeId) return;
    const response = await fetch(`${API_BASE}/graph?node_id=${encodeURIComponent(nodeId)}&depth=${nextDepth}`);
    const payload = await response.json();
    setGraph(payload);
    setSelectedNodeId(nodeId);
    setHighlightNodeIds(nextHighlights);
    await loadNode(nodeId);
  }

  async function loadNode(nodeId) {
    const response = await fetch(`${API_BASE}/node/${encodeURIComponent(nodeId)}`);
    if (response.ok) {
      setSelectedNode(await response.json());
    } else {
      setSelectedNode(null);
    }
  }

  async function handleSearch(value) {
    setSearchText(value);
    const trimmed = value.trim();
    if (!trimmed || trimmed.includes(":")) {
      setSearchResults([]);
      return;
    }
    const response = await fetch(`${API_BASE}/search?q=${encodeURIComponent(trimmed)}`);
    const payload = await response.json();
    setSearchResults(payload);
  }

  async function handleSendMessage(text) {
    const nextHistory = [...messages];
    const response = await fetch(`${API_BASE}/chat`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: text,
        history: nextHistory.map((item) => ({
          role: item.role,
          content: item.content,
          context: item.context || null
        }))
      })
    });
    const payload = await response.json();
    const nextMessages = [
      ...nextHistory,
      { role: "user", content: text },
      {
        role: "assistant",
        content: payload.answer,
        context: payload.context,
        evidence: payload.evidence,
        queryPlan: payload.query_plan,
        status: payload.status
      }
    ];
    setMessages(nextMessages);
    if (payload.focus_node_id) {
      await loadGraph(payload.focus_node_id, depth, payload.highlight_node_ids || []);
    }
  }

  const nodeCounts = summary?.node_counts || [];
  const dataNotes = summary?.data_quality_notes || [];

  return (
    <div className="app-shell">
      <aside className="left-rail">
        <div className="brand-card">
          <p className="eyebrow">Graph + Query Demo</p>
          <h1>Graph-Based Data Modeling and Query System</h1>
          <p>
            Dataset-grounded exploration of the supplied SAP O2C snapshot with strict
            domain guardrails.
          </p>
          <p className="status-line">{status}</p>
        </div>

        <div className="panel">
          <h2>Graph Controls</h2>
          <label className="field">
            <span>Focus node</span>
            <input
              value={searchText}
              onChange={(event) => handleSearch(event.target.value)}
              placeholder="Search label or paste node id"
            />
          </label>
          {searchResults.length > 0 && (
            <div className="search-results">
              {searchResults.map((result) => (
                <button
                  key={result.id}
                  className="search-item"
                  onClick={() => {
                    setSearchText(result.id);
                    setSearchResults([]);
                    loadGraph(result.id, depth, []);
                  }}
                >
                  <strong>{result.label}</strong>
                  <span>{result.type}</span>
                </button>
              ))}
            </div>
          )}
          <label className="field">
            <span>Neighborhood depth</span>
            <select value={depth} onChange={(event) => setDepth(Number(event.target.value))}>
              <option value={1}>1 hop</option>
              <option value={2}>2 hops</option>
              <option value={3}>3 hops</option>
            </select>
          </label>
          <button className="primary-button" onClick={() => loadGraph(searchText, depth, [])}>
            Load graph
          </button>
        </div>

        <div className="panel">
          <h2>Dataset Notes</h2>
          <div className="metric-grid">
            {nodeCounts.map((item) => (
              <div key={item.type} className="metric-card">
                <span>{item.type.replaceAll("_", " ")}</span>
                <strong>{item.count}</strong>
              </div>
            ))}
          </div>
          <ul className="note-list">
            {dataNotes.slice(0, 5).map((note) => (
              <li key={note}>{note}</li>
            ))}
          </ul>
        </div>

        <div className="panel">
          <h2>Example Questions</h2>
          <div className="question-list">
            {examples.supported.map((question) => (
              <button key={question} className="ghost-button" onClick={() => handleSendMessage(question)}>
                {question}
              </button>
            ))}
          </div>
        </div>
      </aside>

      <main className="main-column">
        <section className="graph-panel">
          <div className="panel-header">
            <div>
              <p className="eyebrow">Interactive Graph</p>
              <h2>Entity and flow visualization</h2>
            </div>
            <div className="legend">
              {Object.entries(typePalette).slice(0, 8).map(([type, color]) => (
                <span key={type}>
                  <i style={{ background: color }} />
                  {type.replaceAll("_", " ")}
                </span>
              ))}
            </div>
          </div>
          <GraphCanvas
            graph={graph}
            selectedNodeId={selectedNodeId}
            highlightNodeIds={highlightNodeIds}
            onSelectNode={(nodeId) => {
              setSelectedNodeId(nodeId);
              loadNode(nodeId);
            }}
            onExpandNode={(nodeId) => loadGraph(nodeId, depth, highlightNodeIds)}
            palette={typePalette}
          />
        </section>

        <section className="bottom-grid">
          <NodeInspector
            node={selectedNode}
            onExpand={() => selectedNodeId && loadGraph(selectedNodeId, depth, highlightNodeIds)}
          />
          <ChatPanel messages={messages} onSend={handleSendMessage} />
        </section>
      </main>
    </div>
  );
}

export default App;

