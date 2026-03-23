import { useState } from "react";

function EvidenceTable({ evidence }) {
  if (!evidence || evidence.rows.length === 0) return null;
  return (
    <div className="evidence-card">
      <h4>{evidence.title}</h4>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              {evidence.columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {evidence.rows.slice(0, 8).map((row, index) => (
              <tr key={`${evidence.title}-${index}`}>
                {evidence.columns.map((column) => (
                  <td key={`${index}-${column}`}>{String(row[column] ?? "")}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default function ChatPanel({ messages, onSend }) {
  const [value, setValue] = useState("");

  async function handleSubmit(event) {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) return;
    setValue("");
    await onSend(trimmed);
  }

  return (
    <div className="panel chat-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Grounded Chat</p>
          <h2>Ask dataset-only questions</h2>
        </div>
      </div>

      <div className="message-list">
        {messages.length === 0 && (
          <div className="empty-state">
            Ask for an order, invoice, delivery, anomaly, or graph neighborhood.
          </div>
        )}
        {messages.map((message, index) => (
          <div key={`${message.role}-${index}`} className={`message ${message.role}`}>
            <p>{message.content}</p>
            {message.role === "assistant" && (
              <>
                {message.queryPlan && (
                  <div className="query-plan">
                    <strong>{message.queryPlan.intent}</strong>
                    {message.queryPlan.sql && <code>{message.queryPlan.sql}</code>}
                    {message.queryPlan.notes?.map((note) => (
                      <span key={note}>{note}</span>
                    ))}
                  </div>
                )}
                {message.evidence?.map((evidence) => (
                  <EvidenceTable key={evidence.title} evidence={evidence} />
                ))}
              </>
            )}
          </div>
        ))}
      </div>

      <form className="chat-form" onSubmit={handleSubmit}>
        <textarea
          rows={4}
          value={value}
          onChange={(event) => setValue(event.target.value)}
          placeholder="Trace the full flow for billing document 90504298"
        />
        <button className="primary-button" type="submit">
          Send
        </button>
      </form>
    </div>
  );
}

