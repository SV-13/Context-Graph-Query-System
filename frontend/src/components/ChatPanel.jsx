import { useState, useRef, useEffect } from 'react'
import ReactMarkdown from 'react-markdown'

/**
 * Chat interface for natural language queries against the O2C dataset.
 * Maintains conversation history for context and displays SQL queries
 * that were generated behind the scenes.
 */

const SAMPLE_QUERIES = [
  "Which products are associated with the highest number of billing documents?",
  "Show me sales orders that were delivered but not billed",
  "Trace the full flow of billing document 90504248",
  "Which customers have the highest total order value?",
  "Find sales orders with incomplete flows",
]

export default function ChatPanel({ apiBase, onHighlightNodes, onExpandNode }) {
  const [messages, setMessages] = useState([])
  const [input, setInput] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const messagesEndRef = useRef(null)

  // Auto-scroll to latest message
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isLoading])

  const sendMessage = async (text) => {
    const question = text || input.trim()
    if (!question || isLoading) return

    setInput('')

    // Add user message
    const userMsg = { role: 'user', content: question }
    const newMessages = [...messages, userMsg]
    setMessages(newMessages)
    setIsLoading(true)

    try {
      const response = await fetch(`${apiBase}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          question,
          conversation_history: newMessages.slice(-8),
        }),
      })

      const data = await response.json()

      const assistantMsg = {
        role: 'assistant',
        content: data.answer,
        sql: data.sql,
        highlight_nodes: data.highlight_nodes,
      }

      setMessages([...newMessages, assistantMsg])

      // Highlight referenced nodes in the graph
      if (data.highlight_nodes?.length > 0) {
        onHighlightNodes(data.highlight_nodes)
        // Expand the first highlighted node
        if (data.highlight_nodes.length <= 5) {
          onExpandNode(data.highlight_nodes[0])
        }
      }
    } catch (error) {
      setMessages([
        ...newMessages,
        {
          role: 'assistant',
          content: 'Sorry, I encountered an error connecting to the server. Please try again.',
        },
      ])
    } finally {
      setIsLoading(false)
    }
  }

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const clearChat = () => {
    setMessages([])
  }

  return (
    <div className="chat-panel">
      <div className="chat-header">
        <div>
          <h2>ERP Copilot</h2>
          <p className="chat-subtitle">Ask anything about O2C flows</p>
        </div>
        {messages.length > 0 && (
          <button className="clear-btn" onClick={clearChat}>Clear</button>
        )}
      </div>

      <div className="chat-messages">
        {messages.length === 0 && (
          <div className="welcome-card">
            <h3>Ask about your ERP Data</h3>
            <p>Try one of these:</p>
            <div className="sample-queries">
              {SAMPLE_QUERIES.map((q, i) => (
                <button
                  key={i}
                  className="sample-query"
                  onClick={() => sendMessage(q)}
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        )}

        {messages.map((msg, idx) => (
          <Message key={idx} message={msg} />
        ))}

        {isLoading && (
          <div className="message assistant">
            <div className="typing-indicator">
              <span className="typing-dot" />
              <span className="typing-dot" />
              <span className="typing-dot" />
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      <div className="chat-input-area">
          <textarea
            className="chat-input"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about sales orders, deliveries, billing..."
            rows={1}
          />
          <button
            className="send-btn"
            onClick={() => sendMessage()}
            disabled={!input.trim() || isLoading}
          >
            ➤
          </button>
        </div>
      </div>
  )
}

function Message({ message }) {
  const [showSql, setShowSql] = useState(false)

  return (
    <div className={`message ${message.role}`}>
      <div className="message-bubble">
        {message.role === 'assistant' ? (
          <ReactMarkdown>{message.content}</ReactMarkdown>
        ) : (
          <p>{message.content}</p>
        )}

      {message.sql && (
        <div className="sql-section">
          <button className="sql-toggle" onClick={() => setShowSql(!showSql)}>
            {showSql ? 'Hide SQL' : 'View SQL Query'}
          </button>
          {showSql && (
            <pre className="sql-block">{message.sql}</pre>
          )}
        </div>
      )}
    </div>
    </div>
  )
}
