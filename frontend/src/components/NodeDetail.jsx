/**
 * Node detail overlay panel.
 * Clean, structured, enterprise-style UI.
 */
export default function NodeDetail({ node, onClose, onNavigate }) {
  if (!node) return null

  const hiddenFields = new Set([
    'nodeId', 'type', 'label', 'color', 'val',
    'neighbors', 'x', 'y', 'vx', 'vy', 'index', 'fx', 'fy'
  ])

  const properties = Object.entries(node)
    .filter(([key]) => !hiddenFields.has(key))
    .filter(([, val]) => val !== null && val !== '' && val !== undefined)

  return (
    <div className="node-detail-overlay">
      <div className="node-detail-card">

        {/* HEADER */}
        <div className="node-detail-header">
          <div className="header-left">
            <span
              className="node-type-badge"
              style={{ background: node.color || '#999' }}
            >
              {node.type}
            </span>

            <h3 className="node-title">
              {node.label || node.nodeId}
            </h3>
          </div>

          <button className="close-btn" onClick={onClose}>
            ✕
          </button>
        </div>

        {/* BODY */}
        <div className="node-detail-body">

          {/* PROPERTIES */}
          <div className="detail-section">
            <h4>Properties</h4>

            <div className="detail-card">
              {properties.map(([key, value]) => (
                <div className="detail-row" key={key}>
                  <span className="detail-key">{formatKey(key)}</span>
                  <span className="detail-value">{formatValue(value)}</span>
                </div>
              ))}
            </div>
          </div>

          {/* CONNECTED NODES */}
          {node.neighbors && node.neighbors.length > 0 && (
            <div className="detail-section">
              <h4>Connected Nodes ({node.neighbors.length})</h4>

              <div className="neighbor-list">
                {node.neighbors.map((neighbor, idx) => (
                  <div
                    key={idx}
                    className="neighbor-item"
                    onClick={() => onNavigate(neighbor.nodeId)}
                  >
                    <span className="neighbor-direction">
                      {neighbor.direction === 'incoming' ? '←' : '→'}
                    </span>

                    <span className="neighbor-label">
                      {neighbor.label || neighbor.id}
                    </span>

                    <span className="neighbor-relation">
                      {neighbor.relation}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          )}

        </div>
      </div>
    </div>
  )
}

//  Helpers

function formatKey(key) {
  return key
    .replace(/([A-Z])/g, ' $1')
    .replace(/^./, s => s.toUpperCase())
}

function formatValue(value) {
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'

  if (typeof value === 'number') {
    if (Number.isInteger(value)) return value.toLocaleString()
    return value.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    })
  }

  if (typeof value === 'string' && value.includes('T00:00:00')) {
    return value.split('T')[0]
  }

  return String(value)
}