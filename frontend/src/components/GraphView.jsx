import { useRef, useCallback, useEffect } from 'react'
import ForceGraph2D from 'react-force-graph-2d'

/**
 * Interactive force-directed graph visualization.
 * Uses react-force-graph-2d for WebGL-accelerated rendering.
 * Nodes are colored by type and sized by connectivity.
 */
export default function GraphView({ graphData, onNodeClick, highlightNodes, nodeColors }) {
  const graphRef = useRef()

   const limitedGraph = {
    nodes: graphData.nodes.slice(0, 300),
    links: graphData.links.slice(0, 500)
  }
  // Zoom to fit on data change
  useEffect(() => {
    if (graphRef.current && graphData.nodes.length > 0) {
      setTimeout(() => {
        graphRef.current.zoomToFit(400, 80)
      }, 400)
    }
  }, [graphData])

  // Custom node rendering with labels
  const paintNode = useCallback((node, ctx, globalScale) => {
    const isHighlighted = highlightNodes.has(node.nodeId)
    const size = Math.max(4, Math.sqrt(node.val || 1) * 3)

    ctx.globalAlpha = highlightNodes.size > 0
      ? (isHighlighted ? 1 : 0.15)
      : 1

    // Draw node circle
    ctx.beginPath()
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI)
    ctx.fillStyle = node.color || '#888'
    ctx.fill()

    // Highlight ring
    if (isHighlighted) {
      ctx.shadowColor = node.color || '#fff'
      ctx.shadowBlur = 15
      ctx.beginPath()
      ctx.arc(node.x, node.y, size + 2, 0, 2 * Math.PI)
      ctx.strokeStyle = '#ffffff'
      ctx.lineWidth = 1.5
      ctx.stroke()
      ctx.shadowBlur = 0
    }

    // Draw label when zoomed in enough
    if (globalScale > 2.2) {
      const label = node.label || node.id || ''
      const fontSize = 10 / globalScale

      ctx.font = `${fontSize}px Inter, sans-serif`
      ctx.textAlign = 'center'
      ctx.textBaseline = 'top'
      ctx.fillStyle = 'rgba(255, 255, 255, 0.85)'
      ctx.fillText(label, node.x, node.y + size + 2)
    }

    ctx.globalAlpha = 1 // reset
  }, [highlightNodes])

  // Custom link rendering
  const paintLink = useCallback((link, ctx) => {
    ctx.strokeStyle = 'rgba(120, 130, 160, 0.15)'
    ctx.lineWidth = 0.6
    ctx.beginPath()
    ctx.moveTo(link.source.x, link.source.y)
    ctx.lineTo(link.target.x, link.target.y)
    ctx.stroke()
  }, [])

  return (
    <ForceGraph2D
      ref={graphRef}
      graphData={graphData}
      width={window.innerWidth * 0.7}
      height={window.innerHeight - 60}
      nodeId="nodeId"
      nodeCanvasObject={paintNode}
      nodePointerAreaPaint={(node, color, ctx) => {
        const size = Math.max(4, Math.sqrt(node.val || 1) * 3)
        ctx.beginPath()
        ctx.arc(node.x, node.y, size + 5, 0, 2 * Math.PI)
        ctx.fillStyle = color
        ctx.fill()
      }}
      linkCanvasObject={paintLink}
      nodeLabel={(node) => `${node.type}: ${node.nodeId}`}
      onNodeClick={(node) => onNodeClick(node.nodeId)}
      backgroundColor="#0b0f1a"
      cooldownTicks={80}
      d3AlphaDecay={0.03}
      d3VelocityDecay={0.4}
      d3Force="charge"
      d3ForceConfig={{ 
        charge : {strength: -120},
        link: {distance: 80} 
      }}
      enableNodeDrag={true}
      enableZoomInteraction={true}
      enablePanInteraction={true}
      nodeRelSize={6}
      linkWidth={1.2}
      linkDirectionalParticles={1}
      linkDirectionalParticleSpeed={0.002}
    />
  )
}
