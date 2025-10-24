import React, { useState } from 'react'
import axios from 'axios'

const InsightsNotebookControls = ({ onInsightsLoaded }) => {
  const [insightsStatus, setInsightsStatus] = useState('idle')
  const [currentRunId, setCurrentRunId] = useState('')
  const [statusMessage, setStatusMessage] = useState('')
  const [notebookInsights, setNotebookInsights] = useState(null)

  const runInsightsNotebook = async () => {
    setInsightsStatus('starting')
    setStatusMessage('Starting Insights Notebook...')
    setNotebookInsights(null)
    
    try {
      const response = await axios.post('/api/pipeline/trigger-insights-notebook')
      
      if (response.data.status === 'success') {
        setCurrentRunId(response.data.run_id)
        setInsightsStatus('running')
        setStatusMessage('Insights Notebook started! Waiting for completion...')
        
        // Start polling for completion and results
        pollInsightsResults(response.data.run_id)
      }
    } catch (err) {
      setInsightsStatus('error')
      setStatusMessage('Failed to start insights notebook: ' + err.response?.data?.message)
    }
  }

  const pollInsightsResults = async (runId) => {
    const maxAttempts = 60 // 5 minutes max
    let attempts = 0
    
    const checkStatusAndResults = async () => {
      try {
        // First check if the run is complete
        const statusResponse = await axios.get(`/api/pipeline/status-real/${runId}`)
        
        if (statusResponse.data.status === 'success') {
          const state = statusResponse.data.life_cycle_state
          const result = statusResponse.data.result_state
          
          setStatusMessage(`Insights Notebook status: ${state}${result ? ` - ${result}` : ''}`)
          
          if (state === 'TERMINATED') {
            if (result === 'SUCCESS') {
              // Notebook completed successfully, now get the output
              setStatusMessage('Notebook completed! Fetching insights...')
              await fetchInsightsOutput(runId)
            } else {
              setInsightsStatus('error')
              setStatusMessage(`Insights Notebook failed: ${statusResponse.data.state_message || 'Unknown error'}`)
            }
            return
          }
          
          // Continue polling if still running
          if (attempts < maxAttempts && state === 'RUNNING') {
            attempts++
            setTimeout(checkStatusAndResults, 5000) // Check every 5 seconds
          } else if (attempts >= maxAttempts) {
            setInsightsStatus('timeout')
            setStatusMessage('Insights Notebook execution timeout')
          }
        }
      } catch (err) {
        setInsightsStatus('error')
        setStatusMessage('Error checking insights notebook status: ' + err.message)
      }
    }
    
    checkStatusAndResults()
  }

  const fetchInsightsOutput = async (runId) => {
    try {
      const response = await axios.get(`/api/pipeline/get-insights-output/${runId}`)
      
      if (response.data.status === 'success') {
        setInsightsStatus('success')
        setNotebookInsights(response.data.insights)
        setStatusMessage('Insights loaded successfully from notebook!')
        
        // Pass insights to parent component
        if (onInsightsLoaded) {
          onInsightsLoaded(response.data.insights)
        }
      } else {
        setInsightsStatus('error')
        setStatusMessage('Failed to fetch insights output: ' + response.data.message)
      }
    } catch (err) {
      setInsightsStatus('error')
      setStatusMessage('Error fetching insights output: ' + err.message)
    }
  }

  const displayNotebookInsights = () => {
    if (!notebookInsights) return null

    return (
      <div style={{ 
        marginTop: '20px', 
        padding: '15px', 
        background: '#e8f5e8', 
        borderRadius: '8px',
        border: '1px solid #c3e6c3'
      }}>
        <h4>📊 Insights from Notebook Execution</h4>
        
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px', marginTop: '15px' }}>
          {/* Top Products */}
          <div>
            <h5>Top Products (From Notebook)</h5>
            {notebookInsights.top_products && notebookInsights.top_products.length > 0 ? (
              <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                {notebookInsights.top_products.map((product, index) => (
                  <div key={index} style={{ 
                    padding: '8px', 
                    margin: '5px 0', 
                    background: 'white', 
                    borderRadius: '4px',
                    border: '1px solid #ddd'
                  }}>
                    <strong>{product.Description}</strong>
                    <br />
                    <span>Quantity: {product.total_quantity_sold}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ padding: '10px', background: '#fff3cd', borderRadius: '4px' }}>
                No product data available
              </div>
            )}
          </div>

          {/* Sales by Country */}
          <div>
            <h5>Sales by Country (From Notebook)</h5>
            {notebookInsights.sales_by_country && notebookInsights.sales_by_country.length > 0 ? (
              <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                {notebookInsights.sales_by_country.map((country, index) => (
                  <div key={index} style={{ 
                    padding: '8px', 
                    margin: '5px 0', 
                    background: 'white', 
                    borderRadius: '4px',
                    border: '1px solid #ddd'
                  }}>
                    <strong>{country.Country}</strong>
                    <br />
                    <span>Revenue: ${country.total_sales_revenue?.toLocaleString()}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div style={{ padding: '10px', background: '#fff3cd', borderRadius: '4px' }}>
                No country data available
              </div>
            )}
          </div>
        </div>

        {/* Raw output for debugging */}
        <details style={{ marginTop: '15px' }}>
          <summary>Debug Information</summary>
          <pre style={{ 
            background: '#f8f9fa', 
            padding: '10px', 
            borderRadius: '4px', 
            fontSize: '12px',
            maxHeight: '150px',
            overflow: 'auto'
          }}>
            Run ID: {currentRunId}
            {notebookInsights && JSON.stringify(notebookInsights, null, 2)}
          </pre>
        </details>
      </div>
    )
  }

  return (
    <div style={{ marginTop: '20px', padding: '20px', background: '#e3f2fd', borderRadius: '10px' }}>
      <h3>📈 Insights Notebook Controls</h3>
      <p style={{ marginBottom: '15px', color: '#666' }}>
        Run the actual Insights notebook from Databricks and display its output
      </p>
      
      <button 
        onClick={runInsightsNotebook}
        disabled={insightsStatus === 'running'}
        style={{
          background: insightsStatus === 'running' ? '#6c757d' : '#17a2b8',
          color: 'white',
          border: 'none',
          padding: '12px 24px',
          borderRadius: '5px',
          cursor: insightsStatus === 'running' ? 'not-allowed' : 'pointer',
          fontSize: '1em',
          fontWeight: 'bold'
        }}
      >
        {insightsStatus === 'running' ? '🔄 Running Insights Notebook...' : '🚀 Run Insights Notebook'}
      </button>
      
      {statusMessage && (
        <div style={{
          marginTop: '15px',
          padding: '10px',
          borderRadius: '5px',
          background: insightsStatus === 'success' ? '#d4edda' : 
                     insightsStatus === 'error' ? '#f8d7da' : 
                     insightsStatus === 'running' ? '#fff3cd' : '#e2e3e5',
          color: insightsStatus === 'success' ? '#155724' : 
                insightsStatus === 'error' ? '#721c24' : 
                insightsStatus === 'running' ? '#856404' : '#383d41',
          border: `1px solid ${
            insightsStatus === 'success' ? '#c3e6cb' : 
            insightsStatus === 'error' ? '#f5c6cb' : 
            insightsStatus === 'running' ? '#ffeaa7' : '#d6d8db'
          }`
        }}>
          {insightsStatus === 'running' && '🔄 '}
          {insightsStatus === 'success' && '✅ '}
          {insightsStatus === 'error' && '❌ '}
          {statusMessage}
        </div>
      )}
      
      {displayNotebookInsights()}
      
      {currentRunId && (
        <div style={{ marginTop: '10px', fontSize: '0.9em', color: '#666' }}>
          Notebook Run ID: {currentRunId}
        </div>
      )}
    </div>
  )
}

export default InsightsNotebookControls
