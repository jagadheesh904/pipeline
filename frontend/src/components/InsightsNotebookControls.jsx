import React, { useState } from 'react'
import axios from 'axios'

const InsightsNotebookControls = ({ onInsightsLoaded }) => {
  const [insightsStatus, setInsightsStatus] = useState('idle')
  const [currentRunId, setCurrentRunId] = useState('')
  const [statusMessage, setStatusMessage] = useState('')
  const [notebookInsights, setNotebookInsights] = useState(null)
  const [pollingInterval, setPollingInterval] = useState(null)

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
        startPolling(response.data.run_id)
      } else {
        setInsightsStatus('error')
        setStatusMessage('Failed to start insights notebook: ' + response.data.message)
      }
    } catch (err) {
      setInsightsStatus('error')
      setStatusMessage('Failed to start insights notebook: ' + err.response?.data?.message || err.message)
    }
  }

  const startPolling = (runId) => {
    // Clear any existing interval
    if (pollingInterval) {
      clearInterval(pollingInterval)
    }

    const interval = setInterval(async () => {
      try {
        const response = await axios.get(`/api/pipeline/get-insights-output/${runId}`)
        
        if (response.data.status === 'running') {
          setStatusMessage(`Insights Notebook status: ${response.data.life_cycle_state} - ${response.data.message}`)
        } else if (response.data.status === 'success') {
          // Notebook completed successfully
          clearInterval(interval)
          setPollingInterval(null)
          setInsightsStatus('success')
          setNotebookInsights(response.data.insights)
          setStatusMessage('Insights loaded successfully!')
          
          // Pass insights to parent component
          if (onInsightsLoaded) {
            onInsightsLoaded(response.data.insights)
          }
        } else if (response.data.status === 'error') {
          // Notebook failed
          clearInterval(interval)
          setPollingInterval(null)
          setInsightsStatus('error')
          setStatusMessage('Insights Notebook failed: ' + response.data.message)
        }
      } catch (err) {
        console.error('Polling error:', err)
        setStatusMessage('Error checking notebook status: ' + err.message)
      }
    }, 3000) // Check every 3 seconds

    setPollingInterval(interval)
  }

  const refreshInsights = async () => {
    setInsightsStatus('refreshing')
    setStatusMessage('Refreshing insights from tables...')
    
    try {
      const response = await axios.post('/api/pipeline/refresh-insights')
      
      if (response.data.status === 'success') {
        setInsightsStatus('success')
        setNotebookInsights(response.data.insights)
        setStatusMessage('Insights refreshed successfully!')
        
        // Pass insights to parent component
        if (onInsightsLoaded) {
          onInsightsLoaded(response.data.insights)
        }
      } else {
        setInsightsStatus('error')
        setStatusMessage('Failed to refresh insights: ' + response.data.message)
      }
    } catch (err) {
      setInsightsStatus('error')
      setStatusMessage('Failed to refresh insights: ' + err.response?.data?.message || err.message)
    }
  }

  const stopPolling = () => {
    if (pollingInterval) {
      clearInterval(pollingInterval)
      setPollingInterval(null)
    }
  }

  // Clean up interval on unmount
  React.useEffect(() => {
    return () => {
      if (pollingInterval) {
        clearInterval(pollingInterval)
      }
    }
  }, [pollingInterval])

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
        <h4>📊 Insights from {notebookInsights.source === 'notebook_output' ? 'Notebook Execution' : 'Database Tables'}</h4>
        
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '20px', marginTop: '15px' }}>
          {/* Top Products */}
          <div>
            <h5>Top Products</h5>
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
            <h5>Sales by Country</h5>
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

        {/* Source information */}
        <div style={{ marginTop: '10px', fontSize: '0.9em', color: '#666' }}>
          Source: {notebookInsights.source} | Run ID: {currentRunId}
        </div>
      </div>
    )
  }

  return (
    <div style={{ marginTop: '20px', padding: '20px', background: '#e3f2fd', borderRadius: '10px' }}>
      <h3>📈 Insights Notebook Controls</h3>
      <p style={{ marginBottom: '15px', color: '#666' }}>
        Run the actual Insights notebook from Databricks and display its output
      </p>
      
      <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', marginBottom: '15px' }}>
        <button 
          onClick={runInsightsNotebook}
          disabled={insightsStatus === 'running' || insightsStatus === 'starting'}
          style={{
            background: (insightsStatus === 'running' || insightsStatus === 'starting') ? '#6c757d' : '#17a2b8',
            color: 'white',
            border: 'none',
            padding: '12px 24px',
            borderRadius: '5px',
            cursor: (insightsStatus === 'running' || insightsStatus === 'starting') ? 'not-allowed' : 'pointer',
            fontSize: '1em',
            fontWeight: 'bold'
          }}
        >
          {insightsStatus === 'running' ? '🔄 Running...' : 
           insightsStatus === 'starting' ? '🚀 Starting...' : '🚀 Run Insights Notebook'}
        </button>
        
        <button 
          onClick={refreshInsights}
          disabled={insightsStatus === 'refreshing'}
          style={{
            background: insightsStatus === 'refreshing' ? '#6c757d' : '#28a745',
            color: 'white',
            border: 'none',
            padding: '12px 24px',
            borderRadius: '5px',
            cursor: insightsStatus === 'refreshing' ? 'not-allowed' : 'pointer',
            fontSize: '1em'
          }}
        >
          {insightsStatus === 'refreshing' ? '🔄 Refreshing...' : '🔄 Refresh Insights'}
        </button>

        {(insightsStatus === 'running' || insightsStatus === 'starting') && (
          <button 
            onClick={stopPolling}
            style={{
              background: '#dc3545',
              color: 'white',
              border: 'none',
              padding: '12px 24px',
              borderRadius: '5px',
              cursor: 'pointer',
              fontSize: '1em'
            }}
          >
            ⏹️ Stop Polling
          </button>
        )}
      </div>
      
      {statusMessage && (
        <div style={{
          marginTop: '15px',
          padding: '10px',
          borderRadius: '5px',
          background: insightsStatus === 'success' ? '#d4edda' : 
                     insightsStatus === 'error' ? '#f8d7da' : 
                     insightsStatus === 'running' || insightsStatus === 'starting' || insightsStatus === 'refreshing' ? '#fff3cd' : '#e2e3e5',
          color: insightsStatus === 'success' ? '#155724' : 
                insightsStatus === 'error' ? '#721c24' : 
                insightsStatus === 'running' || insightsStatus === 'starting' || insightsStatus === 'refreshing' ? '#856404' : '#383d41',
          border: `1px solid ${
            insightsStatus === 'success' ? '#c3e6cb' : 
            insightsStatus === 'error' ? '#f5c6cb' : 
            insightsStatus === 'running' || insightsStatus === 'starting' || insightsStatus === 'refreshing' ? '#ffeaa7' : '#d6d8db'
          }`
        }}>
          {insightsStatus === 'running' && '🔄 '}
          {insightsStatus === 'starting' && '🚀 '}
          {insightsStatus === 'success' && '✅ '}
          {insightsStatus === 'error' && '❌ '}
          {insightsStatus === 'refreshing' && '🔄 '}
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
