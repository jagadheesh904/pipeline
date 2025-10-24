import React, { useState } from 'react'
import axios from 'axios'

const PipelineControls = ({ onPipelineComplete }) => {
  const [realPipelineStatus, setRealPipelineStatus] = useState('idle')
  const [currentRunId, setCurrentRunId] = useState('')
  const [statusMessage, setStatusMessage] = useState('')

  const triggerRealPipeline = async () => {
    setRealPipelineStatus('starting')
    setStatusMessage('Starting Databricks pipeline...')
    
    try {
      const response = await axios.post('/api/pipeline/trigger-real', {
        pipeline_type: 'full'
      })
      
      if (response.data.status === 'success') {
        setCurrentRunId(response.data.run_id)
        setRealPipelineStatus('running')
        setStatusMessage('Pipeline started! Checking status...')
        
        // Start polling for status
        pollPipelineStatus(response.data.run_id)
      }
    } catch (err) {
      setRealPipelineStatus('error')
      setStatusMessage('Failed to start pipeline: ' + err.response?.data?.message)
    }
  }

  const pollPipelineStatus = async (runId) => {
    const maxAttempts = 60 // 5 minutes max
    let attempts = 0
    
    const checkStatus = async () => {
      try {
        const response = await axios.get(`/api/pipeline/status-real/${runId}`)
        
        if (response.data.status === 'success') {
          const state = response.data.life_cycle_state
          const result = response.data.result_state
          
          setStatusMessage(`Pipeline status: ${state}${result ? ` - ${result}` : ''}`)
          
          if (state === 'TERMINATED' || state === 'SKIPPED' || state === 'INTERNAL_ERROR') {
            // Pipeline completed
            if (result === 'SUCCESS') {
              setRealPipelineStatus('success')
              setStatusMessage('Pipeline completed successfully!')
              if (onPipelineComplete) onPipelineComplete()
            } else {
              setRealPipelineStatus('error')
              setStatusMessage(`Pipeline failed: ${response.data.state_message || 'Unknown error'}`)
            }
            return
          }
          
          // Continue polling if still running
          if (attempts < maxAttempts && state === 'RUNNING') {
            attempts++
            setTimeout(checkStatus, 5000) // Check every 5 seconds
          } else if (attempts >= maxAttempts) {
            setRealPipelineStatus('timeout')
            setStatusMessage('Pipeline status check timeout')
          }
        }
      } catch (err) {
        setRealPipelineStatus('error')
        setStatusMessage('Error checking pipeline status: ' + err.message)
      }
    }
    
    checkStatus()
  }

  const runIndividualNotebook = async (notebookName) => {
    try {
      setStatusMessage(`Running ${notebookName}...`)
      
      const response = await axios.post('/api/pipeline/run-single-notebook', {
        notebook_path: `/Workspace/Users/jagadheeshnaidu1@gmail.com/online_ratail_project_pipeline/${notebookName}`
      })
      
      if (response.data.status === 'success') {
        setCurrentRunId(response.data.run_id)
        setStatusMessage(`${notebookName} started! Run ID: ${response.data.run_id}`)
      }
    } catch (err) {
      setStatusMessage(`Failed to run ${notebookName}: ${err.response?.data?.message}`)
    }
  }

  return (
    <div style={{ marginTop: '20px', padding: '20px', background: '#f8f9fa', borderRadius: '10px' }}>
      <h3>🚀 Real Databricks Pipeline Controls</h3>
      
      <div style={{ marginBottom: '15px' }}>
        <button 
          onClick={triggerRealPipeline}
          disabled={realPipelineStatus === 'running'}
          style={{
            background: realPipelineStatus === 'running' ? '#6c757d' : '#007bff',
            color: 'white',
            border: 'none',
            padding: '10px 20px',
            borderRadius: '5px',
            cursor: realPipelineStatus === 'running' ? 'not-allowed' : 'pointer',
            marginRight: '10px'
          }}
        >
          {realPipelineStatus === 'running' ? '🔄 Running...' : '🚀 Run Full Pipeline'}
        </button>
        
        <button 
          onClick={() => runIndividualNotebook('02_Silver_Transformation')}
          style={{
            background: '#28a745',
            color: 'white',
            border: 'none',
            padding: '8px 15px',
            borderRadius: '5px',
            cursor: 'pointer',
            marginRight: '5px',
            fontSize: '0.9em'
          }}
        >
          Silver Transform
        </button>
        
        <button 
          onClick={() => runIndividualNotebook('03_Gold_Star_Schema')}
          style={{
            background: '#ffc107',
            color: 'black',
            border: 'none',
            padding: '8px 15px',
            borderRadius: '5px',
            cursor: 'pointer',
            marginRight: '5px',
            fontSize: '0.9em'
          }}
        >
          Gold Schema
        </button>
      </div>
      
      {statusMessage && (
        <div style={{
          padding: '10px',
          borderRadius: '5px',
          background: realPipelineStatus === 'success' ? '#d4edda' : 
                     realPipelineStatus === 'error' ? '#f8d7da' : '#fff3cd',
          color: realPipelineStatus === 'success' ? '#155724' : 
                realPipelineStatus === 'error' ? '#721c24' : '#856404',
          border: `1px solid ${
            realPipelineStatus === 'success' ? '#c3e6cb' : 
            realPipelineStatus === 'error' ? '#f5c6cb' : '#ffeaa7'
          }`
        }}>
          {realPipelineStatus === 'running' && '🔄 '}
          {realPipelineStatus === 'success' && '✅ '}
          {realPipelineStatus === 'error' && '❌ '}
          {statusMessage}
        </div>
      )}
      
      {currentRunId && (
        <div style={{ marginTop: '10px', fontSize: '0.9em', color: '#666' }}>
          Run ID: {currentRunId}
        </div>
      )}
    </div>
  )
}

export default PipelineControls
