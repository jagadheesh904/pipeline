import React, { useState, useEffect } from 'react'
import axios from 'axios'
import TopProductsChart from './TopProductsChart'
import SalesByCountryChart from './SalesByCountryChart'
import SalesTrendsChart from './SalesTrendsChart'
import CustomerInsightsChart from './CustomerInsightsChart'
import ProductPerformanceChart from './ProductPerformanceChart'
import PipelineControls from './PipelineControls'
import InsightsNotebookControls from './InsightsNotebookControls'

const Dashboard = () => {
  const [pipelineStatus, setPipelineStatus] = useState('idle')
  const [insights, setInsights] = useState({
    topProducts: [],
    salesByCountry: [],
    recentSales: [],
    customerInsights: [],
    productPerformance: []
  })
  const [notebookInsights, setNotebookInsights] = useState(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  // This function will be called when pipeline completes
  const handlePipelineComplete = () => {
    // Refresh insights after pipeline completes
    fetchInsights()
  }

  // This function will be called when insights notebook completes
  const handleInsightsLoaded = (insightsData) => {
    setNotebookInsights(insightsData)
  }

  const fetchInsights = async () => {
    try {
      setError('')
      setLoading(true)
      
      const [productsRes, countriesRes, trendsRes, customersRes, productsPerfRes] = await Promise.all([
        axios.get('/api/insights/top-products'),
        axios.get('/api/insights/sales-by-country'), 
        axios.get('/api/insights/recent-sales'),
        axios.get('/api/insights/customer-insights'),
        axios.get('/api/insights/product-performance')
      ])

      setInsights({
        topProducts: productsRes.data.data || [],
        salesByCountry: countriesRes.data.data || [],
        recentSales: trendsRes.data.data || [],
        customerInsights: customersRes.data.data || [],
        productPerformance: productsPerfRes.data.data || []
      })
    } catch (err) {
      setError('Failed to fetch insights: ' + (err.response?.data?.message || err.message))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchInsights()
  }, [])

  return (
    <div className="dashboard">
      <div className="header">
        <h1>🏪 Retail Analytics Dashboard</h1>
        <p>Real-time insights from your Databricks pipeline</p>
      </div>

      <div className="controls">
        <h2>Pipeline Controls</h2>
        
        {/* Real Databricks Pipeline Controls */}
        <PipelineControls onPipelineComplete={handlePipelineComplete} />
        
        {/* Insights Notebook Controls */}
        <InsightsNotebookControls onInsightsLoaded={handleInsightsLoaded} />
        
        <div style={{ marginTop: '20px' }}>
          <button 
            className="run-button"
            onClick={fetchInsights}
            disabled={loading}
          >
            {loading ? 'Refreshing...' : '📊 Refresh Database Insights'}
          </button>
        </div>

        {error && <div className="error">{error}</div>}
      </div>

      {/* Display Notebook Insights if available */}
      {notebookInsights && (
        <div style={{ 
          background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)', 
          padding: '20px', 
          borderRadius: '15px', 
          marginBottom: '30px',
          color: 'white'
        }}>
          <h2 style={{ color: 'white', textAlign: 'center', marginBottom: '20px' }}>
            🎯 Live Insights from Notebook Execution
          </h2>
          
          <div style={{ 
            display: 'grid', 
            gridTemplateColumns: 'repeat(auto-fit, minmax(300px, 1fr))', 
            gap: '20px' 
          }}>
            {/* Top Products from Notebook */}
            <div style={{ background: 'white', padding: '15px', borderRadius: '10px' }}>
              <h3 style={{ color: '#333', marginBottom: '15px' }}>📦 Top Products (Notebook)</h3>
              {notebookInsights.top_products && notebookInsights.top_products.length > 0 ? (
                <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                  {notebookInsights.top_products.map((product, index) => (
                    <div key={index} style={{ 
                      padding: '8px', 
                      margin: '5px 0', 
                      background: '#f8f9fa', 
                      borderRadius: '4px',
                      borderLeft: '4px solid #667eea'
                    }}>
                      <strong>{product.Description}</strong>
                      <div style={{ color: '#666', fontSize: '0.9em' }}>
                        Quantity Sold: {product.total_quantity_sold}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ padding: '10px', background: '#fff3cd', borderRadius: '4px', color: '#856404' }}>
                  No product data from notebook
                </div>
              )}
            </div>

            {/* Sales by Country from Notebook */}
            <div style={{ background: 'white', padding: '15px', borderRadius: '10px' }}>
              <h3 style={{ color: '#333', marginBottom: '15px' }}>🌍 Sales by Country (Notebook)</h3>
              {notebookInsights.sales_by_country && notebookInsights.sales_by_country.length > 0 ? (
                <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                  {notebookInsights.sales_by_country.map((country, index) => (
                    <div key={index} style={{ 
                      padding: '8px', 
                      margin: '5px 0', 
                      background: '#f8f9fa', 
                      borderRadius: '4px',
                      borderLeft: '4px solid #764ba2'
                    }}>
                      <strong>{country.Country}</strong>
                      <div style={{ color: '#666', fontSize: '0.9em' }}>
                        Revenue: ${country.total_sales_revenue?.toLocaleString()}
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <div style={{ padding: '10px', background: '#fff3cd', borderRadius: '4px', color: '#856404' }}>
                  No country data from notebook
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      <div className="insights-grid">
        <div className="insight-card">
          <h3>📦 Top Selling Products (Database)</h3>
          <div className="chart-container">
            <TopProductsChart data={insights.topProducts} />
          </div>
        </div>

        <div className="insight-card">
          <h3>🌍 Sales by Country (Database)</h3>
          <div className="chart-container">
            <SalesByCountryChart data={insights.salesByCountry} />
          </div>
        </div>

        <div className="insight-card">
          <h3>📈 Sales Trends</h3>
          <div className="chart-container">
            <SalesTrendsChart data={insights.recentSales} />
          </div>
        </div>

        <div className="insight-card">
          <h3>👥 Top Customers</h3>
          <div className="chart-container">
            <CustomerInsightsChart data={insights.customerInsights} />
          </div>
        </div>

        <div className="insight-card">
          <h3>💰 Product Performance</h3>
          <div className="chart-container">
            <ProductPerformanceChart data={insights.productPerformance} />
          </div>
        </div>
      </div>
    </div>
  )
}

export default Dashboard
