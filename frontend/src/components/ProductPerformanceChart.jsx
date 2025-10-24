import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

const ProductPerformanceChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="loading">No product performance data available</div>
  }

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0
    }).format(value)
  }

  // Format data for display
  const formattedData = data.map(item => ({
    ...item,
    shortDescription: item.Description && item.Description.length > 25 
      ? item.Description.substring(0, 25) + '...' 
      : item.Description
  }))

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={formattedData} margin={{ top: 20, right: 30, left: 20, bottom: 80 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="shortDescription" 
          angle={-45}
          textAnchor="end"
          height={80}
          interval={0}
          fontSize={10}
        />
        <YAxis />
        <Tooltip 
          formatter={(value, name) => {
            if (name === 'total_revenue' || name === 'avg_unit_price') {
              return [formatCurrency(value), name === 'total_revenue' ? 'Total Revenue' : 'Avg Price']
            }
            return [value, name === 'times_ordered' ? 'Times Ordered' : 'Total Quantity']
          }}
          labelFormatter={(value, payload) => {
            if (payload && payload[0]) {
              return payload[0].payload.Description || value
            }
            return value
          }}
        />
        <Bar dataKey="total_revenue" fill="#8884d8" name="Total Revenue" />
      </BarChart>
    </ResponsiveContainer>
  )
}

export default ProductPerformanceChart
