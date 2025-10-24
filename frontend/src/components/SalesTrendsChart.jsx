import React from 'react'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'

const SalesTrendsChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="loading">No data available. Run the pipeline first.</div>
  }

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0
    }).format(value)
  }

  // Transform data for better visualization
  const chartData = data.map(item => ({
    date: `${item.year}-${String(item.month).padStart(2, '0')}-${String(item.day).padStart(2, '0')}`,
    revenue: item.daily_revenue,
    quantity: item.daily_quantity
  })).reverse() // Reverse to show chronological order

  return (
    <ResponsiveContainer width="100%" height="100%">
      <LineChart data={chartData} margin={{ top: 20, right: 30, left: 20, bottom: 20 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis dataKey="date" />
        <YAxis yAxisId="left" />
        <YAxis yAxisId="right" orientation="right" />
        <Tooltip 
          formatter={(value, name) => {
            if (name === 'revenue') return [formatCurrency(value), 'Revenue']
            return [value, 'Quantity']
          }}
        />
        <Legend />
        <Line 
          yAxisId="left"
          type="monotone" 
          dataKey="revenue" 
          stroke="#8884d8" 
          strokeWidth={2}
          name="Revenue"
          dot={{ r: 2 }}
        />
        <Line 
          yAxisId="right"
          type="monotone" 
          dataKey="quantity" 
          stroke="#82ca9d" 
          strokeWidth={2}
          name="Quantity"
          dot={{ r: 2 }}
        />
      </LineChart>
    </ResponsiveContainer>
  )
}

export default SalesTrendsChart
