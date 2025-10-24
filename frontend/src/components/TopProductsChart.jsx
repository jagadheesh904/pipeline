import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

const TopProductsChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="loading">No data available. Run the pipeline first.</div>
  }

  // Format data for better display (shorten long descriptions)
  const formattedData = data.map(item => ({
    ...item,
    shortDescription: item.Description && item.Description.length > 30 
      ? item.Description.substring(0, 30) + '...' 
      : item.Description
  }))

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart 
        data={formattedData} 
        margin={{ top: 20, right: 30, left: 20, bottom: 80 }}
      >
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
          formatter={(value) => [`${value} units`, 'Quantity Sold']}
          labelFormatter={(value, payload) => {
            if (payload && payload[0]) {
              return payload[0].payload.Description || value
            }
            return value
          }}
        />
        <Bar 
          dataKey="total_quantity_sold" 
          fill="#8884d8" 
          name="Quantity Sold" 
        />
      </BarChart>
    </ResponsiveContainer>
  )
}

export default TopProductsChart
