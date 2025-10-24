import React from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts'

const CustomerInsightsChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="loading">No customer data available</div>
  }

  const formatCurrency = (value) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0
    }).format(value)
  }

  return (
    <ResponsiveContainer width="100%" height="100%">
      <BarChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 60 }}>
        <CartesianGrid strokeDasharray="3 3" />
        <XAxis 
          dataKey="CustomerID" 
          angle={-45}
          textAnchor="end"
          height={60}
        />
        <YAxis />
        <Tooltip 
          formatter={(value, name) => {
            if (name === 'total_spent' || name === 'avg_order_value') {
              return [formatCurrency(value), name === 'total_spent' ? 'Total Spent' : 'Avg Order Value']
            }
            return [value, name === 'total_orders' ? 'Total Orders' : name]
          }}
        />
        <Bar dataKey="total_spent" fill="#ffc658" name="Total Spent" />
      </BarChart>
    </ResponsiveContainer>
  )
}

export default CustomerInsightsChart
