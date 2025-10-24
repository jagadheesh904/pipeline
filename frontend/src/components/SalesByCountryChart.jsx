import React from 'react'
import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend } from 'recharts'

const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8', '#82CA9D', '#FFC658', '#8DD1E1', '#D084D0', '#FF6B6B']

const SalesByCountryChart = ({ data }) => {
  if (!data || data.length === 0) {
    return <div className="loading">No data available</div>
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
      <PieChart>
        <Pie
          data={data}
          cx="50%"
          cy="50%"
          labelLine={false}
          label={({ Country, total_sales_revenue }) => `${Country}: ${formatCurrency(total_sales_revenue)}`}
          outerRadius={80}
          fill="#8884d8"
          dataKey="total_sales_revenue"
          nameKey="Country"
        >
          {data.map((entry, index) => (
            <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
          ))}
        </Pie>
        <Tooltip formatter={(value) => [formatCurrency(value), 'Sales Revenue']} />
        <Legend />
      </PieChart>
    </ResponsiveContainer>
  )
}

export default SalesByCountryChart
