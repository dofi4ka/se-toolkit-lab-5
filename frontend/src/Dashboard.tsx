import { useState, useEffect } from 'react'
import { Bar, Line } from 'react-chartjs-2'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
} from 'chart.js'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
)

const STORAGE_KEY = 'api_key'

interface ScoresBucket {
  bucket: string
  count: number
}

interface TimelineDay {
  date: string
  submissions: number
}

interface PassRateRow {
  task: string
  avg_score: number
  attempts: number
}

function authHeaders(): HeadersInit {
  const token = localStorage.getItem(STORAGE_KEY)
  return token ? { Authorization: `Bearer ${token}` } : {}
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: authHeaders() })
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json() as Promise<T>
}

export default function Dashboard() {
  const [lab, setLab] = useState('lab-04')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [scores, setScores] = useState<ScoresBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineDay[]>([])
  const [passRates, setPassRates] = useState<PassRateRow[]>([])

  useEffect(() => {
    setLoading(true)
    setError(null)
    const envTarget = (import.meta.env.VITE_API_TARGET as string) ?? ''
    const baseUrl = envTarget
      ? `${envTarget.replace(/\/$/, '')}/analytics`
      : '/analytics'
    const params = new URLSearchParams({ lab })
    Promise.all([
      fetchJson<ScoresBucket[]>(`${baseUrl}/scores?${params}`),
      fetchJson<TimelineDay[]>(`${baseUrl}/timeline?${params}`),
      fetchJson<PassRateRow[]>(`${baseUrl}/pass-rates?${params}`),
    ])
      .then(([s, t, p]) => {
        setScores(s)
        setTimeline(t)
        setPassRates(p)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [lab])

  if (loading) return <p>Loading...</p>
  if (error) return <p>Error: {error}</p>

  const barData = {
    labels: scores.map((b) => b.bucket),
    datasets: [
      {
        label: 'Count',
        data: scores.map((b) => b.count),
        backgroundColor: 'rgba(54, 162, 235, 0.5)',
        borderColor: 'rgb(54, 162, 235)',
        borderWidth: 1,
      },
    ],
  }

  const lineData = {
    labels: timeline.map((d) => d.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((d) => d.submissions),
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        tension: 0.1,
      },
    ],
  }

  return (
    <div className="dashboard">
      <div className="dashboard-controls">
        <label>
          Lab:{' '}
          <select value={lab} onChange={(e) => setLab(e.target.value)}>
            <option value="lab-01">lab-01</option>
            <option value="lab-02">lab-02</option>
            <option value="lab-03">lab-03</option>
            <option value="lab-04">lab-04</option>
            <option value="lab-05">lab-05</option>
          </select>
        </label>
      </div>

      <div className="chart-container">
        <h2>Score distribution</h2>
        <Bar data={barData} options={{ responsive: true }} />
      </div>

      <div className="chart-container">
        <h2>Submissions over time</h2>
        <Line
          data={lineData}
          options={{
            responsive: true,
            scales: {
              y: { beginAtZero: true },
            },
          }}
        />
      </div>

      <div className="dashboard-table">
        <h2>Pass rates</h2>
        <table>
          <thead>
            <tr>
              <th>Task</th>
              <th>Avg score</th>
              <th>Attempts</th>
            </tr>
          </thead>
          <tbody>
            {passRates.map((row) => (
              <tr key={row.task}>
                <td>{row.task}</td>
                <td>{row.avg_score.toFixed(1)}</td>
                <td>{row.attempts}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
