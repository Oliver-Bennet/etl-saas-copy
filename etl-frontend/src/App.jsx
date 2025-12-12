import { Authenticator } from '@aws-amplify/ui-react'
import { useState, useEffect } from 'react'   // ← ĐÃ SỬA: useEffect, không phải useEffect

const BACKEND_URL = 'http://etl-alb-336713034.ap-southeast-1.elb.amazonaws.com' // ← thay bằng IP EC2 hoặc ALB DNS của bạn

function UploadForm({ accessToken }) {
  const [file, setFile] = useState(null)
  const [msg, setMsg] = useState('')

  const upload = async () => {
    if (!file) return setMsg('Vui lòng chọn file CSV')
    const form = new FormData()
    form.append('file', file)

    try {
      const res = await fetch(`${BACKEND_URL}/api/upload`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${accessToken}` },
        body: form
      })
      const data = await res.json()
      setMsg(`Job đã tạo: ${data.job_id}`)
    } catch (e) {
      setMsg('Lỗi: ' + e.message)
    }
  }

  return (
    <div style={{ margin: '30px 0', padding: '20px', background: '#f0f8ff', borderRadius: '10px' }}>
      <h2>Upload file CSV</h2>
      <input type="file" accept=".csv" onChange={e => setFile(e.target.files[0])} />
      <br /><br />
      <button onClick={upload} style={{ padding: '12px 30px', fontSize: '16px', background: '#007bff', color: 'white', border: 'none', borderRadius: '5px' }}>
        Upload & Xử lý
      </button>
      {msg && <p style={{ marginTop: '15px', fontWeight: 'bold' }}>{msg}</p>}
    </div>
  )
}

function JobList({ accessToken }) {
  const [jobs, setJobs] = useState([])

  useEffect(() => {
    const timer = setInterval(async () => {
      try {
        const res = await fetch(`${BACKEND_URL}/api/jobs`, {
          headers: { Authorization: `Bearer ${accessToken}` }
        })
        if (res.ok) {
          const data = await res.json()
          setJobs(data)
        }
      } catch (e) {
        console.log('Lỗi fetch jobs:', e)
      }
    }, 5000) // kiểm tra mỗi 5 giây

    return () => clearInterval(timer)
  }, [accessToken])

  const download = async (jobId) => {
    const res = await fetch(`${BACKEND_URL}/api/jobs/${jobId}/download`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    })
    const { download_url } = await res.json()
    window.open(download_url, '_blank')
  }

  return (
    <div style={{ marginTop: '40px' }}>
      <h2>Danh sách Job</h2>
      {jobs.length === 0 ? (
        <p>Chưa có job nào. Upload file để bắt đầu!</p>
      ) : (
        <div>
          {jobs.map(job => (
            <div key={job.jobId} style={{ padding: '15px', margin: '10px 0', background: '#f9f9f9', borderRadius: '8px' }}>
              <strong>{job.filename}</strong> → <span style={{ color: job.status === 'COMPLETED' ? 'green' : 'orange', fontWeight: 'bold' }}>{job.status}</span>
              {job.status === 'COMPLETED' && (
                <button
                  onClick={() => download(job.jobId)}
                  style={{ marginLeft: '20px', padding: '10px 20px', background: 'green', color: 'white', border: 'none', borderRadius: '5px' }}
                >
                  Download Parquet
                </button>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default function App() {
  return (
    <Authenticator>
      {({ signOut, user }) => (
        <div style={{ padding: '40px', maxWidth: '800px', margin: '0 auto' }}>
          <h1 style={{ color: '#2c3e50' }}>ETL SaaS Dashboard</h1>
          <p>Đã đăng nhập với email: <strong>{user?.signInDetails?.loginId}</strong></p>
          <button onClick={signOut} style={{ padding: '10px 20px', background: '#e74c3c', color: 'white', border: 'none', marginBottom: '30px' }}>
            Đăng xuất
          </button>

          <UploadForm accessToken={user?.accessToken} />
          <JobList accessToken={user?.accessToken} />
        </div>
      )}
    </Authenticator>
  )
}