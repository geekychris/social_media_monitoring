import { useState } from 'react'
import { SearchWorkbench } from './components/SearchWorkbench'
import { Dashboard } from './components/Dashboard'
import { Monitors } from './components/Monitors'
import { PipelineStatus } from './components/PipelineStatus'
import './App.css'

type Tab = 'dashboard' | 'search' | 'monitors' | 'pipeline'

function App() {
  const [activeTab, setActiveTab] = useState<Tab>('dashboard')

  return (
    <div className="app">
      <header className="app-header">
        <h1 className="app-title">SignalDesk</h1>
        <p className="app-subtitle">Social Media Intelligence Workbench</p>
        <nav className="app-nav">
          <button
            className={`nav-btn ${activeTab === 'dashboard' ? 'active' : ''}`}
            onClick={() => setActiveTab('dashboard')}
          >
            Dashboard
          </button>
          <button
            className={`nav-btn ${activeTab === 'search' ? 'active' : ''}`}
            onClick={() => setActiveTab('search')}
          >
            Search
          </button>
          <button
            className={`nav-btn ${activeTab === 'monitors' ? 'active' : ''}`}
            onClick={() => setActiveTab('monitors')}
          >
            Monitors
          </button>
          <button
            className={`nav-btn ${activeTab === 'pipeline' ? 'active' : ''}`}
            onClick={() => setActiveTab('pipeline')}
          >
            Pipeline
          </button>
        </nav>
      </header>
      <main className="app-main">
        {activeTab === 'dashboard' && <Dashboard />}
        {activeTab === 'search' && <SearchWorkbench />}
        {activeTab === 'monitors' && <Monitors />}
        {activeTab === 'pipeline' && <PipelineStatus />}
      </main>
    </div>
  )
}

export default App
