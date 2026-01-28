import { Routes, Route } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import EntityExplorer from './pages/EntityExplorer';
import DetectionResults from './pages/DetectionResults';
import ReportGenerator from './pages/ReportGenerator';
import ValidationDashboard from './pages/ValidationDashboard';
import EntityResolution from './pages/EntityResolution';
import IngestionPage from './pages/IngestionPage';

function App() {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<Dashboard />} />
        <Route path="ingestion" element={<IngestionPage />} />
        <Route path="entities" element={<EntityExplorer />} />
        <Route path="entities/:id" element={<EntityExplorer />} />
        <Route path="detection" element={<DetectionResults />} />
        <Route path="reports" element={<ReportGenerator />} />
        <Route path="validation" element={<ValidationDashboard />} />
        <Route path="resolution" element={<EntityResolution />} />
      </Route>
    </Routes>
  );
}

export default App;
