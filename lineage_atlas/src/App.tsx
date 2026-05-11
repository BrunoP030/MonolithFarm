import React, { useCallback, useEffect, useMemo, useState } from 'react';
import {
  Background,
  Controls,
  Handle,
  MarkerType,
  MiniMap,
  Position,
  ReactFlow,
  useEdgesState,
  useNodesState,
} from '@xyflow/react';
import {
  Activity,
  ArrowRight,
  BarChart3,
  BookOpen,
  Braces,
  CheckCircle2,
  ChevronLeft,
  ChevronRight,
  CircleDot,
  Database,
  Download,
  Eye,
  FileCode2,
  FileText,
  Files,
  Filter,
  GitBranch,
  Image as ImageIcon,
  Layers3,
  LineChart,
  Lock,
  LogIn,
  LogOut,
  Moon,
  Network,
  RotateCcw,
  Search,
  ShieldCheck,
  Sparkles,
  Sun,
  Table2,
  Waypoints,
} from 'lucide-react';

type AnyRecord = Record<string, any>;
type IconComponent = React.ComponentType<{ size?: number }>;

const TYPE_LABELS = {
  rawFile: 'Arquivo bruto',
  rawColumn: 'Coluna bruta',
  intermediate: 'Tabela intermediária',
  feature: 'Feature',
  driver: 'Driver',
  csv: 'CSV final',
  csvColumn: 'Coluna final',
  hypothesis: 'Hipótese',
  chart: 'Gráfico',
  correlation: 'Correlação',
};

const TYPE_COLORS = {
  rawFile: '#64748b',
  rawColumn: '#94a3b8',
  intermediate: '#0f766e',
  feature: '#0e7490',
  driver: '#f97316',
  csv: '#16a34a',
  csvColumn: '#65a30d',
  hypothesis: '#7c3aed',
  chart: '#8b5cf6',
  correlation: '#2563eb',
};

const EDGE_COLORS = {
  contains_column: '#94a3b8',
  creates_feature: '#0e7490',
  feeds_table: '#14b8a6',
  feeds_csv: '#22c55e',
  raw_origin: '#64748b',
  driver_from_flag: '#f97316',
  generates_csv: '#16a34a',
  supports_hypothesis: '#7c3aed',
  generates_chart: '#8b5cf6',
  lineage: '#2563eb',
};

const NAV: Array<[string, string, IconComponent]> = [
  ['overview', 'Visão geral', Sparkles],
  ['canvas', 'Canvas', Waypoints],
  ['files', 'Arquivos', Database],
  ['vault', 'Dados privados', Lock],
  ['columns', 'Colunas', Table2],
  ['features', 'Features', Braces],
  ['tables', 'Tabelas', Layers3],
  ['csvs', 'CSVs finais', Files],
  ['hypotheses', 'H1-H4', ShieldCheck],
  ['correlations', 'Correlações', LineChart],
  ['audit', 'Auditoria', GitBranch],
  ['story', 'Storytelling', BookOpen],
  ['docs', 'Docs FarmLab', BookOpen],
];

const CATEGORY_FILTERS = ['rawFile', 'rawColumn', 'intermediate', 'feature', 'driver', 'csv', 'csvColumn', 'hypothesis', 'chart'];
const PRIVATE_API_START_COMMAND = 'powershell -ExecutionPolicy Bypass -File .\\scripts\\start_lineage_atlas.ps1 -Port 5173';

async function apiJson(url: string, options: RequestInit = {}) {
  const response = await fetch(url, { credentials: 'include', ...options });
  const contentType = response.headers.get('content-type') || '';
  const text = await response.text();
  if (!contentType.includes('application/json')) {
    const error: any = new Error(
      `A API privada não respondeu JSON. Isso normalmente indica que o Vite foi iniciado diretamente. Feche o servidor atual e rode: ${PRIVATE_API_START_COMMAND}`,
    );
    error.status = response.status;
    error.code = 'private_api_unavailable';
    error.privateApiUnavailable = true;
    error.preview = text.slice(0, 80);
    throw error;
  }
  const payload = text ? JSON.parse(text) : {};
  if (!response.ok) {
    const error: any = new Error(payload.message || payload.error || `HTTP ${response.status}`);
    error.status = response.status;
    error.code = payload.error || `HTTP_${response.status}`;
    error.payload = payload;
    throw error;
  }
  return payload;
}

function privateApiMessage(error: any) {
  if (error?.privateApiUnavailable || error?.code === 'private_api_unavailable') {
    return `Servidor de dados privados não está ativo nesta sessão. Feche o Vite atual e rode: ${PRIVATE_API_START_COMMAND}`;
  }
  if (error?.code === 'auth_not_configured') {
    return 'Autenticação ainda não configurada no servidor privado.';
  }
  if (error?.status === 401 || error?.code === 'invalid_credentials') {
    return 'Usuário ou senha inválidos.';
  }
  return error?.message || String(error);
}

function App() {
  const [data, setData] = useState(null);
  const [loadError, setLoadError] = useState('');
  const [activePage, setActivePage] = useState('overview');
  const [query, setQuery] = useState('');
  const [graphMode, setGraphMode] = useState('focus');
  const [detailLevel, setDetailLevel] = useState('curated');
  const [showEdgeLabels, setShowEdgeLabels] = useState(false);
  const [selectedId, setSelectedId] = useState('');
  const [enabledTypes, setEnabledTypes] = useState(() => new Set(CATEGORY_FILTERS));
  const [theme, setTheme] = useState(() => localStorage.getItem('mf-theme') || 'light');
  const [privateAuth, setPrivateAuth] = useState({ status: 'checking', authenticated: false, user: '', configured: true, apiAvailable: true, error: '' });
  const [vaultQuery, setVaultQuery] = useState('');
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('mf-theme', theme);
  }, [theme]);

  useEffect(() => {
    fetch('/atlas-data.json')
      .then((response) => {
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        return response.json();
      })
      .then((payload) => {
        setData(payload);
        setSelectedId(payload.graph?.nodes?.find((node) => node.type === 'feature' && node.label === 'ndvi_mean_week')?.id || payload.graph?.nodes?.[0]?.id || '');
      })
      .catch((error) => setLoadError(String(error)));
  }, []);

  const refreshPrivateAuth = useCallback(() => {
    return apiJson('/api/auth/session')
      .then((payload) => {
        setPrivateAuth({
          status: 'ready',
          authenticated: Boolean(payload.authenticated),
          user: payload.user || '',
          configured: payload.configured !== false,
          apiAvailable: true,
          error: '',
        });
        return payload;
      })
      .catch((error) => {
        setPrivateAuth({
          status: 'ready',
          authenticated: false,
          user: '',
          configured: false,
          apiAvailable: !error?.privateApiUnavailable,
          error: privateApiMessage(error),
        });
        return null;
      });
  }, []);

  useEffect(() => {
    refreshPrivateAuth();
  }, [refreshPrivateAuth]);

  const openVault = useCallback((search = '') => {
    setVaultQuery(search);
    setActivePage('vault');
  }, []);

  const index = useMemo(() => (data ? buildIndex(data) : null), [data]);
  const selected = useMemo(() => (data && index ? describeObject(selectedId, data, index) : null), [data, index, selectedId]);
  const visibleGraph = useMemo(() => {
    if (!data || !index) return { nodes: [], edges: [] };
    return computeVisibleGraph(data.graph, index, { query, selectedId, graphMode, enabledTypes, detailLevel, showEdgeLabels });
  }, [data, index, query, selectedId, graphMode, enabledTypes, detailLevel, showEdgeLabels]);

  useEffect(() => {
    setNodes(visibleGraph.nodes);
    setEdges(visibleGraph.edges);
  }, [visibleGraph, setNodes, setEdges]);

  const onNodeClick = useCallback((_, node) => setSelectedId(node.id), []);
  const resetLayout = useCallback(() => setNodes(visibleGraph.nodes), [setNodes, visibleGraph.nodes]);
  const setTypeEnabled = useCallback((type) => {
    setEnabledTypes((current) => {
      const next = new Set(current);
      if (next.has(type)) next.delete(type);
      else next.add(type);
      return next;
    });
  }, []);

  if (loadError) return <ErrorState error={loadError} />;
  if (!data || !index) return <LoadingState />;

  const isVaultPage = activePage === 'vault';

  return (
    <div className={`app-shell${isVaultPage ? ' app-shell-vault' : ''}`}>
      <aside className="left-rail">
        <div className="brand">
          <div className="brand-main">
            <div className="brand-mark">MF</div>
            <div>
              <div className="brand-title">MonolithFarm Atlas</div>
              <div className="brand-subtitle">NDVI lineage workspace</div>
            </div>
          </div>
          <button
            className="theme-toggle"
            aria-label={theme === 'dark' ? 'Alternar para tema claro' : 'Alternar para tema escuro'}
            title={theme === 'dark' ? 'Tema claro' : 'Tema escuro'}
            onClick={() => setTheme(theme === 'dark' ? 'light' : 'dark')}
          >
            {theme === 'dark' ? <Sun size={17} /> : <Moon size={17} />}
          </button>
        </div>

        <nav className="nav-list">
          {NAV.map(([key, label, Icon]) => (
            <button key={key} className={activePage === key ? 'active' : ''} onClick={() => setActivePage(key)}>
              <Icon size={17} />
              {label}
            </button>
          ))}
        </nav>

        <SearchBox value={query} setValue={setQuery} />
        <GlobalResults data={data} query={query} selectedId={selectedId} onSelect={setSelectedId} />
      </aside>

      <main className="main-surface">
        {!isVaultPage && <Header data={data} activePage={activePage} selected={selected} />}
        {activePage === 'overview' && <OverviewPage data={data} setActivePage={setActivePage} />}
        {activePage === 'canvas' && (
          <CanvasPage
            data={data}
            selected={selected}
            graphMode={graphMode}
            setGraphMode={setGraphMode}
            detailLevel={detailLevel}
            setDetailLevel={setDetailLevel}
            showEdgeLabels={showEdgeLabels}
            setShowEdgeLabels={setShowEdgeLabels}
            enabledTypes={enabledTypes}
            setTypeEnabled={setTypeEnabled}
            nodes={nodes}
            edges={edges}
            onNodesChange={onNodesChange}
            onEdgesChange={onEdgesChange}
            onNodeClick={onNodeClick}
            onSelect={setSelectedId}
            resetLayout={resetLayout}
          />
        )}
        {activePage === 'files' && <FilesPage data={data} query={query} onSelect={setSelectedId} onOpenPrivateFile={openVault} />}
        {activePage === 'vault' && <PrivateDataPage auth={privateAuth} refreshAuth={refreshPrivateAuth} initialQuery={vaultQuery} />}
        {activePage === 'columns' && <ColumnsPage data={data} query={query} setQuery={setQuery} onSelect={setSelectedId} />}
        {activePage === 'features' && <FeaturesPage data={data} query={query} setQuery={setQuery} onSelect={setSelectedId} />}
        {activePage === 'tables' && <TablesPage data={data} query={query} onSelect={setSelectedId} />}
        {activePage === 'csvs' && <CsvsPage data={data} query={query} onSelect={setSelectedId} onOpenPrivateFile={openVault} />}
        {activePage === 'hypotheses' && <HypothesesPage data={data} query={query} onSelect={setSelectedId} />}
        {activePage === 'correlations' && <CorrelationsPage data={data} query={query} onSelect={setSelectedId} />}
        {activePage === 'audit' && <AuditPage data={data} onOpenPrivateFile={openVault} />}
        {activePage === 'story' && <StoryPage data={data} />}
        {activePage === 'docs' && <DocsPage data={data} query={query} setQuery={setQuery} />}
      </main>

      {!isVaultPage && <DetailPanel selected={selected} data={data} index={index} onSelect={setSelectedId} onOpenPrivateFile={openVault} />}
    </div>
  );
}

function Header({ data, activePage, selected }) {
  const title = {
    overview: 'Visão geral',
    canvas: 'Canvas de lineage',
    files: 'Arquivos brutos',
    vault: 'Dados privados',
    columns: 'Catálogo de colunas',
    features: 'Features',
    tables: 'Tabelas intermediárias',
    csvs: 'CSVs finais',
    hypotheses: 'Hipóteses H1-H4',
    correlations: 'Explorer de correlações',
    audit: 'Auditoria semanal',
    story: 'Storytelling final',
    docs: 'Documentação FarmLab',
  }[activePage];

  return (
    <section className="top-header">
      <div>
        <p className="eyebrow">MonolithFarm Atlas NDVI</p>
        <h1>{title}</h1>
        <p className="lead">
          Do dado bruto à hipótese: origem, cálculo, evidência, limitações e interpretação em um fluxo auditável.
        </p>
      </div>
      <div className="metric-grid">
        <Metric label="Brutos" value={data.summary.rawFiles} />
        <Metric label="Colunas" value={data.summary.rawColumns} />
        <Metric label="Lineage" value={data.summary.lineageRecords} />
        <Metric label="Docs" value={data.summary.farmLabDocs} />
      </div>
      {selected && (
        <div className="header-selection">
          <span>{TYPE_LABELS[selected.kind] || selected.kind}</span>
          <strong>{selected.title}</strong>
        </div>
      )}
    </section>
  );
}

function OverviewPage({ data, setActivePage }) {
  const decisions = data.story?.decisionSummary || [];
  return (
    <section className="overview-page">
      <div className="hero-panel">
        <div>
          <p className="eyebrow">Pergunta central</p>
          <h2>Por que as áreas 4.0 não foram melhores no geral?</h2>
          <p>
            O atlas separa grão e silagem, rastreia NDVI, solo exposto, clima, pragas e operação, e mostra qual evidência sustenta cada hipótese.
          </p>
        </div>
        <div className="hero-facts">
          <Metric label="CSVs finais" value={data.summary.finalCsvs} />
          <Metric label="Features" value={data.summary.features} />
          <Metric label="Drivers" value={data.summary.drivers} />
          <Metric label="Cobertura" value={`${Math.round(data.summary.minCoveragePct * 100)}%`} />
        </div>
      </div>

      <div className="journey-grid">
        {[
          ['1. Brutos', 'Arquivos reais em data/: OneSoil, Metos, EKOS, MIIP e Cropman.', 'files'],
          ['2. Colunas', 'Dicionário oficial/inferido, exemplos reais, nulos e uso no pipeline.', 'columns'],
          ['3. Features', 'Transformações como b1_mean -> ndvi_mean_week e b1_pct_solo -> soil_pct_week.', 'features'],
          ['4. Evidência', 'CSVs, correlações, gráficos, hipóteses e auditoria por semana.', 'canvas'],
        ].map(([title, text, page]) => (
          <button key={title} className="journey-card" onClick={() => setActivePage(page)}>
            <span>{title}</span>
            <p>{text}</p>
            <ChevronRight size={16} />
          </button>
        ))}
      </div>

      <div className="two-column">
        <section className="content-panel">
          <h2>Fechamento por par</h2>
          <PreviewTable rows={decisions} />
        </section>
        <section className="content-panel">
          <h2>Gates críticos</h2>
          <StatusList rows={data.criticalTargets || []} primary="target" secondary="business_reason" status="status" />
        </section>
      </div>
    </section>
  );
}

function CanvasPage({
  data,
  selected,
  graphMode,
  setGraphMode,
  detailLevel,
  setDetailLevel,
  showEdgeLabels,
  setShowEdgeLabels,
  enabledTypes,
  setTypeEnabled,
  nodes,
  edges,
  onNodesChange,
  onEdgesChange,
  onNodeClick,
  onSelect,
  resetLayout,
}) {
  const denseGraph = nodes.length > 80 || edges.length > 140;
  return (
    <section className="canvas-layout">
      <div className="canvas-toolbar">
        <div className="segmented">
          {['pipeline', 'focus', 'columns', 'all'].map((mode) => (
            <button key={mode} className={graphMode === mode ? 'active' : ''} onClick={() => setGraphMode(mode)}>
              {mode === 'pipeline' ? 'Pipeline' : mode === 'focus' ? 'Vizinhança' : mode === 'columns' ? 'Colunas' : 'Tudo'}
            </button>
          ))}
        </div>
        <div className="segmented subtle">
          {['curated', 'expanded'].map((mode) => (
            <button key={mode} className={detailLevel === mode ? 'active' : ''} onClick={() => setDetailLevel(mode)}>
              {mode === 'curated' ? 'Curado' : 'Expandido'}
            </button>
          ))}
        </div>
        <button className="icon-action" onClick={() => setShowEdgeLabels(!showEdgeLabels)}>
          <CircleDot size={15} />
          {showEdgeLabels ? 'Ocultar labels' : 'Mostrar labels'}
        </button>
        <button className="icon-action" onClick={resetLayout}>
          <RotateCcw size={15} />
          Resetar layout
        </button>
        <div className="toolbar-note">
          <Network size={15} /> {nodes.length} nós · {edges.length} relações
        </div>
      </div>

      <div className="filter-pills">
        {CATEGORY_FILTERS.map((type) => (
          <button key={type} className={enabledTypes.has(type) ? 'active' : ''} onClick={() => setTypeEnabled(type)}>
            <span style={{ background: TYPE_COLORS[type] }} />
            {TYPE_LABELS[type]}
          </button>
        ))}
      </div>

      <div className="edge-legend">
        <span title="Cinza: arquivo contém coluna ou coluna bruta é origem da feature."><i style={{ background: EDGE_COLORS.raw_origin }} />cinza: origem/coluna</span>
        <span title="Azul/teal: tabela cria feature ou feature alimenta tabela intermediária posterior."><i style={{ background: EDGE_COLORS.creates_feature }} />azul: transformação</span>
        <span title="Azul forte: linhagem direta entre coluna bruta e coluna final."><i style={{ background: EDGE_COLORS.lineage }} />azul forte: lineage</span>
        <span title="Laranja: uma flag técnica vira um driver interpretável."><i style={{ background: EDGE_COLORS.driver_from_flag }} />laranja: driver</span>
        <span title="Verde: feature ou tabela alimenta CSV final."><i style={{ background: EDGE_COLORS.feeds_csv }} />verde: CSV final</span>
        <span title="Roxo: objeto sustenta uma hipótese H1-H4."><i style={{ background: EDGE_COLORS.supports_hypothesis }} />roxo: hipótese</span>
        <span title="Violeta: CSV ou driver gera gráfico."><i style={{ background: EDGE_COLORS.generates_chart }} />violeta: gráfico</span>
      </div>
      <div className="graph-hint">
        Setas apontam de origem para uso. O modo Curado oculta colunas massivas no canvas; os detalhes completos ficam no painel e nas páginas Colunas/CSVs.
      </div>

      <div className="canvas-stage">
        <ReactFlow
          key={`${graphMode}-${detailLevel}-${selected?.id || 'none'}-${nodes.length}-${edges.length}`}
          nodes={nodes}
          edges={edges}
          nodeTypes={{ atlas: AtlasNode }}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          fitView
          fitViewOptions={{ padding: 0.18 }}
          minZoom={0.12}
          maxZoom={1.7}
          defaultEdgeOptions={{ animated: false, type: 'smoothstep' }}
          proOptions={{ hideAttribution: true }}
        >
          <Background color="var(--canvas-grid)" gap={22} />
          {!denseGraph && <MiniMap pannable zoomable nodeColor={(node) => TYPE_COLORS[String(node.data?.kind)] || '#94a3b8'} />}
          <Controls />
        </ReactFlow>
      </div>
      <LineageStrip data={data} selected={selected} onSelect={onSelect} />
    </section>
  );
}

function AtlasNode({ data }) {
  return (
    <div className={`atlas-node node-${data.kind} ${data.selected ? 'node-selected' : ''} ${data.matched ? 'node-matched' : ''}`}>
      <Handle id="in" type="target" position={Position.Left} />
      <div className="node-topline">
        <span className="node-dot" />
        <span>{TYPE_LABELS[data.kind] || data.kind}</span>
      </div>
      <strong>{data.label}</strong>
      {data.subtitle && <small>{data.subtitle}</small>}
      <Handle id="out" type="source" position={Position.Right} />
    </div>
  );
}

function FilesPage({ data, query, onSelect, onOpenPrivateFile }) {
  const q = normalize(query);
  const files = data.rawFiles.filter((file) => !q || normalize(JSON.stringify(file)).includes(q));
  const groups = groupBy(files, (file) => file.source_group || 'Outros');
  return (
    <section className="page-scroll">
      {Object.entries(groups).map(([group, rows]) => (
        <section className="content-panel" key={group}>
          <div className="section-title">
            <h2>{group}</h2>
            <span>{rows.length} objetos</span>
          </div>
          <div className="object-grid">
            {rows.map((file) => (
              <button key={file.id} className="object-card" onClick={() => onSelect(file.id)}>
                <span>{file.kind === 'directory' ? 'Diretório' : 'Arquivo'}</span>
                <strong>{file.source_key}</strong>
                <small>{file.description}</small>
                <em>{fmt(file.rows)} linhas · {fmt(file.columnsDetailed?.length || file.columns)} colunas · {compactPeriod(file.temporal_min, file.temporal_max)}</em>
                {file.kind === 'file' && (
                  <span
                    className="inline-card-action"
                    onClick={(event) => {
                      event.stopPropagation();
                      onOpenPrivateFile(file.source_key);
                    }}
                  >
                    <Eye size={13} /> ver arquivo completo
                  </span>
                )}
              </button>
            ))}
          </div>
        </section>
      ))}
    </section>
  );
}

function ColumnsPage({ data, query, setQuery, onSelect }) {
  const q = normalize(query);
  const rows = [...data.rawColumns.map((item) => ({ ...item, id: `raw-column:${item.source_key}:${item.column}`, layer: 'bruto', table: item.source_key })), ...data.lineage]
    .filter((row) => !q || normalize(JSON.stringify(row)).includes(q))
    .slice(0, 700);
  return (
    <section className="table-page">
      <InlineSearch value={query} setValue={setQuery} placeholder="Buscar coluna, arquivo, feature, hipótese..." />
      <div className="lineage-table">
        {rows.map((row, idx) => (
          <button key={row.id || row.lineage_id || idx} onClick={() => onSelect(row.id || nodeIdFromLineage(row))}>
            <span>{row.layer || row.usage_status}</span>
            <strong>{row.column}</strong>
            <small>{row.table || row.source_key}</small>
            <em>{row.documentation || row.definition || row.raw_columns || row.pipeline_usage}</em>
          </button>
        ))}
      </div>
      {!rows.length && <EmptyResult />}
    </section>
  );
}

function FeaturesPage({ data, query, setQuery, onSelect }) {
  const q = normalize(query);
  const features = data.features.filter((feature) => !q || normalize(JSON.stringify(feature)).includes(q));
  const drivers = (data.drivers || []).filter((driver) => !q || normalize(JSON.stringify(driver)).includes(q));
  return (
    <section className="table-page features-page">
      <div className="features-education">
        <VariableReadingGuide />
        <DriverCatalog drivers={drivers} onSelect={onSelect} />
      </div>
      <InlineSearch value={query} setValue={setQuery} placeholder="Buscar feature, threshold, função ou hipótese..." />
      <div className="section-title feature-list-title">
        <div>
          <h2>Features técnicas</h2>
          <p>Variáveis calculadas pelo pipeline. Abra uma feature para ver origem, função, código, filtros, uso posterior e hipóteses afetadas.</p>
        </div>
        <span>{features.length} variáveis</span>
      </div>
      <div className="feature-grid">
        {features.map((feature) => (
          <button key={feature.id} onClick={() => onSelect(feature.id)}>
            <span>{feature.feature_type || feature.category}</span>
            <strong>{feature.name}</strong>
            <p>{feature.definition}</p>
            <small>{feature.table_where_born || feature.born_table} · {feature.function}</small>
          </button>
        ))}
      </div>
    </section>
  );
}

function VariableReadingGuide() {
  return (
    <section className="content-panel variable-guide">
      <div className="section-title">
        <div>
          <h2>Como ler qualquer variável</h2>
          <p>Use sempre a mesma ordem: origem, regra, motivo analítico e uso downstream.</p>
        </div>
        <span>guia</span>
      </div>
      <div className="variable-steps">
        <article>
          <span>1</span>
          <strong>Origem</strong>
          <p>Veja se nasceu de arquivo bruto, tabela intermediária, feature técnica, driver interpretativo ou CSV final.</p>
        </article>
        <article>
          <span>2</span>
          <strong>Regra</strong>
          <p>Leia função geradora, código, filtros, joins, agregações e thresholds no painel lateral.</p>
        </article>
        <article>
          <span>3</span>
          <strong>Motivo</strong>
          <p>Identifique a pergunta analítica: medir valor contínuo, marcar evento, somar risco, comparar par ou sustentar hipótese.</p>
        </article>
        <article>
          <span>4</span>
          <strong>Uso</strong>
          <p>Siga downstream para CSV final, gráfico, correlação e hipótese; use o canvas só como mapa, e o painel como explicação.</p>
        </article>
      </div>
      <p>
        Convenção útil: <code>_week</code> costuma ser agregação semanal; <code>_flag</code> marca evento por regra/threshold; <code>_share</code> é proporção; <code>_count</code> é contagem; <code>_rate</code> é frequência; <code>_gap</code> é diferença entre tratamentos ou valor esperado/aplicado.
      </p>
    </section>
  );
}

function DriverCatalog({ drivers, onSelect }) {
  if (!drivers.length) return null;
  return (
    <section className="content-panel driver-catalog">
      <div className="section-title">
        <div>
          <h2>Drivers analíticos</h2>
          <p>Drivers resumem flags técnicas para explicar semanas ruins, sem afirmar causalidade sozinhos.</p>
        </div>
        <span>{drivers.length}</span>
      </div>
      <div className="driver-grid">
        {drivers.map((driver) => (
          <article key={driver.id}>
            <div>
              <span>{driver.flag_feature}</span>
              <strong>{driver.name || driver.driver}</strong>
              <p>{driver.definition}</p>
            </div>
            <dl>
              <dt>Regra</dt>
              <dd>{driver.rule}</dd>
              <dt>Entradas</dt>
              <dd>{splitAny(driver.source_columns).join(', ') || 'n/a'}</dd>
              <dt>Fontes</dt>
              <dd>{splitAny(driver.raw_sources).join(', ') || 'n/a'}</dd>
            </dl>
            <div className="card-actions">
              <button onClick={() => onSelect(driver.id)}>Abrir driver</button>
              {driver.flag_feature && <button onClick={() => onSelect(`feature:${driver.flag_feature}`)}>Abrir flag</button>}
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}

function TablesPage({ data, query, onSelect }) {
  const q = normalize(query);
  const tables = data.intermediateTables.filter((table) => !q || normalize(JSON.stringify(table)).includes(q));
  return (
    <section className="page-scroll">
      <div className="object-grid wide">
        {tables.map((table) => (
          <button key={table.id} className="object-card tall" onClick={() => onSelect(table.id)}>
            <span>{table.function}</span>
            <strong>{table.name}</strong>
            <small>{table.description}</small>
            <em>{fmt(table.rowCount)} linhas · {fmt(table.columns?.length)} colunas · {table.file_path}</em>
          </button>
        ))}
      </div>
    </section>
  );
}

function CsvsPage({ data, query, onSelect, onOpenPrivateFile }) {
  const q = normalize(query);
  const csvs = data.finalCsvs.filter((csv) => !q || normalize(JSON.stringify(csv)).includes(q));
  return (
    <section className="page-scroll">
      <div className="object-grid wide">
        {csvs.map((csv) => (
          <button key={csv.id} className="object-card tall" onClick={() => onSelect(csv.id)}>
            <span>CSV final</span>
            <strong>{csv.name}</strong>
            <small>{csv.description}</small>
            <em>{fmt(csv.rowCount)} linhas · {fmt(csv.columns?.length)} colunas · {(csv.related_hypotheses || []).join(', ')}</em>
            <span
              className="inline-card-action"
              onClick={(event) => {
                event.stopPropagation();
                onOpenPrivateFile(csv.name);
              }}
            >
              <Eye size={13} /> ver CSV completo
            </span>
          </button>
        ))}
      </div>
    </section>
  );
}

function PrivateDataPage({ auth, refreshAuth, initialQuery }) {
  const [username, setUsername] = useState(auth.user || 'monolito_farm');
  const [password, setPassword] = useState('');
  const [loginError, setLoginError] = useState('');
  const [files, setFiles] = useState([]);
  const [fileQuery, setFileQuery] = useState(initialQuery || '');
  const [scope, setScope] = useState('all');
  const [selectedFileId, setSelectedFileId] = useState('');
  const [loadingFiles, setLoadingFiles] = useState(false);

  useEffect(() => {
    if (initialQuery) setFileQuery(initialQuery);
  }, [initialQuery]);

  useEffect(() => {
    if (!auth.authenticated) return;
    setLoadingFiles(true);
    apiJson('/api/private/files')
      .then((payload) => {
        const nextFiles = payload.files || [];
        setFiles(nextFiles);
        setSelectedFileId((current) => current || defaultPrivateFile(nextFiles, fileQuery)?.id || '');
      })
      .catch((error) => setLoginError(privateApiMessage(error)))
      .finally(() => setLoadingFiles(false));
  }, [auth.authenticated]);

  useEffect(() => {
    if (!files.length || !fileQuery) return;
    const match = bestPrivateFileMatch(files, fileQuery);
    if (match) setSelectedFileId(match.id);
  }, [files, fileQuery]);

  const login = async (event) => {
    event.preventDefault();
    setLoginError('');
    try {
      await apiJson('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      setPassword('');
      await refreshAuth();
    } catch (error) {
      setLoginError(privateApiMessage(error));
    }
  };

  const logout = async () => {
    await apiJson('/api/auth/logout', { method: 'POST' }).catch(() => null);
    setFiles([]);
    setSelectedFileId('');
    await refreshAuth();
  };

  if (auth.status === 'checking') {
    return <section className="page-scroll"><div className="content-panel private-panel"><p>Verificando sessão privada...</p></div></section>;
  }

  if (!auth.authenticated) {
    return (
      <section className="page-scroll private-data-page">
        <PrivateLoginPanel
          configured={auth.configured}
          username={username}
          password={password}
          setUsername={setUsername}
          setPassword={setPassword}
          login={login}
          error={loginError || auth.error}
          apiAvailable={auth.apiAvailable !== false}
        />
      </section>
    );
  }

  const q = normalize(fileQuery);
  const filtered = files.filter((file) => {
    const scopeOk = scope === 'all' || file.scope === scope || file.view === scope;
    const categoryOk = scope === 'all' || file.category === scope;
    return (scopeOk || categoryOk) && (!q || normalize(`${file.name} ${file.relPath} ${file.scope} ${file.extension} ${file.categoryLabel}`).includes(q));
  });
  const selectedFile = files.find((file) => file.id === selectedFileId) || filtered[0] || files[0];
  const counts = countPrivateFiles(files);

  return (
    <section className="private-data-page">
      <div className="private-toolbar">
        <div>
          <span className="private-kicker"><Lock size={13} /> sessão autenticada</span>
          <h2>Data Vault</h2>
          <p>Todos os arquivos disponíveis: brutos, CSVs finais, tabelas intermediárias, auditoria, imagens e documentos. A leitura é paginada e autenticada.</p>
        </div>
        <div className="private-summary-strip">
          <span><strong>{fmt(files.length)}</strong> arquivos</span>
          <span><strong>{fmt(counts.raw)}</strong> brutos</span>
          <span><strong>{fmt(counts.generated)}</strong> gerados</span>
          <span><strong>{fmt(counts.tables)}</strong> tabelas</span>
        </div>
        <button className="secondary-action" onClick={logout}><LogOut size={15} /> sair</button>
      </div>

      <div className="private-layout">
        <aside className="private-browser">
          <InlineSearch value={fileQuery} setValue={setFileQuery} placeholder="Buscar CSV, bruto, NDVI, event_driver_lift..." />
          <div className="private-filter-row">
            {[
              ['all', 'Todos'],
              ['raw', 'Brutos'],
              ['generated_final', 'CSVs finais'],
              ['generated_intermediate', 'Intermediários'],
              ['generated_lineage', 'Lineage/auditoria'],
              ['generated_review', 'Revisões'],
              ['generated', 'Gerados'],
              ['table', 'Tabelas'],
              ['image', 'Imagens'],
              ['pdf', 'PDFs'],
            ].map(([key, label]) => (
              <button key={key} className={scope === key ? 'active' : ''} onClick={() => setScope(key)}>{label}</button>
            ))}
          </div>
          <div className="private-file-list">
            {loadingFiles && <p>Carregando inventário privado...</p>}
            {filtered.map((file) => (
              <button key={file.id} className={selectedFile?.id === file.id ? 'active' : ''} onClick={() => setSelectedFileId(file.id)}>
                {fileIcon(file.view)}
                <span>
                  <strong>{file.name}</strong>
                  <small>{file.categoryLabel || (file.scope === 'raw' ? 'bruto' : 'gerado')} · {file.relPath}</small>
                </span>
                <em>{formatBytes(file.size)}</em>
              </button>
            ))}
            {!filtered.length && !loadingFiles && <EmptyResult />}
          </div>
        </aside>
        <PrivateFileViewer file={selectedFile} />
      </div>
    </section>
  );
}

function PrivateLoginPanel({ configured, apiAvailable, username, password, setUsername, setPassword, login, error }) {
  return (
    <section className="content-panel private-login-panel">
      <div className="private-login-copy">
        <span className="private-kicker"><Lock size={13} /> acesso restrito</span>
        <h2>Entrar no Data Vault</h2>
        <p>
          O atlas público mostra metadados, lineage e explicações. O conteúdo integral de CSV, parquet, imagens, PDFs e outputs gerados fica em uma área privada com leitura por demanda.
        </p>
        <div className="vault-capability-grid">
          <article>
            <Table2 size={17} />
            <strong>CSV/Parquet paginado</strong>
            <span>Brutos, intermediários, finais, lineage e auditoria.</span>
          </article>
          <article>
            <ImageIcon size={17} />
            <strong>Imagens e documentos</strong>
            <span>NDVI, PDFs e arquivos auxiliares só após login.</span>
          </article>
          <article>
            <ShieldCheck size={17} />
            <strong>Sem dados no bundle</strong>
            <span>O JSON público não carrega conteúdo real dos arquivos.</span>
          </article>
        </div>
      </div>
      <form onSubmit={login} className="login-form">
        {!apiAvailable && (
          <div className="auth-warning">
            A API privada não está ativa. Pare o servidor Vite atual e rode <code>{PRIVATE_API_START_COMMAND}</code>.
          </div>
        )}
        {apiAvailable && !configured && (
          <div className="auth-warning">
            Configure <code>MONOLITH_ATLAS_PASSWORD</code> ou inicie pelo script <code>scripts/start_lineage_atlas.ps1</code>, que gera uma senha temporária local.
          </div>
        )}
        <label>
          Usuário
          <input value={username} onChange={(event) => setUsername(event.target.value)} autoComplete="username" />
        </label>
        <label>
          Senha
          <input value={password} onChange={(event) => setPassword(event.target.value)} type="password" autoComplete="current-password" />
        </label>
        {error && <p className="form-error">{error}</p>}
        <button type="submit" disabled={!apiAvailable || !configured}><LogIn size={16} /> Entrar</button>
      </form>
    </section>
  );
}

function PrivateFileViewer({ file }) {
  const [offset, setOffset] = useState(0);
  const [limit, setLimit] = useState(100);
  const [query, setQuery] = useState('');
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  useEffect(() => {
    setOffset(0);
    setQuery('');
    setPayload(null);
    setError('');
  }, [file?.id]);

  useEffect(() => {
    if (!file || file.view !== 'table') return;
    setLoading(true);
    setError('');
    const params = new URLSearchParams({ offset: String(offset), limit: String(limit), q: query });
    apiJson(`/api/private/files/${encodeURIComponent(file.id)}/rows?${params.toString()}`)
      .then(setPayload)
      .catch((fetchError) => setError(privateApiMessage(fetchError)))
      .finally(() => setLoading(false));
  }, [file?.id, file?.view, offset, limit, query]);

  if (!file) {
    return <section className="private-viewer empty-panel"><h2>Nenhum arquivo selecionado</h2><p>Escolha um arquivo no inventário privado.</p></section>;
  }

  const blobUrl = `/api/private/files/${encodeURIComponent(file.id)}/blob`;
  return (
    <section className="private-viewer">
      <div className="private-viewer-head">
        <div>
          <span>{file.scope === 'raw' ? 'arquivo bruto' : 'arquivo gerado'} · {file.view}</span>
          <h2>{file.name}</h2>
          <p>{file.relPath}</p>
        </div>
        <a className="secondary-action" href={blobUrl} target="_blank" rel="noreferrer"><Download size={15} /> abrir</a>
      </div>
      <div className="fact-grid compact">
        <div><span>Tamanho</span><strong>{formatBytes(file.size)}</strong></div>
        <div><span>Modificado</span><strong>{new Date(file.modifiedAt).toLocaleString('pt-BR')}</strong></div>
        <div><span>Extensão</span><strong>{file.extension || 'sem extensão'}</strong></div>
        <div><span>Acesso</span><strong>autenticado</strong></div>
      </div>

      {file.view === 'table' && (
        <>
          <div className="table-control-row">
            <InlineSearch value={query} setValue={(value) => { setQuery(value); setOffset(0); }} placeholder="Filtrar dentro deste arquivo..." />
            <select value={limit} onChange={(event) => { setLimit(Number(event.target.value)); setOffset(0); }}>
              {[50, 100, 200, 500].map((value) => <option key={value} value={value}>{value} linhas</option>)}
            </select>
          </div>
          {error && <p className="form-error">{error}</p>}
          {loading && <p className="private-loading">Carregando página...</p>}
          {payload && <PrivateRowsTable payload={payload} />}
          {payload && (
            <div className="pagination-row">
              <button onClick={() => setOffset(Math.max(0, offset - limit))} disabled={offset === 0}><ChevronLeft size={15} /> anterior</button>
              <span>{fmt(offset + 1)}-{fmt(Math.min(offset + limit, payload.matchedRows || 0))} de {fmt(payload.matchedRows || 0)} linhas exibíveis</span>
              <button onClick={() => setOffset(offset + limit)} disabled={offset + limit >= (payload.matchedRows || 0)}>próxima <ChevronRight size={15} /></button>
            </div>
          )}
        </>
      )}

      {file.view === 'image' && <img className="private-image-preview" src={blobUrl} alt={file.name} />}
      {file.view === 'pdf' && <iframe className="private-pdf-preview" src={blobUrl} title={file.name} />}
      {file.view === 'text' && <PrivateTextViewer file={file} />}
      {file.view === 'binary' && <p className="private-binary-note">Este formato não tem visualização tabular no navegador. O acesso continua autenticado pelo botão abrir.</p>}
    </section>
  );
}

function PrivateRowsTable({ payload }) {
  const columns = payload.columns || [];
  const rows = payload.rows || [];
  return (
    <>
      <div className="private-table-status">
        <span>{fmt(columns.length)} colunas</span>
        <span>{payload.query ? `${fmt(payload.matchedRows || 0)} linhas filtradas` : payload.totalRows ? `${fmt(payload.totalRows)} linhas` : 'leitura paginada sob demanda'}</span>
        <span>mostrando {fmt(rows.length)} linhas</span>
      </div>
      <div className="private-table-wrap" role="region" aria-label="Visualizacao tabular autenticada">
        <table className="private-data-table">
          <thead>
            <tr>
              <th className="row-number">#</th>
              {columns.map((column) => <th key={column} title={column}>{column}</th>)}
            </tr>
          </thead>
        <tbody>
          {rows.map((row, index) => (
            <tr key={index}>
              <td className="row-number">{fmt((payload.offset || 0) + index + 1)}</td>
              {columns.map((column) => {
                const value = String(row[column] ?? '');
                return <td key={column} title={value}>{value}</td>;
              })}
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && <div className="empty-table-message">Nenhuma linha encontrada nesta pagina.</div>}
    </div>
    </>
  );
}

function PrivateTextViewer({ file }) {
  const [payload, setPayload] = useState(null);
  const [offset, setOffset] = useState(0);
  const limit = 220;
  useEffect(() => {
    apiJson(`/api/private/files/${encodeURIComponent(file.id)}/text?offset=${offset}&limit=${limit}`)
      .then(setPayload);
  }, [file.id, offset]);
  return (
    <>
      <pre className="private-text-preview">{(payload?.lines || []).join('\n')}</pre>
      {payload && (
        <div className="pagination-row">
          <button onClick={() => setOffset(Math.max(0, offset - limit))} disabled={offset === 0}><ChevronLeft size={15} /> anterior</button>
          <span>{fmt(offset + 1)}-{fmt(Math.min(offset + limit, payload.totalLines || 0))} de {fmt(payload.totalLines || 0)} linhas</span>
          <button onClick={() => setOffset(offset + limit)} disabled={offset + limit >= (payload.totalLines || 0)}>próxima <ChevronRight size={15} /></button>
        </div>
      )}
    </>
  );
}

function bestPrivateFileMatch(files, query) {
  const q = normalize(query);
  if (!q) return null;
  return files.find((file) => normalize(file.name) === q || normalize(file.name.replace(/\.[^.]+$/, '')) === q)
    || files.find((file) => normalize(`${file.name} ${file.relPath}`).includes(q));
}

function defaultPrivateFile(files, query) {
  return bestPrivateFileMatch(files, query)
    || files.find((file) => file.name === 'dataset_overview.csv')
    || files.find((file) => file.name === 'ndvi_metadata.csv')
    || files.find((file) => file.category === 'generated_final')
    || files.find((file) => file.view === 'table')
    || files[0];
}

function countPrivateFiles(files) {
  return {
    raw: files.filter((file) => file.scope === 'raw').length,
    generated: files.filter((file) => file.scope === 'generated').length,
    tables: files.filter((file) => file.view === 'table').length,
  };
}

function fileIcon(view) {
  if (view === 'table') return <Table2 size={16} />;
  if (view === 'image') return <ImageIcon size={16} />;
  if (view === 'pdf' || view === 'text') return <FileText size={16} />;
  return <FileCode2 size={16} />;
}

function HypothesesPage({ data, query, onSelect }) {
  const q = normalize(query);
  const register = data.story?.hypothesisRegister || [];
  const rows = data.hypotheses.filter((hyp) => !q || normalize(JSON.stringify(hyp)).includes(q));
  return (
    <section className="page-scroll">
      <div className="feature-grid">
        {rows.map((hyp) => (
          <button key={hyp.id || hyp.key} onClick={() => onSelect(`hypothesis:${hyp.id || hyp.key}`)}>
            <span>Hipótese</span>
            <strong>{hyp.id || hyp.key}</strong>
            <p>{hyp.question}</p>
            <small>{hyp.decision_rule}</small>
          </button>
        ))}
      </div>
      <section className="content-panel">
        <h2>Registro por par</h2>
        <PreviewTable rows={register} />
      </section>
    </section>
  );
}

function CorrelationsPage({ data, query, onSelect }) {
  const [correlationQuery, setCorrelationQuery] = useState(query || '');
  const q = normalize(correlationQuery);
  const allRows = data.correlations || [];
  const rows = allRows
    .filter((row) => !q || correlationSearchText(row).includes(q))
    .slice(0, 80);
  const matchedVariables = unique(rows.flatMap((row) => [row.feature, ...(row.aliases || [])]).filter(Boolean)).slice(0, 10);
  return (
    <section className="correlations-page">
      <div className="correlation-help">
        <div>
          <strong>Como ler correlações</strong>
          <p>
            Esta página não cria drivers. Ela mostra associações calculadas em <code>weekly_correlations.csv</code>, usando variáveis numéricas do dataframe de modelagem. Quando a busca usa um nome interpretativo, o atlas tenta resolver o alias para a medida ou flag técnica correspondente.
          </p>
          <p>
            Para entender por que uma variável existe, abra a feature, o driver ou o CSV final no painel lateral: ali ficam origem, regra, função geradora, código e uso downstream.
          </p>
        </div>
      </div>

      <InlineSearch value={correlationQuery} setValue={setCorrelationQuery} placeholder="Buscar variável, driver, feature, CSV, alvo ou hipótese..." />

      {correlationQuery && (
        <div className="correlation-match-summary">
          <strong>{rows.length ? `${rows.length} correlações encontradas` : 'Nenhuma correlação direta encontrada'}</strong>
          <span>
            {rows.length
              ? `A busca foi resolvida para: ${matchedVariables.join(', ')}`
              : 'Esse termo pode existir no lineage, mas não aparece como variável numérica no CSV de correlações.'}
          </span>
        </div>
      )}

      <div className="correlation-grid">
        {rows.map((row) => (
          <article key={row.id} className="correlation-card">
            <div>
              <span>{row.analysis_target} · {row.comparison_pair}</span>
              <strong>{row.feature}</strong>
              {row.aliases?.length > 0 && (
                <div className="alias-row">
                  {row.aliases.slice(0, 5).map((alias) => <em key={alias}>{alias}</em>)}
                </div>
              )}
              <p>{row.humanInterpretation}</p>
            </div>
            <div className="corr-meter">
              <div style={{ width: `${Math.min(100, Math.abs(Number(row.pearson_r || 0)) * 100)}%` }} />
            </div>
            <FactGrid facts={[
              ['Origem', row.dataframe],
              ['Período', row.period],
              ['Pearson', fmtNum(row.pearson_r, 3)],
              ['p-value', fmtNum(row.pearson_p, 4)],
              ['n', row.observations],
              ['Força', row.strength],
            ]} />
            <p className="detail-desc">{row.caveat}</p>
            {row.originFeature?.raw_columns && <p className="detail-desc">Origem da variável: {row.originFeature.raw_columns}</p>}
            <div className="card-actions">
              {row.originFeature?.column && <button onClick={() => onSelect(`feature:${row.originFeature.column}`)}>Abrir feature</button>}
              <button onClick={() => setCorrelationQuery(row.feature)}>Focar variável</button>
            </div>
          </article>
        ))}
      </div>
      {!rows.length && <EmptyResult />}
    </section>
  );
}

function AuditPage({ data, onOpenPrivateFile }) {
  const [filters, setFilters] = useState({});
  const rows = data.audit || [];
  if (!rows.length && data.meta?.privateSamplesRedacted) {
    return (
      <section className="page-scroll">
        <div className="content-panel private-audit-lock">
          <span className="private-kicker"><Lock size={13} /> dados linha-a-linha</span>
          <h2>Auditoria completa protegida</h2>
          <p>A auditoria por semana/área depende de linhas finais, intermediárias e brutas. Esses registros não ficam no JSON público; abra os CSVs privados paginados depois do login.</p>
          <button className="secondary-action" onClick={() => onOpenPrivateFile('pairwise_weekly_features.csv')}><Eye size={15} /> abrir dados privados</button>
        </div>
      </section>
    );
  }
  const filtered = rows.filter((row) => Object.entries(filters).every(([key, value]) => !value || String(row[key] || '').includes(String(value)))).slice(0, 80);
  const selected = filtered.find((row) => row.intermediateRows?.ndvi_events?.length)
    || filtered.find((row) => row.rawRows?.ndvi_clean?.length)
    || filtered[0];
  const filterKeys = ['season_id', 'area_label', 'comparison_pair', 'treatment', 'week_start'];
  return (
    <section className="audit-page">
      <div className="audit-filters">
        {filterKeys.map((key) => (
          <label key={key}>
            <span>{key}</span>
            <select value={filters[key] || ''} onChange={(event) => setFilters({ ...filters, [key]: event.target.value })}>
              <option value="">Todos</option>
              {unique(rows.map((row) => row[key]).filter(Boolean)).map((value) => <option key={value}>{value}</option>)}
            </select>
          </label>
        ))}
      </div>
      <div className="audit-layout">
        <div className="audit-list">
          {filtered.map((row) => (
            <article key={row.id}>
              <span>{row.week_start} · {row.comparison_pair}</span>
              <strong>{row.area_label}</strong>
              <p>{row.context}</p>
            </article>
          ))}
        </div>
        <div className="content-panel">
          <h2>Primeira linha filtrada</h2>
          {selected ? (
            <>
              <p className="detail-desc">{selected.context}</p>
              {selected.image?.url && <img className="ndvi-image" src={selected.image.url} alt={`NDVI ${selected.image.date}`} />}
              <ListBlock title="Flags ativas" values={selected.activeFlags} />
              <PreviewTable title="Linha final" rows={[selected.finalRow]} />
              <PreviewTable title="Eventos intermediários" rows={selected.intermediateRows?.ndvi_events || []} />
              <PreviewTable title="NDVI limpo" rows={selected.rawRows?.ndvi_clean || []} />
            </>
          ) : (
            <EmptyResult />
          )}
        </div>
      </div>
    </section>
  );
}

function DocsPage({ data, query, setQuery }) {
  const q = normalize(query);
  const docs = (data.docs?.index || []).filter((doc) => !q || normalize(JSON.stringify(doc)).includes(q)).slice(0, q ? 300 : 160);
  return (
    <section className="docs-layout">
      <InlineSearch value={query} setValue={setQuery} placeholder="Buscar docs, schemas, b1_pct_solo, Metos, MIIP..." />
      <div className="docs-columns">
        <section className="content-panel">
          <h2>Rotas /docs mapeadas</h2>
          {(data.docs?.routes || []).map((route) => (
            <a className="doc-route" href={route.url} target="_blank" rel="noreferrer" key={`${route.key}-${route.url}`}>
              <BookOpen size={16} />
              <span>{route.key}</span>
              <small>{route.status}</small>
            </a>
          ))}
        </section>
        <section className="content-panel">
          <h2>Schemas e trechos oficiais</h2>
          <div className="doc-list">
            {docs.map((doc, index) => (
              <article key={`${doc.kind}-${doc.key}-${index}`}>
                <span>{doc.kind}</span>
                <strong>{doc.title || doc.key}</strong>
                <p>{truncate(doc.text, 420)}</p>
                {doc.url && <a href={doc.url} target="_blank" rel="noreferrer">{doc.url}</a>}
              </article>
            ))}
          </div>
        </section>
      </div>
    </section>
  );
}

function StoryPage({ data }) {
  return (
    <section className="story-page">
      <div className="story-hero">
        <p className="eyebrow">Storytelling final</p>
        <h2>{data.story?.headline}</h2>
      </div>
      <div className="story-copy">
        {(data.story?.paragraphs || []).map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
      </div>
      <section className="content-panel">
        <h2>Decisão operacional</h2>
        <PreviewTable rows={data.story?.decisionSummary || []} />
      </section>
      <section className="content-panel">
        <h2>Hipóteses H1-H4</h2>
        <PreviewTable rows={data.story?.hypothesisRegister || []} />
      </section>
    </section>
  );
}

function DetailPanel({ selected, data, index, onSelect, onOpenPrivateFile }) {
  if (!selected) {
    return (
      <aside className="detail-panel empty-panel">
        <Waypoints />
        <h2>Selecione um objeto</h2>
        <p>O painel mostra origem, cálculo, evidência, downstream, documentação e limitações.</p>
      </aside>
    );
  }
  const { kind, item, lineage } = selected;
  return (
    <aside className="detail-panel">
      <div className="detail-type">{TYPE_LABELS[kind] || kind}</div>
      <h2>{selected.title}</h2>
      <p className="detail-desc">{selected.description || selected.subtitle || 'Objeto rastreável do pipeline.'}</p>
      <RelationBadges selected={selected} />
      {kind === 'rawFile' && <RawFileDetail item={item} onSelect={onSelect} onOpenPrivateFile={onOpenPrivateFile} />}
      {kind === 'rawColumn' && <RawColumnDetail item={item} data={data} onSelect={onSelect} />}
      {kind === 'intermediate' && <TableDetail item={item} />}
      {kind === 'feature' && <FeatureDetail item={item} onSelect={onSelect} />}
      {kind === 'driver' && <DriverDetail item={item} onSelect={onSelect} />}
      {kind === 'csv' && <CsvDetail item={item} onSelect={onSelect} onOpenPrivateFile={onOpenPrivateFile} />}
      {kind === 'csvColumn' && <CsvColumnDetail item={item} lineage={lineage} onSelect={onSelect} />}
      {kind === 'hypothesis' && <HypothesisDetail item={item} data={data} onSelect={onSelect} />}
      {kind === 'chart' && <ChartDetail item={item} data={data} />}
      <section className="panel-section">
        <h3>Por que importa</h3>
        <p>{selected.why || 'Ajuda a conectar evidência operacional, NDVI e decisão 4.0 versus convencional.'}</p>
      </section>
    </aside>
  );
}

function RawFileDetail({ item, onSelect, onOpenPrivateFile }) {
  return (
    <>
      <FactGrid facts={[
        ['Fonte', item.source_group],
        ['Tipo', item.kind],
        ['Linhas', fmt(item.rows)],
        ['Colunas', fmt(item.columnsDetailed?.length || item.columns)],
        ['Período', compactPeriod(item.temporal_min, item.temporal_max)],
      ]} />
      <section className="panel-section"><h3>Caminho</h3><code>{item.path}</code></section>
      <ListBlock title="Campos-chave" values={(item.columnsDetailed || []).filter((col) => String(col.usage_status || '').includes('usada')).slice(0, 18).map((col) => col.column)} />
      {item.kind === 'file' && <PrivateOpenBlock label="Abrir arquivo bruto completo" search={item.source_key} onOpenPrivateFile={onOpenPrivateFile} />}
      <ColumnChips columns={item.columnsDetailed} onSelect={(column) => onSelect(`raw-column:${item.source_key}:${column.column}`)} />
      <PreviewTable rows={item.preview} />
      {item.documentation?.farm_docs_url && <a className="doc-link" href={item.documentation.farm_docs_url} target="_blank" rel="noreferrer">Abrir documentação oficial</a>}
    </>
  );
}

function RawColumnDetail({ item, data, onSelect }) {
  const downstream = data.lineage.filter((row) => String(row.raw_columns || '').includes(item.column)).slice(0, 18);
  return (
    <>
      <FactGrid facts={[
        ['Arquivo', item.source_key],
        ['Fonte', item.source_group],
        ['Tipo', item.dtype],
        ['Nulos amostra', `${Math.round(Number(item.null_pct_sample || 0) * 100)}%`],
        ['Status doc', item.documentation_status],
        ['Uso', item.usage_status],
      ]} />
      <section className="panel-section"><h3>Significado</h3><p>{item.documentation || item.practical_interpretation || 'Sem definição oficial específica.'}</p></section>
      <section className="panel-section"><h3>Exemplos reais</h3><p className="sample-values">{item.examples || 'Sem exemplos.'}</p></section>
      <section className="panel-section"><h3>Uso no pipeline</h3><p>{item.pipeline_usage || 'Uso direto não detectado no pipeline NDVI.'}</p></section>
      <section className="panel-section">
        <h3>Usada depois em</h3>
        <div className="relation-list">
          {downstream.map((row) => <button key={row.lineage_id} onClick={() => onSelect(nodeIdFromLineage(row))}><ArrowRight size={13} />{row.table} · {row.column}</button>)}
        </div>
      </section>
    </>
  );
}

function TableDetail({ item }) {
  return (
    <>
      <FactGrid facts={[['Função', item.function], ['Linhas', fmt(item.rowCount)], ['Colunas', fmt(item.columns?.length)], ['Arquivo', item.file_path]]} />
      <ListBlock title="Entradas" values={item.inputs} />
      <ListBlock title="Filtros" values={item.filters} />
      <ListBlock title="Joins" values={item.joins} />
      <ListBlock title="Agregações" values={item.aggregations} />
      <ListBlock title="Colunas criadas" values={item.created_columns} />
      <PreviewTable rows={item.preview} />
      <CodeBlock code={item.code} />
    </>
  );
}

function FeatureDetail({ item, onSelect }) {
  return (
    <>
      <FactGrid facts={[['Tipo', item.feature_type || item.category], ['Nasce em', item.table_where_born || item.born_table], ['Função', item.function], ['Arquivo', item.file_path]]} />
      <FeatureMethodExplainer item={item} />
      <ListBlock title="Colunas de entrada" values={splitAny(item.source_columns)} onClick={(value) => onSelect(`raw-column:any:${value}`)} />
      <ListBlock title="Fontes brutas" values={splitAny(item.raw_sources)} />
      <ListBlock title="Filtros" values={splitAny(item.filters_involved || item.filters)} />
      <ListBlock title="Thresholds" values={splitAny(item.thresholds)} />
      <ListBlock title="Tabelas seguintes" values={splitAny(item.appears_in_tables)} onClick={(value) => onSelect(`intermediate:${value}`)} />
      <ListBlock title="CSVs dependentes" values={splitAny(item.appears_in_csvs)} onClick={(value) => onSelect(`csv:${value}`)} />
      <ListBlock title="Gráficos" values={splitAny(item.related_charts)} onClick={(value) => onSelect(`chart:${value}`)} />
      <ListBlock title="Hipóteses" values={splitAny(item.related_hypotheses || item.hypotheses)} onClick={(value) => onSelect(`hypothesis:${value}`)} />
      <CodeBlock code={item.code} />
    </>
  );
}

function DriverDetail({ item, onSelect }) {
  return (
    <>
      <FactGrid facts={[['Flag', item.flag_feature], ['Tabela', item.born_table], ['Evidência', item.evidence_column], ['Regra', item.rule]]} />
      <section className="panel-section"><h3>Definição</h3><p>{item.definition}</p></section>
      <section className="panel-section"><h3>Interpretação</h3><p>{item.interpretation}</p></section>
      <ListBlock title="Fontes brutas" values={item.raw_sources} />
      <ListBlock title="Gráficos" values={item.charts} onClick={(value) => onSelect(`chart:${value}`)} />
      <ListBlock title="Limitações" values={item.limitations} />
    </>
  );
}

function CsvDetail({ item, onSelect, onOpenPrivateFile }) {
  return (
    <>
      <FactGrid facts={[['Linhas', fmt(item.rowCount)], ['Colunas', fmt(item.columns?.length)], ['Função', item.function || 'não catalogada'], ['Arquivo', item.file_path || 'notebook_outputs/complete_ndvi']]} />
      <CsvMethodExplainer item={item} />
      <PrivateOpenBlock label="Abrir CSV completo" search={item.name} onOpenPrivateFile={onOpenPrivateFile} />
      <ListBlock title="Dependências" values={item.dependencies} />
      <ListBlock title="Gráficos" values={item.related_charts} onClick={(value) => onSelect(`chart:${value}`)} />
      <ListBlock title="Hipóteses" values={item.related_hypotheses} onClick={(value) => onSelect(`hypothesis:${value}`)} />
      <ColumnChips columns={item.profile} onSelect={(column) => onSelect(`csv-column:${item.name}:${column.column}`)} />
      <PreviewTable rows={item.preview} />
      <CodeBlock code={item.code} />
    </>
  );
}

function CsvColumnDetail({ item, lineage, onSelect }) {
  return (
    <>
      <FactGrid facts={[['CSV', item.table], ['Coluna', item.column], ['Status', lineage?.mapping_status], ['Confiança', lineage?.mapping_confidence]]} />
      <section className="panel-section"><h3>Definição</h3><p>{lineage?.definition || item.documentation || 'Coluna final exportada pelo pipeline.'}</p></section>
      <ListBlock title="Colunas brutas de origem" values={splitAny(lineage?.raw_columns)} />
      <ListBlock title="Upstream" values={splitAny(lineage?.upstream_columns)} />
      <ListBlock title="Filtros" values={splitAny(lineage?.filters)} />
      <ListBlock title="Agregações" values={splitAny(lineage?.aggregations)} />
      <ListBlock title="Gráficos" values={splitAny(lineage?.charts)} onClick={(value) => onSelect(`chart:${value}`)} />
      <ListBlock title="Hipóteses" values={splitAny(lineage?.hypotheses)} onClick={(value) => onSelect(`hypothesis:${value}`)} />
      <ListBlock title="Limitações" values={splitAny(lineage?.limitations)} />
    </>
  );
}

function HypothesisDetail({ item, data, onSelect }) {
  const rows = (data.story?.hypothesisRegister || []).filter((row) => row.hypothesis_id === item.id || row.hypothesis_id === item.key);
  return (
    <>
      <section className="panel-section"><h3>Pergunta</h3><p>{item.question}</p></section>
      <section className="panel-section"><h3>Regra de decisão</h3><p>{item.decision_rule}</p></section>
      <ListBlock title="Métricas" values={item.metrics} />
      <ListBlock title="CSVs" values={item.csvs} />
      <ListBlock title="Gráficos" values={item.charts} onClick={(value) => onSelect(`chart:${value}`)} />
      <PreviewTable rows={rows} />
    </>
  );
}

function ChartDetail({ item, data }) {
  return (
    <>
      <ChartPreview chart={item} data={data} />
      <section className="panel-section"><h3>Leitura esperada</h3><p>{item.interpretation}</p></section>
      <ListBlock title="Dataframes de origem" values={item.dataframe_sources} />
      <section className="panel-section"><h3>Cálculo</h3><p>{item.calculation_origin}</p></section>
      <ChartMethodExplainer chart={item} />
      <CodeBlock code={item.chart_code} />
    </>
  );
}

function FeatureMethodExplainer({ item }) {
  const info = featureMethodInfo(item?.name || item?.key);
  if (!info) return null;
  return (
    <section className="panel-section method-explainer compact">
      <div className="method-heading">
        <span>{info.badge}</span>
        <h3>{info.title}</h3>
      </div>
      <p>{info.summary}</p>
      <div className="formula-card">{info.formula}</div>
      <div className="method-grid">
        {info.points.map((point) => (
          <article key={point.label}>
            <span>{point.label}</span>
            <strong>{point.value}</strong>
          </article>
        ))}
      </div>
      <p className="method-note">{info.note}</p>
    </section>
  );
}

function CsvMethodExplainer({ item }) {
  const name = item?.name || item?.key;
  if (name === 'event_driver_lift.csv') return <EventDriverLiftExplainer item={item} />;
  if (name === 'transition_model_frame.csv') return <TransitionModelExplainer />;
  if (name === 'weekly_correlations.csv') return <WeeklyCorrelationsExplainer />;
  return null;
}

function EventDriverLiftExplainer({ item }) {
  const preview = item?.preview || [];
  const topRow = preview.find((row) => Number.isFinite(Number(row.delta_pp))) || preview[0] || {};
  const drivers = unique(preview.map((row) => row.driver).filter(Boolean)).slice(0, 6);
  return (
    <section className="panel-section method-explainer">
      <div className="method-heading">
        <span>Método</span>
        <h3>Como o event_driver_lift é criado</h3>
      </div>
      <p>
        Este CSV não cria uma causa nova. Ele compara a frequência de cada driver nas semanas-problema contra a frequência do mesmo driver nas semanas sem problema, sempre dentro do mesmo par de comparação.
      </p>
      <div className="method-steps">
        <article><span>1</span><p><strong>Entrada</strong> vem de <code>ndvi_phase_timeline</code>, onde NDVI, clima, pragas e operações já estão alinhados por <code>season_id</code> e <code>week_start</code>.</p></article>
        <article><span>2</span><p><strong>Semana-problema</strong> é marcada quando existe <code>major_drop_flag</code>, <code>low_vigor_flag</code> ou evento <code>queda</code>, <code>queda_forte</code> ou <code>baixo_vigor</code>.</p></article>
        <article><span>3</span><p><strong>Drivers</strong> são flags interpretativas: <code>high_soil_flag</code> vira <code>solo_exposto</code>, <code>engine_risk_flag</code> vira <code>risco_de_motor</code>, e assim por diante.</p></article>
        <article><span>4</span><p><strong>Lift</strong> calcula se o driver ficou sobre-representado nas semanas-problema.</p></article>
      </div>
      <div className="formula-card">
        problem_rate = média(flag nas semanas-problema); baseline_rate = média(flag nas demais semanas); delta_pp = (problem_rate - baseline_rate) * 100; lift_ratio = problem_rate / baseline_rate.
      </div>
      <div className="method-grid">
        <article><span>Drivers no preview</span><strong>{drivers.join(', ') || 'n/a'}</strong></article>
        <article><span>Exemplo visível</span><strong>{topRow.driver ? `${topRow.driver}: delta_pp ${fmtNum(topRow.delta_pp, 1)}` : 'n/a'}</strong></article>
        <article><span>Evidência alta</span><strong>{'pelo menos 3 semanas-problema e delta_pp >= 20'}</strong></article>
        <article><span>Evidência média</span><strong>{'pelo menos 3 semanas-problema e delta_pp >= 10'}</strong></article>
      </div>
      <div className="column-role-list">
        {eventDriverColumnRoles().map((role) => (
          <article key={role.column}>
            <strong>{role.column}</strong>
            <p>{role.meaning}</p>
            <small>{role.reason}</small>
          </article>
        ))}
      </div>
      <p className="method-note">
        Faz sentido para triagem porque responde “o que aparece junto das semanas ruins?”. Não prova causalidade: cobertura de dados, thresholds e defasagens ainda precisam ser auditados por linha/semana.
      </p>
    </section>
  );
}

function TransitionModelExplainer() {
  return (
    <section className="panel-section method-explainer">
      <div className="method-heading">
        <span>Base de modelagem</span>
        <h3>Por que existem as features de problema</h3>
      </div>
      <p>
        O <code>transition_model_frame.csv</code> pega a timeline semanal e cria um alvo simples: quanto o NDVI muda na semana seguinte. As features de solo, clima, praga e operação entram para testar associação temporal, não para substituir o dado bruto.
      </p>
      <div className="formula-card">target_next_ndvi_delta = next_ndvi_mean_week - ndvi_mean_week</div>
      <div className="method-grid">
        <article><span>Numéricas</span><strong>soil_pct_week, water_balance_mm_week, avg_pest_count_week, engine_temp_hot_share_week...</strong></article>
        <article><span>Flags</span><strong>low_vigor_flag, major_drop_flag, pest_risk_flag, ops_risk_flag...</strong></article>
        <article><span>Score</span><strong>risk_flag_count soma as flags de risco da semana</strong></article>
        <article><span>Uso</span><strong>correlações e modelo linear ridge interpretável</strong></article>
      </div>
    </section>
  );
}

function WeeklyCorrelationsExplainer() {
  return (
    <section className="panel-section method-explainer">
      <div className="method-heading">
        <span>Pearson</span>
        <h3>Como as correlações finais são calculadas</h3>
      </div>
      <p>
        As correlações usam apenas variáveis numéricas do <code>transition_model_frame.csv</code>. Por isso uma busca por <code>solo_exposto</code> mostra <code>soil_pct_week</code> e/ou <code>high_soil_flag</code>: o primeiro é a medida; o segundo é a flag que vira o driver.
      </p>
      <div className="method-steps">
        <article><span>1</span><p>Para cada alvo, o código usa <code>ndvi_mean_week</code> e <code>target_next_ndvi_delta</code>.</p></article>
        <article><span>2</span><p>Para cada feature candidata, remove nulos e exige pelo menos 5 observações.</p></article>
        <article><span>3</span><p>Calcula Pearson, Spearman, p-value, direção e força da associação.</p></article>
      </div>
      <p className="method-note">Pearson mede relação linear. Um valor negativo entre solo exposto e NDVI sugere associação, mas não comprova que solo exposto causou a queda.</p>
    </section>
  );
}

function ChartMethodExplainer({ chart }) {
  const key = chart?.key;
  if (key === 'drivers_problem_weeks') {
    return (
      <section className="panel-section method-explainer compact">
        <div className="method-heading"><span>Gráfico final</span><h3>De onde vêm as barras de driver</h3></div>
        <p>As barras usam <code>event_driver_lift.csv</code>. O eixo do driver mostra nomes humanos; a altura normalmente usa <code>delta_pp</code>, e a cor separa <code>evidence_level</code>.</p>
        <div className="formula-card">{'driver flag semanal -> problem_rate/baseline_rate -> delta_pp -> barra do gráfico'}</div>
      </section>
    );
  }
  if (key === 'correlations') {
    return (
      <section className="panel-section method-explainer compact">
        <div className="method-heading"><span>Gráfico final</span><h3>De onde vêm as barras de correlação</h3></div>
        <p>O gráfico resume <code>weekly_correlations.csv</code>. Cada barra é a maior associação absoluta encontrada para uma feature, calculada a partir de Pearson/Spearman no dataframe de transição semanal.</p>
      </section>
    );
  }
  return null;
}

function eventDriverColumnRoles() {
  return [
    {
      column: 'comparison_pair',
      meaning: 'Par analítico onde a frequência do driver foi comparada.',
      reason: 'Evita misturar culturas/tratamentos diferentes; cada par tem seu próprio baseline.',
    },
    {
      column: 'driver',
      meaning: 'Nome humano do tipo de evidência observado.',
      reason: 'Resume uma flag técnica em linguagem de hipótese, mantendo a flag rastreável no painel.',
    },
    {
      column: 'problem_weeks',
      meaning: 'Quantidade de semanas ruins no par.',
      reason: 'Mostra o tamanho da base usada para comparar driver em semana-problema versus baseline.',
    },
    {
      column: 'problem_rate',
      meaning: 'Frequência do driver dentro das semanas-problema.',
      reason: 'Responde: quando o NDVI estava ruim, esse driver aparecia em que proporção?',
    },
    {
      column: 'baseline_rate',
      meaning: 'Frequência do mesmo driver fora das semanas-problema.',
      reason: 'Cria a referência justa dentro do mesmo par, sem comparar com outro contexto.',
    },
    {
      column: 'delta_pp',
      meaning: 'Diferença em pontos percentuais entre problem_rate e baseline_rate.',
      reason: 'Mostra sobre-representação absoluta do driver nas semanas ruins.',
    },
    {
      column: 'lift_ratio',
      meaning: 'Razão problem_rate / baseline_rate.',
      reason: 'Mostra multiplicador relativo; fica nulo quando o baseline não permite divisão confiável.',
    },
    {
      column: 'evidence_level',
      meaning: 'Classe qualitativa da evidência.',
      reason: 'Transforma delta_pp e número de semanas em leitura rápida: baixa, média ou alta.',
    },
  ];
}

function ChartPreview({ chart, data }) {
  const source = findChartSource(chart, data);
  const rows = source?.preview || [];
  const config = chartConfig(chart?.key, rows);
  return (
    <section className="panel-section">
      <h3>Prévia do gráfico</h3>
      <div className="chart-preview">
        <div className="chart-preview-head">
          <strong>{chart.title || chart.key}</strong>
          <span>{source?.name || chart.dataframe_sources?.[0] || 'fonte não encontrada'}</span>
        </div>
        {!rows.length && <p className="detail-desc">Sem preview suficiente no CSV de origem para desenhar este gráfico.</p>}
        {rows.length > 0 && config.kind === 'line' && <LinePreview rows={config.rows} xKey={config.xKey} yKey={config.yKey} seriesKey={config.seriesKey} />}
        {rows.length > 0 && config.kind === 'scatter' && <ScatterPreview rows={config.rows} xKey={config.xKey} yKey={config.yKey} seriesKey={config.seriesKey} />}
        {rows.length > 0 && config.kind === 'matrix' && <MatrixPreview rows={config.rows} />}
        {rows.length > 0 && config.kind === 'bar' && <BarPreview rows={config.rows} xKey={config.xKey} yKey={config.yKey} />}
      </div>
    </section>
  );
}

function BarPreview({ rows, xKey, yKey }) {
  const values = rows.map((row) => Number(row[yKey])).filter(Number.isFinite);
  const maxAbs = Math.max(1, ...values.map((value) => Math.abs(value)));
  return (
    <div className="bar-preview">
      {rows.slice(0, 10).map((row, index) => {
        const value = Number(row[yKey]);
        const width = Number.isFinite(value) ? Math.max(3, Math.abs(value) / maxAbs * 100) : 0;
        return (
          <div className="bar-row" key={`${row[xKey]}-${index}`}>
            <span>{shortLabel(row[xKey])}</span>
            <div><i className={value < 0 ? 'negative' : ''} style={{ width: `${width}%` }} /></div>
            <strong>{fmtNum(value, Math.abs(value) < 1 ? 3 : 1)}</strong>
          </div>
        );
      })}
    </div>
  );
}

function LinePreview({ rows, xKey, yKey, seriesKey }) {
  const series = Object.entries(groupBy(rows.filter((row) => Number.isFinite(Number(row[yKey]))), (row) => row[seriesKey] || 'série'));
  const values = rows.map((row) => Number(row[yKey])).filter(Number.isFinite);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min || 1;
  return (
    <svg className="line-preview" viewBox="0 0 320 150" role="img" aria-label={`Prévia de ${yKey}`}>
      <line x1="24" x2="304" y1="124" y2="124" />
      {series.slice(0, 5).map(([name, points], seriesIndex) => {
        const ordered = [...points].sort((a, b) => String(a[xKey]).localeCompare(String(b[xKey])));
        const d = ordered.map((row, index) => {
          const x = 28 + (index / Math.max(1, ordered.length - 1)) * 268;
          const y = 120 - ((Number(row[yKey]) - min) / spread) * 92;
          return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
        }).join(' ');
        return <path key={name} d={d} className={`series-${seriesIndex % 5}`} />;
      })}
    </svg>
  );
}

function ScatterPreview({ rows, xKey, yKey, seriesKey }) {
  const valid = rows.filter((row) => Number.isFinite(Number(row[yKey]))).slice(0, 80);
  const values = valid.map((row) => Number(row[yKey]));
  const min = Math.min(...values);
  const max = Math.max(...values);
  const spread = max - min || 1;
  const groups = unique(valid.map((row) => row[seriesKey] || 'série'));
  return (
    <svg className="line-preview" viewBox="0 0 320 150" role="img" aria-label={`Prévia de ${yKey}`}>
      <line x1="24" x2="304" y1="124" y2="124" />
      {valid.map((row, index) => {
        const x = 28 + (index / Math.max(1, valid.length - 1)) * 268;
        const y = 120 - ((Number(row[yKey]) - min) / spread) * 92;
        const seriesIndex = groups.indexOf(row[seriesKey] || 'série');
        return <circle key={`${row[xKey]}-${index}`} cx={x} cy={y} r="3.4" className={`series-${seriesIndex % 5}`} />;
      })}
    </svg>
  );
}

function MatrixPreview({ rows }) {
  const pairs = unique(rows.map((row) => row.comparison_pair));
  const hypotheses = unique(rows.map((row) => row.hypothesis_id));
  return (
    <div className="matrix-preview">
      <span />
      {pairs.map((pair) => <strong key={pair}>{pair}</strong>)}
      {hypotheses.map((hypothesis) => (
        <React.Fragment key={hypothesis}>
          <strong>{hypothesis}</strong>
          {pairs.map((pair) => {
            const row = rows.find((item) => item.hypothesis_id === hypothesis && item.comparison_pair === pair);
            return <i key={`${hypothesis}-${pair}`} className={`status-${normalize(row?.status || 'desconhecido')}`}>{row?.status || 'n/a'}</i>;
          })}
        </React.Fragment>
      ))}
    </div>
  );
}

function GlobalResults({ data, query, selectedId, onSelect }) {
  const results = useMemo(() => {
    const q = normalize(query);
    return data.graph.nodes
      .filter((node) => !q || normalize(`${node.label} ${node.subtitle} ${node.search}`).includes(q))
      .slice(0, q ? 30 : 18);
  }, [data, query]);
  return (
    <div className="global-results">
      <div className="rail-heading">Itens do lineage</div>
      {results.map((node) => (
        <button key={node.id} className={selectedId === node.id ? 'active' : ''} onClick={() => onSelect(node.id)}>
          <span style={{ background: TYPE_COLORS[node.type] }} />
          <div><strong>{node.label}</strong><small>{TYPE_LABELS[node.type] || node.type}</small></div>
        </button>
      ))}
    </div>
  );
}

function SearchBox({ value, setValue }) {
  return (
    <div className="search-card">
      <label><Search size={15} />Buscar qualquer coisa</label>
      <input value={value} onChange={(event) => setValue(event.target.value)} placeholder="ndvi_mean_week, b1_mean, solo_exposto..." />
    </div>
  );
}

function InlineSearch({ value, setValue, placeholder }) {
  return (
    <div className="inline-search">
      <Search size={16} />
      <input value={value} onChange={(event) => setValue(event.target.value)} placeholder={placeholder} />
    </div>
  );
}

function LineageStrip({ data, selected, onSelect }) {
  const lineage = selected?.lineage || (selected?.kind === 'feature' ? data.lineage.find((row) => row.lineage_id === `feature::${selected.item?.name}`) : null);
  const related = lineage
    ? [
        ...splitAny(lineage.raw_columns).map((value) => ({ label: value, kind: 'raw-column:any' })),
        ...splitAny(lineage.upstream_columns).map((value) => ({ label: value, kind: 'feature' })),
        ...splitAny(lineage.downstream_csvs).map((value) => ({ label: value, kind: 'csv' })),
        ...splitAny(lineage.hypotheses).map((value) => ({ label: value, kind: 'hypothesis' })),
      ]
    : [];
  return (
    <div className="lineage-strip">
      <strong>Relações rápidas</strong>
      <div>
        {related.slice(0, 22).map((item, idx) => (
          <button key={`${item.kind}-${item.label}-${idx}`} onClick={() => onSelect(`${item.kind}:${item.label}`)}>{item.kind} · {item.label}</button>
        ))}
        {!related.length && <span>Clique em uma coluna final, feature ou driver para ver upstream/downstream.</span>}
      </div>
    </div>
  );
}

function RelationBadges({ selected }) {
  const lineage = selected.lineage;
  if (!lineage) return null;
  return (
    <div className="badge-row">
      <span>{lineage.mapping_status}</span>
      {splitAny(lineage.hypotheses).map((hyp) => <span key={hyp}>{hyp}</span>)}
      {splitAny(lineage.charts).slice(0, 2).map((chart) => <span key={chart}>{chart}</span>)}
    </div>
  );
}

function FactGrid({ facts }) {
  return (
    <div className="fact-grid">
      {facts.filter(([, value]) => value !== undefined && value !== null && value !== '').map(([label, value]) => (
        <div key={label}><span>{label}</span><strong>{String(value)}</strong></div>
      ))}
    </div>
  );
}

function ListBlock({ title, values, onClick }: { title: string; values: any; onClick?: (value: string) => void }) {
  const list = splitAny(values);
  if (!list.length) return null;
  return (
    <section className="panel-section">
      <h3>{title}</h3>
      <div className="relation-list">
        {list.map((value) => (
          <button key={value} onClick={() => onClick?.(value)} disabled={!onClick}><ArrowRight size={13} />{value}</button>
        ))}
      </div>
    </section>
  );
}

function PrivateOpenBlock({ label, search, onOpenPrivateFile }) {
  if (!onOpenPrivateFile || !search) return null;
  return (
    <section className="panel-section private-open-block">
      <button onClick={() => onOpenPrivateFile(search)}>
        <Lock size={14} />
        <span>{label}</span>
      </button>
      <p>Conteúdo completo é carregado por API privada depois do login.</p>
    </section>
  );
}

function ColumnChips({ columns = [], onSelect }) {
  if (!columns?.length) return null;
  return (
    <section className="panel-section">
      <h3>Colunas</h3>
      <div className="chip-grid">
        {columns.slice(0, 120).map((column) => <button key={column.column} onClick={() => onSelect(column)}>{column.column}</button>)}
      </div>
    </section>
  );
}

function PreviewTable({ rows = [], title = 'Preview real' }) {
  if (!rows?.length) return null;
  const columns = Object.keys(rows[0]).slice(0, 12);
  return (
    <section className="panel-section preview-section">
      <h3>{title}</h3>
      <div className="preview-table">
        <table>
          <thead><tr>{columns.map((column) => <th key={column}>{column}</th>)}</tr></thead>
          <tbody>
            {rows.slice(0, 28).map((row, idx) => (
              <tr key={idx}>{columns.map((column) => <td key={column}>{String(row[column] ?? '')}</td>)}</tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function CodeBlock({ code }) {
  if (!code) return null;
  return <section className="panel-section"><h3>Código gerador</h3><pre><code>{code}</code></pre></section>;
}

function StatusList({ rows, primary, secondary, status }) {
  return (
    <div className="status-list">
      {rows.slice(0, 12).map((row) => (
        <article key={row[primary]}>
          <CheckCircle2 size={16} />
          <div><strong>{row[primary]}</strong><p>{row[secondary]}</p></div>
          <span>{row[status]}</span>
        </article>
      ))}
    </div>
  );
}

function Metric({ label, value }) {
  return <div className="metric"><span>{label}</span><strong>{value}</strong></div>;
}

function LoadingState() {
  return <div className="loading-state"><Activity /><h1>Carregando Atlas NDVI</h1><p>Lendo grafo, documentação e previews reais.</p></div>;
}

function ErrorState({ error }) {
  return (
    <div className="loading-state">
      <FileCode2 />
      <h1>Não foi possível abrir o Atlas</h1>
      <p>Gere os dados com <code>python scripts/export_lineage_atlas_data.py</code>.</p>
      <pre>{error}</pre>
    </div>
  );
}

function EmptyResult() {
  return <div className="empty-result"><Search /><p>Nenhum objeto encontrado com esse filtro.</p></div>;
}

function buildIndex(data) {
  const nodeById = new Map(data.graph.nodes.map((node) => [node.id, node]));
  const rawFileById = new Map(data.rawFiles.map((item) => [item.id, item]));
  const rawColumnById = new Map();
  for (const column of data.rawColumns || []) {
    rawColumnById.set(`raw-column:${column.source_key}:${column.column}`, column);
    if (!rawColumnById.has(`raw-column:any:${column.column}`)) rawColumnById.set(`raw-column:any:${column.column}`, column);
  }
  return {
    nodeById,
    rawFileById,
    rawColumnById,
    intermediateById: new Map(data.intermediateTables.map((item) => [item.id, item])),
    featureById: new Map(data.features.map((item) => [item.id, item])),
    driverById: new Map(data.drivers.map((item) => [item.id, item])),
    csvById: new Map(data.finalCsvs.map((item) => [item.id, item])),
    hypothesisById: new Map(data.hypotheses.map((item) => [`hypothesis:${item.id || item.key}`, item])),
    chartById: new Map(data.charts.map((item) => [`chart:${item.key}`, item])),
    lineageByCsvColumn: new Map(data.lineage.filter((row) => row.layer === 'csv_final').map((row) => [`csv-column:${row.table}:${row.column}`, row])),
    lineageByFeature: new Map(data.lineage.filter((row) => row.layer === 'feature').map((row) => [`feature:${row.column}`, row])),
    adjacency: buildAdjacency(data.graph.edges),
  };
}

function describeObject(id, data, index) {
  if (!id) return null;
  const normalizedId = resolveLooseId(id, index);
  const node = index.nodeById.get(normalizedId) || index.nodeById.get(id);
  const nodeId = node?.id || normalizedId;
  const kind = node?.type || typeFromId(nodeId);
  let item = null;
  let lineage = null;
  if (kind === 'rawFile') item = index.rawFileById.get(nodeId);
  if (kind === 'rawColumn') item = index.rawColumnById.get(nodeId);
  if (kind === 'intermediate') item = index.intermediateById.get(nodeId);
  if (kind === 'feature') { item = index.featureById.get(nodeId); lineage = index.lineageByFeature.get(nodeId); }
  if (kind === 'driver') item = index.driverById.get(nodeId);
  if (kind === 'csv') item = index.csvById.get(nodeId);
  if (kind === 'csvColumn') { lineage = index.lineageByCsvColumn.get(nodeId); item = lineage || { table: nodeId.split(':')[1], column: nodeId.split(':').slice(2).join(':') }; }
  if (kind === 'hypothesis') item = index.hypothesisById.get(nodeId);
  if (kind === 'chart') item = index.chartById.get(nodeId);
  if (!item && node) item = node;
  if (!lineage && item?.lineage_id) lineage = item;
  return {
    id: nodeId,
    kind,
    item,
    lineage,
    title: objectTitle(kind, item, node, nodeId),
    subtitle: node?.subtitle,
    description: item?.description || item?.definition || item?.summary || item?.question || node?.subtitle,
    why: item?.practical_interpretation || lineage?.plain_language || item?.interpretation,
  };
}

function objectTitle(kind, item, node, fallbackId) {
  if (kind === 'rawColumn') return item?.column || node?.label || fallbackId;
  if (kind === 'rawFile') return item?.source_key || item?.name || node?.label || fallbackId;
  if (kind === 'csvColumn') return item?.column || node?.label || fallbackId;
  return item?.name || item?.id || item?.key || item?.column || item?.source_key || node?.label || fallbackId;
}

function computeVisibleGraph(graph, index, { query, selectedId, graphMode, enabledTypes, detailLevel, showEdgeLabels }) {
  const q = normalize(query);
  const selectedResolved = resolveLooseId(selectedId, index);
  const selectedKind = index.nodeById.get(selectedResolved)?.type || typeFromId(selectedResolved);
  const keep = new Set();
  const matches = new Set();
  const pipelineLevel = new Set(
    detailLevel === 'expanded'
      ? ['rawFile', 'intermediate', 'feature', 'driver', 'csv', 'hypothesis', 'chart']
      : ['rawFile', 'intermediate', 'csv', 'hypothesis']
  );

  for (const node of graph.nodes) {
    if (!enabledTypes.has(node.type)) continue;
    const isMatch = q && normalize(`${node.label} ${node.subtitle} ${node.search}`).includes(q);
    if (isMatch) matches.add(node.id);
    if (graphMode === 'all') keep.add(node.id);
    if (!q && graphMode === 'pipeline' && pipelineLevel.has(node.type)) keep.add(node.id);
    if (!q && graphMode === 'columns' && (node.type === 'rawColumn' || node.type === 'csvColumn' || isMatch)) keep.add(node.id);
  }

  const focusSeeds = q ? [...matches] : selectedResolved ? [selectedResolved] : [];
  if (graphMode === 'focus' || q) {
    const focusRadius = detailLevel === 'expanded' && !q ? 2 : 1;
    for (const seed of focusSeeds.slice(0, 8)) {
      for (const id of collectNeighborhood(index.adjacency, seed, focusRadius)) keep.add(id);
    }
  }

  if (selectedResolved && !q) {
    keep.add(selectedResolved);
    if (graphMode === 'pipeline') {
      for (const id of collectNeighborhood(index.adjacency, selectedResolved, 1)) keep.add(id);
    }
  }
  if (graphMode === 'columns' && !q && selectedResolved) {
    for (const id of collectNeighborhood(index.adjacency, selectedResolved, 1)) keep.add(id);
  }
  if (q && keep.size === 0) {
    for (const id of matches) keep.add(id);
  }

  if (!q && graphMode === 'focus' && detailLevel === 'curated' && selectedKind) {
    for (const id of [...keep]) {
      if (id === selectedResolved || matches.has(id)) continue;
      const node = index.nodeById.get(id);
      if (node && !curatedFocusAllows(selectedKind, node.type)) keep.delete(id);
    }
  }

  const highlightedNodes = new Set([selectedResolved, ...matches].filter(Boolean));

  let flowNodes = graph.nodes
    .filter((node) => keep.has(node.id) && enabledTypes.has(node.type))
    .map((node) => ({
      id: node.id,
      type: 'atlas',
      position: node.position,
      data: { ...node, kind: node.type, selected: node.id === selectedResolved, matched: matches.has(node.id) },
      draggable: true,
    }));
  flowNodes = limitGraphNodes(flowNodes, graph.edges, {
    selectedId: selectedResolved,
    matches,
    graphMode,
    detailLevel,
  });
  const visibleIds = new Set(flowNodes.map((node) => node.id));
  let baseEdges = graph.edges.filter((edge) => keep.has(edge.source) && keep.has(edge.target) && visibleIds.has(edge.source) && visibleIds.has(edge.target));
  baseEdges = limitGraphEdges(baseEdges, {
    selectedId: selectedResolved,
    matches,
    graphMode,
    detailLevel,
  });
  const shouldAnimate = baseEdges.length <= 80 && flowNodes.length <= 70;

  const flowEdges = baseEdges
    .map((edge) => {
      const color = EDGE_COLORS[edge.relationType] || 'var(--edge)';
      const highlighted = highlightedNodes.has(edge.source) || highlightedNodes.has(edge.target);
      const focusView = graphMode === 'focus' || q;
      return {
        id: edge.id,
        source: edge.source,
        target: edge.target,
        sourceHandle: 'out',
        targetHandle: 'in',
        label: showEdgeLabels && flowNodes.length <= 55 && baseEdges.length <= 85 ? edge.humanLabel || edge.label : undefined,
        type: focusView ? 'simplebezier' : 'smoothstep',
        animated: shouldAnimate && highlighted,
        data: edge,
        markerEnd: { type: MarkerType.ArrowClosed, color },
        interactionWidth: 18,
        style: {
          stroke: color,
          strokeWidth: highlighted ? 2.8 : focusView ? 1.7 : 1.15,
          opacity: highlighted ? 0.92 : focusView ? 0.54 : 0.22,
        },
        labelStyle: { fill: 'var(--text-muted)', fontSize: 11, fontWeight: 700 },
      };
    });
  return layoutGraph(flowNodes, flowEdges, {
    focusView: graphMode === 'focus' || Boolean(q),
    selectedId: selectedResolved,
    seeds: focusSeeds,
  });
}

function curatedFocusAllows(selectedKind, candidateKind) {
  const compactRules = {
    rawFile: new Set(['rawFile', 'intermediate', 'feature', 'driver', 'csv', 'hypothesis', 'chart']),
    csv: new Set(['intermediate', 'feature', 'driver', 'csv', 'hypothesis', 'chart']),
    hypothesis: new Set(['driver', 'csv', 'hypothesis', 'chart']),
    chart: new Set(['driver', 'csv', 'hypothesis', 'chart']),
    intermediate: new Set(['intermediate', 'feature', 'driver', 'csv', 'hypothesis', 'chart']),
  };
  return compactRules[selectedKind]?.has(candidateKind) ?? true;
}

function limitGraphNodes(nodes, rawEdges, { selectedId, matches, graphMode, detailLevel }) {
  const maxNodes = graphMode === 'all' ? 180 : graphMode === 'columns' ? 120 : detailLevel === 'expanded' ? 110 : 56;
  if (nodes.length <= maxNodes) return nodes;

  const directNeighbors = new Set();
  for (const edge of rawEdges) {
    if (edge.source === selectedId) directNeighbors.add(edge.target);
    if (edge.target === selectedId) directNeighbors.add(edge.source);
  }

  return [...nodes]
    .sort((a, b) => {
      const scoreA = graphNodePriority(a, selectedId, matches, directNeighbors);
      const scoreB = graphNodePriority(b, selectedId, matches, directNeighbors);
      if (scoreA !== scoreB) return scoreA - scoreB;
      return String(a.data.kind + a.data.label).localeCompare(String(b.data.kind + b.data.label));
    })
    .slice(0, maxNodes);
}

function graphNodePriority(node, selectedId, matches, directNeighbors) {
  if (node.id === selectedId) return 0;
  if (matches.has(node.id)) return 1;
  if (directNeighbors.has(node.id)) return 2 + graphKindPriority(node.data.kind);
  return 20 + graphKindPriority(node.data.kind);
}

function graphKindPriority(kind) {
  return {
    driver: 0,
    csv: 1,
    hypothesis: 2,
    chart: 3,
    feature: 4,
    intermediate: 5,
    rawFile: 6,
    rawColumn: 7,
    csvColumn: 8,
  }[kind] ?? 9;
}

function limitGraphEdges(edges, { selectedId, matches, graphMode, detailLevel }) {
  const maxEdges = graphMode === 'all' ? 300 : graphMode === 'columns' ? 180 : detailLevel === 'expanded' ? 180 : 100;
  if (edges.length <= maxEdges) return edges;
  return [...edges]
    .sort((a, b) => {
      const scoreA = graphEdgePriority(a, selectedId, matches);
      const scoreB = graphEdgePriority(b, selectedId, matches);
      if (scoreA !== scoreB) return scoreA - scoreB;
      return String(a.source + a.target).localeCompare(String(b.source + b.target));
    })
    .slice(0, maxEdges);
}

function graphEdgePriority(edge, selectedId, matches) {
  if (edge.source === selectedId || edge.target === selectedId) return 0 + edgeRelationPriority(edge.relationType);
  if (matches.has(edge.source) || matches.has(edge.target)) return 10 + edgeRelationPriority(edge.relationType);
  return 20 + edgeRelationPriority(edge.relationType);
}

function edgeRelationPriority(type) {
  return {
    supports_hypothesis: 0,
    generates_csv: 1,
    generates_chart: 2,
    driver_from_flag: 3,
    feeds_csv: 4,
    creates_feature: 5,
    feeds_table: 6,
    raw_origin: 7,
    contains_column: 8,
    lineage: 9,
  }[type] ?? 10;
}

function layoutGraph(nodes, edges, { focusView, selectedId, seeds }) {
  if (!nodes.length) return { nodes, edges };
  return { nodes: layoutSequenceNodes(nodes, edges, { focusView, selectedId, seeds }), edges };
}

function layoutSequenceNodes(nodes, edges, { focusView, selectedId, seeds }) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const direct = new Set();
  const seedSet = new Set((selectedId && nodeIds.has(selectedId) ? [selectedId] : seeds || []).filter((id) => nodeIds.has(id)));
  for (const edge of edges) {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
    if (seedSet.has(edge.source)) direct.add(edge.target);
    if (seedSet.has(edge.target)) direct.add(edge.source);
  }

  const layerById = computeSequenceLayers(nodes, edges);
  const minLayer = Math.min(...Array.from(layerById.values()));
  const normalizedLayer = (node) => Number(layerById.get(node.id) ?? 0) - minLayer;
  const grouped = groupBy(nodes, (node) => String(normalizedLayer(node)));
  const positioned = [];
  const layerGap = focusView ? 250 : 285;
  const columnGap = focusView ? 188 : 198;
  const rowGap = focusView ? 88 : 94;

  for (const layerKey of Object.keys(grouped).sort((a, b) => Number(a) - Number(b))) {
    const rows = grouped[layerKey].sort((a, b) => sequenceSort(a, b, selectedId, direct));
    const rowsPerColumn = focusView ? 11 : rows.length > 22 ? 10 : 12;
    rows.forEach((node, index) => {
      const column = Math.floor(index / rowsPerColumn);
      const row = index % rowsPerColumn;
      positioned.push({
        ...node,
        position: {
          x: Number(layerKey) * layerGap + column * columnGap,
          y: 36 + row * rowGap,
        },
      });
    });
  }
  return positioned;
}

function computeSequenceLayers(nodes, edges) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const layerById: Map<string, number> = new Map(nodes.map((node) => [String(node.id), semanticLayer(node.data.kind)]));
  const sequenceEdges = edges
    .filter((edge) => nodeIds.has(edge.source) && nodeIds.has(edge.target))
    .filter((edge) => !isBackReference(edge.data?.relationType));

  for (let pass = 0; pass < 5; pass += 1) {
    let changed = false;
    for (const edge of sequenceEdges) {
      const sourceLayer = Number(layerById.get(edge.source) ?? 0);
      const targetLayer = Number(layerById.get(edge.target) ?? 0);
      const nextLayer = Math.min(11, sourceLayer + 1);
      if (targetLayer <= sourceLayer && targetLayer < nextLayer) {
        layerById.set(edge.target, nextLayer);
        changed = true;
      }
    }
    if (!changed) break;
  }
  return layerById;
}

function isBackReference(type) {
  return type === 'raw_origin';
}

function semanticLayer(kind) {
  return {
    rawFile: 0,
    rawColumn: 1,
    intermediate: 2,
    feature: 3,
    driver: 4,
    csv: 5,
    csvColumn: 6,
    hypothesis: 7,
    chart: 8,
  }[kind] ?? 9;
}

function sequenceSort(a, b, selectedId, direct) {
  const selectedDelta = Number(b.id === selectedId) - Number(a.id === selectedId);
  if (selectedDelta) return selectedDelta;
  const directDelta = Number(direct.has(b.id)) - Number(direct.has(a.id));
  if (directDelta) return directDelta;
  const kindDelta = semanticLayer(a.data.kind) - semanticLayer(b.data.kind);
  if (kindDelta) return kindDelta;
  return String(a.data.label).localeCompare(String(b.data.label));
}

function layoutFocusNodes(nodes, edges, selectedId, seeds) {
  const nodeIds = new Set(nodes.map((node) => node.id));
  const sourceToTargets = new Map();
  const targetToSources = new Map();
  for (const edge of edges) {
    if (!nodeIds.has(edge.source) || !nodeIds.has(edge.target)) continue;
    if (!sourceToTargets.has(edge.source)) sourceToTargets.set(edge.source, new Set());
    if (!targetToSources.has(edge.target)) targetToSources.set(edge.target, new Set());
    sourceToTargets.get(edge.source).add(edge.target);
    targetToSources.get(edge.target).add(edge.source);
  }

  const layerById = new Map();
  const focusSeeds = (selectedId && nodeIds.has(selectedId) ? [selectedId] : seeds.filter((id) => nodeIds.has(id))).slice(0, 5);
  for (const seed of focusSeeds) layerById.set(seed, 0);
  walkLayers(focusSeeds, targetToSources, layerById, -1);
  walkLayers(focusSeeds, sourceToTargets, layerById, 1);

  for (const node of nodes) {
    if (layerById.has(node.id)) continue;
    layerById.set(node.id, fallbackLayer(node.data.kind));
  }

  const minLayer = Math.min(...layerById.values());
  const grouped = groupBy(nodes, (node) => String(layerById.get(node.id)));
  const positioned = [];
  for (const [layerKey, rows] of Object.entries(grouped)) {
    const layer = Number(layerKey);
    const rowsPerColumn = rows.length > 24 ? 10 : 12;
    rows
      .sort((a, b) => {
        const selectedDelta = Number(b.id === selectedId) - Number(a.id === selectedId);
        if (selectedDelta) return selectedDelta;
        return String(a.data.kind + a.data.label).localeCompare(String(b.data.kind + b.data.label));
      })
      .forEach((node, index) => {
        const column = Math.floor(index / rowsPerColumn);
        const row = index % rowsPerColumn;
        positioned.push({
          ...node,
          position: {
            x: (layer - minLayer) * 310 + column * 190,
            y: 44 + row * 98,
          },
        });
      });
  }
  return positioned;
}

function walkLayers(seeds, adjacency, layerById, direction) {
  let frontier = seeds;
  for (let depth = 1; depth <= 3; depth += 1) {
    const next = [];
    for (const id of frontier) {
      for (const neighbor of adjacency.get(id) || []) {
        const layer = depth * direction;
        if (!layerById.has(neighbor) || Math.abs(layer) < Math.abs(layerById.get(neighbor))) {
          layerById.set(neighbor, layer);
          next.push(neighbor);
        }
      }
    }
    frontier = next;
  }
}

function fallbackLayer(kind) {
  return {
    rawFile: -3,
    rawColumn: -3,
    intermediate: -2,
    feature: -1,
    driver: 0,
    csv: 1,
    csvColumn: 1,
    hypothesis: 2,
    chart: 2,
  }[kind] ?? 0;
}

function findChartSource(chart, data) {
  const sourceNames = chart?.dataframe_sources || [];
  const candidates = new Set(sourceNames.flatMap((name) => [name, `${name}.csv`, String(name).replace(/\.csv$/, '')]));
  return (data.finalCsvs || []).find((csv) => {
    const stem = String(csv.name).replace(/\.csv$/, '');
    return candidates.has(csv.name) || candidates.has(stem);
  });
}

function chartConfig(key, rows) {
  const configs = {
    ndvi_weekly_by_area: { kind: 'line', xKey: 'week_start', yKey: 'ndvi_mean_week', seriesKey: 'area_label' },
    gap_weekly: { kind: 'line', xKey: 'week_start', yKey: 'gap_ndvi_mean_week_4_0_minus_convencional', seriesKey: 'comparison_pair' },
    ndvi_mean_by_area: { kind: 'bar', xKey: 'area_label', yKey: 'mean' },
    outliers_ndvi: { kind: 'scatter', xKey: 'date', yKey: 'ndvi_zscore', seriesKey: 'area_label' },
    drivers_problem_weeks: { kind: 'bar', xKey: 'driver', yKey: 'delta_pp' },
    correlations: { kind: 'bar', xKey: 'feature', yKey: 'strongest_abs_correlation' },
    hypothesis_summary: { kind: 'matrix' },
    hypothesis_h1_effect: { kind: 'bar', xKey: 'comparison_pair', yKey: 'advantage_4_0', filter: (row) => row.metric === 'ndvi_mean_week' },
    hypothesis_h2_problem_rates: { kind: 'bar', xKey: 'metric', yKey: 'advantage_4_0', filter: (row) => String(row.metric).includes('flag') },
    outlook_pre_harvest: { kind: 'bar', xKey: 'area_label', yKey: 'trajectory_score' },
  };
  const config = configs[key] || inferChartConfig(rows);
  const filteredRows = (config.filter ? rows.filter(config.filter) : rows)
    .filter((row) => config.kind === 'matrix' || Number.isFinite(Number(row[config.yKey])));
  return { ...config, rows: filteredRows };
}

function inferChartConfig(rows) {
  const first = rows[0] || {};
  const keys = Object.keys(first);
  const yKey = keys.find((key) => Number.isFinite(Number(first[key]))) || keys[0];
  const xKey = keys.find((key) => key !== yKey) || keys[0];
  return { kind: 'bar', xKey, yKey };
}

function buildAdjacency(edges) {
  const map = new Map();
  for (const edge of edges) {
    if (!map.has(edge.source)) map.set(edge.source, new Set());
    if (!map.has(edge.target)) map.set(edge.target, new Set());
    map.get(edge.source).add(edge.target);
    map.get(edge.target).add(edge.source);
  }
  return map;
}

function collectNeighborhood(adjacency, start, radius) {
  const seen = new Set([start]);
  let frontier = new Set([start]);
  for (let depth = 0; depth < radius; depth += 1) {
    const next = new Set();
    for (const id of frontier) {
      for (const neighbor of adjacency.get(id) || []) {
        if (!seen.has(neighbor)) {
          seen.add(neighbor);
          next.add(neighbor);
        }
      }
    }
    frontier = next;
  }
  return seen;
}

function resolveLooseId(id, index) {
  if (!id) return id;
  if (index.nodeById.has(id)) return id;
  if (id.startsWith('raw-column:any:')) {
    const column = id.replace('raw-column:any:', '');
    const found = [...index.rawColumnById.keys()].find((key) => key !== id && key.endsWith(`:${column}`));
    return found || id;
  }
  return id;
}

function nodeIdFromLineage(row) {
  if (row.layer === 'bruto') return `raw-column:any:${row.column}`;
  if (row.layer === 'feature') return `feature:${row.column}`;
  if (row.layer === 'driver') return `driver:${row.column}`;
  if (row.layer === 'csv_final') return `csv-column:${row.table}:${row.column}`;
  if (row.layer === 'intermediario') return `intermediate:${row.table}`;
  return row.lineage_id || row.id;
}

function typeFromId(id) {
  if (id.startsWith('raw-file:')) return 'rawFile';
  if (id.startsWith('raw-column:')) return 'rawColumn';
  if (id.startsWith('intermediate:')) return 'intermediate';
  if (id.startsWith('feature:')) return 'feature';
  if (id.startsWith('driver:')) return 'driver';
  if (id.startsWith('csv-column:')) return 'csvColumn';
  if (id.startsWith('csv:')) return 'csv';
  if (id.startsWith('hypothesis:')) return 'hypothesis';
  if (id.startsWith('chart:')) return 'chart';
  return 'unknown';
}

function featureMethodInfo(name: string) {
  const methods: AnyRecord = {
    soil_pct_week: {
      badge: 'Agregação semanal',
      title: 'Como solo exposto vira feature',
      summary: 'A coluna bruta b1_pct_solo do NDVI é renomeada para soil_pct e agregada por área e semana. Ela é a medida numérica usada nas correlações.',
      formula: 'soil_pct_week = média semanal de soil_pct por season_id + week_start',
      note: 'Quando o usuário busca solo_exposto, o atlas mostra soil_pct_week porque o nome humano do driver nasce dessa medida.',
      points: [
        { label: 'Origem bruta', value: 'ndvi_metadata.csv / b1_pct_solo' },
        { label: 'Tabela onde nasce', value: 'pairwise_weekly_features' },
        { label: 'Flag derivada', value: 'high_soil_flag' },
        { label: 'Driver humano', value: 'solo_exposto' },
      ],
    },
    high_soil_flag: {
      badge: 'Flag de driver',
      title: 'Por que existe high_soil_flag',
      summary: 'A flag transforma a medida contínua de solo exposto em um evento semanal auditável: solo acima do limiar observado no pacote.',
      formula: 'high_soil_flag = soil_pct_week >= quantil 75% de soil_pct_week; fallback técnico 20%',
      note: 'Essa flag não substitui o valor original. Ela serve para contar semanas com solo exposto relevante e alimentar o driver solo_exposto.',
      points: [
        { label: 'Medida de entrada', value: 'soil_pct_week' },
        { label: 'Driver', value: 'solo_exposto' },
        { label: 'CSV final', value: 'event_driver_lift.csv' },
        { label: 'Hipóteses', value: 'H3, H4' },
      ],
    },
    engine_temp_hot_share_week: {
      badge: 'Agregação semanal',
      title: 'Como temperatura de motor vira evidência',
      summary: 'A telemetria de temperatura de motor é resumida como proporção semanal de leituras acima do limiar interno de temperatura.',
      formula: 'engine_temp_hot_share_week = proporção semanal de pontos com EngineTemperature acima do limiar',
      note: 'É uma medida numérica; por isso aparece no explorer de correlação quando a busca é risco_de_motor.',
      points: [
        { label: 'Origem bruta', value: 'LAYER_MAP_ENGINE_TEMPERATURE.csv' },
        { label: 'Coluna bruta', value: 'EngineTemperature - ºC' },
        { label: 'Flag derivada', value: 'engine_risk_flag' },
        { label: 'Driver humano', value: 'risco_de_motor' },
      ],
    },
    engine_risk_flag: {
      badge: 'Flag de driver',
      title: 'Por que existe engine_risk_flag',
      summary: 'A flag junta três sinais operacionais de máquina em um marcador semanal de risco: temperatura alta, marcha lenta alta ou consumo de combustível zerado.',
      formula: 'engine_risk_flag = engine_temp_max_c_week >= q90 ou engine_idle_share_week >= q75 ou fuel_zero_share_week >= 0.4',
      note: 'A união por OU é deliberada: o objetivo é triagem de risco operacional, não diagnóstico mecânico fechado.',
      points: [
        { label: 'Driver', value: 'risco_de_motor' },
        { label: 'Entradas', value: 'temperatura, rotação e consumo' },
        { label: 'CSV final', value: 'event_driver_lift.csv' },
        { label: 'Correlação', value: 'busca por risco_de_motor resolve para engine_* e risk flags' },
      ],
    },
    risk_flag_count: {
      badge: 'Score',
      title: 'Por que existe risk_flag_count',
      summary: 'O score conta quantas flags de risco estavam ativas na mesma semana. Ele mede acúmulo de pressão, não um tipo único de problema.',
      formula: 'risk_flag_count = soma de high_soil_flag + weather_stress_flag + pest_risk_flag + fert/ops/telemetry/alert/engine flags',
      note: 'É útil para correlação porque captura semanas com múltiplas pressões simultâneas, mas precisa ser interpretado junto das flags individuais.',
      points: [
        { label: 'Tipo', value: 'score semanal' },
        { label: 'Tabela onde nasce', value: 'ndvi_phase_timeline' },
        { label: 'CSV final', value: 'transition_model_frame.csv, weekly_correlations.csv' },
        { label: 'Limite', value: 'não diz qual driver foi dominante' },
      ],
    },
  };

  if (methods[name]) return methods[name];
  if (String(name || '').endsWith('_risk_flag')) {
    return {
      badge: 'Flag de driver',
      title: `Por que existe ${name}`,
      summary: 'Flags de risco padronizam evidências de fontes diferentes em eventos semanais comparáveis. Isso permite contar frequência, testar lift e correlacionar com NDVI.',
      formula: `${name} = medida semanal do domínio >= threshold observado ou regra booleana documentada no pipeline`,
      note: 'A flag preserva rastreabilidade: o valor bruto continua disponível e a regra aparece em thresholds/código gerador.',
      points: [
        { label: 'Tabela onde nasce', value: 'ndvi_phase_timeline' },
        { label: 'Usada em', value: 'event_driver_lift.csv e weekly_correlations.csv' },
        { label: 'Leitura correta', value: 'associação semanal, não causalidade fechada' },
        { label: 'Auditoria', value: 'abrir colunas de entrada e semana/área' },
      ],
    };
  }
  return null;
}

function splitAny(value: any): string[] {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean).map(String);
  return String(value).split('|').flatMap((part) => part.split(',')).map((part) => part.trim()).filter(Boolean);
}

function normalize(value: any): string {
  return String(value ?? '').toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '');
}

function correlationSearchText(row: AnyRecord): string {
  return normalize([
    row.feature,
    row.analysis_target,
    row.comparison_pair,
    row.direction,
    row.strength,
    row.dataframe,
    row.filter,
    row.search_terms,
    ...(row.aliases || []),
    row.originFeature?.column,
    row.originFeature?.raw_columns,
    row.originFeature?.upstream_columns,
    row.originFeature?.definition,
    row.originTarget?.column,
    row.originTarget?.raw_columns,
  ].filter(Boolean).join(' | '));
}

function fmt(value: any): string {
  if (value === null || value === undefined || value === '' || Number.isNaN(value)) return 'n/a';
  if (Number.isFinite(Number(value))) return Number(value).toLocaleString('pt-BR');
  return String(value);
}

function fmtNum(value: any, digits: number): string {
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(digits) : 'n/a';
}

function formatBytes(value: any): string {
  const bytes = Number(value);
  if (!Number.isFinite(bytes)) return 'n/a';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KB', 'MB', 'GB'];
  let size = bytes / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toLocaleString('pt-BR', { maximumFractionDigits: 1 })} ${units[unit]}`;
}

function compactPeriod(min: any, max: any): string {
  if (!min && !max) return 'n/a';
  return `${String(min || '?').slice(0, 10)} -> ${String(max || '?').slice(0, 10)}`;
}

function truncate(value: any, maxLength: number): string {
  const text = String(value ?? '');
  return text.length <= maxLength ? text : `${text.slice(0, maxLength).trim()}...`;
}

function shortLabel(value: any): string {
  const text = String(value ?? 'n/a');
  return text.length <= 24 ? text : `${text.slice(0, 22)}...`;
}

function groupBy<T extends AnyRecord>(rows: T[], fn: (row: T) => string): Record<string, T[]> {
  return rows.reduce((acc, row) => {
    const key = fn(row);
    acc[key] ||= [];
    acc[key].push(row);
    return acc;
  }, {});
}

function unique(values: any[]): string[] {
  return Array.from(new Set(values.map(String))).sort();
}

export default App;
