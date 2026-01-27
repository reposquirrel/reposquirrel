// static/app.js - Fixed version
console.log("app.js loaded");

const APP_CONFIG = (typeof window !== "undefined" && window.APP_CONFIG) ? window.APP_CONFIG : {};
const READ_ONLY_MODE = !!(APP_CONFIG && APP_CONFIG.readOnly);

let state = {
  mode: "subsystems", // "users", "teams", or "subsystems"
  users: [],
  teams: [],
  subsystems: [], // Unified subsystems (services and standalone repos)
  selectedUser: null,
  selectedUserMonth: null, // {from, to, label, is_yearly}
  selectedTeam: null,
  selectedTeamPeriod: null,
  selectedSubsystem: null,
  selectedSubsystemPeriod: null,
  charts: {}, // to keep references to Chart.js instances
  rendering: false, // flag to prevent concurrent renders
  loadingUsersOverview: false, // flag to prevent concurrent users overview loads
  loadingTeamsOverview: false, // flag to prevent concurrent teams overview loads
  userRequestToken: 0,
  userRenderTokenCounter: 0,
  activeUserRenderToken: null,
  integrations: {
    pagerduty: {
      has_token: false,
      token_preview: null,
      updated_at: null
    }
  },
  alerts: {
    overview: null,
    loading: false,
    error: null,
    selectedResponder: null,
    responderIncidents: {},
    responderIncidentFilters: null,
    overviewOpenFilters: null,
    allIncidentsFilters: null,
    allIncidentsData: null,
    currentView: "overview"
  }
};


const VISUALIZATION_DEFINITIONS = [
  {
    id: "user-kpis",
    scope: "user",
    label: "User · KPI Summary",
    description: "Total commits plus lines added/deleted for a single developer.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "manual" }
  },
  {
    id: "user-contribution-heatmap",
    scope: "user",
    label: "User · Contribution Heatmap",
    description: "Calendar view of daily commits for the chosen user.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Contribution activity" }
  },
  {
    id: "user-daily-activity",
    scope: "user",
    label: "User · Daily Activity",
    description: "Lines added and deleted per day for the most recent month of work.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Daily Activity" }
  },
  {
    id: "user-language-breakdown",
    scope: "user",
    label: "User · Languages",
    description: "Per-language breakdown of changed lines for the selected user.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Lines changed per language" }
  },
  {
    id: "user-weekday-commits",
    scope: "user",
    label: "User · Commits by Weekday",
    description: "Histogram showing which days of the week the user commits code.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Commits by weekday" }
  },
  {
    id: "user-hourly-commits",
    scope: "user",
    label: "User · Commits by Hour",
    description: "Hourly commit distribution for the selected developer.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Commits by hour" }
  },
  {
    id: "user-monthly-lines",
    scope: "user",
    label: "User · Monthly Lines Trend",
    description: "Lines added and deleted per month for the chosen year.",
    supportedPeriods: ["yearly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Monthly Lines" }
  },
  {
    id: "team-kpis",
    scope: "team",
    label: "Team · KPI Summary",
    description: "Team commits and line deltas over the selected period.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "manual" }
  },
  {
    id: "team-contribution-heatmap",
    scope: "team",
    label: "Team · Contribution Heatmap",
    description: "Calendar heatmap aggregating all team member commits.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Contribution Activity" }
  },
  {
    id: "team-daily-activity",
    scope: "team",
    label: "Team · Daily Activity",
    description: "Lines added/deleted each day aggregated across the team.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Daily Activity" }
  },
  {
    id: "subsystem-kpis",
    scope: "subsystem",
    label: "Subsystem · KPI Summary",
    description: "Key commit and line statistics for a subsystem or repository.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "manual" }
  },
  {
    id: "subsystem-heatmap",
    scope: "subsystem",
    label: "Subsystem · Contribution Heatmap",
    description: "Daily subsystem activity derived from contributing developers.",
    supportedPeriods: ["yearly", "monthly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Contribution Activity" }
  },
  {
    id: "subsystem-line-timeline",
    scope: "subsystem",
    label: "Subsystem · Line Change Timeline",
    description: "Bar/line visualization of monthly additions, deletions, and net lines.",
    supportedPeriods: ["yearly"],
    requiresEntity: true,
    match: { mode: "includes", text: "Line Change Timeline" }
  },
  {
    id: "alerts-kpis",
    scope: "alerts",
    label: "Alerts · KPI Summary",
    description: "PagerDuty incident KPIs for the synced window.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-open-incidents",
    scope: "alerts",
    label: "Alerts · Open incidents trend",
    description: "Line chart showing how many PagerDuty incidents were open per day.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-daily-open-vs-closed",
    scope: "alerts",
    label: "Alerts · Daily opened vs. closed",
    description: "Daily comparison of incident openings and resolutions.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-weekly-trend",
    scope: "alerts",
    label: "Alerts · Weekly opened vs. closed",
    description: "Weekly incident cadence, opened vs resolved.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-hourly-arrivals",
    scope: "alerts",
    label: "Alerts · Incidents by hour",
    description: "Hourly distribution of PagerDuty incident creations.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-weekly-severity",
    scope: "alerts",
    label: "Alerts · Severity over time",
    description: "Stacked weekly severity counts plus average trend line.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-severity-mix",
    scope: "alerts",
    label: "Alerts · Severity mix",
    description: "Breakdown of incidents by severity for the selected window.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-severity-cadence",
    scope: "alerts",
    label: "Alerts · Severity cadence",
    description: "Table summarizing frequency and share of each severity.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-top-services",
    scope: "alerts",
    label: "Alerts · Top services",
    description: "Services with the highest incident counts.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-team-mentions",
    scope: "alerts",
    label: "Alerts · Team mentions",
    description: "Counts of how often teams are attached to incidents.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-team-activity",
    scope: "alerts",
    label: "Alerts · Team activity",
    description: "Assignments, acknowledgements, and resolutions grouped by RepoSquirrel team.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-top-responders",
    scope: "alerts",
    label: "Alerts · Top responders",
    description: "Ranked responders linked to RepoSquirrel developers.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-open-incidents-list",
    scope: "alerts",
    label: "Alerts · Active incidents",
    description: "Filterable list of currently open PagerDuty incidents.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-recent-incidents",
    scope: "alerts",
    label: "Alerts · Recent incidents",
    description: "Latest PagerDuty incidents regardless of status.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "overview",
    match: { mode: "manual" }
  },
  {
    id: "alerts-all-incidents-timeline",
    scope: "alerts",
    label: "Alerts · Incident timeline",
    description: "Filter-aware stacked severity timeline from the All incidents explorer.",
    supportedPeriods: [],
    requiresEntity: false,
    kioskView: "all-incidents",
    match: { mode: "manual" }
  }
];

const VISUALIZATION_REGISTRY = {};
VISUALIZATION_DEFINITIONS.forEach((definition) => {
  VISUALIZATION_REGISTRY[definition.id] = definition;
});

function normalizeHeadingText(text) {
  if (!text) return "";
  return text.replace(/^[^A-Za-z0-9]+/, "").trim().toLowerCase();
}

function buildPeriodKey(period) {
  if (!period) return "latest";
  if (period.is_yearly) {
    return `year:${period.label || period.from || ''}`;
  }
  return `range:${period.from || ''}:${period.to || ''}`;
}

function startUserRenderCycle() {
  state.userRenderTokenCounter = (state.userRenderTokenCounter || 0) + 1;
  state.activeUserRenderToken = state.userRenderTokenCounter;
  return state.activeUserRenderToken;
}

function isActiveUserRender(token) {
  if (!token) {
    return true;
  }
  return state.activeUserRenderToken === token;
}

function tagVisualization(element, vizId, context = {}) {
  if (!element || !vizId) {
    return element;
  }
  element.dataset.visualizationId = vizId;
  const def = VISUALIZATION_REGISTRY[vizId];
  const scope = context.scope || def?.scope;
  if (scope) {
    element.dataset.visualizationScope = scope;
  }
  if (context.entityId) {
    element.dataset.visualizationEntity = context.entityId;
  }
  if (context.entityLabel) {
    element.dataset.visualizationEntityLabel = context.entityLabel;
  }
  if (context.periodKey) {
    element.dataset.visualizationPeriod = context.periodKey;
  } else if (context.period) {
    element.dataset.visualizationPeriod = buildPeriodKey(context.period);
  }
  if (context.periodLabel) {
    element.dataset.visualizationPeriodLabel = context.periodLabel;
  }
  return element;
}

function autoTagVisualizations(scope, context = {}) {
  const container = $("main-content");
  if (!container) return;
  const defs = VISUALIZATION_DEFINITIONS.filter((def) => def.scope === scope && def.match?.mode !== "manual");
  defs.forEach((def) => {
    if (container.querySelector(`[data-visualization-id="${def.id}"]`)) {
      return;
    }
    const el = findVisualizationElement(def, container);
    if (el) {
      tagVisualization(el, def.id, context);
    }
  });
}

function findVisualizationElement(definition, container) {
  if (!definition || !container) return null;
  if (definition.match?.selector) {
    const el = container.querySelector(definition.match.selector);
    if (el && !el.dataset.visualizationId) {
      return el;
    }
    return null;
  }
  const candidates = container.querySelectorAll('.card, .dashboard-section, .kpi-grid');
  for (const candidate of candidates) {
    if (candidate.dataset.visualizationId) {
      continue;
    }
    const heading = candidate.querySelector('h1, h2, h3, h4');
    const headingText = normalizeHeadingText(heading?.textContent || "");
    if (!headingText) continue;
    const matchText = (definition.match?.text || '').toLowerCase();
    if (!matchText) continue;
    if (definition.match.mode === 'includes' && headingText.includes(matchText.toLowerCase())) {
      return candidate;
    }
    if (definition.match.mode === 'equals' && headingText === matchText.toLowerCase()) {
      return candidate;
    }
    if (definition.match.mode === 'startsWith' && headingText.startsWith(matchText.toLowerCase())) {
      return candidate;
    }
  }
  return null;
}


const kioskState = {
  initialized: false,
  slides: [],
  currentIndex: -1,
  rotationSeconds: 30,
  refreshMinutes: 15,
  rotationTimer: null,
  refreshTimer: null,
  clockTimer: null,
  stage: null,
  placeholder: null,
  slideContainer: null,
  overlayTitle: null,
  overlayMeta: null,
  overlayClock: null
};

const ALLOWED_KIOSK_LAYOUTS = ["grid", "vertical", "horizontal"];

function normalizeKioskLayout(value) {
  const layout = (value || "grid").toString().trim().toLowerCase();
  return ALLOWED_KIOSK_LAYOUTS.includes(layout) ? layout : "grid";
}

function sanitizeKioskItem(item, pageId, index) {
  if (!item || !item.visualization_id) {
    return null;
  }
  const options = typeof item.options === "object" && item.options !== null ? item.options : {};
  return {
    id: item.id || `${pageId}-item-${index + 1}`,
    visualization_id: item.visualization_id,
    scope: item.scope,
    entity_id: item.entity_id || "",
    entity_label: item.entity_label || "",
    period_mode: item.period_mode || "latest-year",
    period: item.period || null,
    custom_title: item.custom_title || "",
    options,
    notes: item.notes || ""
  };
}

function sanitizeKioskPage(page, index) {
  const safePage = page || {};
  const pageId = safePage.id || `page-${index + 1}`;
  const title = (safePage.title || "").trim() || `Page ${index + 1}`;
  const description = (safePage.description || "").trim();
  const layout = normalizeKioskLayout(safePage.layout);
  const rawItems = Array.isArray(safePage.items) ? safePage.items : [];
  const items = rawItems
    .map((item, itemIndex) => sanitizeKioskItem(item, pageId, itemIndex))
    .filter(Boolean);
  return { id: pageId, title, description, layout, items };
}

function normalizeKioskPages(source, options = {}) {
  const config = source || {};
  let rawPages = [];
  if (Array.isArray(config.pages) && config.pages.length) {
    rawPages = config.pages;
  } else if (Array.isArray(config.items) && config.items.length) {
    rawPages = [{
      id: config.id || "page-1",
      title: config.title || "Slide 1",
      description: config.description || "",
      layout: normalizeKioskLayout(config.layout),
      items: config.items
    }];
  }
  const pages = rawPages.map((page, index) => sanitizeKioskPage(page, index));
  if (pages.length === 0 && options.ensurePage) {
    const fallback = sanitizeKioskPage({ id: "page-1", title: "Page 1", items: [] }, 0);
    return [fallback];
  }
  return pages;
}

function isKioskMode() {
  return !!(APP_CONFIG && APP_CONFIG.kioskMode);
}

const PAGERDUTY_SEVERITY_ORDER = ["p-down", "critical", "high", "medium", "low", "info", "unknown"];
const PAGERDUTY_SEVERITY_COLORS = {
  "p-down": "rgba(127, 29, 29, 0.95)",
  critical: "rgba(220, 38, 38, 0.9)",
  high: "rgba(249, 115, 22, 0.9)",
  medium: "rgba(250, 204, 21, 0.85)",
  low: "rgba(34, 197, 94, 0.85)",
  info: "rgba(14, 165, 233, 0.85)",
  unknown: "rgba(148, 163, 184, 0.85)"
};
const PAGERDUTY_SEVERITY_BORDER_COLORS = {
  "p-down": "rgba(127, 29, 29, 1)",
  critical: "rgba(220, 38, 38, 1)",
  high: "rgba(249, 115, 22, 1)",
  medium: "rgba(250, 204, 21, 1)",
  low: "rgba(34, 197, 94, 1)",
  info: "rgba(14, 165, 233, 1)",
  unknown: "rgba(148, 163, 184, 1)"
};

let suppressAlertsModeWarning = false;

function $(id) {
  return document.getElementById(id);
}

// Progress tracking system for async components
const progressTracker = {
  tasks: new Map(),
  container: null,
  abortController: null,
  
  init() {
    // Create progress indicator container
    if (!this.container) {
      this.container = document.createElement('div');
      this.container.id = 'async-progress-tracker';
      this.container.className = 'progress-tracker';
      this.container.innerHTML = `
        <div class="progress-header">
          <div class="progress-title">Loading Components</div>
          <div class="progress-summary">
            <span class="progress-completed">0</span> / <span class="progress-total">0</span> complete
            <button class="progress-cancel" title="Cancel loading">✕</button>
          </div>
        </div>
        <div class="progress-list"></div>
      `;
      
      // Add cancel button functionality
      const cancelBtn = this.container.querySelector('.progress-cancel');
      cancelBtn.addEventListener('click', () => {
        this.cancel();
      });
    }
    
    // Create new abort controller for this session
    this.abortController = new AbortController();
    return this.container;
  },
  
  addTask(id, title) {
    this.tasks.set(id, { 
      title, 
      status: 'loading', 
      startTime: Date.now() 
    });
    this.updateDisplay();
  },
  
  completeTask(id, success = true) {
    if (this.tasks.has(id)) {
      const task = this.tasks.get(id);
      task.status = success ? 'completed' : 'failed';
      task.endTime = Date.now();
      this.updateDisplay();
      
      // Auto-hide if all tasks complete
      setTimeout(() => {
        if (this.isAllComplete()) {
          this.hide();
        }
      }, 2000);
    }
  },
  
  cancel() {
    console.log("🚫 Progress tracker: Canceling async operations");
    
    // Abort any ongoing fetch operations
    if (this.abortController) {
      this.abortController.abort();
    }
    
    // Mark all loading tasks as cancelled
    this.tasks.forEach((task, id) => {
      if (task.status === 'loading') {
        task.status = 'cancelled';
        task.endTime = Date.now();
      }
    });
    
    this.updateDisplay();
    
    // Hide after short delay
    setTimeout(() => {
      this.hide();
    }, 1000);
  },
  
  updateDisplay() {
    if (!this.container) return;
    const total = this.tasks.size;
    const completed = Array.from(this.tasks.values()).filter(t => t.status !== 'loading').length;
    this.container.querySelector('.progress-completed').textContent = completed;
    this.container.querySelector('.progress-total').textContent = total;
    const list = this.container.querySelector('.progress-list');
    list.innerHTML = '';
    this.tasks.forEach((task) => {
      const item = document.createElement('div');
      item.className = `progress-item progress-${task.status}`;
      let icon;
      switch (task.status) {
        case 'loading':
          icon = '<div class="inline-spinner"></div>';
          break;
        case 'completed':
          icon = '✅';
          break;
        case 'failed':
          icon = '❌';
          break;
        case 'cancelled':
          icon = '🚫';
          break;
        default:
          icon = '⏳';
      }
      const duration = task.endTime ? ` (${((task.endTime - task.startTime) / 1000).toFixed(1)}s)` : '';
      item.innerHTML = `
        <span class="progress-icon">${icon}</span>
        <span class="progress-task-title">${task.title}${duration}</span>
      `;
      list.appendChild(item);
    });
  },
  
  show() {
    if (this.container && !document.body.contains(this.container)) {
      document.body.appendChild(this.container);
      this.container.classList.add('visible');
    }
  },
  
  hide() {
    if (this.container && document.body.contains(this.container)) {
      this.container.classList.remove('visible');
      setTimeout(() => {
        if (document.body.contains(this.container)) {
          document.body.removeChild(this.container);
        }
        this.reset();
      }, 300);
    }
  },
  
  reset() {
    this.tasks.clear();
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
  },
  
  isAllComplete() {
    return Array.from(this.tasks.values()).every(task => task.status !== 'loading');
  },
  
  getAbortSignal() {
    return this.abortController ? this.abortController.signal : null;
  }
};

function isPagerDutyConfigured() {
  return !!(state.integrations && state.integrations.pagerduty && state.integrations.pagerduty.has_token);
}

function updateAlertsModeVisibility() {
  const alertsButton = $("mode-alerts");
  const alertsSidebar = $("sidebar-alerts");
  const configured = isPagerDutyConfigured();
  if (alertsButton) {
    alertsButton.style.display = configured ? "inline-block" : "none";
    alertsButton.setAttribute("aria-hidden", configured ? "false" : "true");
    if (!configured) {
      alertsButton.classList.remove("active");
    }
  }
  if (alertsSidebar) {
    alertsSidebar.style.display = configured && state.mode === "alerts" ? "block" : "none";
  }
  if (!configured) {
    state.alerts.overview = null;
    state.alerts.error = null;
    state.alerts.selectedResponder = null;
    state.alerts.responderIncidents = {};
    state.alerts.responderIncidentFilters = null;
    state.alerts.overviewOpenFilters = null;
    state.alerts.allIncidentsFilters = null;
    state.alerts.allIncidentsData = null;
    state.alerts.currentView = "overview";
    renderPagerDutyResponderList();
  }
  if (!configured && state.mode === "alerts") {
    suppressAlertsModeWarning = true;
    setMode("subsystems");
  }
}

async function refreshIntegrationsStatus(silent = false) {
  try {
    const response = await fetch("/api/settings/integrations");
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.error) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    state.integrations = data || state.integrations;
    updateAlertsModeVisibility();
    return data;
  } catch (error) {
    if (!silent) {
      console.warn("Failed to refresh integrations status:", error);
    }
    return null;
  }
}

// Helper function to create enhanced loading indicators
function createLoadingIndicator(title = "Loading", subtitle = "Please wait while data is being processed...") {
  return `
    <div class="loading">
      <div class="loading-spinner"></div>
      <div class="loading-text">${title}</div>
      <div class="loading-subtext">${subtitle}</div>
    </div>
  `;
}

// Helper function for inline loading (smaller components)
function createInlineLoading(text = "Loading...") {
  return `
    <div class="inline-loading">
      <div class="inline-spinner"></div>
      <span>${text}</span>
    </div>
  `;
}

function clearMain() {
  const main = $("main-content");
  
  // Cancel any ongoing progress tracking when clearing the main content
  if (progressTracker && progressTracker.tasks.size > 0) {
    console.log("🧹 Cleaning up progress tracker due to navigation");
    progressTracker.cancel();
  }
  
  // Destroy all Chart.js instances on all canvas elements before clearing HTML
  main.querySelectorAll('canvas').forEach(canvas => {
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
      existingChart.destroy();
    }
  });
  
  main.innerHTML = "";
  // destroy charts
  Object.values(state.charts).forEach((c) => c.destroy && c.destroy());
  state.charts = {};
}

function formatDateTime(value) {
  if (!value) return "--";
  try {
    const date = new Date(value);
    if (isNaN(date.getTime())) {
      return value;
    }
    return date.toLocaleString();
  } catch (error) {
    return value;
  }
}

async function refreshLastUpdateBanner() {
  try {
    const response = await fetch("/api/update/last-run");
    if (!response.ok) {
      throw new Error("Failed to fetch last update info");
    }
    const data = await response.json();
    renderLastUpdateBanner(data);
    renderUpdateSettingsStatus(data);
  } catch (error) {
    console.error("Failed to refresh last update info:", error);
  }
}

function renderLastUpdateBanner(data) {
  const textEl = $("last-update-text");
  const kindEl = $("last-update-kind");
  const nextEl = $("next-background-run");
  const statusEl = $("background-status-label");
  const info = data?.last_update;
  if (textEl) {
    if (info?.timestamp) {
      textEl.textContent = formatDateTime(info.timestamp);
    } else {
      textEl.textContent = "Never";
    }
  }
  if (kindEl) {
    kindEl.textContent = info?.type || "--";
    kindEl.classList.remove("pill-success", "pill-warning", "pill-error");
    if (info?.status === "success") {
      kindEl.classList.add("pill-success");
    } else if (info?.status === "failed") {
      kindEl.classList.add("pill-error");
    }
  }
  if (nextEl) {
    if (data?.background_enabled) {
      const nextText = data?.next_run ? formatDateTime(data.next_run) : "Scheduling…";
      nextEl.textContent = `Next background refresh: ${nextText}`;
    } else {
      nextEl.textContent = "Background refresh disabled";
    }
  }
  if (statusEl) {
    const running = !!data?.background_running;
    const enabled = !!data?.background_enabled;
    statusEl.classList.remove("active", "error", "idle");
    if (!enabled) {
      statusEl.textContent = "Background refresh disabled";
      statusEl.classList.add("idle");
    } else if (running) {
      statusEl.textContent = "Background refresh running";
      statusEl.classList.add("active");
    } else {
      statusEl.textContent = "Idle";
      statusEl.classList.add("idle");
    }
  }
}

function renderUpdateSettingsStatus(data) {
  const lastEl = $("background-last-update");
  const nextEl = $("background-next-run");
  const statusEl = $("background-current-status");
  if (lastEl) {
    const lastInfo = data?.last_update;
    lastEl.textContent = lastInfo?.timestamp ? `${formatDateTime(lastInfo.timestamp)} (${lastInfo.type || 'manual'})` : 'Never';
  }
  if (nextEl) {
    if (data?.background_enabled) {
      nextEl.textContent = data?.next_run ? formatDateTime(data.next_run) : 'Scheduling…';
    } else {
      nextEl.textContent = 'Background refresh disabled';
    }
  }
  if (statusEl) {
    statusEl.classList.remove("active", "error", "idle");
    if (!data?.background_enabled) {
      statusEl.textContent = 'Disabled';
      statusEl.classList.add("idle");
    } else if (data?.background_running) {
      statusEl.textContent = 'Running';
      statusEl.classList.add("active");
    } else {
      statusEl.textContent = 'Idle';
      statusEl.classList.add("idle");
    }
  }
}

async function loadUpdateSettings() {
  try {
    const response = await fetch("/api/settings/update-config");
    if (!response.ok) {
      throw new Error("Failed to load update settings");
    }
    const data = await response.json();
    const enabledToggle = $("background-update-enabled");
    const intervalInput = $("background-update-interval");
    if (enabledToggle) {
      enabledToggle.checked = !!data.background_enabled;
    }
    if (intervalInput) {
      intervalInput.value = data.interval_hours || 24;
    }
    renderUpdateSettingsStatus(data);
  } catch (error) {
    console.error("Failed to load background update settings:", error);
  }
}

async function saveUpdateSettings() {
  if (READ_ONLY_MODE) {
    alert("Settings are disabled in read-only mode.");
    return;
  }
  const enabled = $("background-update-enabled")?.checked ?? false;
  const intervalValue = parseInt($("background-update-interval")?.value || "24", 10);
  if (Number.isNaN(intervalValue) || intervalValue < 1) {
    alert("Please enter a valid interval (minimum 1 hour).");
    return;
  }
  try {
    try {
      await fetch("/api/update/reset", { method: "POST" });
    } catch (resetError) {
      console.warn("Unable to reset update state before saving:", resetError);
    }

    const response = await fetch("/api/settings/update-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        background_enabled: enabled,
        interval_hours: intervalValue
      })
    });
    const result = await response.json();
    if (!response.ok || result.error) {
      throw new Error(result.error || "Failed to save update settings");
    }
    const settingsPayload = result.settings || result;
    renderUpdateSettingsStatus(settingsPayload);
    const messageParts = ["Background update settings saved."];
    if (result.background_started) {
      messageParts.push("Background refresh started in the background.");
    }
    alert(messageParts.join(" "));
    refreshLastUpdateBanner();
    scheduleLastUpdateRefresh();
  } catch (error) {
    console.error("Failed to save update settings:", error);
    alert(error.message || "Failed to save update settings");
  }
}

let lastUpdateBannerInterval;
function scheduleLastUpdateRefresh() {
  if (lastUpdateBannerInterval) {
    clearInterval(lastUpdateBannerInterval);
  }
  lastUpdateBannerInterval = setInterval(refreshLastUpdateBanner, 60000);
}

function setViewHeader(title, subtitle, pillText) {
  $("view-title").textContent = title;
  $("view-subtitle").textContent = subtitle || "";
  const pill = $("view-pill");
  pill.textContent = pillText || "";
  pill.classList.toggle("hidden", !pillText);
}

// Helper function to create title with tooltip
function createTitleWithTooltip(titleText, tooltipText, level = "h2") {
  return `
    <div class="title-with-help">
      ${level === "h2" ? `<h2>${titleText}</h2>` : `<h3>${titleText}</h3>`}
      <span class="help-icon">?
        <span class="tooltip">${tooltipText}</span>
      </span>
    </div>
  `;
}

// --------------------------
// API helpers
// --------------------------

async function fetchJSON(url, options = {}) {
  console.log("Fetching:", url);
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 3000000); // 50 minute timeout for enterprise operations
    
    // Use progress tracker abort signal if available, otherwise use our own
    const abortSignal = progressTracker.getAbortSignal() || controller.signal;
    
    const res = await fetch(url, { 
      signal: abortSignal,
      headers: {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
      },
      ...options
    });
    clearTimeout(timeoutId);
    
    if (!res.ok) {
      throw new Error("Request failed: " + res.status + " " + res.statusText);
    }
    
    const data = await res.json();
    console.log("Fetch successful for:", url);
    return data;
  } catch (error) {
    console.error("Fetch failed for:", url, "Error:", error.message);
    throw error;
  }
}

async function loadUsersAndSubsystems() {
  try {
    console.log("Loading users, teams, and subsystems...");
    
    // Add loading indicator
    const main = $("main-content");
    if (main) {
      main.innerHTML = createLoadingIndicator(
        "Loading Application Data", 
        "Initializing users, teams, and subsystems..."
      );
    }
    
    // First, check if repositories are configured
    let repositoriesConfigured = false;
    try {
      const repoResponse = await fetchJSON("/api/settings/repositories");
      const repositories = repoResponse.repositories || [];
      repositoriesConfigured = repositories.length > 0;
      console.log(`Found ${repositories.length} repositories configured`);
      
      if (!repositoriesConfigured) {
        console.log("No repositories configured - redirecting to settings");
        if (READ_ONLY_MODE) {
          if (main) {
            main.innerHTML = `
              <div class="empty-state">
                <p>No repositories are configured, and settings are disabled in read-only mode.</p>
                <p>Restart the dashboard without --read-only to configure repositories.</p>
              </div>
            `;
          }
        } else {
          // Auto-open settings focused on repositories tab for first-time users
          openSettings("repositories");
        }
        return; // Don't proceed with loading other data yet
      }
    } catch (error) {
      console.warn("Could not check repository configuration:", error);
      // Assume repositories might exist and continue
      repositoriesConfigured = true;
    }

    // Load users, teams, and subsystems with individual error handling
    let userData = { users: [] };
    let teamsData = { teams: [] };
    let subsystemData = { subsystems: [] };
    let deadStatusData = { subsystem_status: {} };
    
    try {
      console.log("Fetching users...");
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      
      const response = await fetch("/api/users", { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      userData = await response.json();
      console.log("Loaded users:", userData.users?.length || 0);
    } catch (userError) {
      console.error("Failed to load users:", userError);
      // Continue with empty user data
    }
    
    try {
      console.log("Fetching teams...");
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      
      const response = await fetch("/api/teams", { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      teamsData = await response.json();
      console.log("Loaded teams:", teamsData.teams?.length || 0);
    } catch (teamsError) {
      console.error("Failed to load teams:", teamsError);
      // Continue with empty teams data
    }
    
    try {
      console.log("Fetching subsystems...");
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      
      const response = await fetch("/api/subsystems", { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      subsystemData = await response.json();
      console.log("Loaded subsystems:", subsystemData.subsystems?.length || 0);
    } catch (subsystemError) {
      console.error("Failed to load subsystems:", subsystemError);
      // Continue with empty subsystem data
    }
    
    try {
      console.log("Fetching subsystem dead status...");
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000);
      
      const response = await fetch("/api/subsystems/dead-status", { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }
      
      deadStatusData = await response.json();
      console.log("Loaded dead status for:", Object.keys(deadStatusData.subsystem_status || {}).length, "subsystems");
    } catch (deadStatusError) {
      console.error("Failed to load dead status:", deadStatusError);
      // Continue with empty dead status data
    }
    
    console.log("Updating state...");
    state.users = userData.users || [];
    state.teams = teamsData.teams || [];
    state.subsystems = subsystemData.subsystems || [];
    state.subsystemDeadStatus = deadStatusData.subsystem_status || {};
    
    console.log("Rendering lists...");
    try {
      renderUserList();
      console.log("User list rendered, checking container contents...");
      const userContainer = $("user-list");
      if (userContainer) {
        console.log("User list container children count:", userContainer.children.length);
        console.log("User list HTML length:", userContainer.innerHTML.length);
      }
    } catch (e) {
      console.error("Error rendering user list:", e);
    }
    
    try {
      renderTeamList();
      console.log("Team list rendered");
    } catch (e) {
      console.error("Error rendering team list:", e);
    }
    
    try {
      renderSubsystemList();
      console.log("Subsystem list rendered, checking container contents...");
      const subsystemContainer = $("subsystem-list");
      if (subsystemContainer) {
        console.log("Subsystem list container children count:", subsystemContainer.children.length);
        console.log("Subsystem list HTML length:", subsystemContainer.innerHTML.length);
      }
    } catch (e) {
      console.error("Error rendering subsystem list:", e);
    }
    
    // Force update of current mode visibility
    console.log("Setting mode to:", state.mode);
    setMode(state.mode, !isKioskMode());
    
    // Clear loading indicator
    if (main) {
      main.innerHTML = '<div class="empty-state"><p>Use the selector on the left to pick a user/month, team/period, subsystem/period, or open the Alerts (PD) mode for PagerDuty insights.</p></div>';
    }
    
    console.log("loadUsersAndSubsystems completed successfully");
    
  } catch (error) {
    console.error("Error loading data:", error);
    // Show error to user
    const main = $("main-content");
    if (main) {
      main.innerHTML = '<div class="error">Failed to load data from backend: ' + error.message + '<br>Check console for details.</div>';
    }
  }
}

// --------------------------
// Sidebar rendering
// --------------------------

function renderUserList() {
  console.log("renderUserList called with", state.users.length, "users");
  const container = $("user-list");
  if (!container) {
    console.error("user-list container not found");
    return;
  }
  container.innerHTML = "";
  
  if (state.users.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No users found</div>';
    return;
  }
  
  // Sort users alphabetically by display name
  const sortedUsers = [...state.users].sort((a, b) => {
    const nameA = (a.display_name || a.slug).toLowerCase();
    const nameB = (b.display_name || b.slug).toLowerCase();
    return nameA.localeCompare(nameB);
  });
  
  sortedUsers.forEach((user) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedUser && state.selectedUser.slug === user.slug) {
      div.classList.add("active");
    }
    div.textContent = user.display_name || user.slug;
    div.addEventListener("click", () => selectUser(user));
    container.appendChild(div);
  });
  console.log("renderUserList completed");
}

function renderUserMonthList() {
  const container = $("user-month-list");
  container.innerHTML = "";
  if (!state.selectedUser) return;

  const months = state.selectedUser.months || [];
  if (months.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No data</div>';
    return;
  }

  months.forEach((month) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedUserMonth && state.selectedUserMonth.folder === month.folder) {
      div.classList.add("active");
    }
    div.textContent = month.label + (month.is_yearly ? " (yearly)" : "");
    div.addEventListener("click", () => selectUserMonth(month));
    container.appendChild(div);
  });
}

function renderTeamList() {
  console.log("renderTeamList called with", state.teams.length, "teams");
  const container = $("team-list");
  if (!container) {
    console.error("team-list container not found");
    return;
  }
  container.innerHTML = "";
  
  if (state.teams.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No teams found</div>';
    return;
  }
  
  // Sort teams alphabetically by name
  const sortedTeams = [...state.teams].sort((a, b) => {
    const nameA = (a.name || a.id).toLowerCase();
    const nameB = (b.name || b.id).toLowerCase();
    return nameA.localeCompare(nameB);
  });
  
  sortedTeams.forEach((team) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedTeam && state.selectedTeam.id === team.id) {
      div.classList.add("active");
    }
    div.textContent = team.name || team.id;
    div.addEventListener("click", () => selectTeam(team));
    container.appendChild(div);
  });
  console.log("renderTeamList completed");
}

function renderTeamPeriodList() {
  const container = $("team-period-list");
  container.innerHTML = "";
  if (!state.selectedTeam) return;

  const periods = state.selectedTeam.periods || [];
  if (periods.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No data</div>';
    return;
  }

  periods.forEach((period) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedTeamPeriod && state.selectedTeamPeriod.from === period.from && state.selectedTeamPeriod.to === period.to) {
      div.classList.add("active");
    }
    div.textContent = period.label + (period.is_yearly ? " (yearly)" : "");
    div.addEventListener("click", () => selectTeamPeriod(period));
    container.appendChild(div);
  });
}

function renderSubsystemList() {
  console.log("renderSubsystemList called with", state.subsystems.length, "subsystems");
  const container = $("subsystem-list");
  if (!container) {
    console.error("subsystem-list container not found");
    return;
  }
  container.innerHTML = "";
  
  if (state.subsystems.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No subsystems found</div>';
    return;
  }
  
  // Sort subsystems alphabetically
  const sortedSubsystems = [...state.subsystems].sort((a, b) => {
    const nameA = a.name.toLowerCase();
    const nameB = b.name.toLowerCase();
    return nameA.localeCompare(nameB);
  });
  
  sortedSubsystems.forEach((subsystem) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedSubsystem && state.selectedSubsystem.name === subsystem.name) {
      div.classList.add("active");
    }
    
    // Check if subsystem is dead
    const deadStatus = state.subsystemDeadStatus && state.subsystemDeadStatus[subsystem.name];
    const isDead = deadStatus && deadStatus.is_dead;
    
    if (isDead) {
      div.classList.add("dead-subsystem");
      
      // Create container for name and icon
      const itemContent = document.createElement("div");
      itemContent.className = "sidebar-item-content";
      itemContent.style.display = "flex";
      itemContent.style.justifyContent = "space-between";
      itemContent.style.alignItems = "center";
      
      const nameSpan = document.createElement("span");
      nameSpan.textContent = subsystem.name;
      
      const deadIcon = document.createElement("span");
      deadIcon.className = "dead-icon";
      deadIcon.textContent = "⚠️";
      deadIcon.title = `Potentially dead - No activity for ${deadStatus.months_since_activity || 3}+ months`;
      
      itemContent.appendChild(nameSpan);
      itemContent.appendChild(deadIcon);
      div.appendChild(itemContent);
    } else {
      div.textContent = subsystem.name;
    }
    
    div.addEventListener("click", () => selectSubsystem(subsystem));
    container.appendChild(div);
  });
  console.log("renderSubsystemList completed");
}

function renderSubsystemPeriodList() {
  const container = $("subsystem-period-list");
  container.innerHTML = "";
  if (!state.selectedSubsystem) return;

  const periods = state.selectedSubsystem.periods || [];
  if (periods.length === 0) {
    container.innerHTML = '<div class="sidebar-item">No data</div>';
    return;
  }

  periods.forEach((period) => {
    const div = document.createElement("div");
    div.className = "sidebar-item";
    if (state.selectedSubsystemPeriod && state.selectedSubsystemPeriod.folder === period.folder) {
      div.classList.add("active");
    }
    div.textContent = period.label + (period.is_yearly ? " (yearly)" : "");
    div.addEventListener("click", () => selectSubsystemPeriod(period));
    container.appendChild(div);
  });
}

// --------------------------
// Mode switching
// --------------------------

function setMode(mode, showOverview = true) {
  if (mode === "alerts" && !isPagerDutyConfigured()) {
    if (!suppressAlertsModeWarning) {
      alert("Configure a PagerDuty API token in Integrations and run Run Update to view alerts.");
    }
    suppressAlertsModeWarning = false;
    mode = "subsystems";
  } else {
    suppressAlertsModeWarning = false;
  }
  state.mode = mode;
  
  // Update button states
  const userBtn = $("mode-users");
  const teamsBtn = $("mode-teams");
  const subsystemBtn = $("mode-subsystems");
  const alertsBtn = $("mode-alerts");
  
  userBtn.classList.toggle("active", mode === "users");
  teamsBtn.classList.toggle("active", mode === "teams");
  subsystemBtn.classList.toggle("active", mode === "subsystems");
  if (alertsBtn) {
    alertsBtn.classList.toggle("active", mode === "alerts");
  }
  
  // Update sidebar visibility
  const userSidebar = $("sidebar-users");
  const teamsSidebar = $("sidebar-teams");
  const subsystemSidebar = $("sidebar-subsystems");
  const alertsSidebar = $("sidebar-alerts");
  
  if (userSidebar && teamsSidebar && subsystemSidebar) {
    userSidebar.style.display = mode === "users" ? "block" : "none";
    teamsSidebar.style.display = mode === "teams" ? "block" : "none";
    subsystemSidebar.style.display = mode === "subsystems" ? "block" : "none";
  }
  if (alertsSidebar) {
    alertsSidebar.style.display = mode === "alerts" && isPagerDutyConfigured() ? "block" : "none";
  }
  
  // Clear main content when switching modes
  if (showOverview) {
    clearMain();
    
    // Show overview dashboard for the selected mode
    if (mode === "users") {
      showUsersOverviewDashboard();
    } else if (mode === "teams") {
      showTeamsOverviewDashboard();
    } else if (mode === "alerts") {
      showAlertsOverviewDashboard();
    } else {
      showSubsystemsOverviewDashboard();
    }
  }
}

// --------------------------
// Selection handlers
// --------------------------

function selectUser(user) {
  state.selectedUser = user;
  state.selectedUserMonth = null; // Reset month selection
  renderUserList(); // Update active states
  renderUserMonthList();

  // Check if we have yearly data and show the most recent by default
  const yearlyPeriods = (user.months || []).filter(m => m.is_yearly);
  let yearlyData = null;
  
  if (yearlyPeriods.length > 0) {
    // Sort by year and pick the most recent
    yearlyData = yearlyPeriods.sort((a, b) => b.from.localeCompare(a.from))[0];
  }
  
  if (yearlyData) {
    state.selectedUserMonth = yearlyData;
    loadUserMonth(user, yearlyData);
  } else {
    clearMain();
    setViewHeader(
      "User: " + (user.display_name || user.slug),
      "Select a time period to view stats",
      "User"
    );
  }
}

function navigateToUser(userSlug, currentPeriod = null) {
  console.log('Attempting to navigate to user:', userSlug, 'with period:', currentPeriod);
  
  // Show loading immediately
  const main = $("main-content");
  if (main) {
    main.innerHTML = createLoadingIndicator(
      `Loading User: ${userSlug}`,
      "Fetching user statistics and activity data..."
    );
  }
  
  const user = state.users.find(u => u.slug === userSlug);
  if (!user) {
    console.warn('User ' + userSlug + ' not found in loaded users');
    console.log('Available users:', state.users.map(u => u.slug));
    return;
  }
  
  console.log('Found user:', user.slug);
  
  // Switch to users mode without showing overview
  setMode("users", false);
  
  // Select the user
  state.selectedUser = user;
  state.selectedUserMonth = null;
  
  // Update the UI lists
  renderUserList();
  renderUserMonthList();
  
  // Try to find matching period
  let targetPeriod = null;
  if (currentPeriod) {
    targetPeriod = (user.months || []).find(m => m.label === currentPeriod.label);
  }
  
  // Default to most recent yearly period if available, otherwise first period
  if (!targetPeriod) {
    const yearlyPeriods = (user.months || []).filter(m => m.is_yearly);
    if (yearlyPeriods.length > 0) {
      // Sort by year and pick the most recent
      targetPeriod = yearlyPeriods.sort((a, b) => b.from.localeCompare(a.from))[0];
    } else {
      // Fallback to first available period
      targetPeriod = (user.months || [])[0];
    }
  }
  
  if (targetPeriod) {
    state.selectedUserMonth = targetPeriod;
    renderUserMonthList(); // Update active state in month list
    loadUserMonth(user, targetPeriod);
  } else {
    clearMain();
    setViewHeader(
      "User: " + (user.display_name || user.slug),
      "No data available",
      "User"
    );
  }
}

// Team selection functions
function selectTeam(team) {
  state.selectedTeam = team;
  state.selectedTeamPeriod = null; // Reset period selection
  renderTeamList(); // Update active states
  renderTeamPeriodList();

  // Check if we have yearly data and show the most recent by default
  const yearlyPeriods = (team.periods || []).filter(p => p.is_yearly);
  let yearlyData = null;
  
  if (yearlyPeriods.length > 0) {
    // Sort by year and pick the most recent
    yearlyData = yearlyPeriods.sort((a, b) => b.from.localeCompare(a.from))[0];
  }
  
  if (yearlyData) {
    state.selectedTeamPeriod = yearlyData;
    loadTeamPeriod(team, yearlyData);
  } else {
    clearMain();
    setViewHeader(
      "Team: " + (team.name || team.id),
      "Select a time period to view stats",
      "Team"
    );
  }
}

function selectTeamPeriod(period) {
  state.selectedTeamPeriod = period;
  renderTeamPeriodList(); // Update active states
  loadTeamPeriod(state.selectedTeam, period);
}

function getUserTeams(userSlug) {
  // Find all teams that this user is a member of
  const userTeams = [];
  
  if (state.teams && Array.isArray(state.teams)) {
    state.teams.forEach(team => {
      if (team.members && team.members.includes(userSlug)) {
        userTeams.push(team);
      }
    });
  }
  
  return userTeams;
}

function renderUserTeamMembership(userSlug, container) {
  const userTeams = getUserTeams(userSlug);
  
  if (userTeams.length === 0) {
    return; // Don't show anything if user is not in any teams
  }
  
  const teamsCard = document.createElement("div");
  teamsCard.className = "card";
  
  const title = document.createElement("h2");
  title.textContent = "Team Membership";
  teamsCard.appendChild(title);
  
  const teamsList = document.createElement("ul");
  teamsList.className = "link-list";
  
  userTeams.forEach(team => {
    const li = document.createElement("li");
    li.className = "link-list-item clickable-item";
    li.textContent = team.name;
    li.onclick = () => {
      // Switch to teams mode and navigate to this team
      navigateToTeam(team.id || team.name);
    };
    teamsList.appendChild(li);
  });
  
  teamsCard.appendChild(teamsList);
  container.appendChild(teamsCard);
}

function navigateToSubsystem(subsystemName, currentPeriod = null) {
  console.log('Attempting to navigate to subsystem:', subsystemName, 'with period:', currentPeriod);
  console.log('Available subsystems:', state.subsystems.map(s => s.name));
  
  // Show loading immediately
  const main = $("main-content");
  if (main) {
    main.innerHTML = createLoadingIndicator(
      `Loading Subsystem: ${subsystemName}`,
      "Gathering subsystem metrics and analysis..."
    );
  }
  
  const subsystem = findSubsystemByRepoName(subsystemName);
  if (!subsystem) {
    console.warn('Subsystem matching ' + subsystemName + ' not found');
    // Show error to user
    clearMain();
    const main = $("main-content");
    if (main) {
      main.innerHTML = '<div class="error">Could not find subsystem matching "' + subsystemName + '". Available subsystems: ' + state.subsystems.map(s => s.name).sort().join(', ') + '</div>';
    }
    return;
  }
  
  console.log('Found subsystem:', subsystem.name);
  
  // Switch to subsystems mode without showing overview
  setMode("subsystems", false);
  
  // Select the subsystem
  state.selectedSubsystem = subsystem;
  state.selectedSubsystemPeriod = null;
  renderSubsystemList();
  renderSubsystemPeriodList();
  
  // Try to find matching period
  let targetPeriod = null;
  if (currentPeriod) {
    targetPeriod = (subsystem.periods || []).find(p => p.label === currentPeriod.label);
  }
  
  // Default to most recent yearly period if available, otherwise first period
  if (!targetPeriod) {
    const yearlyPeriods = (subsystem.periods || []).filter(p => p.is_yearly);
    if (yearlyPeriods.length > 0) {
      // Sort by year (from date) and pick the most recent
      targetPeriod = yearlyPeriods.sort((a, b) => b.from.localeCompare(a.from))[0];
    } else {
      // Fallback to first available period
      targetPeriod = (subsystem.periods || [])[0];
    }
  }
  
  if (targetPeriod) {
    state.selectedSubsystemPeriod = targetPeriod;
    renderSubsystemPeriodList(); // Update active state in period list
    loadSubsystemPeriod(subsystem, targetPeriod).catch(error => {
      console.error("Failed to load subsystem period:", error);
      clearMain();
      setViewHeader("Error", "Failed to load subsystem data: " + error.message, "Error");
    });
  } else {
    clearMain();
    setViewHeader(
      "Subsystem: " + subsystem.name,
      "No data available",
      "Subsystem"
    );
  }
}

function navigateToTeam(teamId) {
  console.log('Attempting to navigate to team:', teamId);
  console.log('Available teams:', state.teams ? state.teams.map(t => t.id || t.name) : 'No teams loaded');
  
  if (!state.teams || !Array.isArray(state.teams)) {
    console.error('Teams data not loaded or invalid');
    clearMain();
    const main = $("main-content");
    if (main) {
      main.innerHTML = '<div class="error">Teams data not available. Please try refreshing the page.</div>';
    }
    return;
  }
  
  const team = state.teams.find(t => (t.id === teamId) || (t.name === teamId));
  if (!team) {
    console.warn('Team with ID/name ' + teamId + ' not found');
    // Show error to user
    clearMain();
    const main = $("main-content");
    if (main) {
      main.innerHTML = '<div class="error">Could not find team "' + teamId + '". Available teams: ' + state.teams.map(t => t.name || t.id).sort().join(', ') + '</div>';
    }
    return;
  }
  
  console.log('Found team:', team.name, 'with ID:', team.id);
  
  // Switch to teams mode but don't show overview
  setMode("teams", false);
  
  // Select the team
  selectTeam(team);
}

function getUserDisplayName(developerSlug) {
  const user = (state.users || []).find((entry) => entry.slug === developerSlug);
  if (user) {
    return user.display_name || developerSlug;
  }
  return developerSlug;
}

function createClickableDeveloperName(developerSlug, displayName, style = "block") {
  const nameElement = document.createElement("span");
  
  // Check if user is active (exists in current user list)
  const isActive = state.users.some(user => user.slug === developerSlug);
  
  if (isActive) {
    // Active user - make it clickable
    nameElement.className = "developer-name clickable" + (style === "inline" ? " inline" : "");
    nameElement.textContent = displayName || developerSlug;
    nameElement.style.cursor = "pointer";
    nameElement.onclick = (e) => {
      e.preventDefault();
      e.stopPropagation();
      console.log('Navigating to user:', developerSlug);
      
      // Ensure we have the data loaded
      if (state.users.length === 0) {
        console.warn('No users loaded yet, cannot navigate');
        return;
      }
      
      navigateToUser(developerSlug);
    };
  } else {
    // Inactive user - mark as red, not clickable
    nameElement.className = "developer-name inactive" + (style === "inline" ? " inline" : "");
    nameElement.textContent = displayName || developerSlug;
    nameElement.style.color = "#dc2626"; // Red color for inactive users
    nameElement.style.cursor = "default";
    nameElement.title = "Inactive contributor (no recent activity in analysis period)";
  }
  
  return nameElement;
}

async function openPagerDutyForUser(userSlug) {
  if (!isPagerDutyConfigured()) {
    alert("Configure a PagerDuty API token under Integrations to open responder dashboards.");
    return;
  }
  try {
    const overview = await ensurePagerDutyOverview(false);
    const responders = overview?.responders?.entries || [];
    const responderEntry = responders.find((entry) => entry?.github_user?.slug === userSlug);
    if (responderEntry) {
      selectPagerDutyResponder(responderEntry);
      return;
    }
    if (state.mode !== "alerts") {
      setMode("alerts", false);
    }
    await showAlertsOverviewDashboard(false);
    alert(`${getUserDisplayName(userSlug)} is not linked to a PagerDuty responder yet.`);
  } catch (error) {
    console.error("Failed to open PagerDuty responder for", userSlug, error);
    alert(error?.message || "Failed to open PagerDuty data for this user.");
  }
}

async function loadUserBadges(userSlug) {
  try {
    console.log("Loading badges for user:", userSlug);
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Badge loading timeout')), 10000)
    );
    
    const badgesPromise = fetchJSON("/api/users/" + encodeURIComponent(userSlug) + "/badges");
    
    const response = await Promise.race([badgesPromise, timeoutPromise]);
    console.log("Loaded badges for", userSlug, ":", response.badges?.length || 0, "badges");
    return response.badges || [];
  } catch (err) {
    console.error("Failed to load user badges for", userSlug, ":", err);
    return [];
  }
}

function renderUserBadges(badges, container) {
  try {
    console.log("renderUserBadges called with", badges?.length || 0, "badges");
    
    if (!badges || badges.length === 0) {
      console.log("No badges to render");
      return;
    }
    
    // Separate badges by type
    const ownershipBadges = badges.filter(b => b.type === "ownership");
    const maintainerBadges = badges.filter(b => b.type === "maintainer");
    const productivityBadges = badges.filter(b => b.type === "productivity");
    const ownershipPercentageBadges = badges.filter(b => b.type === "ownership_percentage");
    
    console.log("Badge counts:", {
      ownership: ownershipBadges.length,
      maintainer: maintainerBadges.length, 
      productivity: productivityBadges.length,
      ownershipPercentage: ownershipPercentageBadges.length
    });
    
    // Render productivity badges section first (most prestigious)
    if (productivityBadges.length > 0) {
      const productivitySection = document.createElement("div");
      productivitySection.className = "card badges-section";
      productivitySection.innerHTML = createTitleWithTooltip(
        "🏆 Achievement Badges", 
        "Special recognitions for outstanding contributions. 'Most Productive Developer' is awarded to the developer with the most lines added across all subsystems for the current year.",
        "h2"
      );
      
      const badgeList = document.createElement("div");
      badgeList.className = "badge-list";
      
      productivityBadges.forEach(badge => {
        const badgeElement = createBadgeElement(badge);
        badgeList.appendChild(badgeElement);
      });
      
      productivitySection.appendChild(badgeList);
      container.appendChild(productivitySection);
    }
    
    // Render ownership badges section
    if (ownershipBadges.length > 0) {
      const ownershipSection = document.createElement("div");
      ownershipSection.className = "card badges-section";
      ownershipSection.innerHTML = '<h2>👑 Top Ownership Badges</h2>';
      
      const badgeList = document.createElement("div");
      badgeList.className = "badge-list";
      
      ownershipBadges.forEach(badge => {
        const badgeElement = createBadgeElement(badge);
        badgeList.appendChild(badgeElement);
      });
      
      ownershipSection.appendChild(badgeList);
      container.appendChild(ownershipSection);
    }
    
    // Render maintainer badges section
    if (maintainerBadges.length > 0) {
      const maintainerSection = document.createElement("div");
      maintainerSection.className = "card badges-section";
      maintainerSection.innerHTML = '<h2>🔧 Maintainer Badges</h2>';
      
      const badgeList = document.createElement("div");
      badgeList.className = "badge-list";
      
      maintainerBadges.forEach(badge => {
        const badgeElement = createBadgeElement(badge);
        badgeList.appendChild(badgeElement);
      });
      
      maintainerSection.appendChild(badgeList);
      container.appendChild(maintainerSection);
    }
    
    // Render ownership percentage badges section
    if (ownershipPercentageBadges.length > 0) {
      const ownershipPercentageSection = document.createElement("div");
      ownershipPercentageSection.className = "card badges-section";
      ownershipPercentageSection.innerHTML = '<h2>📊 Significant Ownership</h2>';
      
      const badgeList = document.createElement("div");
      badgeList.className = "badge-list ownership-list";
      
      ownershipPercentageBadges.forEach(badge => {
        const badgeElement = createOwnershipBadgeElement(badge);
        badgeList.appendChild(badgeElement);
      });
      
      ownershipPercentageSection.appendChild(badgeList);
      container.appendChild(ownershipPercentageSection);
    }
    
    console.log("renderUserBadges completed successfully");
  } catch (error) {
    console.error("Error in renderUserBadges:", error);
    // Don't show error to user, just log it
  }
}

function createBadgeElement(badge) {
  const badgeElement = document.createElement("div");
  badgeElement.className = "badge-item";
  
  // Create title with tooltip if we have explanation text
  const titleContainer = document.createElement("div");
  titleContainer.className = "badge-title";
  
  // Get explanation text based on badge type
  const tooltipText = getBadgeTooltipText(badge);
  
  if (tooltipText) {
    titleContainer.innerHTML = `
      <div class="title-with-help">
        <span>${badge.title}</span>
        <span class="help-icon">?
          <span class="tooltip">${tooltipText}</span>
        </span>
      </div>
    `;
  } else {
    titleContainer.textContent = badge.title;
  }
  
  const subtitleElement = document.createElement("div");
  subtitleElement.className = "badge-subtitle";
  subtitleElement.textContent = badge.subtitle;
  
  badgeElement.appendChild(titleContainer);
  badgeElement.appendChild(subtitleElement);
  
  return badgeElement;
}

function getBadgeTooltipText(badge) {
  // Use the description from badge data if available
  if (badge.description) {
    return badge.description;
  }
  
  // Fallback to type-based explanations for older badges
  if (badge.badge_type === "most_productive") {
    return `Awarded to the developer with the highest total lines added across all subsystems for the year ${badge.year}. Calculated by summing 'lines_added' from all subsystem yearly summaries. Minimum threshold: 1,000 lines.`;
  }
  
  if (badge.badge_type === "top_maintainer") {
    return `Awarded to the developer with the most commits in the '${badge.subsystem}' subsystem over the last 3 months. Based on commit count analysis. Minimum threshold: 3 commits.`;
  }
  
  if (badge.badge_type === "domain_expert") {
    return `Awarded to developers who own more than 10% of the codebase in the '${badge.subsystem}' subsystem. Based on git blame analysis of file ownership.`;
  }
  
  // Return null for badges without specific explanations
  return null;
}

function createOwnershipBadgeElement(badge) {
  const badgeElement = document.createElement("div");
  badgeElement.className = "ownership-badge-item";
  
  const subsystemElement = document.createElement("div");
  subsystemElement.className = "ownership-subsystem clickable";
  subsystemElement.textContent = badge.subsystem;
  subsystemElement.style.cursor = "pointer";
  subsystemElement.onclick = (e) => {
    e.preventDefault();
    e.stopPropagation();
    console.log('Navigating to subsystem from ownership badge:', badge.subsystem);
    navigateToSubsystem(badge.subsystem);
  };
  
  const percentageElement = document.createElement("div");
  percentageElement.className = "ownership-percentage";
  percentageElement.textContent = (badge.share * 100).toFixed(1) + "%";
  
  badgeElement.appendChild(subsystemElement);
  badgeElement.appendChild(percentageElement);
  
  return badgeElement;
}

function selectUserMonth(month) {
  state.selectedUserMonth = month;
  renderUserMonthList();
  loadUserMonth(state.selectedUser, month);
}

function selectSubsystem(subsystem) {
  state.selectedSubsystem = subsystem;
  state.selectedSubsystemPeriod = null;
  renderSubsystemList();
  renderSubsystemPeriodList();

  // Default to most recent yearly period if available
  const yearlyPeriods = (subsystem.periods || []).filter(p => p.is_yearly);
  let yearlyPeriod = null;
  
  if (yearlyPeriods.length > 0) {
    // Sort by year (from date) and pick the most recent
    yearlyPeriod = yearlyPeriods.sort((a, b) => b.from.localeCompare(a.from))[0];
  }
  
  if (yearlyPeriod) {
    state.selectedSubsystemPeriod = yearlyPeriod;
    loadSubsystemPeriod(subsystem, yearlyPeriod).catch(error => {
      console.error("Failed to load subsystem period:", error);
      clearMain();
      setViewHeader("Error", "Failed to load subsystem data: " + error.message, "Error");
    });
  } else {
    clearMain();
    setViewHeader(
      "Subsystem: " + subsystem.name,
      "Select a time period to view stats",
      "Subsystem"
    );
  }
}

function selectSubsystemPeriod(period) {
  state.selectedSubsystemPeriod = period;
  renderSubsystemPeriodList();
  loadSubsystemPeriod(state.selectedSubsystem, period).catch(error => {
    console.error("Failed to load subsystem period:", error);
    clearMain();
    setViewHeader("Error", "Failed to load subsystem data: " + error.message, "Error");
  });
}

function findSubsystemByRepoName(repoName) {
  // Direct match first
  let match = state.subsystems.find(s => s.name === repoName);
  if (match) return match;
  
  // Try fuzzy matching - remove common repo name parts and match
  const cleanRepoName = repoName.replace(/^(appgate-sdp-int\/)?/, '').toLowerCase();
  match = state.subsystems.find(s => s.name.toLowerCase() === cleanRepoName);
  if (match) return match;
  
  // Try partial matching
  match = state.subsystems.find(s => s.name.toLowerCase().includes(cleanRepoName) || cleanRepoName.includes(s.name.toLowerCase()));
  if (match) return match;
  
  return null;
}

// --------------------------
// Data loading
// --------------------------

async function loadUserMonth(user, month) {
  if (!user || !month) {
    return;
  }
  const requestToken = (state.userRequestToken || 0) + 1;
  state.userRequestToken = requestToken;
  try {
    let url;
    if (month.is_yearly) {
      // Extract year from the label (e.g., "2025")
      const year = month.label;
      url = "/api/users/" + encodeURIComponent(user.slug) + "/year/" + year;
    } else {
      url = "/api/users/" + encodeURIComponent(user.slug) + "/month/" + encodeURIComponent(month.from) + "/" + encodeURIComponent(month.to);
    }
    const data = await fetchJSON(url);
    if (state.userRequestToken !== requestToken) {
      console.log("Stale user month response discarded", user.slug, month.label);
      return;
    }
    const renderToken = startUserRenderCycle();
    await renderUserDashboard(user, month, data, renderToken);
  } catch (err) {
    if (state.userRequestToken !== requestToken) {
      return;
    }
    clearMain();
    setViewHeader("Error", "Failed to load user stats: " + err.message, "Error");
  }
}

async function loadSubsystemPeriod(subsystem, period) {
  try {
    let url;
    if (period.is_yearly) {
      // Extract year from the label (e.g., "2025")
      const year = period.label;
      url = "/api/subsystems/" + encodeURIComponent(subsystem.name) + "/year/" + year;
    } else {
      url = "/api/subsystems/" + encodeURIComponent(subsystem.name) + "/month/" + encodeURIComponent(period.from) + "/" + encodeURIComponent(period.to);
    }
    const data = await fetchJSON(url);
    await renderSubsystemDashboard(subsystem, period, data);
  } catch (err) {
    clearMain();
    setViewHeader("Error", "Failed to load subsystem stats: " + err.message, "Error");
  }
}

async function loadTeamPeriod(team, period) {
  try {
    let url;
    if (period.is_yearly) {
      // Extract year from the label (e.g., "2025")
      const year = period.label;
      url = "/api/teams/" + encodeURIComponent(team.id) + "/year/" + year;
    } else {
      url = "/api/teams/" + encodeURIComponent(team.id) + "/month/" + encodeURIComponent(period.from) + "/" + encodeURIComponent(period.to);
    }
    const data = await fetchJSON(url);
    await renderTeamDashboard(team, period, data);
  } catch (err) {
    clearMain();
    setViewHeader("Error", "Failed to load team stats: " + err.message, "Error");
  }
}

// --------------------------
// PagerDuty Alerts (PD)
// --------------------------

function updatePagerDutySidebarStatus(status = "idle", payload = null) {
  const statusEl = $("pagerduty-sidebar-status");
  if (!statusEl) {
    return;
  }
  statusEl.classList.toggle("status-error", status === "error");
  if (status === "ready" && payload) {
    const total = payload.totals?.total || 0;
    const generated = payload.generated_at ? formatDateTime(payload.generated_at) : "recently";
    statusEl.textContent = `Last sync ${generated} • ${total.toLocaleString()} incidents`;
  } else if (status === "loading") {
    statusEl.textContent = "Loading PagerDuty data…";
  } else if (status === "error") {
    const message = payload?.status === 404
      ? "PagerDuty data not found. Configure a token and run Run Update."
      : `Unable to load PagerDuty data${payload?.message ? `: ${payload.message}` : ''}`;
    statusEl.textContent = message;
  } else {
    statusEl.textContent = "Configure a PagerDuty token and run Run Update to enable alerts.";
  }
}

function formatDurationMinutes(minutes) {
  if (minutes == null || Number.isNaN(minutes)) {
    return "--";
  }
  const totalMinutes = Math.max(0, Math.round(minutes));
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const mins = totalMinutes % 60;
  const parts = [];
  if (days) parts.push(`${days}d`);
  if (hours) parts.push(`${hours}h`);
  if (mins || parts.length === 0) parts.push(`${mins}m`);
  return parts.join(" ");
}

function describePagerDutyPeriod(period, fallbackDays = 365) {
  if (!period || !period.from || !period.to) {
    return `Last ${fallbackDays} days`;
  }
  try {
    const fromDate = new Date(period.from);
    const toDate = new Date(period.to);
    return `${fromDate.toLocaleDateString()} → ${toDate.toLocaleDateString()}`;
  } catch (error) {
    return `Last ${fallbackDays} days`;
  }
}

function formatRankSummary(rankInfo) {
  if (!rankInfo || typeof rankInfo.rank !== "number" || typeof rankInfo.total !== "number" || rankInfo.total <= 0) {
    return "";
  }
  const rank = Math.max(1, Math.round(rankInfo.rank));
  const total = Math.max(1, Math.round(rankInfo.total));
  let text = `#${rank} of ${total}`;
  if (typeof rankInfo.percentile === "number" && total > 1) {
    const percentile = Math.max(1, Math.min(100, Math.round(rankInfo.percentile)));
    text += ` · top ${percentile}%`;
  }
  return text;
}

function computeCommitsPerWeek(summary) {
  if (!summary) {
    return 0;
  }
  const commits = Number(summary.total_commits) || 0;
  const fromDate = summary.from ? new Date(summary.from) : null;
  const toDate = summary.to ? new Date(summary.to) : null;
  if (!fromDate || !toDate || Number.isNaN(fromDate.getTime()) || Number.isNaN(toDate.getTime())) {
    return commits;
  }
  const msPerDay = 24 * 60 * 60 * 1000;
  const daySpan = Math.max(1, Math.round((toDate - fromDate) / msPerDay) + 1);
  const weeks = daySpan / 7;
  if (!Number.isFinite(weeks) || weeks <= 0) {
    return commits;
  }
  return commits / weeks;
}

async function ensurePagerDutyOverview(forceReload = false) {
  if (!forceReload && state.alerts.overview) {
    return state.alerts.overview;
  }
  updatePagerDutySidebarStatus("loading");
  state.alerts.loading = true;
  try {
    const response = await fetch("/api/pagerduty/overview", {
      headers: { Accept: "application/json" }
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch (parseError) {
      if (response.ok) {
        throw parseError;
      }
    }
    if (!response.ok) {
      const error = new Error(payload?.error || `Request failed: ${response.status}`);
      error.status = response.status;
      throw error;
    }
    state.alerts.overview = payload || {};
    state.alerts.error = null;
    state.alerts.responderIncidents = {};
    state.alerts.allIncidentsData = null;
    updatePagerDutySidebarStatus("ready", payload);
    return state.alerts.overview;
  } catch (error) {
    state.alerts.overview = null;
    state.alerts.error = error;
    updatePagerDutySidebarStatus("error", error);
    throw error;
  } finally {
    state.alerts.loading = false;
  }
}

async function showAlertsOverviewDashboard(forceReload = false) {
  state.alerts.currentView = "overview";
  state.alerts.selectedResponder = null;
  renderPagerDutyResponderList();
  const main = $("main-content");
  clearMain();
  main.innerHTML = createLoadingIndicator(
    "Loading PagerDuty Alerts",
    "Fetching incidents and trends from the cache…"
  );
  try {
    const overview = await ensurePagerDutyOverview(forceReload);
    renderAlertsOverview(overview);
  } catch (error) {
    console.error("Failed to load PagerDuty overview:", error);
    const isNotFound = error?.status === 404;
    const message = isNotFound
      ? "No PagerDuty data found. Configure a token under Integrations and run ‘Run Update’."
      : `Unable to load PagerDuty data: ${error.message || error}`;
    setViewHeader("Alerts (PagerDuty)", "", "Alerts · PD");
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h2>PagerDuty Alerts</h2>
      <p>${message}</p>
      ${READ_ONLY_MODE ? "" : '<button class="btn btn-primary" id="retry-pagerduty">Retry</button>'}
    `;
    main.innerHTML = "";
    main.appendChild(card);
    const retryBtn = $("retry-pagerduty");
    if (retryBtn) {
      retryBtn.addEventListener("click", () => showAlertsOverviewDashboard(true));
    }
  }
}

async function showAllPagerDutyIncidentsView(forceReload = false) {
  if (state.mode !== "alerts") {
    setMode("alerts", false);
  }
  state.alerts.currentView = "all-incidents";
  state.alerts.selectedResponder = null;
  renderPagerDutyResponderList();
  const main = $("main-content");
  clearMain();
  setViewHeader("Alerts · All incidents", "Loading incident history…", "Alerts · PD");
  main.innerHTML = createLoadingIndicator(
    "Loading incidents",
    "Fetching the cached PagerDuty incident history…"
  );
  try {
    const payload = await fetchAllPagerDutyIncidents(forceReload);
    renderAllIncidentsExplorer(payload);
  } catch (error) {
    console.error("Failed to load PagerDuty incidents:", error);
    const isNotFound = error?.status === 404;
    const message = isNotFound
      ? "No PagerDuty incident history found. Configure a token under Integrations and run ‘Run Update’."
      : `Unable to load incidents: ${error.message || error}`;
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h2>All incidents</h2>
      <p>${message}</p>
      ${READ_ONLY_MODE ? "" : '<button class="btn btn-primary" id="retry-all-incidents">Retry</button>'}
    `;
    main.innerHTML = "";
    main.appendChild(card);
    const retryBtn = $("retry-all-incidents");
    if (retryBtn) {
      retryBtn.addEventListener("click", () => showAllPagerDutyIncidentsView(true));
    }
  }
}

function renderAllIncidentsExplorer(payload) {
  const main = $("main-content");
  if (!main) {
    return;
  }
  const incidents = Array.isArray(payload?.incidents) ? payload.incidents : [];
  const totalCount = typeof payload?.total === "number" ? payload.total : incidents.length;
  const lookbackDays = state.alerts.overview?.lookback_days || 365;
  const subtitle = incidents.length
    ? `Filter ${totalCount.toLocaleString()} incidents captured over the last ${lookbackDays} days.`
    : "No PagerDuty incidents captured yet. Run ‘Run Update’ after configuring the integration.";
  setViewHeader("Alerts · All incidents", subtitle, "Alerts · PD");
  main.innerHTML = "";
  if (!incidents.length) {
    const emptyCard = document.createElement("div");
    emptyCard.className = "card";
    emptyCard.innerHTML = `
      <h2>No incidents available</h2>
      <p>Run ‘Run Update’ after configuring PagerDuty to populate incident history.</p>
    `;
    main.appendChild(emptyCard);
    return;
  }

  const cachedCount = incidents.length;
  const hasMoreThanCached = totalCount > cachedCount;
  const card = document.createElement("div");
  card.className = "card pd-all-incidents-card";
  card.innerHTML = createTitleWithTooltip(
    "Incident explorer",
    "Search and filter the cached PagerDuty incidents.",
    "h2"
  );

  const note = document.createElement("p");
  note.className = "pd-responder-chart-note";
  note.textContent = hasMoreThanCached
    ? `Showing up to ${cachedCount.toLocaleString()} of ${totalCount.toLocaleString()} most recent incidents.`
    : `Showing ${cachedCount.toLocaleString()} cached incidents from the last ${lookbackDays} days.`;
  card.appendChild(note);

  const timelineChartId = "chart-pd-all-incidents-trend";
  if (state.charts && state.charts[timelineChartId]) {
    try {
      state.charts[timelineChartId].destroy();
    } catch (error) {
      console.warn("Failed to destroy PagerDuty chart", timelineChartId, error);
    }
    delete state.charts[timelineChartId];
  }
  const timelineSection = document.createElement("div");
  timelineSection.className = "pd-all-incidents-timeline";
  timelineSection.innerHTML = createTitleWithTooltip(
    "Incidents over time",
    "Weekly incident counts that react to the filters below.",
    "h3"
  );
  const timelineChartWrapper = document.createElement("div");
  timelineChartWrapper.className = "chart-container";
  const timelineCanvas = document.createElement("canvas");
  timelineCanvas.id = timelineChartId;
  timelineChartWrapper.appendChild(timelineCanvas);
  timelineSection.appendChild(timelineChartWrapper);
  const timelineEmptyMessage = document.createElement("div");
  timelineEmptyMessage.className = "pd-breakdown-empty";
  timelineEmptyMessage.textContent = "No incidents match the selected filters.";
  timelineEmptyMessage.style.display = "none";
  timelineSection.appendChild(timelineEmptyMessage);
  card.appendChild(timelineSection);
  tagVisualization(timelineSection, "alerts-all-incidents-timeline", { scope: "alerts" });

  const filters = ensureAllIncidentsFilters();
  const controls = document.createElement("div");
  controls.className = "pd-responder-filters";

  const severitySelect = document.createElement("select");
  severitySelect.className = "pd-filter-select";
  severitySelect.innerHTML = '<option value="">All severities</option>';
  const severities = Array.from(
    new Set(
      incidents
        .map((incident) => (incident.severity || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  severities.forEach((severity) => {
    const option = document.createElement("option");
    option.value = severity;
    option.textContent = severity.toUpperCase();
    if (filters.severity === severity) {
      option.selected = true;
    }
    severitySelect.appendChild(option);
  });
  severitySelect.addEventListener("change", () => {
    filters.severity = severitySelect.value;
    renderIncidents();
  });

  const statusSelect = document.createElement("select");
  statusSelect.className = "pd-filter-select";
  statusSelect.innerHTML = '<option value="">All statuses</option>';
  const statuses = Array.from(
    new Set(
      incidents
        .map((incident) => (incident.status || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  statuses.forEach((status) => {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status.toUpperCase();
    if (filters.status === status) {
      option.selected = true;
    }
    statusSelect.appendChild(option);
  });
  statusSelect.addEventListener("change", () => {
    filters.status = statusSelect.value;
    renderIncidents();
  });

  const searchInput = document.createElement("input");
  searchInput.type = "search";
  searchInput.placeholder = "Search title or service";
  searchInput.value = filters.query || "";
  searchInput.className = "pd-filter-search";
  searchInput.addEventListener("input", (event) => {
    filters.query = event.target.value;
    renderIncidents();
  });

  controls.appendChild(severitySelect);
  controls.appendChild(statusSelect);
  controls.appendChild(searchInput);
  card.appendChild(controls);

  const meta = document.createElement("div");
  meta.className = "pd-responders-meta";
  card.appendChild(meta);

  const list = document.createElement("div");
  list.className = "pd-incidents-list";
  card.appendChild(list);

  function updateTimeline(filteredIncidents = []) {
    if (!timelineCanvas) {
      return;
    }
    state.charts = state.charts || {};
    const existingChart = state.charts[timelineChartId];
    if (existingChart) {
      try {
        existingChart.destroy();
      } catch (error) {
        console.warn("Failed to destroy PagerDuty chart", timelineChartId, error);
      }
      delete state.charts[timelineChartId];
    }
    const timelineData = buildAllIncidentsTimelineData(filteredIncidents);
    if (!timelineData || !timelineData.labels.length) {
      timelineChartWrapper.style.display = "none";
      timelineEmptyMessage.style.display = "block";
      return;
    }
    timelineChartWrapper.style.display = "";
    timelineEmptyMessage.style.display = "none";
    const ctx = timelineCanvas.getContext("2d");
    const tooltipWeeks = timelineData.rawKeys || [];
    const datasets = timelineData.severityOrder.map((severity) => ({
      label: severity.toUpperCase(),
      data: timelineData.series[severity],
      backgroundColor: PAGERDUTY_SEVERITY_COLORS[severity] || PAGERDUTY_SEVERITY_COLORS.unknown,
      borderColor: PAGERDUTY_SEVERITY_BORDER_COLORS[severity] || PAGERDUTY_SEVERITY_BORDER_COLORS.unknown,
      borderWidth: 1,
      stack: "all-incidents"
    }));
    state.charts[timelineChartId] = new Chart(ctx, {
      type: "bar",
      data: {
        labels: timelineData.labels,
        datasets
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: {
            callbacks: {
              title(items) {
                if (!items || !items.length) {
                  return "";
                }
                const index = items[0].dataIndex;
                if (typeof index === "number" && tooltipWeeks[index]) {
                  return `Week of ${formatPagerDutyWeekLabel(tooltipWeeks[index], true)}`;
                }
                return items[0].label || "";
              },
              footer(items) {
                if (!items || !items.length || !Array.isArray(timelineData.totals)) {
                  return "";
                }
                const index = items[0].dataIndex;
                const total = timelineData.totals[index];
                return typeof total === "number"
                  ? `Total: ${total.toLocaleString()} incidents`
                  : "";
              }
            }
          }
        },
        scales: {
          x: { stacked: true, ticks: { maxRotation: 45, minRotation: 45 } },
          y: { stacked: true, beginAtZero: true, ticks: { precision: 0 } }
        }
      }
    });
  }

  const totalLabel = hasMoreThanCached
    ? `${cachedCount.toLocaleString()} cached of ${totalCount.toLocaleString()} total`
    : `${totalCount.toLocaleString()} cached incidents`;
  const listLimit = 200;

  function renderIncidents() {
    list.innerHTML = "";
    const filtered = applyResponderIncidentFilters(incidents, filters);
    updateTimeline(filtered);
    if (!filtered.length) {
      list.innerHTML = '<div class="pd-breakdown-empty">No incidents match the selected filters.</div>';
      meta.textContent = `Showing 0 incidents (${totalLabel})`;
      return;
    }
    const limited = filtered
      .slice()
      .sort((a, b) => {
        const left = a.created_at || a.updated_at || "";
        const right = b.created_at || b.updated_at || "";
        return right.localeCompare(left);
      })
      .slice(0, listLimit);
    limited.forEach((incident) => {
      list.appendChild(createPagerDutyIncidentRow(incident, true));
    });
    const limitNote = filtered.length > listLimit ? " · Limited to 200 most recent matches" : "";
    meta.textContent = `Showing ${limited.length} of ${filtered.length} incidents (${totalLabel})${limitNote}`;
  }

  renderIncidents();
  main.appendChild(card);
}

function renderAlertsOverview(overview) {
  const main = $("main-content");
  main.innerHTML = "";
  const periodLabel = describePagerDutyPeriod(overview.period, overview.lookback_days || 365);
  setViewHeader("Alerts (PagerDuty)", periodLabel, "Alerts · PD");
  renderPagerDutyResponderList();

  renderPagerDutyKpis(main, overview);
  renderPagerDutyCharts(main, overview);
  renderPagerDutyBreakdowns(main, overview);
  renderPagerDutyIncidents(main, overview);
}

function renderPagerDutyKpis(container, overview) {
  const totals = overview.totals || {};
  const metrics = overview.metrics || {};
  const kpis = [
    {
      label: "Total incidents",
      value: totals.total || 0,
      detail: "Last 12 months"
    },
    {
      label: "Open incidents",
      value: totals.open || 0,
      detail: "Currently open"
    },
    {
      label: "Resolved",
      value: totals.resolved || 0,
      detail: "Closed in window"
    },
    {
      label: "Avg resolution",
      value: formatDurationMinutes(metrics.avg_resolution_minutes),
      detail: "Mean time to resolve"
    },
    {
      label: "Median resolution",
      value: formatDurationMinutes(metrics.median_resolution_minutes),
      detail: "Median MTTR"
    },
    {
      label: "Resolved < 24h",
      value: metrics.resolved_within_24h_percent != null
        ? `${metrics.resolved_within_24h_percent.toFixed(1)}%`
        : "--",
      detail: "Percentage resolved in 24h"
    }
  ];
  const grid = document.createElement("div");
  grid.className = "kpi-grid pd-kpi-grid";
  kpis.forEach((item) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    card.innerHTML = `
      <div class="kpi-label">${item.label}</div>
      <div class="kpi-value">${item.value}</div>
      <div class="kpi-detail">${item.detail || ""}</div>
    `;
    grid.appendChild(card);
  });
  container.appendChild(grid);
  tagVisualization(grid, "alerts-kpis", { scope: "alerts" });
}

function renderPagerDutyCharts(container, overview) {
  const trend = overview.trend || {};
  const chartConfigs = [];

  if (Array.isArray(trend.daily_open) && trend.daily_open.length > 0) {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = createTitleWithTooltip(
      "📈 Open incidents",
      "Snapshot of how many incidents were open at the end of each day.",
      "h2"
    ) + '<div class="chart-container"><canvas id="chart-pd-open"></canvas></div>';
    container.appendChild(card);
    tagVisualization(card, "alerts-open-incidents", { scope: "alerts" });
    chartConfigs.push({
      id: "chart-pd-open",
      type: "line",
      labels: trend.daily_open.map((point) => point.date),
      datasets: [
        {
          label: "Open incidents",
          data: trend.daily_open.map((point) => point.open),
          borderColor: "#21c55d",
          backgroundColor: "rgba(33, 197, 93, 0.2)",
          fill: true,
          tension: 0.35
        }
      ]
    });
  }

  if (Array.isArray(trend.daily_open_vs_closed) && trend.daily_open_vs_closed.length > 0) {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = createTitleWithTooltip(
      "📅 Daily opened vs closed",
      "Compares how many incidents were created and resolved per day.",
      "h2"
    ) + '<div class="chart-container"><canvas id="chart-pd-daily"></canvas></div>';
    container.appendChild(card);
    tagVisualization(card, "alerts-daily-open-vs-closed", { scope: "alerts" });
    chartConfigs.push({
      id: "chart-pd-daily",
      type: "line",
      labels: trend.daily_open_vs_closed.map((point) => point.date),
      datasets: [
        {
          label: "Opened",
          data: trend.daily_open_vs_closed.map((point) => point.opened),
          borderColor: "#0ea5e9",
          backgroundColor: "rgba(14, 165, 233, 0.15)",
          fill: false,
          tension: 0.2
        },
        {
          label: "Closed",
          data: trend.daily_open_vs_closed.map((point) => point.closed),
          borderColor: "#f97316",
          backgroundColor: "rgba(249, 115, 22, 0.15)",
          fill: false,
          tension: 0.2
        }
      ]
    });
  }

  if (Array.isArray(trend.weekly_open_vs_closed) && trend.weekly_open_vs_closed.length > 0) {
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = createTitleWithTooltip(
      "📊 Weekly trend",
      "Aggregated opened vs closed incidents per week.",
      "h2"
    ) + '<div class="chart-container"><canvas id="chart-pd-weekly"></canvas></div>';
    container.appendChild(card);
    tagVisualization(card, "alerts-weekly-trend", { scope: "alerts" });
    chartConfigs.push({
      id: "chart-pd-weekly",
      type: "bar",
      labels: trend.weekly_open_vs_closed.map((point) => point.week_start),
      datasets: [
        {
          label: "Opened",
          data: trend.weekly_open_vs_closed.map((point) => point.opened),
          backgroundColor: "rgba(14, 165, 233, 0.7)",
          borderRadius: 4
        },
        {
          label: "Closed",
          data: trend.weekly_open_vs_closed.map((point) => point.closed),
          backgroundColor: "rgba(249, 115, 22, 0.7)",
          borderRadius: 4
        }
      ]
    });
  }

  if (Array.isArray(trend.hourly_arrivals) && trend.hourly_arrivals.length > 0) {
    const normalized = trend.hourly_arrivals
      .map((point, index) => ({
        hour: Number(point?.hour ?? index),
        count: Number(point?.count) || 0
      }))
      .sort((a, b) => a.hour - b.hour);
    const labels = normalized.map((entry) => {
      const hour = Number.isFinite(entry.hour) ? entry.hour : 0;
      return `${hour.toString().padStart(2, "0")}:00`;
    });
    const values = normalized.map((entry) => entry.count);
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = createTitleWithTooltip(
      "🕒 Incidents by hour",
      "When PagerDuty incidents are created throughout the day (UTC).",
      "h2"
    ) + '<div class="chart-container"><canvas id="chart-pd-hourly"></canvas></div>';
    container.appendChild(card);
    tagVisualization(card, "alerts-hourly-arrivals", { scope: "alerts" });
    chartConfigs.push({
      id: "chart-pd-hourly",
      type: "bar",
      labels,
      datasets: [
        {
          label: "Incidents created",
          data: values,
          backgroundColor: "rgba(99, 102, 241, 0.8)",
          borderRadius: 4
        }
      ]
    });
  }

  if (Array.isArray(trend.weekly_severity) && trend.weekly_severity.length > 0) {
    const weeklySeverityPoints = trend.weekly_severity.map((point) => point || {});
    const normalizedCounts = weeklySeverityPoints.map((point) => {
      const counts = {};
      const entries = point && typeof point.severities === "object" ? point.severities : {};
      Object.entries(entries).forEach(([severity, rawValue]) => {
        const normalized = normalizePagerDutySeverity(severity);
        const numericValue = Number(rawValue) || 0;
        if (!Number.isFinite(numericValue) || numericValue <= 0) {
          return;
        }
        counts[normalized] = (counts[normalized] || 0) + numericValue;
      });
      return counts;
    });
    const weeklyTotals = normalizedCounts.map((counts) =>
      Object.values(counts || {}).reduce((sum, value) => sum + (Number(value) || 0), 0)
    );
    const averageWeeklyIncidents =
      weeklyTotals.length > 0
        ? weeklyTotals.reduce((sum, total) => sum + total, 0) / weeklyTotals.length
        : 0;
    const severitySet = new Set();
    normalizedCounts.forEach((counts) => {
      Object.keys(counts).forEach((severity) => severitySet.add(severity));
    });
    if (severitySet.size > 0) {
      const weekKeys = weeklySeverityPoints.map((point) => point.week_start || "");
      const severityOrder = [
        ...PAGERDUTY_SEVERITY_ORDER,
        ...Array.from(severitySet).filter((severity) => !PAGERDUTY_SEVERITY_ORDER.includes(severity))
      ].filter((severity) => severitySet.has(severity));
      const datasets = severityOrder.map((severity) => ({
        label: severity.toUpperCase(),
        data: normalizedCounts.map((counts) => counts[severity] || 0),
        backgroundColor: PAGERDUTY_SEVERITY_COLORS[severity] || PAGERDUTY_SEVERITY_COLORS.unknown,
        borderColor: PAGERDUTY_SEVERITY_BORDER_COLORS[severity] || PAGERDUTY_SEVERITY_BORDER_COLORS.unknown,
        borderWidth: 1,
        stack: "severity"
      }));
      if (averageWeeklyIncidents > 0) {
        const averagePerWeek = Number(averageWeeklyIncidents.toFixed(2));
        datasets.push({
          type: "line",
          label: "Average incidents/week",
          data: weekKeys.map(() => averagePerWeek),
          borderColor: "#1f2937",
          borderWidth: 2,
          borderDash: [6, 4],
          pointRadius: 0,
          fill: false,
          tension: 0.25,
          yAxisID: "y",
          order: 0
        });
      }
      const card = document.createElement("div");
      card.className = "card";
      card.innerHTML = createTitleWithTooltip(
        "Severity over time",
        "Weekly PagerDuty incidents grouped by severity level.",
        "h2"
      ) + '<div class="chart-container"><canvas id="chart-pd-weekly-severity"></canvas></div>';
      container.appendChild(card);
      tagVisualization(card, "alerts-weekly-severity", { scope: "alerts" });
      chartConfigs.push({
        id: "chart-pd-weekly-severity",
        type: "bar",
        labels: weekKeys.map((key) => formatPagerDutyWeekLabel(key)),
        datasets,
        stackAxes: true,
        tooltipFormatter(index) {
          if (typeof index !== "number" || index < 0 || index >= weekKeys.length) {
            return "";
          }
          return `Week of ${formatPagerDutyWeekLabel(weekKeys[index], true)}`;
        }
      });
    }
  }

  setTimeout(() => {
    chartConfigs.forEach((config) => {
      const canvas = document.getElementById(config.id);
      if (!canvas) return;
      if (state.charts[config.id]) {
        state.charts[config.id].destroy();
      }
      const tooltipOptions = { intersect: false };
      if (typeof config.tooltipFormatter === "function") {
        tooltipOptions.callbacks = {
          title(items) {
            if (!items || !items.length) {
              return "";
            }
            const idx = items[0].dataIndex;
            return config.tooltipFormatter(idx, items);
          }
        };
      }
      state.charts[config.id] = new Chart(canvas, {
        type: config.type,
        data: {
          labels: config.labels,
          datasets: config.datasets
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          plugins: {
            legend: { position: "top" },
            tooltip: tooltipOptions
          },
          scales: {
            x: {
              ticks: { maxRotation: 45, minRotation: 45 },
              stacked: !!config.stackAxes
            },
            y: {
              beginAtZero: true,
              stacked: !!config.stackAxes
            }
          }
        }
      });
    });
  }, 50);
}

function renderPagerDutyBreakdowns(container, overview) {
  const grid = document.createElement("div");
  grid.className = "alerts-breakdown-grid";

  const severityCard = document.createElement("div");
  severityCard.className = "card";
  severityCard.innerHTML = createTitleWithTooltip(
    "Severity mix",
    "Distribution of severities or priorities for the period.",
    "h3"
  );
  severityCard.appendChild(createPagerDutyBreakdownList(overview.severity_breakdown));
  tagVisualization(severityCard, "alerts-severity-mix", { scope: "alerts" });
  grid.appendChild(severityCard);

  const severityCadenceCard = document.createElement("div");
  severityCadenceCard.className = "card";
  severityCadenceCard.innerHTML = createTitleWithTooltip(
    "Severity cadence",
    "How frequently each severity fires plus its share of the window.",
    "h3"
  );
  severityCadenceCard.appendChild(
    createPagerDutySeverityStatsTable(
      overview.severity_breakdown,
      overview.lookback_days || 365,
      overview.totals?.total
    )
  );
  tagVisualization(severityCadenceCard, "alerts-severity-cadence", { scope: "alerts" });
  grid.appendChild(severityCadenceCard);

  const serviceCard = document.createElement("div");
  serviceCard.className = "card";
  serviceCard.innerHTML = createTitleWithTooltip(
    "Top services",
    "Services with the highest number of incidents in the selected window.",
    "h3"
  );
  serviceCard.appendChild(createPagerDutyServiceTable(overview.service_breakdown));
  tagVisualization(serviceCard, "alerts-top-services", { scope: "alerts" });
  grid.appendChild(serviceCard);

  const teamCard = document.createElement("div");
  teamCard.className = "card";
  teamCard.innerHTML = createTitleWithTooltip(
    "Team mentions",
    "Counts of how often teams were attached to incidents.",
    "h3"
  );
  teamCard.appendChild(createPagerDutyBreakdownList(overview.team_breakdown));
  tagVisualization(teamCard, "alerts-team-mentions", { scope: "alerts" });
  grid.appendChild(teamCard);

  container.appendChild(grid);
  renderPagerDutyTeamActivity(container, overview);
  renderPagerDutyResponders(container, overview);
}

function renderPagerDutyResponders(container, overview) {
  const responderData = overview && overview.responders;
  if (!responderData || !Array.isArray(responderData.entries) || responderData.entries.length === 0) {
    return;
  }

  const card = document.createElement("div");
  card.className = "card pd-responders-card";
  card.innerHTML = createTitleWithTooltip(
    "Top responders",
    "Matches PagerDuty resolvers to RepoSquirrel developers via shared email addresses.",
    "h3"
  );

  const meta = document.createElement("div");
  const totalResponders = responderData.total_responders ?? responderData.entries.length;
  const matchedResponders = responderData.matched_responders ?? responderData.entries.filter((entry) => entry.github_user).length;
  meta.className = "pd-responders-meta";
  meta.textContent = `${matchedResponders}/${totalResponders} responders linked to developer profiles`;
  card.appendChild(meta);

  const tableWrapper = document.createElement("div");
  tableWrapper.className = "pd-responders-table-wrapper";
  const table = document.createElement("div");
  table.className = "pd-responders-table";
  table.appendChild(createPagerDutyRespondersHeader());
  responderData.entries.slice(0, 15).forEach((entry, index) => {
    table.appendChild(createPagerDutyResponderRow(entry, index));
  });
  tableWrapper.appendChild(table);
  card.appendChild(tableWrapper);
  tagVisualization(card, "alerts-top-responders", { scope: "alerts" });
  container.appendChild(card);
}

function renderPagerDutyTeamActivity(container, overview) {
  const repoTeamsConfigured = Array.isArray(state?.teams) && state.teams.length > 0;
  const activity = overview?.team_activity;
  const teams = Array.isArray(activity?.teams) ? activity.teams : [];
  if (!repoTeamsConfigured || teams.length === 0) {
    return;
  }

  const card = document.createElement("div");
  card.className = "card pd-team-activity-card";
  card.innerHTML = createTitleWithTooltip(
    "RepoSquirrel teams on PagerDuty",
    "Aggregates assignments, acknowledgements, and resolutions by RepoSquirrel team membership (GitHub-linked responders only).",
    "h3"
  );

  const note = document.createElement("p");
  note.className = "pd-team-activity-note";
  const teamCount = typeof activity.team_count === "number" ? activity.team_count : teams.length;
  const responderCount = typeof activity.unique_responders === "number" ? activity.unique_responders : null;
  const noteParts = [`${teamCount} team${teamCount === 1 ? "" : "s"} with PagerDuty activity`];
  if (responderCount) {
    noteParts.push(`${responderCount} linked responder${responderCount === 1 ? "" : "s"}`);
  }
  note.textContent = noteParts.join(" • ");
  card.appendChild(note);

  const sortTeams = teams
    .slice()
    .sort((a, b) => {
      const resolvedDiff = (b.resolved || 0) - (a.resolved || 0);
      if (resolvedDiff !== 0) {
        return resolvedDiff;
      }
      const ackDiff = (b.acknowledged || 0) - (a.acknowledged || 0);
      if (ackDiff !== 0) {
        return ackDiff;
      }
      return (b.assigned || 0) - (a.assigned || 0);
    });

  const topForChart = sortTeams.slice(0, 8);
  if (topForChart.length > 0) {
    const labels = topForChart.map((team) => team.team_name || team.team_id || "Team");
    const chartId = "chart-pd-team-activity";
    const chartWrapper = document.createElement("div");
    chartWrapper.className = "pd-team-activity-chart";
    const canvas = document.createElement("canvas");
    canvas.id = chartId;
    chartWrapper.appendChild(canvas);
    card.appendChild(chartWrapper);

    if (state.charts[chartId]) {
      state.charts[chartId].destroy();
      delete state.charts[chartId];
    }

    const ctx = canvas.getContext("2d");
    state.charts[chartId] = new Chart(ctx, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "Assigned",
            data: topForChart.map((team) => team.assigned || 0),
            backgroundColor: "rgba(56, 189, 248, 0.85)",
          },
          {
            label: "Acknowledged",
            data: topForChart.map((team) => team.acknowledged || 0),
            backgroundColor: "rgba(249, 115, 22, 0.85)",
          },
          {
            label: "Resolved",
            data: topForChart.map((team) => team.resolved || 0),
            backgroundColor: "rgba(34, 197, 94, 0.9)",
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        indexAxis: "y",
        interaction: { mode: "index", intersect: false },
        plugins: {
          legend: { position: "bottom" },
          tooltip: {
            callbacks: {
              title(items) {
                if (!items || !items.length) {
                  return "";
                }
                return labels[items[0].dataIndex] || "Team";
              },
            },
          },
        },
        scales: {
          x: { beginAtZero: true, ticks: { precision: 0 } },
          y: { ticks: { autoSkip: false } },
        },
      },
    });
  }

  const list = document.createElement("div");
  list.className = "pd-team-activity-list";
  const header = document.createElement("div");
  header.className = "pd-team-activity-row pd-team-activity-header";
  header.innerHTML = `
    <span>Team</span>
    <span>Resolved</span>
    <span>Ack'd</span>
    <span>Assigned</span>
    <span>Responders</span>
  `;
  list.appendChild(header);

  const formatCount = (value) => (Number(value) || 0).toLocaleString();
  sortTeams.slice(0, 10).forEach((team) => {
    const row = document.createElement("div");
    row.className = "pd-team-activity-row";
    const metaParts = [];
    if (typeof team.member_count === "number") {
      metaParts.push(`${team.member_count} member${team.member_count === 1 ? "" : "s"}`);
    }
    if (typeof team.touch_count === "number" && team.touch_count > 0) {
      metaParts.push(`${team.touch_count.toLocaleString()} touches`);
    }
    row.innerHTML = `
      <div class="pd-team-activity-team">
        <strong>${team.team_name || team.team_id || "Team"}</strong>
        ${metaParts.length ? `<span class="pd-team-activity-meta">${metaParts.join(" • ")}</span>` : ""}
      </div>
      <div class="pd-team-activity-metric">${formatCount(team.resolved)}</div>
      <div class="pd-team-activity-metric">${formatCount(team.acknowledged)}</div>
      <div class="pd-team-activity-metric">${formatCount(team.assigned)}</div>
      <div class="pd-team-activity-metric">${formatCount(team.responder_count)}</div>
    `;
    list.appendChild(row);
  });
  card.appendChild(list);
  tagVisualization(card, "alerts-team-activity", { scope: "alerts" });
  container.appendChild(card);
}

function createPagerDutyRespondersHeader() {
  const header = document.createElement("div");
  header.className = "pd-responder-row pd-responder-header";
  const labels = [
    "#",
    "Responder",
    "Resolved",
    "Ack'd",
    "Assignments",
    "Avg MTTR",
    "Details",
    "PagerDuty"
  ];
  labels.forEach((label) => {
    const cell = document.createElement("div");
    cell.className = "pd-responder-cell";
    cell.textContent = label;
    header.appendChild(cell);
  });
  return header;
}

function createPagerDutyResponderRow(entry, index) {
  const row = document.createElement("div");
  row.className = "pd-responder-row";

  const rank = document.createElement("div");
  rank.className = "pd-responder-rank";
  rank.textContent = `#${index + 1}`;
  row.appendChild(rank);

  const info = document.createElement("div");
  info.className = "pd-responder-info";
  const displayName = (entry.github_user && entry.github_user.display_name) || entry.pagerduty_name || "Unknown responder";
  if (entry.github_user && typeof navigateToUser === "function") {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "btn-link pd-responder-link";
    button.textContent = displayName;
    button.addEventListener("click", () => navigateToUser(entry.github_user.slug));
    info.appendChild(button);
  } else {
    const name = document.createElement("span");
    name.className = "pd-responder-name";
    name.textContent = displayName;
    info.appendChild(name);
  }

  const badge = document.createElement("span");
  badge.className = `pd-responder-badge ${entry.github_user ? "linked" : "unlinked"}`;
  badge.textContent = entry.github_user ? "Linked" : "PagerDuty only";
  info.appendChild(badge);

  const details = document.createElement("div");
  details.className = "pd-responder-subline";
  const detailParts = [];
  if (entry.pagerduty_name && entry.pagerduty_name !== displayName) {
    detailParts.push(`PD: ${entry.pagerduty_name}`);
  }
  const fallbackText = entry.github_user ? "Linked to GitHub profile" : "PagerDuty only";
  details.textContent = detailParts.length ? detailParts.join(" • ") : fallbackText;
  info.appendChild(details);

  row.appendChild(info);

  row.appendChild(createPagerDutyResponderMetricCell(entry.resolved_count));
  row.appendChild(createPagerDutyResponderMetricCell(entry.acknowledged_count));
  row.appendChild(createPagerDutyResponderMetricCell(entry.assignment_count));

  const avgCell = document.createElement("div");
  avgCell.className = "pd-responder-metric";
  avgCell.textContent = formatDurationMinutes(entry.avg_resolution_minutes);
  row.appendChild(avgCell);

  const detailsCell = document.createElement("div");
  detailsCell.className = "pd-responder-link-cell";
  const detailsButton = document.createElement("button");
  detailsButton.type = "button";
  detailsButton.className = "btn-link pd-responder-detail-link";
  detailsButton.textContent = "Details";
  detailsButton.addEventListener("click", () => selectPagerDutyResponder(entry));
  detailsCell.appendChild(detailsButton);
  row.appendChild(detailsCell);

  const pagerDutyCell = document.createElement("div");
  pagerDutyCell.className = "pd-responder-link-cell";
  if (entry.pagerduty_html_url) {
    const link = document.createElement("a");
    link.href = entry.pagerduty_html_url;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = "PagerDuty";
    pagerDutyCell.appendChild(link);
  } else {
    pagerDutyCell.textContent = "—";
  }
  row.appendChild(pagerDutyCell);

  return row;
}

function createPagerDutyResponderMetricCell(value) {
  const cell = document.createElement("div");
  cell.className = "pd-responder-metric";
  const safeValue = value ?? 0;
  cell.textContent = Number(safeValue).toLocaleString();
  return cell;
}

function renderPagerDutyResponderList() {
  const container = $("pagerduty-responder-list");
  if (!container) {
    return;
  }
  container.innerHTML = "";
  const overview = state.alerts.overview;
  const responderData = overview?.responders;
  const entries = Array.isArray(responderData?.entries) ? responderData.entries : [];
  const currentView = state.alerts.currentView || (state.alerts.selectedResponder ? "responder" : "overview");

  const navItems = [
    {
      view: "all-incidents",
      label: "All incidents",
      handler: () => showAllPagerDutyIncidentsView(false)
    },
    {
      view: "overview",
      label: "All responder overview",
      handler: () => showAlertsOverviewDashboard(false)
    }
  ];

  navItems.forEach((nav) => {
    const item = document.createElement("div");
    item.className = "sidebar-item";
    if (currentView === nav.view) {
      item.classList.add("active");
    }
    item.textContent = nav.label;
    item.addEventListener("click", () => {
      if (state.alerts.currentView === nav.view) {
        return;
      }
      nav.handler();
    });
    container.appendChild(item);
  });

  if (!overview) {
    const note = document.createElement("div");
    note.className = "sidebar-item small";
    note.textContent = "Run ‘Run Update’ to load PagerDuty responders.";
    container.appendChild(note);
    return;
  }

  if (entries.length === 0) {
    const empty = document.createElement("div");
    empty.className = "sidebar-item small";
    empty.textContent = "No responder data captured yet.";
    container.appendChild(empty);
    return;
  }

  entries.forEach((entry) => {
    const item = document.createElement("div");
    item.className = "sidebar-item";
    if (
      state.alerts.selectedResponder &&
      state.alerts.selectedResponder.pagerduty_user_id === entry.pagerduty_user_id
    ) {
      item.classList.add("active");
    }
    const label = getResponderDisplayName(entry);
    const content = document.createElement("div");
    content.className = "sidebar-item-content";
    const name = document.createElement("span");
    name.textContent = label;
    const meta = document.createElement("span");
    meta.className = "sidebar-item-meta";
    const resolvedCount = entry.resolved_count ?? 0;
    meta.textContent = `${resolvedCount.toLocaleString()} resolved`;
    content.appendChild(name);
    content.appendChild(meta);
    item.appendChild(content);
    item.addEventListener("click", () => selectPagerDutyResponder(entry));
    container.appendChild(item);
  });
}

function getResponderDisplayName(entry) {
  if (!entry) {
    return "Responder";
  }
  if (entry.github_user && entry.github_user.display_name) {
    return entry.github_user.display_name;
  }
  if (entry.pagerduty_name) {
    return entry.pagerduty_name;
  }
  if (entry.pagerduty_email) {
    return entry.pagerduty_email;
  }
  return entry.pagerduty_user_id ? `Responder ${entry.pagerduty_user_id}` : "Responder";
}

function selectPagerDutyResponder(entry) {
  if (!entry) {
    showAlertsOverviewDashboard(false);
    return;
  }
  state.alerts.currentView = "responder";
  state.alerts.selectedResponder = entry;
  renderPagerDutyResponderList();
  if (state.mode !== "alerts") {
    setMode("alerts", false);
  }
  showAlertsResponderDashboard(entry);
}

async function showAlertsResponderDashboard(responder) {
  if (!responder) {
    showAlertsOverviewDashboard(false);
    return;
  }
  const main = $("main-content");
  clearMain();
  const responderName = getResponderDisplayName(responder);
  setViewHeader(
    `Alerts · ${responderName}`,
    "Responder-level PagerDuty activity",
    "Alerts · PD"
  );
  main.innerHTML = createLoadingIndicator(
    `Loading ${responderName}`,
    "Gathering incidents and responder statistics…"
  );
  try {
    const data = await fetchPagerDutyIncidentsForResponder(responder.pagerduty_user_id);
    if (
      !state.alerts.selectedResponder ||
      state.alerts.selectedResponder.pagerduty_user_id !== responder.pagerduty_user_id
    ) {
      return;
    }
    buildPagerDutyResponderDashboard(main, responder, data);
  } catch (error) {
    console.error("Failed to load responder incidents:", error);
    if (
      !state.alerts.selectedResponder ||
      state.alerts.selectedResponder.pagerduty_user_id !== responder.pagerduty_user_id
    ) {
      return;
    }
    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h2>Unable to load responder data</h2>
      <p>${error.message || error}</p>
    `;
    main.innerHTML = "";
    main.appendChild(card);
  }
}

function buildPagerDutyResponderDashboard(main, responder, incidentsData) {
  const container = document.createElement("div");
  container.className = "pd-responder-dashboard";
  const incidents = Array.isArray(incidentsData?.incidents) ? incidentsData.incidents : [];
  const totalCount = typeof incidentsData?.total === "number" ? incidentsData.total : incidents.length;
  destroyPagerDutyResponderCharts();
  container.appendChild(buildResponderSummaryCard(responder));
  container.appendChild(buildResponderKpiGrid(responder));
  const timelineCard = buildResponderTimelineCard(responder, incidents);
  if (timelineCard) {
    container.appendChild(timelineCard);
  }
  const acknowledgementSeverityCard = buildResponderSeverityCard(responder, incidents, {
    title: "Acknowledgement severity timeline",
    tooltip: "Stacked weekly counts of incidents this responder acknowledged, grouped by PagerDuty severity.",
    noteText: "Stacked bars show weekly acknowledgement counts grouped by PagerDuty severity.",
    timeline: computeResponderRoleSeverityTimeline(incidents, "acknowledged"),
    chartSuffix: "ack-severity",
    emptyMessageWhenIncidents: "We couldn't find acknowledgement severity history for this responder yet.",
  });
  if (acknowledgementSeverityCard) {
    container.appendChild(acknowledgementSeverityCard);
  }
  const resolutionSeverityCard = buildResponderSeverityCard(responder, incidents, {
    title: "Resolution severity timeline",
    tooltip: "Stacked weekly counts of incidents this responder resolved, grouped by PagerDuty severity.",
    noteText: "Stacked bars show weekly resolution counts grouped by PagerDuty severity.",
    timeline: computeResponderRoleSeverityTimeline(incidents, "resolved"),
    chartSuffix: "resolved-severity",
    emptyMessageWhenIncidents: "We couldn't find resolution severity history for this responder yet.",
  });
  if (resolutionSeverityCard) {
    container.appendChild(resolutionSeverityCard);
  }
  const openIncidentsCard = buildResponderOpenIncidentsCard(responder, incidents);
  if (openIncidentsCard) {
    container.appendChild(openIncidentsCard);
  }
  container.appendChild(buildResponderIncidentsCard(incidents, totalCount));
  main.innerHTML = "";
  main.appendChild(container);
}

function buildResponderSummaryCard(responder) {
  const card = document.createElement("div");
  card.className = "card pd-responder-summary";
  const name = getResponderDisplayName(responder);
  const lookbackDays = state.alerts.overview?.lookback_days || 365;
  card.innerHTML = `
    <h2>${name}</h2>
    <p>Responder metrics aggregated across the last ${lookbackDays} days.</p>
  `;

  const meta = document.createElement("ul");
  meta.className = "pd-responder-meta";
  const pdLine = document.createElement("li");
  pdLine.innerHTML = `<span>PagerDuty:</span> ${responder.pagerduty_name || "Unknown"}${responder.pagerduty_email ? ` · ${responder.pagerduty_email}` : ""}`;
  meta.appendChild(pdLine);
  const githubLine = document.createElement("li");
  githubLine.innerHTML = `<span>Matched developer:</span> ${responder.github_user ? responder.github_user.display_name : "No linked RepoSquirrel user"}`;
  meta.appendChild(githubLine);
  card.appendChild(meta);

  const actions = document.createElement("div");
  actions.className = "pd-responder-actions";
  let hasAction = false;
  if (responder.github_user && typeof navigateToUser === "function") {
    const btn = document.createElement("button");
    btn.className = "btn btn-secondary";
    btn.textContent = "Open developer view";
    btn.addEventListener("click", () => navigateToUser(responder.github_user.slug));
    actions.appendChild(btn);
    hasAction = true;
  }
  if (responder.pagerduty_html_url) {
    const link = document.createElement("a");
    link.className = "btn btn-secondary";
    link.href = responder.pagerduty_html_url;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = "View in PagerDuty";
    actions.appendChild(link);
    hasAction = true;
  }
  if (hasAction) {
    card.appendChild(actions);
  }
  return card;
}

function buildResponderKpiGrid(responder) {
  const touchCount = responder.touch_count ?? (
    (responder.resolved_count || 0) +
    (responder.acknowledged_count || 0) +
    (responder.assignment_count || 0)
  );
  const kpis = [
    {
      label: "Resolved incidents",
      value: responder.resolved_count || 0,
      detail: "Closed by this responder"
    },
    {
      label: "Acknowledged",
      value: responder.acknowledged_count || 0,
      detail: "Times acknowledged"
    },
    {
      label: "Assignments",
      value: responder.assignment_count || 0,
      detail: "Direct assignments"
    },
    {
      label: "Unique touches",
      value: touchCount,
      detail: "Incidents touched"
    },
    {
      label: "Avg MTTR",
      value: formatDurationMinutes(responder.avg_resolution_minutes),
      detail: "Average resolution"
    },
    {
      label: "Median MTTR",
      value: formatDurationMinutes(responder.median_resolution_minutes),
      detail: "Median resolution"
    },
    {
      label: "Fastest fix",
      value: formatDurationMinutes(responder.fastest_resolution_minutes),
      detail: "Fastest resolution"
    },
    {
      label: "Slowest fix",
      value: formatDurationMinutes(responder.slowest_resolution_minutes),
      detail: "Longest resolution"
    }
  ];
  const grid = document.createElement("div");
  grid.className = "kpi-grid pd-kpi-grid";
  kpis.forEach((item) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    card.innerHTML = `
      <div class="kpi-label">${item.label}</div>
      <div class="kpi-value">${item.value ?? "--"}</div>
      <div class="kpi-detail">${item.detail || ""}</div>
    `;
    grid.appendChild(card);
  });
  return grid;
}

function buildResponderTimelineCard(responder, incidents = []) {
  const card = document.createElement("div");
  card.className = "card pd-responder-timeline-card";
  card.innerHTML = createTitleWithTooltip(
    "Assignments vs. acknowledgements vs. resolutions",
    "Weekly counts showing when this responder was assigned incidents, acknowledged pages, and resolved work.",
    "h3"
  );
  const timeline = computeResponderTimeline(responder, incidents);
  if (!timeline) {
    const empty = document.createElement("div");
    empty.className = "pd-responder-empty";
    empty.textContent = incidents.length
      ? "We couldn't find assignment or resolution timestamps for this responder yet."
      : "No PagerDuty incidents recorded for this responder in the cached window.";
    card.appendChild(empty);
    return card;
  }

  const note = document.createElement("p");
  note.className = "pd-responder-chart-note";
  note.textContent = "Points represent weekly assignment, acknowledgement, and resolution counts across the cached period.";
  card.appendChild(note);

  const chartWrapper = document.createElement("div");
  chartWrapper.className = "pd-responder-chart";
  const canvas = document.createElement("canvas");
  const responderId = responder?.pagerduty_user_id || Date.now();
  const chartId = `pd-responder-timeline-${responderId}`;
  canvas.id = chartId;
  chartWrapper.appendChild(canvas);
  card.appendChild(chartWrapper);

  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
    delete state.charts[chartId];
  }

  const ctx = canvas.getContext("2d");
  const tooltipDates = timeline.rawKeys;
  state.charts[chartId] = new Chart(ctx, {
    type: "line",
    data: {
      labels: timeline.labels,
      datasets: [
        {
          label: "Assigned",
          data: timeline.assignedSeries,
          borderColor: "rgba(56, 189, 248, 1)",
          backgroundColor: "rgba(56, 189, 248, 0.2)",
          pointRadius: 2,
          tension: 0.3,
          fill: false
        },
        {
          label: "Acknowledged",
          data: timeline.acknowledgedSeries,
          borderColor: "rgba(249, 115, 22, 1)",
          backgroundColor: "rgba(249, 115, 22, 0.2)",
          pointRadius: 2,
          tension: 0.3,
          fill: false
        },
        {
          label: "Resolved",
          data: timeline.resolvedSeries,
          borderColor: "rgba(52, 211, 153, 1)",
          backgroundColor: "rgba(52, 211, 153, 0.2)",
          pointRadius: 2,
          tension: 0.3,
          fill: false
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      plugins: {
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            title(items) {
              if (!items || !items.length) {
                return "";
              }
              const idx = items[0].dataIndex;
              const key = tooltipDates[idx];
              return formatPagerDutyWeekLabel(key, true);
            }
          }
        }
      },
      scales: {
        x: {
          ticks: {
            maxTicksLimit: 10
          },
          title: {
            display: true,
            text: "Date"
          }
        },
        y: {
          beginAtZero: true,
          title: {
            display: true,
            text: "Incidents"
          },
          ticks: {
            precision: 0,
            callback(value) {
              return typeof value === "number" ? value.toLocaleString() : value;
            }
          }
        }
      }
    }
  });

  return card;
}

function buildResponderSeverityCard(responder, incidents = [], options = {}) {
  const {
    title = "Severity timeline",
    tooltip = "Stacked weekly counts of incidents touched by this responder, grouped by PagerDuty severity.",
    noteText = "Stacked bars show weekly incident counts grouped by PagerDuty severity.",
    timeline = computeResponderSeverityTimeline(incidents),
    chartSuffix = "severity",
    emptyMessageWhenIncidents = "We couldn't find severity history for this responder yet.",
    emptyMessageWhenNoIncidents = "No PagerDuty incidents recorded for this responder in the cached window."
  } = options || {};

  const card = document.createElement("div");
  card.className = "card pd-responder-severity-card";
  card.innerHTML = createTitleWithTooltip(title, tooltip, "h3");
  const timelineData = timeline;
  if (!timelineData) {
    const empty = document.createElement("div");
    empty.className = "pd-responder-empty";
    empty.textContent = incidents.length ? emptyMessageWhenIncidents : emptyMessageWhenNoIncidents;
    card.appendChild(empty);
    return card;
  }

  const note = document.createElement("p");
  note.className = "pd-responder-chart-note";
  note.textContent = noteText;
  card.appendChild(note);

  const chartWrapper = document.createElement("div");
  chartWrapper.className = "pd-responder-chart";
  const canvas = document.createElement("canvas");
  const responderId = responder?.pagerduty_user_id || Date.now();
  const chartId = `pd-responder-${chartSuffix}-${responderId}`;
  canvas.id = chartId;
  chartWrapper.appendChild(canvas);
  card.appendChild(chartWrapper);

  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
    delete state.charts[chartId];
  }

  const datasets = timelineData.severityOrder.map((severity) => ({
    label: severity.toUpperCase(),
    data: timelineData.series[severity],
    backgroundColor: PAGERDUTY_SEVERITY_COLORS[severity] || PAGERDUTY_SEVERITY_COLORS.unknown,
    borderColor: PAGERDUTY_SEVERITY_BORDER_COLORS[severity] || PAGERDUTY_SEVERITY_BORDER_COLORS.unknown,
    borderWidth: 1,
    stack: chartSuffix
  }));

  const ctx = canvas.getContext("2d");
  const tooltipWeeks = timelineData.rawKeys;
  state.charts[chartId] = new Chart(ctx, {
    type: "bar",
    data: {
      labels: timelineData.labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            title(items) {
              if (!items || !items.length) {
                return "";
              }
              const index = items[0].dataIndex;
              return `Week of ${formatPagerDutyWeekLabel(tooltipWeeks[index], true)}`;
            }
          }
        }
      },
      scales: {
        x: { stacked: true },
        y: { stacked: true, beginAtZero: true, ticks: { precision: 0 } }
      }
    }
  });

  return card;
}

function buildResponderOpenIncidentsCard(responder, incidents = []) {
  const card = document.createElement("div");
  card.className = "card pd-responder-open-incidents";
  card.innerHTML = createTitleWithTooltip(
    "Active incidents",
    "Open PagerDuty incidents this responder is currently involved in.",
    "h3"
  );

  const list = document.createElement("div");
  list.className = "pd-incidents-list";
  const openIncidents = Array.isArray(incidents)
    ? incidents.filter((incident) => isIncidentAssignedToResponder(incident, responder))
    : [];

  if (openIncidents.length === 0) {
    list.innerHTML = '<div class="pd-responder-empty">No active assignments for this responder.</div>';
  } else {
    openIncidents
      .sort((a, b) => {
        const left = a.created_at || a.updated_at || "";
        const right = b.created_at || b.updated_at || "";
        return right.localeCompare(left);
      })
      .slice(0, 10)
      .forEach((incident) => {
        const roleSummary = formatResponderRoles(incident.matched_roles);
        list.appendChild(createPagerDutyIncidentRow(incident, true, roleSummary));
      });
  }

  card.appendChild(list);
  return card;
}

function isIncidentAssignedToResponder(incident, responder) {
  if (!incident || !responder) {
    return false;
  }
  const responderId = responder.pagerduty_user_id || responder.id || responder.user_id;
  if (!responderId) {
    return false;
  }
  const status = (incident.status || "").toLowerCase();
  if (status === "resolved") {
    return false;
  }
  const assignments = Array.isArray(incident.assignments) ? incident.assignments : [];
  const matchesAssignment = assignments.some((assignment) => {
    const assignee = assignment?.assignee || assignment?.user || assignment;
    if (!assignee) {
      return false;
    }
    const assigneeId = assignee.id || assignee.user_id || assignee.pagerduty_user_id;
    return assigneeId && String(assigneeId) === String(responderId);
  });
  if (assignments.length > 0) {
    return matchesAssignment;
  }
  if (matchesAssignment) {
    return true;
  }
  const matchedEvents = Array.isArray(incident.matched_events) ? incident.matched_events : [];
  if (!matchedEvents.length) {
    return false;
  }
  const lastRoleEvent = [...matchedEvents]
    .filter((event) => event && event.role)
    .reverse()
    .find((event) => ["assigned", "acknowledged", "resolved"].includes(event.role));
  if (!lastRoleEvent) {
    return false;
  }
  return lastRoleEvent.role !== "resolved";
}

function computeResponderTimeline(responder, incidents = []) {
  if (!responder || !Array.isArray(incidents) || incidents.length === 0) {
    return null;
  }
  const assignedBuckets = new Map();
  const acknowledgedBuckets = new Map();
  const resolvedBuckets = new Map();

  incidents.forEach((incident) => {
    const matchedEvents = Array.isArray(incident.matched_events) ? incident.matched_events : [];
    matchedEvents.forEach((event) => {
      const role = (event.role || "").toLowerCase();
      if (!["assigned", "acknowledged", "resolved"].includes(role)) {
        return;
      }
      const fallbackTimestamps = [event.at, incident.resolved_at, incident.created_at, incident.updated_at];
      const eventDate = fallbackTimestamps
        .map((value) => parsePagerDutyDate(value))
        .find((value) => value instanceof Date && !Number.isNaN(value.getTime()));
      if (!eventDate) {
        return;
      }
      const weekKey = getWeekStartKey(eventDate);
      if (!weekKey) {
        return;
      }
      const targetMap =
        role === "assigned"
          ? assignedBuckets
          : role === "acknowledged"
            ? acknowledgedBuckets
            : resolvedBuckets;
      targetMap.set(weekKey, (targetMap.get(weekKey) || 0) + 1);
    });
  });

  if (!assignedBuckets.size && !acknowledgedBuckets.size && !resolvedBuckets.size) {
    return null;
  }

  const allKeys = Array.from(
    new Set([...assignedBuckets.keys(), ...acknowledgedBuckets.keys(), ...resolvedBuckets.keys()])
  ).sort();
  const labels = allKeys.map((key) => formatPagerDutyWeekLabel(key));
  const assignedSeries = allKeys.map((key) => assignedBuckets.get(key) || 0);
  const acknowledgedSeries = allKeys.map((key) => acknowledgedBuckets.get(key) || 0);
  const resolvedSeries = allKeys.map((key) => resolvedBuckets.get(key) || 0);

  return { labels, assignedSeries, acknowledgedSeries, resolvedSeries, rawKeys: allKeys };
}

function computeResponderSeverityTimeline(incidents = []) {
  if (!Array.isArray(incidents) || incidents.length === 0) {
    return null;
  }
  const severityBuckets = new Map();
  incidents.forEach((incident) => {
    const severity = normalizePagerDutySeverity(incident?.severity);
    const timestamps = [
      incident?.created_at,
      incident?.first_trigger_log_entry?.created_at,
      incident?.last_status_change_at,
      incident?.updated_at
    ];
    const eventDate = timestamps
      .map((value) => parsePagerDutyDate(value))
      .find((value) => value instanceof Date && !Number.isNaN(value.getTime()));
    if (!eventDate) {
      return;
    }
    const weekKey = getWeekStartKey(eventDate);
    if (!weekKey) {
      return;
    }
    if (!severityBuckets.has(severity)) {
      severityBuckets.set(severity, new Map());
    }
    const bucket = severityBuckets.get(severity);
    bucket.set(weekKey, (bucket.get(weekKey) || 0) + 1);
  });
  return buildSeverityTimelineFromBuckets(severityBuckets);
}

function computeResponderRoleSeverityTimeline(incidents = [], role = "") {
  if (!Array.isArray(incidents) || incidents.length === 0) {
    return null;
  }
  const normalizedRole = String(role || "").toLowerCase();
  if (!normalizedRole) {
    return null;
  }
  const severityBuckets = new Map();
  incidents.forEach((incident) => {
    const matchedEvents = Array.isArray(incident.matched_events) ? incident.matched_events : [];
    if (!matchedEvents.length) {
      return;
    }
    const severity = normalizePagerDutySeverity(incident?.severity);
    matchedEvents.forEach((event) => {
      if ((event?.role || "").toLowerCase() !== normalizedRole) {
        return;
      }
      const eventDate =
        parsePagerDutyDate(event.at) ||
        parsePagerDutyDate(incident.last_status_change_at) ||
        parsePagerDutyDate(incident.updated_at) ||
        parsePagerDutyDate(incident.created_at);
      if (!eventDate) {
        return;
      }
      const weekKey = getWeekStartKey(eventDate);
      if (!weekKey) {
        return;
      }
      if (!severityBuckets.has(severity)) {
        severityBuckets.set(severity, new Map());
      }
      const bucket = severityBuckets.get(severity);
      bucket.set(weekKey, (bucket.get(weekKey) || 0) + 1);
    });
  });
  return buildSeverityTimelineFromBuckets(severityBuckets);
}

function buildSeverityTimelineFromBuckets(severityBuckets) {
  if (!(severityBuckets instanceof Map) || severityBuckets.size === 0) {
    return null;
  }
  const weekKeys = new Set();
  severityBuckets.forEach((bucket) => {
    if (!(bucket instanceof Map)) {
      return;
    }
    bucket.forEach((_, key) => weekKeys.add(key));
  });
  if (!weekKeys.size) {
    return null;
  }
  const sortedWeeks = Array.from(weekKeys).sort();
  const order = [
    ...PAGERDUTY_SEVERITY_ORDER,
    ...Array.from(severityBuckets.keys()).filter((severity) => !PAGERDUTY_SEVERITY_ORDER.includes(severity))
  ];
  const activeOrder = order.filter((severity) => {
    const bucket = severityBuckets.get(severity);
    if (!(bucket instanceof Map)) {
      return false;
    }
    return Array.from(bucket.values()).some((count) => count > 0);
  });
  if (!activeOrder.length) {
    return null;
  }
  const series = {};
  activeOrder.forEach((severity) => {
    const bucket = severityBuckets.get(severity) || new Map();
    series[severity] = sortedWeeks.map((week) => bucket.get(week) || 0);
  });
  const labels = sortedWeeks.map((key) => formatPagerDutyWeekLabel(key));
  return { labels, rawKeys: sortedWeeks, severityOrder: activeOrder, series };
}

function buildAllIncidentsTimelineData(incidents = []) {
  if (!Array.isArray(incidents) || incidents.length === 0) {
    return null;
  }
  const severityBuckets = new Map();
  incidents.forEach((incident) => {
    const severity = normalizePagerDutySeverity(incident?.severity);
    const timestamps = [
      incident?.created_at,
      incident?.first_trigger_log_entry?.created_at,
      incident?.last_status_change_at,
      incident?.updated_at,
      incident?.resolved_at
    ];
    const eventDate = timestamps
      .map((value) => parsePagerDutyDate(value))
      .find((value) => value instanceof Date && !Number.isNaN(value.getTime()));
    if (!eventDate) {
      return;
    }
    const weekKey = getWeekStartKey(eventDate);
    if (!weekKey) {
      return;
    }
    if (!severityBuckets.has(severity)) {
      severityBuckets.set(severity, new Map());
    }
    const bucket = severityBuckets.get(severity);
    bucket.set(weekKey, (bucket.get(weekKey) || 0) + 1);
  });
  const timeline = buildSeverityTimelineFromBuckets(severityBuckets);
  if (!timeline) {
    return null;
  }
  const totals = timeline.labels.map((_, index) =>
    timeline.severityOrder.reduce((sum, severity) => sum + (timeline.series[severity][index] || 0), 0)
  );
  return {
    ...timeline,
    totals
  };
}

function normalizePagerDutySeverity(value) {
  if (value == null) {
    return "unknown";
  }
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) {
    return "unknown";
  }
  const alias = {
    sev1: "critical",
    sev2: "high",
    sev3: "medium",
    sev4: "low",
    sev5: "info",
    p1: "critical",
    p2: "high",
    p3: "medium",
    p4: "low",
    p5: "info",
    informational: "info",
    information: "info",
    warn: "medium",
    warning: "medium"
  };
  if (alias[normalized]) {
    return alias[normalized];
  }
  if (PAGERDUTY_SEVERITY_ORDER.includes(normalized)) {
    return normalized;
  }
  return normalized || "unknown";
}

function parsePagerDutyDate(value) {
  if (!value) {
    return null;
  }
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function getWeekStartKey(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }
  const copy = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const weekday = copy.getUTCDay();
  const diff = (weekday + 6) % 7; // Convert Sunday(0) to 6, Monday(1) to 0
  copy.setUTCDate(copy.getUTCDate() - diff);
  return copy.toISOString().slice(0, 10);
}

function formatPagerDutyWeekLabel(dateKey, includeYear = false) {
  if (!dateKey) {
    return "";
  }
  const parts = dateKey.split("-").map((part) => parseInt(part, 10));
  if (parts.length < 3 || parts.some((value) => Number.isNaN(value))) {
    return dateKey;
  }
  const [year, month, day] = parts;
  const date = new Date(Date.UTC(year, month - 1, day));
  const options = includeYear
    ? { month: "short", day: "numeric", year: "numeric" }
    : { month: "short", day: "numeric" };
  const formatted = date.toLocaleDateString(undefined, options);
  return includeYear ? `Week of ${formatted}` : formatted;
}

function destroyPagerDutyResponderCharts() {
  Object.keys(state.charts || {}).forEach((chartId) => {
    if (chartId.startsWith("pd-responder-")) {
      try {
        state.charts[chartId].destroy();
      } catch (error) {
        console.warn("Failed to destroy PagerDuty chart", chartId, error);
      }
      delete state.charts[chartId];
    }
  });
}

function ensureOverviewOpenIncidentFilters() {
  if (!state.alerts.overviewOpenFilters) {
    state.alerts.overviewOpenFilters = {
      severity: "",
      status: "",
      query: ""
    };
  }
  return state.alerts.overviewOpenFilters;
}

function ensureResponderIncidentFilters() {
  if (!state.alerts.responderIncidentFilters) {
    state.alerts.responderIncidentFilters = {
      severity: "",
      status: "",
      query: ""
    };
  }
  return state.alerts.responderIncidentFilters;
}

function ensureAllIncidentsFilters() {
  if (!state.alerts.allIncidentsFilters) {
    state.alerts.allIncidentsFilters = {
      severity: "",
      status: "",
      query: ""
    };
  }
  return state.alerts.allIncidentsFilters;
}

function applyResponderIncidentFilters(incidents = [], filters = ensureResponderIncidentFilters()) {
  if (!Array.isArray(incidents) || incidents.length === 0) {
    return [];
  }
  const severityFilter = (filters.severity || "").toLowerCase();
  const statusFilter = (filters.status || "").toLowerCase();
  const query = (filters.query || "").trim().toLowerCase();
  return incidents.filter((incident) => {
    if (severityFilter && (incident.severity || "").toLowerCase() !== severityFilter) {
      return false;
    }
    if (statusFilter && (incident.status || "").toLowerCase() !== statusFilter) {
      return false;
    }
    if (query) {
      const haystack = [incident.title, incident.summary, incident.service]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      if (!haystack.includes(query)) {
        return false;
      }
    }
    return true;
  });
}

function buildResponderIncidentsCard(incidents = [], total = incidents.length) {
  const card = document.createElement("div");
  card.className = "card pd-responder-incidents-card";
  card.innerHTML = createTitleWithTooltip(
    "Incidents touched",
    "Incidents this responder acknowledged, was assigned, or resolved.",
    "h3"
  );

  const filters = ensureResponderIncidentFilters();
  const controls = document.createElement("div");
  controls.className = "pd-responder-filters";

  const severitySelect = document.createElement("select");
  severitySelect.className = "pd-filter-select";
  severitySelect.innerHTML = '<option value="">All severities</option>';
  const severities = Array.from(
    new Set(
      incidents
        .map((incident) => (incident.severity || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  severities.forEach((severity) => {
    const option = document.createElement("option");
    option.value = severity;
    option.textContent = severity.toUpperCase();
    if (filters.severity === severity) {
      option.selected = true;
    }
    severitySelect.appendChild(option);
  });
  severitySelect.addEventListener("change", () => {
    filters.severity = severitySelect.value;
    renderIncidents();
  });

  const statusSelect = document.createElement("select");
  statusSelect.className = "pd-filter-select";
  statusSelect.innerHTML = '<option value="">All statuses</option>';
  const statuses = Array.from(
    new Set(
      incidents
        .map((incident) => (incident.status || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  statuses.forEach((status) => {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status.toUpperCase();
    if (filters.status === status) {
      option.selected = true;
    }
    statusSelect.appendChild(option);
  });
  statusSelect.addEventListener("change", () => {
    filters.status = statusSelect.value;
    renderIncidents();
  });

  const searchInput = document.createElement("input");
  searchInput.type = "search";
  searchInput.placeholder = "Search title or service";
  searchInput.value = filters.query;
  searchInput.className = "pd-filter-search";
  searchInput.addEventListener("input", (event) => {
    filters.query = event.target.value;
    renderIncidents();
  });

  controls.appendChild(severitySelect);
  controls.appendChild(statusSelect);
  controls.appendChild(searchInput);
  card.appendChild(controls);

  const meta = document.createElement("div");
  meta.className = "pd-responders-meta";
  card.appendChild(meta);

  const list = document.createElement("div");
  list.className = "pd-incidents-list";
  card.appendChild(list);

  const emptyMessage = () => {
    list.innerHTML = '<div class="pd-responder-empty">No incidents match the selected filters.</div>';
    meta.textContent = "Showing 0 incidents";
  };

  function renderIncidents() {
    list.innerHTML = "";
    const filtered = applyResponderIncidentFilters(incidents, filters);
    if (!filtered.length) {
      emptyMessage();
      return;
    }
    const limited = filtered
      .sort((a, b) => {
        const left = a.created_at || a.updated_at || "";
        const right = b.created_at || b.updated_at || "";
        return right.localeCompare(left);
      })
      .slice(0, 50);

    meta.textContent = `Showing ${limited.length} of ${filtered.length} incidents (filtered from ${total})`;

    limited.forEach((incident) => {
      const roles = formatResponderRoles(incident.matched_roles);
      list.appendChild(createPagerDutyIncidentRow(incident, false, roles));
    });
  }

  if (!incidents.length) {
    emptyMessage();
  } else {
    renderIncidents();
  }

  return card;
}

function formatResponderRoles(roles = []) {
  if (!Array.isArray(roles) || roles.length === 0) {
    return null;
  }
  const labels = {
    resolved: "Resolved",
    acknowledged: "Acknowledged",
    assigned: "Assigned"
  };
  return roles.map((role) => labels[role] || role).join(", ");
}

async function fetchPagerDutyIncidentsForResponder(responderId, forceReload = false) {
  if (!responderId) {
    throw new Error("Responder ID missing");
  }
  state.alerts.responderIncidents = state.alerts.responderIncidents || {};
  if (!forceReload && state.alerts.responderIncidents[responderId]) {
    return state.alerts.responderIncidents[responderId];
  }
  const response = await fetch(`/api/pagerduty/incidents?responder_id=${encodeURIComponent(responderId)}&limit=500`);
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.error) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }
  state.alerts.responderIncidents[responderId] = data;
  return data;
}

async function fetchAllPagerDutyIncidents(forceReload = false) {
  if (!forceReload && state.alerts.allIncidentsData) {
    return state.alerts.allIncidentsData;
  }
  const response = await fetch("/api/pagerduty/incidents?limit=1000");
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.error) {
    const error = new Error(data.error || `HTTP ${response.status}`);
    error.status = response.status;
    throw error;
  }
  state.alerts.allIncidentsData = data;
  return data;
}

function createPagerDutyBreakdownList(items = []) {
  const list = document.createElement("ul");
  list.className = "pd-breakdown-list";
  if (!items || items.length === 0) {
    const empty = document.createElement("li");
    empty.className = "pd-breakdown-empty";
    empty.textContent = "No data";
    list.appendChild(empty);
    return list;
  }
  items.slice(0, 10).forEach((item) => {
    const li = document.createElement("li");
    li.className = "pd-breakdown-item";
    const label = item.label || item.service || item.team || "Unknown";
    const count = item.count ?? item.total ?? 0;
    const percent = item.percent != null ? `${item.percent.toFixed(1)}%` : "";
    li.innerHTML = `
      <span class="pd-breakdown-label">${label}</span>
      <span class="pd-breakdown-value">${count.toLocaleString()}</span>
      <span class="pd-breakdown-percent">${percent}</span>
    `;
    list.appendChild(li);
  });
  return list;
}

function createPagerDutySeverityStatsTable(items = [], lookbackDays = 365, totalCount = null) {
  const container = document.createElement("div");
  container.className = "pd-severity-stats";
  if (!items || items.length === 0) {
    const empty = document.createElement("div");
    empty.className = "pd-breakdown-empty";
    empty.textContent = "No data";
    container.appendChild(empty);
    return container;
  }
  const days = Math.max(1, Number(lookbackDays) || 365);
  const weeks = Math.max(1, days / 7);
  const table = document.createElement("div");
  table.className = "pd-severity-stats-table";
  items.forEach((item) => {
    const count = Number(item?.count) || 0;
    const normalized = normalizePagerDutySeverity(item?.label);
    const color = PAGERDUTY_SEVERITY_COLORS[normalized] || PAGERDUTY_SEVERITY_COLORS.unknown;
    const percentSource = item?.percent;
    const fallbackPercent = totalCount ? (count / totalCount) * 100 : null;
    const percent = percentSource != null ? percentSource : fallbackPercent;
    const perWeek = count / weeks;
    const frequencyLabel = perWeek >= 10 ? perWeek.toFixed(0) : perWeek.toFixed(1);
    const row = document.createElement("div");
    row.className = "pd-severity-row";
    row.innerHTML = `
      <div class="pd-severity-label">
        <span class="pd-severity-dot" style="background:${color}"></span>
        ${item?.label || "Unknown"}
      </div>
      <div class="pd-severity-metrics">
        <span class="pd-severity-count">${count.toLocaleString()} total</span>
        <span class="pd-severity-frequency">${frequencyLabel}/wk</span>
        ${percent != null ? `<span class="pd-severity-share">${percent.toFixed(1)}%</span>` : ""}
      </div>
    `;
    table.appendChild(row);
  });
  container.appendChild(table);
  return container;
}

function createPagerDutyServiceTable(items = []) {
  const container = document.createElement("div");
  container.className = "pd-service-table";
  if (!items || items.length === 0) {
    const empty = document.createElement("div");
    empty.className = "pd-breakdown-empty";
    empty.textContent = "No data";
    container.appendChild(empty);
    return container;
  }
  items.slice(0, 8).forEach((item) => {
    const row = document.createElement("div");
    row.className = "pd-service-row";
    row.innerHTML = `
      <div class="pd-service-name">${item.service}</div>
      <div class="pd-service-count">${item.total.toLocaleString()} incidents</div>
      <div class="pd-service-meta">${item.open} open · ${(item.percent || 0).toFixed(1)}%</div>
    `;
    container.appendChild(row);
  });
  return container;
}

function renderPagerDutyIncidents(container, overview) {
  const cardsWrapper = document.createElement("div");
  cardsWrapper.className = "alerts-breakdown-grid";

  const openIncidents = Array.isArray(overview.open_incidents) ? overview.open_incidents : [];
  const totalOpen = overview?.totals?.open ?? null;
  cardsWrapper.appendChild(buildOverviewOpenIncidentsCard(openIncidents, totalOpen));

  const recentCard = document.createElement("div");
  recentCard.className = "card";
  recentCard.innerHTML = createTitleWithTooltip(
    "Recent activity",
    "Latest incidents regardless of status.",
    "h3"
  );
  const recentList = document.createElement("div");
  recentList.className = "pd-incidents-list";
  const recentIncidents = overview.recent_incidents || [];
  if (recentIncidents.length === 0) {
    recentList.innerHTML = '<div class="pd-breakdown-empty">No incidents recorded.</div>';
  } else {
    recentIncidents.slice(0, 10).forEach((incident) => {
      recentList.appendChild(createPagerDutyIncidentRow(incident, false));
    });
  }
  recentCard.appendChild(recentList);
  tagVisualization(recentCard, "alerts-recent-incidents", { scope: "alerts" });
  cardsWrapper.appendChild(recentCard);

  container.appendChild(cardsWrapper);
}

function buildOverviewOpenIncidentsCard(openIncidents = [], totalOpen = null) {
  const card = document.createElement("div");
  card.className = "card pd-overview-open-incidents";
  card.innerHTML = createTitleWithTooltip(
    "Active incidents",
    "Open incidents pulled from the latest snapshot. Filter by severity, status, or keywords to focus the list.",
    "h3"
  );

  const filters = ensureOverviewOpenIncidentFilters();
  const controls = document.createElement("div");
  controls.className = "pd-responder-filters";

  const severitySelect = document.createElement("select");
  severitySelect.className = "pd-filter-select";
  severitySelect.innerHTML = '<option value="">All severities</option>';
  const severities = Array.from(
    new Set(
      openIncidents
        .map((incident) => (incident.severity || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  severities.forEach((severity) => {
    const option = document.createElement("option");
    option.value = severity;
    option.textContent = severity.toUpperCase();
    if (filters.severity === severity) {
      option.selected = true;
    }
    severitySelect.appendChild(option);
  });
  severitySelect.addEventListener("change", () => {
    filters.severity = severitySelect.value;
    renderIncidents();
  });

  const statusSelect = document.createElement("select");
  statusSelect.className = "pd-filter-select";
  statusSelect.innerHTML = '<option value="">All statuses</option>';
  const statuses = Array.from(
    new Set(
      openIncidents
        .map((incident) => (incident.status || "").toLowerCase())
        .filter((value) => value)
    )
  ).sort();
  statuses.forEach((status) => {
    const option = document.createElement("option");
    option.value = status;
    option.textContent = status.toUpperCase();
    if (filters.status === status) {
      option.selected = true;
    }
    statusSelect.appendChild(option);
  });
  statusSelect.addEventListener("change", () => {
    filters.status = statusSelect.value;
    renderIncidents();
  });

  const searchInput = document.createElement("input");
  searchInput.type = "search";
  searchInput.placeholder = "Search title or service";
  searchInput.value = filters.query || "";
  searchInput.className = "pd-filter-search";
  searchInput.addEventListener("input", (event) => {
    filters.query = event.target.value;
    renderIncidents();
  });

  controls.appendChild(severitySelect);
  controls.appendChild(statusSelect);
  controls.appendChild(searchInput);
  card.appendChild(controls);

  const meta = document.createElement("div");
  meta.className = "pd-responders-meta";
  card.appendChild(meta);

  const list = document.createElement("div");
  list.className = "pd-incidents-list";
  card.appendChild(list);

  const cachedCount = openIncidents.length;
  const describeTotals = () => {
    if (totalOpen != null && totalOpen > cachedCount) {
      return `${cachedCount.toLocaleString()} cached · ${totalOpen.toLocaleString()} open overall`;
    }
    if (totalOpen != null) {
      return `${cachedCount.toLocaleString()} of ${totalOpen.toLocaleString()} open overall`;
    }
    return `${cachedCount.toLocaleString()} tracked`;
  };

  const renderEmpty = (message) => {
    list.innerHTML = `<div class="pd-breakdown-empty">${message}</div>`;
    meta.textContent = `Showing 0 incidents (${describeTotals()})`;
  };

  function renderIncidents() {
    if (!openIncidents.length) {
      renderEmpty("No active incidents 🎉");
      return;
    }
    const filtered = applyResponderIncidentFilters(openIncidents, filters);
    if (!filtered.length) {
      renderEmpty("No open incidents match the selected filters.");
      return;
    }
    const limited = filtered
      .slice()
      .sort((a, b) => {
        const left = a.created_at || a.updated_at || "";
        const right = b.created_at || b.updated_at || "";
        return right.localeCompare(left);
      })
      .slice(0, 50);
    list.innerHTML = "";
    limited.forEach((incident) => {
      list.appendChild(createPagerDutyIncidentRow(incident, true));
    });
    meta.textContent = `Showing ${limited.length} of ${filtered.length} incidents (${describeTotals()})`;
  }

  renderIncidents();
  tagVisualization(card, "alerts-open-incidents-list", { scope: "alerts" });
  return card;
}

function createPagerDutyIncidentRow(incident, highlightOpen = false, roleSummary = null) {
  const row = document.createElement("div");
  row.className = "pd-incident-row";
  if (highlightOpen && (incident.status || "").toLowerCase() !== "resolved") {
    row.classList.add("pd-incident-open");
  }
  const title = incident.title || `Incident #${incident.number}`;
  const service = incident.service || "Unassigned";
  const severity = incident.severity || "--";
  const status = (incident.status || "").toUpperCase();
  const duration = formatDurationMinutes(incident.duration_minutes);
  const created = incident.created_at ? formatDateTime(incident.created_at) : "--";
  const resolved = incident.resolved_at ? formatDateTime(incident.resolved_at) : null;
  const link = incident.html_url ? `<a href="${incident.html_url}" target="_blank" rel="noopener">View</a>` : "";
  const roleChip = roleSummary ? `<span class="pd-role-chip">${roleSummary}</span>` : "";
  row.innerHTML = `
    <div class="pd-incident-header">
      <div class="pd-incident-title">${title}</div>
      <div class="pd-incident-meta">${service} • Severity: ${severity}</div>
    </div>
    <div class="pd-incident-footer">
      <span class="pd-status-pill">${status}</span>
      ${roleChip}
      <span class="pd-incident-time">Opened: ${created}${resolved ? ` · Resolved: ${resolved}` : ""}</span>
      <span class="pd-incident-duration">${duration}</span>
      ${link}
    </div>
  `;
  return row;
}

// --------------------------
// Chart data helpers
// --------------------------

function getLanguageStats(summary) {
  const langs = summary.languages || {};
  
  // Define languages we consider "real programming languages"
  const realLanguages = new Set([
    // Major programming languages
    'JavaScript', 'TypeScript', 'Python', 'Java', 'C#', 'C++', 'C', 
    'Go', 'Rust', 'Swift', 'Kotlin', 'PHP', 'Ruby', 'Scala', 'Dart',
    'Objective-C', 'R', 'MATLAB', 'Perl', 'Haskell', 'Clojure', 'F#',
    'Elixir', 'Erlang', 'Lua', 'Julia', 'Assembly', 'Groovy',
    'Vim Script', 'Vim script', 'Emacs Lisp', 'OCaml', 'Scheme', 'Common Lisp', 
    'Forth', 'Ada', 'Fortran', 'COBOL', 'Pascal', 'D', 'Nim', 
    'Crystal', 'Zig', 'V', 'Odin', 'Raku', 'Awk',
    // Shell/Scripting languages (programming)
    'Shell', 'Bash', 'Bourne Again Shell', 'Bourne Shell',
    'PowerShell', 'Zsh', 'Fish', 'Tcl',
    // SQL variants (programming)
    'SQL', 'PLpgSQL', 'PL/SQL', 'T-SQL', 'PostgreSQL',
    // Other functional/config programming
    'Nix', 'Dhall', 'HCL', 'Jsonnet', 'CUE',
    // Assembly variants
    'x86 Assembly', 'ARM Assembly', 'MIPS Assembly',
    // Classic languages
    'BASIC', 'Visual Basic', 'VBScript', 'Delphi', 'ActionScript',
    // Modern systems languages
    'WebAssembly', 'WASM'
  ]);

  // Languages to explicitly exclude (data/markup/config formats)
  const excludeLanguages = new Set([
    'HTML', 'CSS', 'SCSS', 'Sass', 'Less',
    'JSON', 'YAML', 'XML', 'TOML', 'INI',
    'Markdown', 'reStructuredText', 'AsciiDoc', 'LaTeX', 'TeX',
    'CSV', 'TSV', 'Properties', 'Dockerfile', 'Makefile',
    'Text', 'Binary', 'Data', 'Image', 'Video', 'Audio',
    'Protocol Buffer', 'Thrift', 'Avro', 'GraphQL',
    'Mustache', 'Handlebars', 'Jinja', 'Smarty',
    'SVG', 'PostScript', 'Rich Text Format', 'Unknown'
  ]);

  const labels = [];
  const values = [];
  
  for (const [lang, stats] of Object.entries(langs)) {
    // Include if it's explicitly in real languages, exclude if it's in exclude list
    const shouldInclude = realLanguages.has(lang) && !excludeLanguages.has(lang);
    
    if (shouldInclude) {
      // Handle both formats: object with additions/deletions or just a number
      let lineCount;
      if (typeof stats === 'object' && stats !== null) {
        const added = stats.additions || 0;
        const deleted = stats.deletions || 0;
        lineCount = added + deleted;
      } else {
        lineCount = stats || 0;
      }
      
      if (lineCount > 0) {
        labels.push(lang);
        values.push(lineCount);
      }
    }
  }
  
  return { labels, values };
}

function getSubsystemLanguageStats(languageData) {
  const langs = languageData.languages || {};
  
  // Define languages we consider "real programming languages" (same as above)
  const realLanguages = new Set([
    'JavaScript', 'TypeScript', 'Python', 'Java', 'C#', 'C++', 'C', 
    'Go', 'Rust', 'Swift', 'Kotlin', 'PHP', 'Ruby', 'Scala', 'Dart',
    'Objective-C', 'R', 'MATLAB', 'Perl', 'Haskell', 'Clojure', 'F#',
    'Elixir', 'Erlang', 'Lua', 'Julia', 'Assembly', 'Groovy',
    'Vim Script', 'Vim script', 'Emacs Lisp', 'OCaml', 'Scheme', 'Common Lisp', 
    'Forth', 'Ada', 'Fortran', 'COBOL', 'Pascal', 'D', 'Nim', 
    'Crystal', 'Zig', 'V', 'Odin', 'Raku', 'Awk',
    'Shell', 'Bash', 'Bourne Again Shell', 'Bourne Shell',
    'PowerShell', 'Zsh', 'Fish', 'Tcl',
    'SQL', 'PLpgSQL', 'PL/SQL', 'T-SQL', 'PostgreSQL',
    'Nix', 'Dhall', 'HCL', 'Jsonnet', 'CUE',
    'x86 Assembly', 'ARM Assembly', 'MIPS Assembly',
    'BASIC', 'Visual Basic', 'VBScript', 'Delphi', 'ActionScript',
    'WebAssembly', 'WASM',
    'JSX', 'TSX'  // Add JSX and TSX for React/frontend projects
  ]);

  const labels = [];
  const values = [];
  let othersTotal = 0;
  
  for (const [lang, stats] of Object.entries(langs)) {
    if (stats.code_lines > 0) {
      const isRealLanguage = realLanguages.has(lang);
      
      if (isRealLanguage) {
        labels.push(lang);
        values.push(stats.code_lines);
      } else {
        // Add to "Others" category
        othersTotal += stats.code_lines;
      }
    }
  }
  
  // Add "Others" category if there are any non-programming languages
  if (othersTotal > 0) {
    labels.push('Others');
    values.push(othersTotal);
  }
  
  return { labels, values };
}

function getWeekdayStats(summary) {
  const weekdays = summary.per_weekday || {};
  const dayNames = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"];
  const labels = [];
  const values = [];
  for (let i = 0; i < 7; i++) {
    const dayName = dayNames[i];
    labels.push(dayName);
    const dayData = weekdays[dayName] || {};
    values.push(dayData.commits || 0);
  }
  return { labels, values };
}

function getHourStats(summary) {
  const hours = summary.per_hour || {};
  const labels = [];
  const values = [];
  for (let h = 0; h < 24; h++) {
    labels.push(h.toString().padStart(2, "0") + ":00");
    const hourData = hours[h.toString()] || {};
    values.push(hourData.commits || 0);
  }
  return { labels, values };
}

async function createMonthlyChart(containerId, user, year, isTeam = false) {
  try {
    const apiUrl = isTeam 
      ? `/api/teams/${encodeURIComponent(user)}/monthly-stats/${year}`
      : `/api/users/${encodeURIComponent(user)}/monthly-stats/${year}`;
      
    const response = await fetchJSON(apiUrl);
    const monthlyStats = response.monthly_stats || [];
    
    if (monthlyStats.length === 0) {
      return;
    }
    
    const labels = monthlyStats.map(stat => stat.month_name);
    const addedData = monthlyStats.map(stat => stat.lines_added);
    const deletedData = monthlyStats.map(stat => stat.lines_deleted);
    
    const canvas = document.getElementById(containerId);
    if (!canvas) {
      console.error("Canvas not found:", containerId);
      return;
    }
    
    // Destroy existing chart if it exists (multiple cleanup strategies)
    if (state.charts[containerId]) {
      state.charts[containerId].destroy();
      delete state.charts[containerId];
    }
    
    // Additional cleanup: destroy any chart instance associated with this canvas
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
      existingChart.destroy();
    }
    
    const ctx = canvas.getContext("2d");
    
    const chart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Lines Added",
            data: addedData,
            backgroundColor: "rgba(46, 125, 50, 0.7)",
            borderColor: "rgba(46, 125, 50, 1)",
            borderWidth: 1
          },
          {
            label: "Lines Deleted", 
            data: deletedData,
            backgroundColor: "rgba(198, 40, 40, 0.7)",
            borderColor: "rgba(198, 40, 40, 1)",
            borderWidth: 1
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `Monthly Lines Added/Deleted - ${year}`
          },
          legend: {
            display: true,
            position: 'top'
          }
        },
        scales: {
          x: {
            title: {
              display: true,
              text: 'Month'
            }
          },
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Lines of Code'
            }
          }
        },
        interaction: {
          mode: 'index',
          intersect: false
        }
      }
    });
    
    // Store chart reference for cleanup
    state.charts[containerId] = chart;
    
  } catch (error) {
    console.error("Error creating monthly chart:", error);
    
    // Show error message in the container
    const container = document.getElementById(containerId);
    if (container && container.parentElement) {
      const errorDiv = document.createElement("div");
      errorDiv.className = "error";
      errorDiv.textContent = "Failed to load monthly statistics: " + error.message;
      container.parentElement.appendChild(errorDiv);
    }
  }
}

async function createDailyChart(containerId, user, year, month, isTeam = false) {
  try {
    const apiUrl = isTeam 
      ? `/api/teams/${encodeURIComponent(user)}/daily-stats/${year}/${month}`
      : `/api/users/${encodeURIComponent(user)}/daily-stats/${year}/${month}`;
      
    console.log(`Fetching daily stats from: ${apiUrl}`);
    const response = await fetchJSON(apiUrl);
    const dailyStats = response.daily_stats || [];
    
    console.log(`Daily stats for ${user} (${year}-${month}):`, dailyStats.length, 'days of data');
    
    if (dailyStats.length === 0) {
      console.log(`No daily stats found for ${user} in ${year}-${month}`);
      // Instead of silently failing, show a message
      const canvas = document.getElementById(containerId);
      if (canvas && canvas.parentElement) {
        const messageDiv = document.createElement("div");
        messageDiv.className = "no-data-message";
        messageDiv.style.padding = "20px";
        messageDiv.style.textAlign = "center";
        messageDiv.style.color = "#6B7280";
        messageDiv.innerHTML = `<p>No daily activity data available for ${new Date(year, month-1).toLocaleString('default', { month: 'long', year: 'numeric' })}.</p>`;
        canvas.parentElement.replaceChild(messageDiv, canvas);
      }
      return;
    }
    
    const labels = dailyStats.map(stat => stat.day.toString());
    const addedData = dailyStats.map(stat => stat.lines_added);
    const deletedData = dailyStats.map(stat => stat.lines_deleted);
    
    const canvas = document.getElementById(containerId);
    if (!canvas) {
      console.error("Canvas not found:", containerId);
      return;
    }
    
    // Destroy existing chart if it exists (multiple cleanup strategies)
    if (state.charts[containerId]) {
      state.charts[containerId].destroy();
      delete state.charts[containerId];
    }
    
    // Additional cleanup: destroy any chart instance associated with this canvas
    const existingChart = Chart.getChart(canvas);
    if (existingChart) {
      existingChart.destroy();
    }
    
    const ctx = canvas.getContext("2d");
    
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                       'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const monthName = monthNames[month - 1];
    
    const chart = new Chart(ctx, {
      type: "bar",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Lines Added",
            data: addedData,
            backgroundColor: "rgba(46, 125, 50, 0.7)",
            borderColor: "rgba(46, 125, 50, 1)",
            borderWidth: 1
          },
          {
            label: "Lines Deleted",
            data: deletedData,
            backgroundColor: "rgba(198, 40, 40, 0.7)",
            borderColor: "rgba(198, 40, 40, 1)",
            borderWidth: 1
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          title: {
            display: true,
            text: `Daily Lines Added/Deleted - ${monthName} ${year}`
          },
          legend: {
            display: true,
            position: 'top'
          },
          tooltip: {
            callbacks: {
              title: function(tooltipItems) {
                const dayNum = tooltipItems[0].label;
                return `${monthName} ${dayNum}, ${year}`;
              },
              footer: function(tooltipItems) {
                const addedLines = tooltipItems.find(item => item.datasetIndex === 0)?.raw || 0;
                const deletedLines = tooltipItems.find(item => item.datasetIndex === 1)?.raw || 0;
                const netLines = addedLines - deletedLines;
                return `Net: ${netLines >= 0 ? '+' : ''}${netLines} lines`;
              }
            }
          }
        },
        scales: {
          x: {
            title: {
              display: true,
              text: 'Day of Month'
            }
          },
          y: {
            beginAtZero: true,
            title: {
              display: true,
              text: 'Lines of Code'
            }
          }
        },
        interaction: {
          mode: 'index',
          intersect: false
        }
      }
    });
    
    // Store chart reference for cleanup
    state.charts[containerId] = chart;
    
  } catch (error) {
    console.error("Error creating daily chart:", error);
    
    // Show error message in the container
    const container = document.getElementById(containerId);
    if (container && container.parentElement) {
      const errorDiv = document.createElement("div");
      errorDiv.className = "error";
      errorDiv.textContent = "Failed to load daily statistics: " + error.message;
      container.parentElement.appendChild(errorDiv);
    }
  }
}

async function createSelectedMonthStatsCard(user, month, summary, isTeam = false) {
  try {
    console.log("Creating selected month stats for", user, "month:", month.label, "isTeam:", isTeam);

    // Use the data from the summary object instead of making additional API calls
    const stats = {
      commits: summary.total_commits || 0,
      lines_added: isTeam ? (summary.total_additions || 0) : (summary.total_lines_added || 0),
      lines_deleted: isTeam ? (summary.total_deletions || 0) : (summary.total_lines_deleted || 0),
      month_name: month.label
    };

    const card = document.createElement("div");
    card.className = "card";
    
    card.innerHTML = `
      ${createTitleWithTooltip(
        `📊 ${month.label} Statistics`, 
        `Summary of activity for ${month.label}. Includes commits made, lines added/deleted, and total changes during the selected month.`,
        "h2"
      )}
      <div class="stats-grid">
        <div class="stat-item">
          <div class="stat-value">${stats.commits.toLocaleString()}</div>
          <div class="stat-label">Commits</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">+${stats.lines_added.toLocaleString()}</div>
          <div class="stat-label">Lines Added</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">-${stats.lines_deleted.toLocaleString()}</div>
          <div class="stat-label">Lines Deleted</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">${(stats.lines_added + stats.lines_deleted).toLocaleString()}</div>
          <div class="stat-label">Total Changes</div>
        </div>
      </div>
    `;
    
    return card;
    
  } catch (error) {
    console.error("Error creating selected month stats card:", error);
    
    const errorCard = document.createElement("div");
    errorCard.className = "card error-card";
    errorCard.innerHTML = createTitleWithTooltip(
      `📊 ${month.label} Statistics`, 
      `Summary of activity for ${month.label}. Includes commits made, lines added/deleted, and total changes.`,
      "h2"
    ) + '<div class="error-message">Failed to load selected month statistics: ' + error.message + '</div>';
    return errorCard;
  }
}

async function createLastMonthStatsCard(user, isTeam = false) {
  try {
    console.log("Creating last month stats for", user, "isTeam:", isTeam);

    const url = isTeam
      ? `/api/teams/${encodeURIComponent(user)}/last-month-stats`
      : `/api/users/${encodeURIComponent(user)}/last-month-stats`;
    
    const response = await fetchJSON(url);
    const stats = response.last_month_stats;
    
    if (!stats) {
      console.log("No last month data available for", user);
      return null;
    }

    const card = document.createElement("div");
    card.className = "card";
    
    card.innerHTML = `
      ${createTitleWithTooltip(
        `📊 Last Month Statistics (${stats.month_name})`, 
        "Summary of activity from the previous month. Includes commits made, lines added/deleted, and total changes. Useful for understanding recent activity patterns.",
        "h2"
      )}
      <div class="stats-grid">
        <div class="stat-item">
          <div class="stat-value">${stats.commits.toLocaleString()}</div>
          <div class="stat-label">Commits</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">+${stats.lines_added.toLocaleString()}</div>
          <div class="stat-label">Lines Added</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">-${stats.lines_deleted.toLocaleString()}</div>
          <div class="stat-label">Lines Deleted</div>
        </div>
        <div class="stat-item">
          <div class="stat-value">${(stats.lines_added + stats.lines_deleted).toLocaleString()}</div>
          <div class="stat-label">Total Changes</div>
        </div>
      </div>
    `;
    
    return card;
  } catch (error) {
    console.error("Error creating last month stats card:", error);
    
    const errorCard = document.createElement("div");
    errorCard.className = "card error-card";
    errorCard.innerHTML = createTitleWithTooltip(
      "📊 Last Month Statistics", 
      "Summary of activity from the previous month. Includes commits made, lines added/deleted, and total changes.",
      "h2"
    ) + '<div class="error-message">Failed to load last month statistics: ' + error.message + '</div>';
    return errorCard;
  }
}

// --------------------------
// Dashboard rendering
// --------------------------

async function renderUserDashboard(user, month, summary, renderToken = null) {
  if (!isActiveUserRender(renderToken)) {
    return;
  }
  clearMain();

  const periodType = month.is_yearly ? "Yearly" : "Monthly";
  const periodLabel = month.is_yearly ? month.label : month.label + " (" + summary.from + " → " + summary.to + ")";

  setViewHeader(
    "User: " + (summary.author_name || user.slug),
    periodLabel,
    "User · " + periodType
  );

  const main = $("main-content");

  const vizContext = {
    scope: "user",
    entityId: user.slug,
    entityLabel: summary.author_name || user.slug,
    period: month,
    periodKey: buildPeriodKey(month),
    periodLabel
  };

  let subsystemTimelineAnchor = null;
  if (month.is_yearly) {
    subsystemTimelineAnchor = document.createElement("div");
    subsystemTimelineAnchor.className = "async-card-anchor";
  }

  // KPIs
  const kpiContainer = document.createElement("div");
  kpiContainer.className = "kpi-grid";

  const kpiRankings = summary.peer_rankings || {};
  const kpis = [
    { label: "Total commits", value: summary.total_commits || 0, metric: "total_commits" },
    { label: "Lines added", value: summary.total_lines_added || 0, metric: "total_lines_added" },
    { label: "Lines deleted", value: summary.total_lines_deleted || 0, metric: "total_lines_deleted" },
    {
      label: "Net lines",
      value: summary.net_lines || 0,
      metric: "net_lines"
    }
  ];

  kpis.forEach((k) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    const displayValue = typeof k.value === "number" ? k.value.toLocaleString() : k.value;
    const rankText = formatRankSummary(kpiRankings[k.metric]);
    card.innerHTML = `
      <div class="kpi-label">${k.label}</div>
      <div class="kpi-value">${displayValue}</div>
      ${rankText ? `<div class="kpi-rank">${rankText}</div>` : ""}
    `;
    kpiContainer.appendChild(card);
  });

  main.appendChild(kpiContainer);
  tagVisualization(kpiContainer, "user-kpis", vizContext);

  // Show monthly view info card
  if (!month.is_yearly) {
    const monthlyInfoCard = document.createElement("div");
    monthlyInfoCard.className = "card info-card";
    monthlyInfoCard.innerHTML = `
      <h2>📅 Monthly User View</h2>
      <p>Viewing data for <strong>${month.label}</strong> only.</p>
      <p>For comprehensive user statistics including achievement badges, ownership analysis, and yearly trends, please select a yearly view.</p>
    `;
    main.appendChild(monthlyInfoCard);
  }

  // Team Membership (show which teams this user belongs to)
  renderUserTeamMembership(user.slug, main);

  // Load and render badges (only for yearly view)
  if (month.is_yearly) {
    loadUserBadges(user.slug).then(badges => {
      if (!isActiveUserRender(renderToken)) {
        return;
      }
      try {
        console.log("Rendering badges for user", user.slug, ":", badges?.length || 0, "badges");
        if (badges && badges.length > 0) {
          renderUserBadges(badges, main);
        } else {
          console.log("No badges to render for user", user.slug);
        }
      } catch (error) {
        console.error("Error rendering user badges:", error);
        if (!isActiveUserRender(renderToken)) {
          return;
        }
        const errorDiv = document.createElement("div");
        errorDiv.className = "error";
        errorDiv.textContent = "Error loading badges: " + error.message;
        main.appendChild(errorDiv);
      }
    }).catch(error => {
      if (!isActiveUserRender(renderToken)) {
        return;
      }
      console.error("Error loading user badges:", error);
    });
    
    // Load and render ownership timelines for subsystems where user is top maintainer
    loadUserOwnershipTimeline(user.slug).then(timelines => {
      if (!isActiveUserRender(renderToken)) {
        return;
      }
      try {
        if (timelines && Object.keys(timelines).length > 0) {
          renderUserOwnershipTimelines(user.slug, timelines, main);
        }
      } catch (error) {
        console.error("Error rendering ownership timelines:", error);
      }
    }).catch(error => {
      if (!isActiveUserRender(renderToken)) {
        return;
      }
      console.error("Error loading ownership timelines:", error);
    });
  }

  if (month.is_yearly && summary.developer_capacity_profile && summary.developer_capacity_profile.developer_equivalent >= 0.9) {
    const capacityProfile = summary.developer_capacity_profile;
    const capacityCard = document.createElement("div");
    capacityCard.className = "card";
    capacityCard.innerHTML = createTitleWithTooltip(
      "🧠 Ownership Capacity (Blame)",
      "Git blame ownership converted into theoretical developer equivalents using the same language thresholds as team capacity. Uses the developer's share of each subsystem's languages.",
      "h2"
    );

    const capacityHeader = document.createElement("div");
    capacityHeader.style.display = "flex";
    capacityHeader.style.justifyContent = "space-between";
    capacityHeader.style.alignItems = "center";
    capacityHeader.style.marginBottom = "12px";

    const headcount = document.createElement("div");
    headcount.style.fontSize = "2rem";
    headcount.style.fontWeight = "700";
    headcount.style.color = "var(--accent-blue)";
    headcount.textContent = `${capacityProfile.developer_equivalent.toFixed(1)} devs`;

    const linesOwned = document.createElement("div");
    linesOwned.style.fontFamily = "monospace";
    linesOwned.style.color = "var(--text-secondary)";
    linesOwned.textContent = `${(capacityProfile.total_lines || 0).toLocaleString()} lines owned`;

    capacityHeader.appendChild(headcount);
    capacityHeader.appendChild(linesOwned);
    capacityCard.appendChild(capacityHeader);

    const languageList = document.createElement("div");
    languageList.style.display = "flex";
    languageList.style.flexWrap = "wrap";
    languageList.style.gap = "10px";

    const languageEntries = Object.entries(capacityProfile.language_breakdown || {})
      .sort((a, b) => (b[1].lines || 0) - (a[1].lines || 0));

    const maxLanguagePills = 8;
    languageEntries.slice(0, maxLanguagePills).forEach(([lang, data]) => {
      const pill = document.createElement("div");
      pill.style.border = "1px solid var(--border)";
      pill.style.borderRadius = "999px";
      pill.style.padding = "6px 12px";
      pill.style.backgroundColor = "var(--background-secondary)";
      pill.style.fontSize = "0.9em";
      pill.innerHTML = `<strong>${lang}</strong>: ${data.lines.toLocaleString()} lines → ${data.theoretical_devs.toFixed(1)} devs`;
      languageList.appendChild(pill);
    });

    if (languageEntries.length > maxLanguagePills) {
      const remaining = languageEntries.length - maxLanguagePills;
      const morePill = document.createElement("div");
      morePill.style.border = "1px dashed var(--border)";
      morePill.style.borderRadius = "999px";
      morePill.style.padding = "6px 12px";
      morePill.style.fontSize = "0.9em";
      morePill.textContent = `+${remaining} more languages`;
      languageList.appendChild(morePill);
    }

    const thresholdNote = document.createElement("div");
    thresholdNote.className = "note-text";
    thresholdNote.style.marginTop = "10px";
    thresholdNote.style.color = "var(--text-secondary)";
    thresholdNote.textContent = "Shown only when the theoretical ownership exceeds 0.9 developers.";

    capacityCard.appendChild(languageList);
    capacityCard.appendChild(thresholdNote);
    main.appendChild(capacityCard);
  }

  // Per-repo breakdown
  const repos = summary.per_repo || {};
  if (Object.keys(repos).length > 0) {
    const repoBox = document.createElement("div");
    repoBox.className = "card";
    repoBox.innerHTML = '<h2>Repos / Services <span class="clickable-text">(click to view repo stats)</span></h2>';

    const repoList = document.createElement("ul");
    repoList.className = "link-list";

    // Sort repos by commit count
    const sortedRepos = Object.entries(repos).sort((a, b) => (b[1].commits || 0) - (a[1].commits || 0));

    sortedRepos.forEach(([repoName, repoData]) => {
      const li = document.createElement("li");
      li.className = "link-list-item clickable-item";
      li.textContent = repoName + ": " + (repoData.commits || 0) + " commits, " + ((repoData.additions || 0) - (repoData.deletions || 0)) + " net lines";
      
      li.onclick = () => {
        console.log('Repo clicked:', repoName, 'with current period:', month);
        console.log('Available subsystems:', state.subsystems.map(s => s.name));
        
        // Try to find a matching subsystem more intelligently
        let match = findSubsystemByRepoName(repoName);
        if (!match) {
          // Try to match just the repo name without organization prefix
          const shortName = repoName.split('/').pop();
          console.log('Trying short name:', shortName);
          match = findSubsystemByRepoName(shortName);
        }
        
        if (match) {
          console.log('Found matching subsystem:', match.name);
          navigateToSubsystem(match.name, month);
        } else {
          console.warn('No matching subsystem found for:', repoName);
          // Show all available options to user
          alert('Could not find subsystem for "' + repoName + '".\nAvailable subsystems: ' + state.subsystems.map(s => s.name).sort().join(', '));
        }
      };
      
      repoList.appendChild(li);
    });

    repoBox.appendChild(repoList);
    main.appendChild(repoBox);
  }

  if (subsystemTimelineAnchor) {
    main.appendChild(subsystemTimelineAnchor);
  }

  if (month.is_yearly && subsystemTimelineAnchor) {
    const timelineYear = parseInt(month.label, 10);
    loadUserSubsystemActivity(user.slug, timelineYear)
      .then(activity => {
        if (!isActiveUserRender(renderToken)) {
          return;
        }
        try {
          renderUserSubsystemTimeline(user.slug, activity, subsystemTimelineAnchor);
        } catch (error) {
          console.error("Error rendering user subsystem timeline:", error);
          if (subsystemTimelineAnchor.parentElement) {
            subsystemTimelineAnchor.remove();
          }
        }
      })
      .catch(error => {
        if (!isActiveUserRender(renderToken)) {
          return;
        }
        console.error("Failed to load user subsystem timeline:", error);
        if (subsystemTimelineAnchor.parentElement) {
          subsystemTimelineAnchor.remove();
        }
      });
  }

  // Add contribution heatmap (build data even if summary.per_date is missing)
  try {
    const heatmapCard = document.createElement("div");
    heatmapCard.className = "card";
    heatmapCard.innerHTML = createTitleWithTooltip(
      "Contribution activity", 
      "GitHub-style contribution heatmap showing daily commit activity for the selected time period. For monthly views, shows only the selected month's commits across the full year layout. For yearly views, shows the full year. Darker green indicates more commits on that day.",
      "h2"
    );
    
    const heatmapContainer = document.createElement("div");
    heatmapContainer.className = "contribution-heatmap";
    
    // Show contribution activity for the selected time period
    let heatmapData = {};
    let heatmapFromDate, heatmapToDate;
    
    if (month.is_yearly) {
      // Fetch aggregated daily stats for the year to ensure complete data
      const year = month.label;
      try {
        const resp = await fetchJSON(`/api/users/${encodeURIComponent(user.slug)}/daily-stats/${year}`);
        if (!isActiveUserRender(renderToken)) {
          return;
        }
        const dailyStats = resp.daily_stats || [];
        dailyStats.forEach(ds => {
          heatmapData[ds.date] = {
            additions: ds.lines_added || 0,
            deletions: ds.lines_deleted || 0,
            commits: ds.commits || 0
          };
        });
        heatmapFromDate = `${year}-01-01`;
        heatmapToDate = `${year}-12-31`;
        console.log("Using aggregated yearly daily stats for heatmap:", dailyStats.length, "days");
      } catch (e) {
        console.warn("Yearly daily-stats fetch failed, falling back to summary per_date:", e);
        heatmapData = summary.per_date || {};
        heatmapFromDate = summary.from || month.from;
        heatmapToDate = summary.to || month.to;
      }
    } else {
      // For monthly view, show only selected month's data but display full year layout
      const monthStart = summary.from;
      const monthEnd = summary.to;
      const year = monthStart.split('-')[0];
      
      // Only include commits from the selected month, but prepare for full year display
      heatmapData = {};
      if (summary.per_date) {
        for (const [date, data] of Object.entries(summary.per_date)) {
          if (date >= monthStart && date <= monthEnd) {
            heatmapData[date] = {
              ...data,
              isHighlighted: true
            };
          }
        }
      }
      heatmapFromDate = `${year}-01-01`;
      heatmapToDate = `${year}-12-31`;
      console.log(`Using selected month data only (${monthStart} to ${monthEnd}):`, Object.keys(heatmapData).length, "days");
    }
    
    console.log("Creating heatmap for period:", heatmapFromDate, "to", heatmapToDate, "with", Object.keys(heatmapData).length, "data points");
    const heatmapElement = createContributionHeatmap(heatmapData, heatmapFromDate, heatmapToDate);
    heatmapContainer.appendChild(heatmapElement);
    
    heatmapCard.appendChild(heatmapContainer);

    const commitsPerWeekRank = kpiRankings.commits_per_week;
    const commitsPerWeekValue = commitsPerWeekRank && typeof commitsPerWeekRank.value === "number"
      ? commitsPerWeekRank.value
      : computeCommitsPerWeek(summary);
    if (Number.isFinite(commitsPerWeekValue)) {
      const frequencyBlock = document.createElement("div");
      frequencyBlock.className = "contribution-frequency";
      const rankText = formatRankSummary(commitsPerWeekRank);
      frequencyBlock.innerHTML = `
        <span class="frequency-label">Average commits per week</span>
        <strong>${commitsPerWeekValue.toFixed(1)}</strong>
        ${rankText ? `<span class="frequency-rank">${rankText}</span>` : ""}
      `;
      frequencyBlock.title = "Total commits divided by the number of weeks in this period.";
      heatmapCard.appendChild(frequencyBlock);
    }

    if (!isActiveUserRender(renderToken)) {
      return;
    }

    main.appendChild(heatmapCard);
  } catch (error) {
    console.error("Error creating contribution heatmap:", error);
    // Don't add the heatmap if there's an error
  }

  // Monthly Lines Chart (only for yearly view)
  if (month.is_yearly) {
    const monthlyChartCard = document.createElement("div");
    monthlyChartCard.className = "card";
    monthlyChartCard.innerHTML = createTitleWithTooltip(
    "Monthly Lines Added/Deleted", 
    "Shows the number of lines added (green) and deleted (red) by this developer each month throughout the year. Net changes indicate overall code contribution growth.",
    "h2"
  ) + '<div style="height: 300px;"><canvas id="chart-monthly"></canvas></div>';
    main.appendChild(monthlyChartCard);
    tagVisualization(monthlyChartCard, "user-monthly-lines", vizContext);
    
    // Create the monthly chart asynchronously
    const year = parseInt(month.label);
    const chartRenderToken = renderToken;
    setTimeout(() => {
      if (!isActiveUserRender(chartRenderToken)) {
        return;
      }
      createMonthlyChart("chart-monthly", user.slug, year, false);
    }, 100);
  }

  // Monthly Statistics Card (different behavior for monthly vs yearly view)
  if (month.is_yearly) {
    // For yearly view: show last month statistics
    const lastMonthCard = await createLastMonthStatsCard(user.slug, false);
    if (isActiveUserRender(renderToken) && lastMonthCard) {
      main.appendChild(lastMonthCard);
    }
  } else {
    // For monthly view: show selected month statistics using summary data
    const selectedMonthCard = await createSelectedMonthStatsCard(user.slug, month, summary, false);
    if (isActiveUserRender(renderToken) && selectedMonthCard) {
      main.appendChild(selectedMonthCard);
    }
  }

  // Daily Activity Chart
  const dailyChartCard = document.createElement("div");
  dailyChartCard.className = "card";
  
  let chartTitle, chartTooltip, chartYear, chartMonth;
  
  if (month.is_yearly) {
    // For yearly view: show current month's activity, but fall back to most recent month with data
    const now = new Date();
    chartYear = now.getFullYear();
    chartMonth = now.getMonth() + 1;
    
    // If current month has no data, try to find the most recent month with data
    const availableMonths = (state.selectedUser?.months || [])
      .filter(m => !m.is_yearly && m.from.startsWith(chartYear.toString()))
      .sort((a, b) => b.from.localeCompare(a.from));
    
    if (availableMonths.length > 0) {
      const mostRecent = availableMonths[0];
      const recentDate = new Date(mostRecent.from);
      const recentYear = recentDate.getFullYear();
      const recentMonth = recentDate.getMonth() + 1;
      
      // If we're looking at current month but it's early (less than 5 days) and there's recent data
      if (now.getDate() < 5 && (recentYear !== chartYear || recentMonth !== chartMonth)) {
        chartYear = recentYear;
        chartMonth = recentMonth;
        chartTitle = `📈 Recent Month Daily Activity (${mostRecent.label})`;
        chartTooltip = `Daily breakdown of lines added (green) and deleted (red) for ${mostRecent.label}, showing the most recent month with activity data.`;
      } else {
        chartTitle = "📈 Current Month Daily Activity";
        chartTooltip = "Daily breakdown of lines added (green) and deleted (red) for the current month. Shows day-to-day coding activity and helps identify productive periods and work patterns.";
      }
    } else {
      chartTitle = "📈 Current Month Daily Activity";
      chartTooltip = "Daily breakdown of lines added (green) and deleted (red) for the current month. Shows day-to-day coding activity and helps identify productive periods and work patterns.";
    }
  } else {
    // For monthly view: show the selected month's activity
    const periodStart = summary.from;
    const selectedDate = new Date(periodStart);
    chartYear = selectedDate.getFullYear();
    chartMonth = selectedDate.getMonth() + 1;
    chartTitle = `📈 ${month.label} Daily Activity`;
    chartTooltip = `Daily breakdown of lines added (green) and deleted (red) for ${month.label}. Shows day-to-day coding activity and productivity patterns during the selected month.`;
  }
  
  dailyChartCard.innerHTML = createTitleWithTooltip(
    chartTitle, 
    chartTooltip,
    "h2"
  ) + `
    <div style="height: 300px;">
      <canvas id="chart-daily-activity"></canvas>
    </div>
  `;
  main.appendChild(dailyChartCard);
  
  // Create the daily chart asynchronously
  const dailyChartRenderToken = renderToken;
  setTimeout(() => {
    if (!isActiveUserRender(dailyChartRenderToken)) {
      return;
    }
    createDailyChart("chart-daily-activity", user.slug, chartYear, chartMonth, false);
  }, 100);

  // Chart containers
  const chartRow = document.createElement("div");
  chartRow.className = "chart-grid";

  // Languages
  const langCard = document.createElement("div");
  langCard.className = "card";
  langCard.innerHTML = createTitleWithTooltip(
    "Lines changed per language", 
    "Shows the total lines added and deleted by this developer for each programming language. Calculated by analyzing file extensions and content of all commits.",
    "h2"
  ) + '<canvas id="chart-languages"></canvas>';
  chartRow.appendChild(langCard);

  // Weekday
  const weekdayCard = document.createElement("div");
  weekdayCard.className = "card";
  weekdayCard.innerHTML = createTitleWithTooltip(
    "Commits by weekday", 
    "Distribution of commits across days of the week. Shows developer's work patterns and preferred coding days.",
    "h2"
  ) + '<canvas id="chart-weekday"></canvas>';
  chartRow.appendChild(weekdayCard);

  // Hour
  const hourCard = document.createElement("div");
  hourCard.className = "card";
  hourCard.innerHTML = createTitleWithTooltip(
    "Commits by hour", 
    "Distribution of commits across hours of the day (24-hour format). Reveals developer's preferred working hours and coding schedule.",
    "h2"
  ) + '<canvas id="chart-hour"></canvas>';
  chartRow.appendChild(hourCard);

  main.appendChild(chartRow);

  // Build charts
  const langStats = getLanguageStats(summary);
  if (langStats.labels.length > 0) {
    try {
      const ctx = document.getElementById("chart-languages");
      if (ctx) {
        // Destroy existing chart if it exists
        if (state.charts.languages) {
          state.charts.languages.destroy();
        }
        state.charts.languages = new Chart(ctx, {
          type: "bar",
          data: {
            labels: langStats.labels,
            datasets: [
              {
                label: "Lines changed (add+del)",
                data: langStats.values
              }
            ]
          },
          options: {
            responsive: true,
            plugins: {
              legend: { display: false }
            },
            scales: {
              x: { ticks: { autoSkip: false } },
              y: { beginAtZero: true }
            }
          }
        });
      }
    } catch (error) {
      console.error("Error creating languages chart:", error);
    }
  }

  try {
    const weekdayStats = getWeekdayStats(summary);
    const ctxWeekday = document.getElementById("chart-weekday");
    if (ctxWeekday) {
      // Destroy existing chart if it exists
      if (state.charts.weekday) {
        state.charts.weekday.destroy();
      }
      state.charts.weekday = new Chart(ctxWeekday, {
        type: "bar",
        data: {
          labels: weekdayStats.labels,
          datasets: [
            {
              label: "Commits",
              data: weekdayStats.values
            }
          ]
        },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { autoSkip: false } },
            y: { beginAtZero: true }
          }
        }
      });
    }
  } catch (error) {
    console.error("Error creating weekday chart:", error);
  }

  try {
    const hourStats = getHourStats(summary);
    const ctxHour = document.getElementById("chart-hour");
    if (ctxHour) {
      // Destroy existing chart if it exists
      if (state.charts.hour) {
        state.charts.hour.destroy();
      }
      state.charts.hour = new Chart(ctxHour, {
        type: "bar",
        data: {
          labels: hourStats.labels,
          datasets: [
            {
              label: "Commits",
              data: hourStats.values
            }
          ]
        },
        options: {
          responsive: true,
          plugins: { legend: { display: false } },
          scales: {
            x: { ticks: { autoSkip: false } },
            y: { beginAtZero: true }
          }
        }
      });
    }
  } catch (error) {
    console.error("Error creating hour chart:", error);
  }

  autoTagVisualizations("user", vizContext);
}

async function renderSubsystemDashboard(subsystem, period, summary) {
  try {
    // Prevent concurrent renders
    if (state.rendering) {
      console.log("Render already in progress, skipping duplicate render");
      return;
    }
    
    state.rendering = true;
    console.log("Starting subsystem dashboard render for", subsystem.name, period.label);
    
    clearMain();

    const periodType = period.is_yearly ? "Yearly" : "Monthly";
    const periodLabel = period.is_yearly ? period.label : period.label + " (" + summary.from + " → " + summary.to + ")";

    setViewHeader(
      "Subsystem: " + (summary.service || subsystem.name),
      periodLabel,
      "Subsystem · " + periodType
    );

    const main = $("main-content");

    const vizContext = {
      scope: "subsystem",
      entityId: subsystem.name,
      entityLabel: summary.service || subsystem.name,
      period,
      periodKey: buildPeriodKey(period),
      periodLabel
    };

    // Show dead subsystem warning if applicable
    if (summary.dead_status && summary.dead_status.is_dead) {
      const warningContainer = document.createElement("div");
      warningContainer.className = "dead-warning";
      
      const warningIcon = document.createElement("span");
      warningIcon.className = "warning-icon";
      warningIcon.textContent = "⚠️";
      
      const warningText = document.createElement("div");
      warningText.className = "warning-text";
      
      let warningMessage = "This subsystem appears to be potentially dead - no commits found in the last 3+ months.";
      if (summary.dead_status.last_activity_date) {
        warningMessage = `This subsystem appears to be potentially dead - last activity was on ${summary.dead_status.last_activity_date}`;
        if (summary.dead_status.months_since_activity) {
          warningMessage += ` (${summary.dead_status.months_since_activity} months ago)`;
        }
        warningMessage += ".";
      }
      
      warningText.textContent = warningMessage;
      
      warningContainer.appendChild(warningIcon);
      warningContainer.appendChild(warningText);
      main.appendChild(warningContainer);
    }

    const kpiContainer = document.createElement("div");
    kpiContainer.className = "kpi-grid";

    // Use unified service data structure for all subsystems
    const kpis = [
      { label: "Total commits", value: summary.total_commits || 0 },
      { label: "Lines added", value: summary.total_lines_added || 0 },
      { label: "Lines deleted", value: summary.total_lines_deleted || 0 },
      { label: "Net lines", value: (summary.total_lines_added || 0) - (summary.total_lines_deleted || 0) },
      { label: "Changed lines", value: summary.total_changed_lines || 0 }
    ];

    kpis.forEach((k) => {
      const card = document.createElement("div");
      card.className = "kpi-card";
      card.innerHTML = '<div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>';
      kpiContainer.appendChild(card);
    });

    main.appendChild(kpiContainer);
    tagVisualization(kpiContainer, "subsystem-kpis", vizContext);

    // Top developer
    const topDev = summary.top_developer;
    if (topDev && topDev.slug) {
      const topDevCard = document.createElement("div");
      topDevCard.className = "card";
      topDevCard.innerHTML = createTitleWithTooltip(
        'Top Developer', 
        'The developer with the most commits in this subsystem during the selected period. Indicates the primary contributor and likely maintainer.',
        'h2'
      );
      
      const topDevInfo = document.createElement("div");
      topDevInfo.className = "top-developer-info";
      
      // Create clickable developer name
      const nameElement = createClickableDeveloperName(topDev.slug, topDev.display_name);
      
      const statsElement = document.createElement("div");
      statsElement.className = "developer-stats";
      statsElement.innerHTML = (topDev.commits || 0) + " commits · " + (topDev.changed_lines || 0) + " lines changed";
      
      topDevInfo.appendChild(nameElement);
      topDevInfo.appendChild(statsElement);
      topDevCard.appendChild(topDevInfo);
      main.appendChild(topDevCard);
    }

    // Only add yearly/all-time sections when viewing yearly data
    if (period.is_yearly) {
      // Add top maintainers section (from recent activity)
      try {
        await addTopMaintainersSection(main, subsystem.name);
      } catch (error) {
        console.error("Error loading top maintainers:", error);
        // Don't let this error break the whole dashboard
      }

      // Add significant ownership section 
      try {
        await addSignificantOwnershipSection(main, subsystem.name);
      } catch (error) {
        console.error("Error loading significant ownership:", error);
        // Don't let this error break the whole dashboard
      }

      // Add language statistics section
      try {
        await addSubsystemLanguageSection(main, subsystem.name);
      } catch (error) {
        console.error("Error loading language statistics:", error);
        // Don't let this error break the whole dashboard
      }

      // Add size ranking section
      try {
        await addSubsystemSizeRankingSection(main, subsystem.name);
      } catch (error) {
        console.error("Error loading size ranking:", error);
        // Don't let this error break the whole dashboard
      }
    } else {
      // For monthly view, add period-specific content
      const monthlyInfoCard = document.createElement("div");
      monthlyInfoCard.className = "card info-card";
      monthlyInfoCard.innerHTML = `
        <h2>📅 Monthly View</h2>
        <p>Viewing data for <strong>${period.label}</strong> only.</p>
        <p>For comprehensive statistics including ownership, language breakdown, and maintainer analysis, please select a yearly view.</p>
      `;
      main.appendChild(monthlyInfoCard);
    }

    // Add contribution activity heatmap
    try {
      console.log("Adding contribution activity heatmap for", subsystem.name);
      await addSubsystemContributionHeatmap(main, subsystem.name, period, summary);
      console.log("Contribution activity heatmap added successfully");
    } catch (error) {
      console.error("Error loading contribution activity:", error);
      // Don't let this error break the whole dashboard
    }

    // Show all developers if we have the data
    const developers = summary.developers || {};
    if (Object.keys(developers).length > 0) {
      console.log("Adding developers section with", Object.keys(developers).length, "developers");
      const devCard = document.createElement("div");
      devCard.className = "card";
      devCard.innerHTML = '<h2>All Developers</h2>';
      
      const devList = document.createElement("ul");
      devList.className = "link-list";

      // Sort developers by changed lines (descending)
      const sortedDevs = Object.entries(developers).sort((a, b) => 
        (b[1].changed_lines || 0) - (a[1].changed_lines || 0)
      );

      sortedDevs.forEach(([devSlug, devData]) => {
        const li = document.createElement("li");
        li.className = "link-list-item";
        
        // Create clickable developer name
        const nameElement = createClickableDeveloperName(devSlug, devData.display_name);
        
        const statsElement = document.createElement("div");
        statsElement.className = "developer-stats";
        statsElement.innerHTML = (devData.commits || 0) + " commits · " + (devData.changed_lines || 0) + " lines changed · " + ((devData.lines_added || 0) - (devData.lines_deleted || 0)) + " net";
        
        li.appendChild(nameElement);
        li.appendChild(statsElement);
        devList.appendChild(li);
      });

      devCard.appendChild(devList);
      main.appendChild(devCard);
    }

    autoTagVisualizations("subsystem", vizContext);

    state.rendering = false;
    console.log("Subsystem dashboard render completed successfully");

  } catch (error) {
    console.error("Error rendering subsystem dashboard:", error);
    clearMain();
    const main = $("main-content");
    main.innerHTML = '<div class="error">Error rendering subsystem dashboard: ' + error.message + '</div>';
  } finally {
    state.rendering = false;
    console.log("Subsystem dashboard render completed");
  }
}

async function renderTeamDashboard(team, period, summary) {
  clearMain();

  const periodType = period.is_yearly ? "Yearly" : "Monthly";
  const periodLabel = period.is_yearly ? period.label : period.label + " (" + summary.from + " → " + summary.to + ")";

  setViewHeader(
    "Team: " + (team.name || team.id),
    periodLabel,
    "Team · " + periodType
  );

  const main = $("main-content");

  const vizContext = {
    scope: "team",
    entityId: team.id || team.name,
    entityLabel: team.name || team.id,
    period,
    periodKey: buildPeriodKey(period),
    periodLabel
  };

  // Team description
  if (summary.description) {
    const descContainer = document.createElement("div");
    descContainer.className = "team-description";
    descContainer.style.marginBottom = "20px";
    descContainer.style.padding = "10px";
    descContainer.style.backgroundColor = "var(--background-secondary)";
    descContainer.style.borderRadius = "8px";
    descContainer.innerHTML = '<strong>Team Description:</strong> ' + summary.description;
    main.appendChild(descContainer);
  }

  const uniqueMembers = Array.isArray(summary.members) ? Array.from(new Set(summary.members)) : [];
  if (uniqueMembers.length > 0) {
    const hasPagerDuty = isPagerDutyConfigured();
    const membersCard = document.createElement("div");
    membersCard.className = "card team-members-card";
    membersCard.innerHTML = createTitleWithTooltip(
      "👥 Team members",
      "View who is on this team and jump directly to developer or PagerDuty details.",
      "h2"
    );

    const memberStats = summary.member_contributions || {};
    const toNumber = (value) => {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : 0;
    };
    const formatNumber = (value) => (Number(value) || 0).toLocaleString();

    const membersData = uniqueMembers.map((memberSlug) => {
      const stats = memberStats[memberSlug] || {};
      const displayName = getUserDisplayName(memberSlug);
      const additions = toNumber(stats.additions ?? stats.total_additions ?? stats.total_lines_added ?? 0);
      const deletions = toNumber(stats.deletions ?? stats.total_deletions ?? stats.total_lines_deleted ?? 0);
      const commits = toNumber(stats.commits ?? stats.total_commits ?? 0);
      const netLines = toNumber(stats.net_lines ?? (additions - deletions));
      const subsystemKeys = Array.isArray(stats.subsystem_keys) ? stats.subsystem_keys.filter(Boolean) : [];
      const languageKeys = Array.isArray(stats.language_keys) ? stats.language_keys.filter(Boolean) : [];
      return {
        slug: memberSlug,
        displayName,
        subsystems_touched: toNumber(stats.subsystems_touched),
        languages_used: toNumber(stats.languages_used),
        commits,
        additions,
        deletions,
        net_lines: netLines,
        subsystemKeys,
        languageKeys
      };
    });

    const totalsAccumulator = membersData.reduce((acc, member) => {
      if (member.subsystemKeys.length > 0) {
        member.subsystemKeys.forEach((key) => acc.uniqueSubsystems.add(key));
      } else if (member.subsystems_touched > 0) {
        acc.subsystemsFallback += member.subsystems_touched;
      }
      if (member.languageKeys.length > 0) {
        member.languageKeys.forEach((key) => acc.uniqueLanguages.add(key));
      } else if (member.languages_used > 0) {
        acc.languagesFallback += member.languages_used;
      }
      acc.commits += member.commits;
      acc.additions += member.additions;
      acc.deletions += member.deletions;
      acc.net_lines += member.net_lines;
      return acc;
    }, {
      uniqueSubsystems: new Set(),
      uniqueLanguages: new Set(),
      subsystemsFallback: 0,
      languagesFallback: 0,
      commits: 0,
      additions: 0,
      deletions: 0,
      net_lines: 0
    });

    const totals = {
      subsystems_touched: totalsAccumulator.uniqueSubsystems.size || totalsAccumulator.subsystemsFallback,
      languages_used: totalsAccumulator.uniqueLanguages.size || totalsAccumulator.languagesFallback,
      commits: totalsAccumulator.commits,
      additions: totalsAccumulator.additions,
      deletions: totalsAccumulator.deletions,
      net_lines: totalsAccumulator.net_lines
    };

    const columns = [
      { key: "displayName", label: "Member", sortable: true, type: "string", defaultDirection: "asc" },
      { key: "subsystems_touched", label: "Subsystems touched", sortable: true, type: "number", defaultDirection: "desc" },
      { key: "languages_used", label: "Languages", sortable: true, type: "number", defaultDirection: "desc" },
      { key: "commits", label: "Commits", sortable: true, type: "number", defaultDirection: "desc" },
      { key: "additions", label: "Lines added", sortable: true, type: "number", defaultDirection: "desc" },
      { key: "deletions", label: "Lines deleted", sortable: true, type: "number", defaultDirection: "desc" },
      { key: "net_lines", label: "Net lines", sortable: true, type: "number", defaultDirection: "desc" }
    ];

    let sortState = { key: "commits", direction: "desc" };
    const columnMap = columns.reduce((acc, column) => {
      acc[column.key] = column;
      return acc;
    }, {});

    const membersTable = document.createElement("table");
    membersTable.className = "data-table team-members-table sortable-table";

    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    const columnIndicators = {};

    columns.forEach((column) => {
      const th = document.createElement("th");
      th.textContent = column.label;
      if (column.sortable) {
        th.dataset.sortKey = column.key;
        th.style.cursor = "pointer";
        const indicator = document.createElement("span");
        indicator.className = "sort-indicator";
        indicator.style.marginLeft = "6px";
        indicator.style.fontSize = "0.8em";
        indicator.style.opacity = "0.7";
        indicator.style.visibility = "hidden";
        columnIndicators[column.key] = indicator;
        th.appendChild(indicator);
        th.addEventListener("click", () => {
          if (sortState.key === column.key) {
            sortState.direction = sortState.direction === "asc" ? "desc" : "asc";
          } else {
            sortState = {
              key: column.key,
              direction: column.defaultDirection || (column.type === "string" ? "asc" : "desc")
            };
          }
          renderRows();
        });
      }
      headerRow.appendChild(th);
    });

    const detailsTh = document.createElement("th");
    detailsTh.textContent = "Developer details";
    headerRow.appendChild(detailsTh);

    if (hasPagerDuty) {
      const pdTh = document.createElement("th");
      pdTh.textContent = "PagerDuty";
      headerRow.appendChild(pdTh);
    }

    thead.appendChild(headerRow);
    membersTable.appendChild(thead);

    const tableBody = document.createElement("tbody");
    membersTable.appendChild(tableBody);

    const tfoot = document.createElement("tfoot");
    const totalsRow = document.createElement("tr");
    totalsRow.style.fontWeight = "bold";
    totalsRow.style.borderTop = "2px solid var(--border)";

    const totalLabelCell = document.createElement("td");
    totalLabelCell.textContent = "Total";
    totalsRow.appendChild(totalLabelCell);

    columns.slice(1).forEach((column) => {
      const cell = document.createElement("td");
      const totalValue = totals[column.key] || 0;
      let displayValue = formatNumber(totalValue);
      if (column.key === "subsystems_touched" || column.key === "languages_used") {
        displayValue = `${displayValue}*`;
        cell.title = "Unique count across the team";
      }
      cell.textContent = displayValue;
      if (column.key === "additions" || (column.key === "net_lines" && totalValue >= 0)) {
        cell.style.color = "#22c55e";
      } else if (column.key === "deletions" || (column.key === "net_lines" && totalValue < 0)) {
        cell.style.color = "#ef4444";
      }
      totalsRow.appendChild(cell);
    });

    const totalActionsCell = document.createElement("td");
    totalActionsCell.colSpan = hasPagerDuty ? 2 : 1;
    totalsRow.appendChild(totalActionsCell);

    tfoot.appendChild(totalsRow);
    membersTable.appendChild(tfoot);

    function compareMembers(a, b) {
      const column = columnMap[sortState.key] || columns[0];
      let valueA;
      let valueB;
      if (column.type === "string") {
        valueA = (a[column.key] || "").toLowerCase();
        valueB = (b[column.key] || "").toLowerCase();
      } else {
        valueA = Number(a[column.key]) || 0;
        valueB = Number(b[column.key]) || 0;
      }
      if (valueA === valueB) {
        return a.displayName.localeCompare(b.displayName);
      }
      const direction = sortState.direction === "asc" ? 1 : -1;
      return valueA < valueB ? -1 * direction : 1 * direction;
    }

    function updateSortIndicators() {
      Object.entries(columnIndicators).forEach(([key, indicator]) => {
        if (!indicator) {
          return;
        }
        if (sortState.key === key) {
          indicator.textContent = sortState.direction === "asc" ? "↑" : "↓";
          indicator.style.visibility = "visible";
        } else {
          indicator.textContent = "";
          indicator.style.visibility = "hidden";
        }
      });
    }

    function renderRows() {
      const sortedMembers = [...membersData].sort(compareMembers);
      tableBody.innerHTML = "";
      sortedMembers.forEach((member) => {
        const row = document.createElement("tr");
        row.className = "clickable-row";
        row.dataset.member = member.slug;
        row.addEventListener("click", (event) => {
          if (event.target.closest("button")) {
            return;
          }
          navigateToUser(member.slug, period);
        });

        const nameCell = document.createElement("td");
        nameCell.appendChild(createClickableDeveloperName(member.slug, member.displayName, "inline"));
        row.appendChild(nameCell);

        columns.slice(1).forEach((column) => {
          const cell = document.createElement("td");
          const value = member[column.key] || 0;
          cell.textContent = formatNumber(value);
          if (column.key === "additions" || (column.key === "net_lines" && value >= 0)) {
            cell.style.color = "#22c55e";
          } else if (column.key === "deletions" || (column.key === "net_lines" && value < 0)) {
            cell.style.color = "#ef4444";
          }
          row.appendChild(cell);
        });

        const detailsCell = document.createElement("td");
        detailsCell.className = "team-member-action-cell";
        const detailsButton = document.createElement("button");
        detailsButton.type = "button";
        detailsButton.className = "btn-link team-member-action";
        detailsButton.textContent = "Open user";
        detailsButton.title = "Open developer dashboard";
        detailsButton.addEventListener("click", (event) => {
          event.preventDefault();
          event.stopPropagation();
          navigateToUser(member.slug, period);
        });
        detailsCell.appendChild(detailsButton);
        row.appendChild(detailsCell);

        if (hasPagerDuty) {
          const pdCell = document.createElement("td");
          pdCell.className = "team-member-action-cell";
          const pdButton = document.createElement("button");
          pdButton.type = "button";
          pdButton.className = "btn-link team-member-action";
          pdButton.textContent = "PagerDuty";
          pdButton.title = "Open PagerDuty responder dashboard";
          pdButton.addEventListener("click", (event) => {
            event.preventDefault();
            event.stopPropagation();
            openPagerDutyForUser(member.slug);
          });
          pdCell.appendChild(pdButton);
          row.appendChild(pdCell);
        }

        tableBody.appendChild(row);
      });
      updateSortIndicators();
    }

    renderRows();

    membersCard.appendChild(membersTable);

    const uniqueTotalsNote = document.createElement("div");
    uniqueTotalsNote.className = "table-footnote";
    uniqueTotalsNote.textContent = "* Totals marked with * represent unique counts across the team.";
    uniqueTotalsNote.style.marginTop = "8px";
    uniqueTotalsNote.style.fontSize = "0.85em";
    uniqueTotalsNote.style.color = "var(--text-muted)";
    membersCard.appendChild(uniqueTotalsNote);

    main.appendChild(membersCard);
  }

  // Team responsibilities with detailed line counts (only for yearly view)
  if (period.is_yearly && summary.responsible_subsystems && summary.responsible_subsystems.length > 0) {
    const responsibilitiesContainer = document.createElement("div");
    responsibilitiesContainer.className = "team-responsibilities";
    responsibilitiesContainer.style.marginBottom = "20px";
    responsibilitiesContainer.style.padding = "15px";
    responsibilitiesContainer.style.backgroundColor = "var(--background-secondary)";
    responsibilitiesContainer.style.borderRadius = "8px";
    responsibilitiesContainer.style.borderLeft = "4px solid var(--accent-blue)";
    
    const responsibilitiesTitle = document.createElement("h4");
    responsibilitiesTitle.style.margin = "0 0 15px 0";
    responsibilitiesTitle.style.color = "var(--text-primary)";
    const totalLines = summary.total_responsible_lines || 0;
    responsibilitiesTitle.innerHTML = `<strong>🎯 Responsible for ${summary.responsible_subsystems.length} Subsystems (${totalLines.toLocaleString()} total lines)</strong>`;
    
    // Create a detailed list of subsystems with line counts
    const subsystemsList = document.createElement("div");
    subsystemsList.style.marginBottom = "15px";
    
    // Sort subsystems by line count (highest first)
    const sortedSubsystems = summary.responsible_subsystems.map(subsystemName => {
      const details = summary.responsible_subsystem_details?.[subsystemName] || { name: subsystemName, lines: 0 };
      return details;
    }).sort((a, b) => (b.lines || 0) - (a.lines || 0));
    
    sortedSubsystems.forEach(subsystemDetail => {
      const subsystemRow = document.createElement("div");
      subsystemRow.style.display = "flex";
      subsystemRow.style.justifyContent = "space-between";
      subsystemRow.style.alignItems = "center";
      subsystemRow.style.padding = "8px 0";
      subsystemRow.style.borderBottom = "1px solid var(--border)";
      
      const subsystemName = document.createElement("span");
      subsystemName.className = "subsystem-name clickable";
      subsystemName.style.fontWeight = "500";
      subsystemName.style.cursor = "pointer";
      subsystemName.style.color = "var(--accent-blue)";
      subsystemName.style.textDecoration = "underline";
      subsystemName.textContent = subsystemDetail.name;
      
      subsystemName.addEventListener("click", () => {
        // Find the full subsystem object from the loaded subsystems
        const fullSubsystem = state.subsystems.find(s => s.name === subsystemDetail.name);
        if (fullSubsystem) {
          // Switch to subsystems mode and select the subsystem
          setMode("subsystems", false);
          selectSubsystem(fullSubsystem);
        } else {
          console.warn(`Subsystem ${subsystemDetail.name} not found in loaded subsystems`);
        }
      });
      
      const lineCount = document.createElement("span");
      lineCount.style.color = "var(--text-secondary)";
      lineCount.style.fontFamily = "monospace";
      lineCount.textContent = `${(subsystemDetail.lines || 0).toLocaleString()} lines`;
      
      subsystemRow.appendChild(subsystemName);
      subsystemRow.appendChild(lineCount);
      subsystemsList.appendChild(subsystemRow);
    });
    
    // Add total row
    const totalRow = document.createElement("div");
    totalRow.style.display = "flex";
    totalRow.style.justifyContent = "space-between";
    totalRow.style.alignItems = "center";
    totalRow.style.padding = "10px 0 5px 0";
    totalRow.style.marginTop = "10px";
    totalRow.style.borderTop = "2px solid var(--accent-blue)";
    totalRow.style.fontWeight = "bold";
    
    const totalLabel = document.createElement("span");
    totalLabel.textContent = "Total";
    totalLabel.style.color = "var(--text-primary)";
    
    const totalValue = document.createElement("span");
    totalValue.style.color = "var(--accent-blue)";
    totalValue.style.fontFamily = "monospace";
    totalValue.textContent = `${totalLines.toLocaleString()} lines`;
    
    totalRow.appendChild(totalLabel);
    totalRow.appendChild(totalValue);
    subsystemsList.appendChild(totalRow);
    
    responsibilitiesContainer.appendChild(responsibilitiesTitle);
    responsibilitiesContainer.appendChild(subsystemsList);
    main.appendChild(responsibilitiesContainer);
  }

  // Team capacity analysis (only for yearly view)
  if (period.is_yearly && summary.capacity_analysis) {
    const capacityContainer = document.createElement("div");
    capacityContainer.className = "team-capacity-analysis";
    capacityContainer.style.marginBottom = "20px";
    capacityContainer.style.padding = "15px";
    capacityContainer.style.backgroundColor = "var(--background-secondary)";
    capacityContainer.style.borderRadius = "8px";
    
    const analysis = summary.capacity_analysis;
    
    // Set border color based on status
    const borderColors = {
      'green': '#10b981',
      'yellow': '#f59e0b',
      'red': '#ef4444',
      'gray': '#6b7280'
    };
    capacityContainer.style.borderLeft = `4px solid ${borderColors[analysis.status_color] || borderColors.gray}`;
    
    const capacityTitle = document.createElement("h4");
    capacityTitle.style.margin = "0 0 15px 0";
    capacityTitle.style.color = "var(--text-primary)";
    
    // Status emoji based on status
    const statusEmoji = {
      'healthy': '✅',
      'warning': '⚠️',
      'critical': '🔴',
      'unknown': '❓'
    };
    
    capacityTitle.innerHTML = `<strong>${statusEmoji[analysis.status] || '📊'} Team Capacity Analysis</strong>`;
    
    // Summary row
    const summaryRow = document.createElement("div");
    summaryRow.style.display = "flex";
    summaryRow.style.justifyContent = "space-between";
    summaryRow.style.marginBottom = "15px";
    summaryRow.style.padding = "12px";
    summaryRow.style.backgroundColor = "var(--background-primary)";
    summaryRow.style.borderRadius = "6px";
    
    const teamSizeText = document.createElement("div");
    teamSizeText.innerHTML = `<strong>Team Size:</strong> ${analysis.team_size} developers`;
    
    const requiredText = document.createElement("div");
    requiredText.innerHTML = `<strong>Theoretical Need:</strong> ${analysis.required_developers.toFixed(1)} developers`;
    
    const capacityText = document.createElement("div");
    capacityText.innerHTML = `<strong>Capacity:</strong> <span style="color: ${borderColors[analysis.status_color]}; font-weight: bold;">${analysis.capacity_percent}%</span>`;
    
    summaryRow.appendChild(teamSizeText);
    summaryRow.appendChild(requiredText);
    summaryRow.appendChild(capacityText);
    
    // Language breakdown
    const languageBreakdown = document.createElement("div");
    languageBreakdown.style.marginTop = "15px";
    
    const breakdownTitle = document.createElement("div");
    breakdownTitle.innerHTML = "<strong>Lines per Language:</strong>";
    breakdownTitle.style.marginBottom = "8px";
    languageBreakdown.appendChild(breakdownTitle);
    
    for (const [lang, data] of Object.entries(analysis.language_breakdown)) {
      const langRow = document.createElement("div");
      langRow.style.display = "flex";
      langRow.style.justifyContent = "space-between";
      langRow.style.padding = "4px 8px";
      langRow.style.fontSize = "0.9em";
      
      const langName = document.createElement("span");
      langName.textContent = lang;
      
      const langStats = document.createElement("span");
      langStats.innerHTML = `${data.lines.toLocaleString()} lines → ${data.theoretical_devs.toFixed(1)} devs`;
      langStats.style.color = "var(--text-secondary)";
      
      langRow.appendChild(langName);
      langRow.appendChild(langStats);
      languageBreakdown.appendChild(langRow);
    }
    
    capacityContainer.appendChild(capacityTitle);
    capacityContainer.appendChild(summaryRow);
    capacityContainer.appendChild(languageBreakdown);
    main.appendChild(capacityContainer);
  }

  if (period.is_yearly && Array.isArray(summary.developer_capacity_profiles) && summary.developer_capacity_profiles.length > 0) {
    const developerWorthCard = document.createElement("div");
    developerWorthCard.className = "card";
    developerWorthCard.innerHTML = createTitleWithTooltip(
      "🧠 Individual Capacity (Blame)",
      "Git blame ownership for this team's responsible subsystems, converted into theoretical developer equivalents using the same language rules as the team capacity view. Only developers with ≥0.9 headcount are shown.",
      "h2"
    );

    const developerWorthList = document.createElement("div");
    developerWorthList.style.display = "flex";
    developerWorthList.style.flexDirection = "column";
    developerWorthList.style.gap = "12px";

    summary.developer_capacity_profiles.forEach(profile => {
      const devEntry = document.createElement("div");
      devEntry.style.border = "1px solid var(--border)";
      devEntry.style.borderRadius = "10px";
      devEntry.style.padding = "12px";
      devEntry.style.backgroundColor = "var(--background-secondary)";
      devEntry.style.display = "flex";
      devEntry.style.flexDirection = "column";
      devEntry.style.gap = "8px";

      const header = document.createElement("div");
      header.style.display = "flex";
      header.style.justifyContent = "space-between";
      header.style.alignItems = "center";

      const nameWrapper = document.createElement("div");
      nameWrapper.appendChild(createClickableDeveloperName(profile.slug, profile.display_name, "inline"));

      const worthBadge = document.createElement("div");
      worthBadge.style.fontWeight = "600";
      worthBadge.style.color = "var(--accent-blue)";
      worthBadge.textContent = `${profile.developer_equivalent.toFixed(1)} devs`;

      header.appendChild(nameWrapper);
      header.appendChild(worthBadge);
      devEntry.appendChild(header);

      const ownedLines = document.createElement("div");
      ownedLines.className = "developer-stats";
      ownedLines.style.color = "var(--text-secondary)";
      ownedLines.textContent = `${(profile.total_lines || 0).toLocaleString()} lines owned`;
      devEntry.appendChild(ownedLines);

      const languagesContainer = document.createElement("div");
      languagesContainer.style.display = "flex";
      languagesContainer.style.flexWrap = "wrap";
      languagesContainer.style.gap = "8px";

      const languages = Object.entries(profile.language_breakdown || {})
        .sort((a, b) => (b[1].lines || 0) - (a[1].lines || 0));
      const maxLanguages = 6;

      languages.slice(0, maxLanguages).forEach(([lang, data]) => {
        const pill = document.createElement("div");
        pill.style.backgroundColor = "var(--background-primary)";
        pill.style.borderRadius = "999px";
        pill.style.padding = "6px 10px";
        pill.style.fontSize = "0.85em";
        pill.style.border = "1px solid var(--border)";
        pill.innerHTML = `<strong>${lang}</strong>: ${data.lines.toLocaleString()} lines → ${data.theoretical_devs.toFixed(1)} devs`;
        languagesContainer.appendChild(pill);
      });

      if (languages.length > maxLanguages) {
        const morePill = document.createElement("div");
        morePill.style.backgroundColor = "var(--background-primary)";
        morePill.style.borderRadius = "999px";
        morePill.style.padding = "6px 10px";
        morePill.style.fontSize = "0.85em";
        morePill.style.border = "1px dashed var(--border)";
        morePill.textContent = `+${languages.length - maxLanguages} more`;
        languagesContainer.appendChild(morePill);
      }

      devEntry.appendChild(languagesContainer);
      developerWorthList.appendChild(devEntry);
    });

    const thresholdNote = document.createElement("div");
    thresholdNote.className = "note-text";
    thresholdNote.style.marginTop = "12px";
    thresholdNote.style.color = "var(--text-secondary)";
    thresholdNote.textContent = "Only developers with ≥0.9 theoretical headcount are shown.";

    developerWorthCard.appendChild(developerWorthList);
    developerWorthCard.appendChild(thresholdNote);
    main.appendChild(developerWorthCard);
  }

  // KPIs
  const kpiContainer = document.createElement("div");
  kpiContainer.className = "kpi-grid";

  // Calculate correct primary language if language data is available
  let primaryLanguage = "Not available";
  if (summary.languages && Object.keys(summary.languages).length > 0) {
    const correctPrimary = getCorrectPrimaryLanguage(summary.languages);
    primaryLanguage = correctPrimary || "None detected";
  }

  const totalCommits = summary.total_commits || summary.commits || 0;
  const totalAdditions = summary.total_additions || summary.lines_added || 0;
  const totalDeletions = summary.total_deletions || summary.lines_deleted || 0;
  const totalLinesChanged = summary.total_lines_changed || (totalAdditions + totalDeletions);
  const subsystemsTouched = summary.subsystems_touched || Object.keys(summary.subsystems || {}).length || 0;
  const teamPeerRankings = summary.peer_rankings || {};
 
  const kpis = [
    { label: "Team Members", value: summary.members?.length || 0 },
    { label: "Total Commits", value: totalCommits, metric: "total_commits" },
    { label: "Lines Changed", value: totalLinesChanged, metric: "total_lines_changed" },
    { label: "Subsystems Touched", value: subsystemsTouched, metric: "subsystems_touched" },
    { label: "Primary Language", value: primaryLanguage },
    { label: "Lines Added", value: totalAdditions },
    { label: "Lines Deleted", value: totalDeletions }
  ];

  kpis.forEach((k) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    const rankText = k.metric ? formatRankSummary(teamPeerRankings[k.metric]) : "";
    const displayValue = typeof k.value === "number" ? k.value.toLocaleString() : k.value;
    card.innerHTML = `
      <div class="kpi-label">${k.label}</div>
      <div class="kpi-value">${displayValue}</div>
      ${rankText ? `<div class="kpi-rank">${rankText}</div>` : ""}
    `;
    kpiContainer.appendChild(card);
  });

  main.appendChild(kpiContainer);
  tagVisualization(kpiContainer, "team-kpis", vizContext);

  if (period.is_yearly) {
    const parsedLabelYear = parseInt(period.label, 10);
    let timelineYear = Number.isNaN(parsedLabelYear) ? null : parsedLabelYear;
    if (!timelineYear) {
      const fallbackSource = summary.from || period.from || "";
      if (fallbackSource && fallbackSource.length >= 4) {
        const fallbackYear = parseInt(fallbackSource.slice(0, 4), 10);
        if (!Number.isNaN(fallbackYear)) {
          timelineYear = fallbackYear;
        }
      }
    }

    if (timelineYear) {
      const teamTimelineAnchor = document.createElement("div");
      teamTimelineAnchor.className = "async-card-anchor";
      main.appendChild(teamTimelineAnchor);

      loadTeamSubsystemActivity(team.id, timelineYear)
        .then((activityData) => {
          if (
            activityData &&
            Array.isArray(activityData.timeline) &&
            activityData.timeline.length > 0
          ) {
            renderTeamSubsystemTimeline(team, activityData, teamTimelineAnchor);
          } else if (teamTimelineAnchor.parentElement) {
            teamTimelineAnchor.remove();
          }
        })
        .catch((error) => {
          console.error("Failed to load team subsystem activity:", error);
          if (teamTimelineAnchor.parentElement) {
            teamTimelineAnchor.remove();
          }
        });
    }
  }

  // Show monthly view info card
  if (!period.is_yearly) {
    const monthlyInfoCard = document.createElement("div");
    monthlyInfoCard.className = "card info-card";
    monthlyInfoCard.innerHTML = `
      <h2>📅 Monthly Team View</h2>
      <p>Viewing data for <strong>${period.label}</strong> only.</p>
      <p>For comprehensive team statistics including responsibility breakdown, language analysis, and yearly trends, please select a yearly view.</p>
    `;
    main.appendChild(monthlyInfoCard);
  }

  // Add contribution activity heatmap if we have daily data
  if (summary.per_date && Object.keys(summary.per_date).length > 0) {
    try {
      const heatmapCard = document.createElement("div");
      heatmapCard.className = "card";
      heatmapCard.innerHTML = createTitleWithTooltip(
        "📊 Contribution Activity", 
        "Combined team contribution heatmap showing daily commit activity for the selected time period. For monthly views, shows only the selected month's commits across the full year layout. For yearly views, shows the full year. Represents the sum of all team members' commits.",
        "h2"
      );
      
      const heatmapContainer = document.createElement("div");
      heatmapContainer.className = "contribution-heatmap";
      
      // Show contribution activity for the selected time period
      let heatmapData = {};
      let heatmapFromDate, heatmapToDate;
      
      if (period.is_yearly) {
        // Show all yearly data
        heatmapData = summary.per_date || {};
        heatmapFromDate = summary.from || period.from;
        heatmapToDate = summary.to || period.to;
        console.log("Using full yearly data for team heatmap:", Object.keys(heatmapData).length, "days");
      } else {
        // For monthly view, show only selected month's data but display full year layout
        const periodStart = summary.from || period.from;
        const periodEnd = summary.to || period.to;
        const year = periodStart.split('-')[0];
        
        // Only include commits from the selected month
        heatmapData = {};
        if (summary.per_date) {
          for (const [date, data] of Object.entries(summary.per_date)) {
            // Only include dates that fall within the selected month
            if (date >= periodStart && date <= periodEnd) {
              heatmapData[date] = {
                ...data,
                isHighlighted: true
              };
            }
          }
        }
        
        // Display full year range so all months are visible
        heatmapFromDate = `${year}-01-01`;
        heatmapToDate = `${year}-12-31`;
        console.log(`Using selected month data only for team heatmap (${periodStart} to ${periodEnd}):`, Object.keys(heatmapData).length, "days");
      }
      
      console.log("Creating team heatmap for period:", heatmapFromDate, "to", heatmapToDate, "with", Object.keys(heatmapData).length, "data points");
      const heatmapElement = createContributionHeatmap(heatmapData, heatmapFromDate, heatmapToDate);
      heatmapContainer.appendChild(heatmapElement);
      
      heatmapCard.appendChild(heatmapContainer);
      main.appendChild(heatmapCard);
    } catch (error) {
      console.error("Error creating team contribution heatmap:", error);
    }
  }

  // Team members contribution breakdown
  if (summary.member_contributions && Object.keys(summary.member_contributions).length > 0) {
    const membersSection = document.createElement("div");
    membersSection.className = "dashboard-section";
    
    // Create title with timespan
    const timespan = period.is_yearly ? `Year ${period.label}` : `Month ${period.label}`;
    membersSection.innerHTML = createTitleWithTooltip(
      `Team Members Contributions - ${timespan}`, 
      "Individual contribution statistics for each team member during the selected period. Shows commits, lines added, and lines deleted per member.",
      "h3"
    );

    const membersContainer = document.createElement("div");
    membersContainer.className = "chart-grid";

    const membersList = Object.entries(summary.member_contributions)
      .sort((a, b) => b[1].commits - a[1].commits)
      .slice(0, 10); // Top 10 contributors

    // Calculate totals
    const totals = membersList.reduce((acc, [_, contrib]) => ({
      commits: acc.commits + (contrib.commits || 0),
      additions: acc.additions + (contrib.additions || 0),
      deletions: acc.deletions + (contrib.deletions || 0)
    }), { commits: 0, additions: 0, deletions: 0 });

    // Create member contributions table
    const table = document.createElement("table");
    table.className = "data-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Member</th>
          <th>Commits</th>
          <th>Lines Added</th>
          <th>Lines Deleted</th>
        </tr>
      </thead>
      <tbody>
        ${membersList.map(([member, contrib]) => {
          // Check if user is active
          const isActive = state.users.some(user => user.slug === member);
          const rowClass = isActive ? 'clickable-row' : 'inactive-row';
          const nameStyle = isActive ? '' : 'style="color: #dc2626; font-style: italic; cursor: default;" title="Inactive contributor (no recent activity in analysis period)"';
          
          return `
            <tr class="${rowClass}" data-member="${member}" data-active="${isActive}">
              <td><strong ${nameStyle}>${member}</strong></td>
              <td>${contrib.commits || 0}</td>
              <td style="color: #22c55e;">${(contrib.additions || 0).toLocaleString()}</td>
              <td style="color: #ef4444;">${(contrib.deletions || 0).toLocaleString()}</td>
            </tr>
          `;
        }).join('')}
      </tbody>
      <tfoot>
        <tr style="font-weight: bold; border-top: 2px solid #e5e7eb;">
          <td>Total</td>
          <td>${totals.commits.toLocaleString()}</td>
          <td style="color: #22c55e;">${totals.additions.toLocaleString()}</td>
          <td style="color: #ef4444;">${totals.deletions.toLocaleString()}</td>
        </tr>
      </tfoot>
    `;

    // Add click handlers to navigate to individual users (only for active users)
    table.addEventListener('click', (e) => {
      const row = e.target.closest('tr[data-member]');
      if (row) {
        const isActive = row.getAttribute('data-active') === 'true';
        if (isActive) {
          const memberSlug = row.getAttribute('data-member');
          if (memberSlug) {
            navigateToUser(memberSlug, period);
          }
        }
      }
    });

    membersContainer.appendChild(table);
    membersSection.appendChild(membersContainer);
    main.appendChild(membersSection);
  }

  // Languages breakdown chart (only for yearly view)
  if (period.is_yearly && summary.languages && Object.keys(summary.languages).length > 0) {
    const languagesSection = document.createElement("div");
    languagesSection.className = "dashboard-section";
    languagesSection.innerHTML = '<h3>Languages</h3>';

    const chartContainer = document.createElement("div");
    chartContainer.className = "chart-container language-chart";
    chartContainer.style.height = "350px"; // Set reasonable height for team language chart

    const languageCanvas = document.createElement("canvas");
    languageCanvas.id = "team-languages-chart";
    chartContainer.appendChild(languageCanvas);

    languagesSection.appendChild(chartContainer);
    main.appendChild(languagesSection);

    // Create language chart using the same pattern as user dashboard
    const langStats = getLanguageStats(summary);
    if (langStats.labels.length > 0) {
      // Create colors for the doughnut chart
      const colors = [
        '#3B82F6', '#EF4444', '#10B981', '#F59E0B', 
        '#8B5CF6', '#06B6D4', '#84CC16', '#F97316',
        '#EC4899', '#6B7280', '#F43F5E', '#14B8A6',
        '#A855F7', '#F59E0B', '#EF4444', '#10B981'
      ];

      // Destroy existing chart if it exists
      if (state.charts["team-languages"]) {
        state.charts["team-languages"].destroy();
      }

      state.charts["team-languages"] = new Chart(languageCanvas, {
        type: "doughnut",
        data: {
          labels: langStats.labels,
          datasets: [{
            data: langStats.values,
            backgroundColor: colors.slice(0, langStats.labels.length),
            borderWidth: 1
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { 
              position: "bottom",
              labels: {
                boxWidth: 10,
                padding: 8,
                font: { size: 11 }
              }
            },
            title: { display: true, text: "Languages Distribution (by lines changed)", font: { size: 12 } }
          }
        }
      });
    }
  }

  // Subsystems breakdown chart
  if (summary.subsystems && Object.keys(summary.subsystems).length > 0) {
    const subsystemsSection = document.createElement("div");
    subsystemsSection.className = "dashboard-section";
    
    // Create title with timespan
    const timespan = period.is_yearly ? `Year ${period.label}` : `Month ${period.label}`;
    subsystemsSection.innerHTML = createTitleWithTooltip(
      `Subsystems Contributions - ${timespan}`, 
      "Team's contributions broken down by subsystem. Shows which subsystems the team is actively working on and their level of contribution to each.",
      "h3"
    );

    const chartContainer = document.createElement("div");
    chartContainer.className = "chart-grid";

    // Create subsystems table
    const subsystemsList = Object.entries(summary.subsystems)
      .sort((a, b) => b[1].commits - a[1].commits)
      .slice(0, 15); // Top 15 subsystems

    // Calculate totals
    const subsystemTotals = subsystemsList.reduce((acc, [_, data]) => ({
      commits: acc.commits + data.commits,
      additions: acc.additions + (data.additions || 0),
      deletions: acc.deletions + (data.deletions || 0)
    }), { commits: 0, additions: 0, deletions: 0 });

    const table = document.createElement("table");
    table.className = "data-table";
    table.innerHTML = `
      <thead>
        <tr>
          <th>Subsystem</th>
          <th>Commits</th>
          <th>Lines Added</th>
          <th>Lines Deleted</th>
        </tr>
      </thead>
      <tbody>
        ${subsystemsList.map(([subsystem, data]) => `
          <tr class="clickable-row" data-subsystem="${subsystem}">
            <td><strong>${subsystem}</strong></td>
            <td>${(data.commits || 0).toLocaleString()}</td>
            <td style="color: #22c55e;">${(data.additions || 0).toLocaleString()}</td>
            <td style="color: #ef4444;">${(data.deletions || 0).toLocaleString()}</td>
          </tr>
        `).join('')}
      </tbody>
      <tfoot>
        <tr style="font-weight: bold; border-top: 2px solid #e5e7eb;">
          <td>Total</td>
          <td>${subsystemTotals.commits.toLocaleString()}</td>
          <td style="color: #22c55e;">${subsystemTotals.additions.toLocaleString()}</td>
          <td style="color: #ef4444;">${subsystemTotals.deletions.toLocaleString()}</td>
        </tr>
      </tfoot>
    `;

    // Add click handlers to navigate to individual subsystems
    table.addEventListener('click', (e) => {
      const row = e.target.closest('.clickable-row');
      if (row) {
        const subsystemName = row.getAttribute('data-subsystem');
        if (subsystemName) {
          navigateToSubsystem(subsystemName, period);
        }
      }
    });

    chartContainer.appendChild(table);
    subsystemsSection.appendChild(chartContainer);
    main.appendChild(subsystemsSection);
  }

  // Monthly Lines Chart (only for yearly view)
  if (period.is_yearly) {
    const monthlyChartCard = document.createElement("div");
    monthlyChartCard.className = "card";
    monthlyChartCard.innerHTML = createTitleWithTooltip(
      "Monthly Lines Added/Deleted", 
      "Combined team lines added (green) and deleted (red) by month. Shows the team's overall productivity and coding activity pattern throughout the year.",
      "h2"
    ) + '<div style="height: 300px;"><canvas id="chart-team-monthly"></canvas></div>';
    main.appendChild(monthlyChartCard);
    
    // Create the monthly chart asynchronously
    const year = parseInt(period.label);
    setTimeout(() => createMonthlyChart("chart-team-monthly", team.id, year, true), 100);
  }

  // Monthly Statistics Card (different behavior for monthly vs yearly view)
  if (period.is_yearly) {
    // For yearly view: show last month statistics
    const lastMonthCard = await createLastMonthStatsCard(team.id, true);
    if (lastMonthCard) {
      main.appendChild(lastMonthCard);
    }
  } else {
    // For monthly view: show selected month statistics using summary data
    const selectedMonthCard = await createSelectedMonthStatsCard(team.id, period, summary, true);
    if (selectedMonthCard) {
      main.appendChild(selectedMonthCard);
    }
  }

  // Daily Activity Chart 
  const dailyChartCard = document.createElement("div");
  dailyChartCard.className = "card";
  
  let chartTitle, chartTooltip, chartYear, chartMonth;
  
  if (period.is_yearly) {
    // For yearly view: show current month's activity, but fall back to most recent month with data
    const now = new Date();
    chartYear = now.getFullYear();
    chartMonth = now.getMonth() + 1;
    
    // If current month has no data, try to find the most recent month with data from team members
    // We'll check if any team member has recent monthly data
    const teamMembers = team.members || [];
    let mostRecentMonth = null;
    
    if (teamMembers.length > 0 && state.users) {
      // Find the most recent month where any team member has data
      for (const memberSlug of teamMembers) {
        const member = state.users.find(u => u.slug === memberSlug);
        if (member && member.months) {
          const memberMonths = member.months
            .filter(m => !m.is_yearly && m.from.startsWith(chartYear.toString()))
            .sort((a, b) => b.from.localeCompare(a.from));
          
          if (memberMonths.length > 0) {
            const recentMonth = memberMonths[0];
            if (!mostRecentMonth || recentMonth.from > mostRecentMonth.from) {
              mostRecentMonth = recentMonth;
            }
          }
        }
      }
    }
    
    if (mostRecentMonth && now.getDate() < 5) {
      const recentDate = new Date(mostRecentMonth.from);
      const recentYear = recentDate.getFullYear();
      const recentMonth = recentDate.getMonth() + 1;
      
      // If we're early in current month and there's recent team data
      if (recentYear !== chartYear || recentMonth !== chartMonth) {
        chartYear = recentYear;
        chartMonth = recentMonth;
        chartTitle = `📈 Recent Month Team Activity (${mostRecentMonth.label})`;
        chartTooltip = `Combined daily breakdown of lines added (green) and deleted (red) by all team members for ${mostRecentMonth.label}, showing the most recent month with team activity data.`;
      } else {
        chartTitle = "📈 Current Month Daily Activity";
        chartTooltip = "Combined daily breakdown of lines added (green) and deleted (red) by all team members for the current month. Shows team's overall daily coding activity and productivity patterns.";
      }
    } else {
      chartTitle = "📈 Current Month Daily Activity";
      chartTooltip = "Combined daily breakdown of lines added (green) and deleted (red) by all team members for the current month. Shows team's overall daily coding activity and productivity patterns.";
    }
  } else {
    // For monthly view: show the selected month's activity
    const periodStart = summary.from || period.from;
    const selectedDate = new Date(periodStart);
    chartYear = selectedDate.getFullYear();
    chartMonth = selectedDate.getMonth() + 1;
    chartTitle = `📈 ${period.label} Daily Activity`;
    chartTooltip = `Combined daily breakdown of lines added (green) and deleted (red) by all team members for ${period.label}. Shows the team's daily coding activity and productivity patterns during the selected month.`;
  }
  
  dailyChartCard.innerHTML = createTitleWithTooltip(
    chartTitle, 
    chartTooltip,
    "h2"
  ) + `
    <div style="height: 300px;">
      <canvas id="chart-team-daily-activity"></canvas>
    </div>
  `;
  main.appendChild(dailyChartCard);
  
  // Create the daily chart asynchronously
  setTimeout(() => createDailyChart("chart-team-daily-activity", team.id, chartYear, chartMonth, true), 100);

  autoTagVisualizations("team", vizContext);
}

async function addSignificantOwnershipSection(container, subsystemName) {
  try {
    console.log("Loading significant ownership for subsystem:", subsystemName);
    
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Significant ownership loading timeout')), 10000)
    );
    
    const ownershipPromise = fetchJSON("/api/subsystems/" + encodeURIComponent(subsystemName) + "/significant-ownership");
    
    const ownershipData = await Promise.race([ownershipPromise, timeoutPromise]);
    
    if (ownershipData.owners && ownershipData.owners.length > 0) {
      console.log("Found", ownershipData.owners.length, "significant owners for", subsystemName);
      
      const ownershipCard = document.createElement("div");
      ownershipCard.className = "card";
      ownershipCard.innerHTML = '<h2>📊 Significant Ownership (>10%)</h2>';
      
      const ownershipList = document.createElement("div");
      ownershipList.className = "ownership-list";

      ownershipData.owners.forEach((owner) => {
        const ownershipItem = document.createElement("div");
        ownershipItem.className = "ownership-badge-item";
        
        // Create clickable developer name
        const nameElement = createClickableDeveloperName(owner.slug, owner.display_name);
        nameElement.className = "ownership-subsystem clickable";
        
        const percentageElement = document.createElement("div");
        percentageElement.className = "ownership-percentage";
        percentageElement.textContent = owner.percentage.toFixed(1) + "%";
        
        ownershipItem.appendChild(nameElement);
        ownershipItem.appendChild(percentageElement);
        ownershipList.appendChild(ownershipItem);
      });

      ownershipCard.appendChild(ownershipList);
      container.appendChild(ownershipCard);
    } else {
      console.log("No significant owners found for", subsystemName);
    }
  } catch (error) {
    console.error("Failed to load significant ownership for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
}

async function addTopMaintainersSection(container, subsystemName) {
  try {
    console.log("Loading top maintainers for subsystem:", subsystemName);
    
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Top maintainers loading timeout')), 10000)
    );
    
    const maintainersPromise = fetchJSON("/api/subsystems/" + encodeURIComponent(subsystemName) + "/top-maintainers");
    
    const maintainers = await Promise.race([
      maintainersPromise,
      timeoutPromise
    ]);
    
    if (maintainers.maintainers && maintainers.maintainers.length > 0) {
      console.log("Found", maintainers.maintainers.length, "top maintainers for", subsystemName);
      
      const maintainerCard = document.createElement("div");
      maintainerCard.className = "card";
      maintainerCard.innerHTML = '<h2>Top Maintainers (Last 3 Months)</h2>';
      
      const maintainerList = document.createElement("ul");
      maintainerList.className = "link-list";

      maintainers.maintainers.forEach((maintainer, index) => {
        const li = document.createElement("li");
        li.className = "link-list-item";
        
        // Create clickable developer name
        const nameElement = createClickableDeveloperName(maintainer.slug, maintainer.display_name);
        
        const statsElement = document.createElement("div");
        statsElement.className = "developer-stats";
        statsElement.innerHTML = maintainer.commits + " commits · " + (maintainer.changed_lines || 0) + " lines changed";
        
        li.appendChild(nameElement);
        li.appendChild(statsElement);
        maintainerList.appendChild(li);
      });

      maintainerCard.appendChild(maintainerList);
      container.appendChild(maintainerCard);
    } else {
      console.log("No top maintainers found for", subsystemName);
    }
  } catch (error) {
    console.error("Failed to load top maintainers for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
}

function createMaintainerTimelineChart(canvasId, maintainerName, timelineData) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) {
    console.error("Canvas not found:", canvasId);
    return;
  }
  
  // Calculate dynamic Y-axis range
  const values = timelineData.ownership;
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  
  // Add 10% padding above and below for better visualization
  const range = maxValue - minValue;
  const padding = range * 0.1;
  const yMin = Math.max(0, minValue - padding); // Don't go below 0
  const yMax = Math.min(100, maxValue + padding); // Don't go above 100
  
  new Chart(ctx, {
    type: "line",
    data: {
      labels: timelineData.months,
      datasets: [{
        label: "Ownership %",
        data: timelineData.ownership,
        backgroundColor: "rgba(75, 192, 192, 0.1)",
        borderColor: "rgba(75, 192, 192, 1)",
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointRadius: 4,
        pointBackgroundColor: "rgba(75, 192, 192, 1)",
        pointBorderColor: "#fff",
        pointBorderWidth: 2,
        pointHoverRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        },
        title: {
          display: true,
          text: `${maintainerName} - Cumulative Ownership`,
          align: 'start',
          font: {
            size: 13,
            weight: '600'
          },
          padding: {
            top: 5,
            bottom: 10
          }
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              return context.parsed.y.toFixed(1) + '% of total contributions';
            }
          }
        }
      },
      scales: {
        y: {
          min: yMin,
          max: yMax,
          ticks: {
            callback: function(value) {
              return value.toFixed(1) + '%';
            },
            font: {
              size: 11
            }
          },
          grid: {
            color: 'rgba(0, 0, 0, 0.05)'
          }
        },
        x: {
          ticks: {
            font: {
              size: 10
            },
            maxRotation: 45,
            minRotation: 45
          },
          grid: {
            display: false
          }
        }
      }
    }
  });
}

async function addSubsystemLanguageSection(container, subsystemName) {
  try {
    console.log("Loading language statistics for subsystem:", subsystemName);
    
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Language statistics loading timeout')), 10000)
    );
    
    const languagePromise = fetchJSON(`/api/subsystems/${encodeURIComponent(subsystemName)}/languages`);
    const languageData = await Promise.race([languagePromise, timeoutPromise]);
    
    if (languageData.languages && Object.keys(languageData.languages).length > 0) {
      const languageCard = document.createElement("div");
      languageCard.className = "card";
      languageCard.innerHTML = '<h2>Programming Languages</h2>';
      
      // Create language chart
      const chartContainer = document.createElement("div");
      chartContainer.className = "chart-container language-chart";
      chartContainer.innerHTML = '<canvas id="subsystem-languages-chart"></canvas>';
      languageCard.appendChild(chartContainer);
      
      container.appendChild(languageCard);
      
      // Create the chart after the element is in the DOM
      setTimeout(() => {
        try {
          const langStats = getSubsystemLanguageStats(languageData);
          if (langStats.labels.length > 0) {
            const ctx = document.getElementById("subsystem-languages-chart");
            if (ctx) {
              // Destroy existing chart if it exists
              if (state.charts.subsystemLanguages) {
                state.charts.subsystemLanguages.destroy();
              }
              state.charts.subsystemLanguages = new Chart(ctx, {
                type: "doughnut",
                data: {
                  labels: langStats.labels,
                  datasets: [{
                    label: "Lines of Code",
                    data: langStats.values,
                    backgroundColor: langStats.labels.map((label, index) => {
                      // Use a more visible color for "Others" 
                      if (label === 'Others') {
                        return '#4B5563'; // Dark gray for better visibility against white
                      }
                      // Use vibrant colors for programming languages
                      const colors = [
                        '#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF',
                        '#FF9F40', '#FF6384', '#C9CBCF', '#4BC0C0', '#FF6384',
                        '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40'
                      ];
                      return colors[index % colors.length];
                    })
                  }]
                },
                options: {
                  responsive: true,
                  maintainAspectRatio: false,
                  plugins: {
                    legend: {
                      position: 'bottom',
                      labels: {
                        boxWidth: 10,
                        padding: 8,
                        font: { size: 11 }
                      }
                    },
                    tooltip: {
                      callbacks: {
                        label: function(context) {
                          const total = context.dataset.data.reduce((a, b) => a + b, 0);
                          const percentage = ((context.parsed * 100) / total).toFixed(1);
                          let label = context.label + ': ' + context.parsed.toLocaleString() + ' lines (' + percentage + '%)';
                          
                          // Add explanation for "Others"
                          if (context.label === 'Others') {
                            label += ' (Config/Markup/Styles)';
                          }
                          
                          return label;
                        }
                      }
                    }
                  }
                }
              });
            }
          }
        } catch (error) {
          console.error("Error creating subsystem languages chart:", error);
        }
      }, 100);
      
      // Add summary information
      if (languageData.totals) {
        const summaryDiv = document.createElement("div");
        summaryDiv.className = "language-summary";
        summaryDiv.innerHTML = `
          <p><strong>Total:</strong> ${languageData.totals.files} files, 
          ${languageData.totals.code_lines.toLocaleString()} lines of code</p>
        `;
        languageCard.appendChild(summaryDiv);
      }
    } else {
      console.log("No language statistics available for", subsystemName);
    }
  } catch (error) {
    console.error("Failed to load language statistics for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
}

async function addSubsystemSizeRankingSection(container, subsystemName) {
  try {
    console.log("Loading size ranking for subsystem:", subsystemName);
    
    const timeoutPromise = new Promise((_, reject) => 
      setTimeout(() => reject(new Error('Size ranking loading timeout')), 10000)
    );
    
    const rankingPromise = fetchJSON('/api/subsystems/size-rankings');
    const rankingData = await Promise.race([rankingPromise, timeoutPromise]);
    
    const subsystemRanking = rankingData.rankings[subsystemName];
    
    if (subsystemRanking) {
      const sizeCard = document.createElement("div");
      sizeCard.className = "card";
      sizeCard.innerHTML = '<h2>Subsystem Size</h2>';
      
      const sizeInfo = document.createElement("div");
      sizeInfo.className = "size-ranking-info";
      
      // Determine bucket info and styling
      const bucket = subsystemRanking.size_bucket;
      const bucketDisplayNames = {
        'big': 'Large',
        'medium': 'Medium', 
        'small': 'Small'
      };
      
      const bucketColors = {
        'big': '#10B981',    // Green for large
        'medium': '#F59E0B', // Orange for medium
        'small': '#6B7280'   // Gray for small
      };
      
      const bucketEmojis = {
        'big': '🏢',
        'medium': '🏬',
        'small': '🏪'
      };
      
      const bucketName = bucketDisplayNames[bucket] || bucket;
      const bucketColor = bucketColors[bucket] || '#6B7280';
      const bucketEmoji = bucketEmojis[bucket] || '📦';
      
      // Create size badge
      const sizeBadge = document.createElement("div");
      sizeBadge.className = "size-badge";
      sizeBadge.style.cssText = `
        display: inline-flex;
        align-items: center;
        padding: 8px 16px;
        background-color: ${bucketColor}20;
        border: 2px solid ${bucketColor};
        border-radius: 8px;
        color: ${bucketColor};
        font-weight: bold;
        margin-bottom: 16px;
      `;
      sizeBadge.innerHTML = `
        <span style="margin-right: 8px; font-size: 18px;">${bucketEmoji}</span>
        ${bucketName} Subsystem
      `;
      
      // Create ranking details
      const rankingDetails = document.createElement("div");
      rankingDetails.className = "ranking-details";
      
      // Calculate percentile for better context
      const percentile = Math.round((1 - (subsystemRanking.rank - 1) / subsystemRanking.total_subsystems) * 100);
      
      rankingDetails.innerHTML = `
        <div class="ranking-stat">
          <span class="ranking-label">Rank:</span>
          <span class="ranking-value">#${subsystemRanking.rank} of ${subsystemRanking.total_subsystems} <small>(${percentile}th percentile)</small></span>
        </div>
        <div class="ranking-stat">
          <span class="ranking-label">Total Lines:</span>
          <span class="ranking-value">${subsystemRanking.total_lines.toLocaleString()} lines</span>
        </div>
        <div class="ranking-stat">
          <span class="ranking-label">Size Category:</span>
          <span class="ranking-value">${bucketName} subsystem</span>
        </div>
      `;
      
      sizeInfo.appendChild(sizeBadge);
      sizeInfo.appendChild(rankingDetails);
      sizeCard.appendChild(sizeInfo);
      container.appendChild(sizeCard);
      
    } else {
      console.log("No size ranking available for", subsystemName);
    }
  } catch (error) {
    console.error("Failed to load size ranking for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
}

async function addSubsystemContributionHeatmap(container, subsystemName, period, summaryData = null) {
  try {
    console.log("Loading contribution activity for subsystem:", subsystemName);
    
    // Determine the time range to display
    let displayStart, displayEnd, dataCollectionYear;
    
    if (period.is_yearly) {
      // For yearly view, use the full year
      dataCollectionYear = period.label;
      displayStart = `${dataCollectionYear}-01-01`;
      displayEnd = `${dataCollectionYear}-12-31`;
    } else {
      // For monthly view, collect full year data but display full year with highlighting
      dataCollectionYear = period.from.split('-')[0]; // Get year for data collection
      displayStart = `${dataCollectionYear}-01-01`; // Always show full year
      displayEnd = `${dataCollectionYear}-12-31`;   // Always show full year
    }
    
    const dailyCommits = {};
    let monthlyData = [];

    const upsertDailyEntry = (dateStr, metrics = {}, highlightRange = null) => {
      if (!dateStr) {
        return;
      }
      const commits = Number(metrics.commits ?? metrics.count ?? 0) || 0;
      const additions = Number(metrics.additions ?? metrics.lines_added ?? metrics.added ?? 0) || 0;
      const deletions = Number(metrics.deletions ?? metrics.lines_deleted ?? metrics.deleted ?? 0) || 0;
      if (!dailyCommits[dateStr]) {
        dailyCommits[dateStr] = { commits: 0, additions: 0, deletions: 0 };
      }
      const entry = dailyCommits[dateStr];
      entry.commits += commits;
      entry.additions += additions;
      entry.deletions += deletions;
      if (
        highlightRange &&
        highlightRange.from &&
        highlightRange.to &&
        dateStr >= highlightRange.from &&
        dateStr <= highlightRange.to
      ) {
        entry.isHighlighted = true;
      }
    };

    const accumulatePerDate = (perDate = {}, highlightRange = null) => {
      if (!perDate || typeof perDate !== "object") {
        return;
      }
      Object.entries(perDate).forEach(([dateStr, metrics]) => {
        upsertDailyEntry(dateStr, metrics || {}, highlightRange);
      });
    };

    if (period.is_yearly) {
      monthlyData = await collectSubsystemMonthlyData(subsystemName, dataCollectionYear);
      monthlyData.forEach((monthSummary) => accumulatePerDate(monthSummary?.per_date));
      if (!Object.keys(dailyCommits).length && summaryData?.per_date) {
        accumulatePerDate(summaryData.per_date, { from: summaryData.from, to: summaryData.to });
      }
    } else {
      monthlyData = await collectSubsystemMonthlyData(subsystemName, dataCollectionYear);
      const highlightRange = {
        from: (summaryData && summaryData.from) || period.from,
        to: (summaryData && summaryData.to) || period.to
      };
      const targetSummary = summaryData?.per_date
        ? summaryData
        : monthlyData.find((monthSummary) => highlightRange.from === monthSummary?.from && highlightRange.to === monthSummary?.to);
      if (targetSummary?.per_date) {
        accumulatePerDate(targetSummary.per_date, highlightRange);
      }
    }

    console.log("Collected daily commit data for", Object.keys(dailyCommits).length, "days");
    
    if (Object.keys(dailyCommits).length === 0) {
      console.log("No contribution data available for", subsystemName);
      return;
    }
    
    // Create the contribution activity card
    const heatmapCard = document.createElement("div");
    heatmapCard.className = "card";
    heatmapCard.innerHTML = createTitleWithTooltip(
      "📊 Contribution Activity", 
      "Daily contribution activity for this subsystem during the selected time period. For monthly views, shows only the selected month's commits across the full year layout. For yearly views, shows the full year. Shows the frequency and consistency of development work.",
      "h2"
    );
    
    const heatmapContainer = document.createElement("div");
    heatmapContainer.className = "heatmap-container";
    
    try {
      const heatmapElement = createContributionHeatmap(dailyCommits, displayStart, displayEnd);
      heatmapContainer.appendChild(heatmapElement);
      
      heatmapCard.appendChild(heatmapContainer);
      container.appendChild(heatmapCard);

      if (monthlyData.length > 0) {
        try {
          const highlightMonth = period.is_yearly ? null : (period.from ? period.from.slice(0, 7) : null);
          renderSubsystemLineChangeTimeline(container, monthlyData, subsystemName, highlightMonth);
        } catch (chartError) {
          console.error("Error creating subsystem line change timeline:", chartError);
        }
      }
      
    } catch (error) {
      console.error("Error creating contribution heatmap:", error);
      // Don't add the heatmap if there's an error
    }
    
    // LOC evolution chart for yearly view
    if (period.is_yearly) {
      try {
        const year = period.label;
        const loc = await fetchJSON(`/api/subsystems/${encodeURIComponent(subsystemName)}/loc-evolution/${year}`);
        const series = loc.series || [];
        const locCard = document.createElement("div");
        locCard.className = "card";
        if (series.length > 0) {
          locCard.innerHTML = createTitleWithTooltip(
            "📈 Lines of Code Evolution", 
            "Monthly total code lines for this subsystem across the selected year.",
            "h2"
          ) + '<div style="height: 260px;"><canvas id="chart-loc-evolution"></canvas></div>';
          container.appendChild(locCard);
          const ctx = document.getElementById("chart-loc-evolution").getContext("2d");
          const labels = series.map(s => s.month);
          const values = series.map(s => s.code_lines);
          new Chart(ctx, {
            type: "line",
            data: {
              labels,
              datasets: [{
                label: "Code lines",
                data: values,
                borderColor: "#3b82f6",
                backgroundColor: "rgba(59,130,246,0.15)",
                tension: 0.2,
                fill: true,
              }]
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: { legend: { display: false } },
              scales: { x: { display: true }, y: { display: true } }
            }
          });
        } else {
          locCard.innerHTML = createTitleWithTooltip(
            "📈 Lines of Code Evolution", 
            "Monthly total code lines for this subsystem across the selected year.",
            "h2"
          ) + '<div class="no-data">No LOC data available for this year.</div>';
          container.appendChild(locCard);
        }
      } catch (e) {
        console.warn("LOC evolution fetch failed:", e);
      }
    }
    
  } catch (error) {
    console.error("Failed to load contribution activity for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
}

function renderSubsystemLineChangeTimeline(container, monthlyData, subsystemName, highlightMonthKey = null) {
  if (!Array.isArray(monthlyData) || monthlyData.length === 0) {
    return;
  }

  const entries = monthlyData
    .map((summary) => {
      if (!summary || !summary.from) {
        return null;
      }
      const totals = extractSubsystemLineTotals(summary);
      return {
        monthKey: summary.from.slice(0, 7),
        label: formatSubsystemMonthLabel(summary.from),
        additions: totals.additions,
        deletions: totals.deletions,
      };
    })
    .filter(Boolean)
    .sort((a, b) => (a.monthKey || "").localeCompare(b.monthKey || ""));

  const hasActivity = entries.some((entry) => entry.additions > 0 || entry.deletions > 0);
  if (!hasActivity) {
    return;
  }

  const safeSubsystemId = (subsystemName || "subsystem")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "subsystem";
  const chartId = `chart-subsystem-line-changes-${safeSubsystemId}-${Date.now()}`;

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = createTitleWithTooltip(
    "📈 Line Change Timeline",
    "Monthly lines added (green) and deleted (red) for this subsystem. Use it to spot bursts of activity and quieter periods over the selected year.",
    "h2"
  );

  const chartWrapper = document.createElement("div");
  chartWrapper.style.height = "280px";
  const canvas = document.createElement("canvas");
  canvas.id = chartId;
  chartWrapper.appendChild(canvas);
  card.appendChild(chartWrapper);
  container.appendChild(card);

  const labels = entries.map((entry) => entry.label);
  const additionsData = entries.map((entry) => entry.additions);
  const deletionsData = entries.map((entry) => entry.deletions);
  const netData = entries.map((entry) => entry.additions - entry.deletions);
  const highlightIndex = highlightMonthKey ? entries.findIndex((entry) => entry.monthKey === highlightMonthKey) : -1;

  const addedColors = labels.map((_, idx) => (idx === highlightIndex ? "rgba(16, 185, 129, 0.85)" : "rgba(34, 197, 94, 0.6)"));
  const addedBorders = labels.map((_, idx) => (idx === highlightIndex ? "rgba(16, 185, 129, 1)" : "rgba(22, 163, 74, 1)"));
  const deletedColors = labels.map((_, idx) => (idx === highlightIndex ? "rgba(248, 113, 113, 0.85)" : "rgba(239, 68, 68, 0.6)"));
  const deletedBorders = labels.map((_, idx) => (idx === highlightIndex ? "rgba(239, 68, 68, 1)" : "rgba(220, 38, 38, 1)"));

  const ctx = canvas.getContext("2d");
  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
  }

  const chart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Lines Added",
          data: additionsData,
          backgroundColor: addedColors,
          borderColor: addedBorders,
          borderWidth: 1,
          order: 1,
        },
        {
          label: "Lines Deleted",
          data: deletionsData,
          backgroundColor: deletedColors,
          borderColor: deletedBorders,
          borderWidth: 1,
          order: 1,
        },
        {
          type: "line",
          label: "Net Lines",
          data: netData,
          borderColor: "#2563eb",
          backgroundColor: "rgba(37, 99, 235, 0.15)",
          borderWidth: 2,
          fill: false,
          tension: 0.25,
          pointRadius: 3,
          order: 3,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        legend: { position: "top" },
      },
      scales: {
        x: {
          title: { display: true, text: "Month" },
        },
        y: {
          beginAtZero: true,
          title: { display: true, text: "Lines of Code" },
        },
      },
    },
  });

  state.charts[chartId] = chart;
}

function extractSubsystemLineTotals(summary) {
  if (!summary || typeof summary !== "object") {
    return { additions: 0, deletions: 0 };
  }

  const hasTotalAdditions = typeof summary.total_lines_added === "number";
  const hasTotalDeletions = typeof summary.total_lines_deleted === "number";
  let additions = hasTotalAdditions ? summary.total_lines_added : null;
  let deletions = hasTotalDeletions ? summary.total_lines_deleted : null;

  if (additions !== null && deletions !== null) {
    return { additions, deletions };
  }

  let repoAdditions = 0;
  let repoDeletions = 0;
  const repos = summary.repositories || {};
  for (const repoData of Object.values(repos)) {
    repoAdditions += repoData.lines_added || 0;
    repoDeletions += repoData.lines_deleted || 0;
  }

  return {
    additions: additions !== null ? additions : repoAdditions,
    deletions: deletions !== null ? deletions : repoDeletions,
  };
}

function formatSubsystemMonthLabel(dateStr) {
  if (!dateStr || typeof dateStr !== "string") {
    return "Unknown";
  }
  const [year, month] = dateStr.split("-");
  if (!year || !month) {
    return dateStr;
  }
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  const monthIndex = parseInt(month, 10) - 1;
  const monthLabel = monthNames[monthIndex] || month;
  return `${monthLabel} ${year}`;
}

async function collectSubsystemMonthlyData(subsystemName, year) {
  try {
    const monthlyData = [];
    
    // Try to fetch monthly summaries for the year
    // We'll try all 12 months and collect what's available
    for (let month = 1; month <= 12; month++) {
      try {
        const monthStr = month.toString().padStart(2, '0');
        const fromDate = `${year}-${monthStr}-01`;
        
        // Calculate last day of month
        const lastDay = new Date(year, month, 0).getDate();
        const toDate = `${year}-${monthStr}-${lastDay.toString().padStart(2, '0')}`;
        
        const response = await fetchJSON(`/api/subsystems/${encodeURIComponent(subsystemName)}/month/${fromDate}/${toDate}`);
        
        if (response && !response.error) {
          monthlyData.push(response);
        }
      } catch (error) {
        // Month data not available, skip
        continue;
      }
    }
    
    console.log("Collected", monthlyData.length, "monthly summaries for", subsystemName, "in", year);
    return monthlyData;
    
  } catch (error) {
    console.error("Error collecting monthly data for", subsystemName, ":", error);
    return [];
  }
}

// --------------------------
// Overview Dashboards
// --------------------------

async function showSubsystemsOverviewDashboard() {
  try {
    setViewHeader("Subsystems Overview", "System-wide subsystem statistics and rankings", "Subsystems");
    
    const main = $("main-content");
    main.innerHTML = createLoadingIndicator(
      "Loading Subsystems Overview", 
      "Processing subsystem data and calculating system-wide statistics..."
    );
    
    const overviewData = await fetchJSON('/api/subsystems/overview');
    
    clearMain();
    setViewHeader("Subsystems Overview", "System-wide subsystem statistics and rankings", "Subsystems");
    
    // System statistics KPI cards
    const kpiContainer = document.createElement("div");
    kpiContainer.className = "kpi-grid";
    
    const totalSystemLines = overviewData.size_data?.total_system_lines || 0;
    const totalGitLines = overviewData.size_data?.total_git_lines || 0;
    const totalSubsystems = overviewData.total_subsystems || 0;
    const deadSubsystems = overviewData.dead_subsystems?.count || 0;
    const averageLinesPerSubsystem = totalSubsystems > 0 ? Math.round(totalSystemLines / totalSubsystems) : 0;
    const ratio = totalSystemLines > 0 ? (totalGitLines / totalSystemLines).toFixed(1) : 0;
    
    const kpis = [
      { 
        label: "Total Code Lines (cloc)", 
        value: totalSystemLines.toLocaleString(),
        tooltip: "Actual code lines only, excluding blanks and comments. Measured by cloc tool."
      },
      { 
        label: "Total Git Lines (blame)", 
        value: totalGitLines.toLocaleString(),
        tooltip: `All lines in tracked files including blanks and comments. Git blame counts ~${ratio}x more lines than cloc.`
      },
      { 
        label: "Total Subsystems", 
        value: totalSubsystems.toLocaleString(),
        tooltip: "Number of subsystems/repositories in the codebase."
      },
      { 
        label: "Dead Subsystems", 
        value: deadSubsystems.toLocaleString(),
        tooltip: "Subsystems with no commits in the last 3 months."
      }
    ];
    
    kpis.forEach((k) => {
      const card = document.createElement("div");
      card.className = "kpi-card";
      card.title = k.tooltip || "";
      card.innerHTML = '<div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>';
      kpiContainer.appendChild(card);
    });
    
    main.appendChild(kpiContainer);
    
    // Top largest subsystems
    const topSizeSection = document.createElement("div");
    topSizeSection.className = "card";
    topSizeSection.innerHTML = createTitleWithTooltip(
      "🎯 Largest Subsystems", 
      "Subsystems ranked by total lines of code. Rankings are divided into three buckets: Big (top third), Medium (middle third), and Small (bottom third) based on codebase size.",
      "h2"
    );
    
    const rankings = overviewData.size_data?.rankings || {};
    const topSubsystems = Object.entries(rankings)
      .sort((a, b) => a[1].rank - b[1].rank)
      .slice(0, 10);
    
    const topSizeList = document.createElement("div");
    topSizeList.className = "ranking-list-no-scroll";
    
    topSubsystems.forEach(([name, data]) => {
      const item = document.createElement("div");
      item.className = "ranking-item clickable";
      item.onclick = () => navigateToSubsystem(name);
      
      const bucketColors = { 'big': '#10B981', 'medium': '#F59E0B', 'small': '#6B7280' };
      const bucketColor = bucketColors[data.size_bucket] || '#6B7280';
      
      item.innerHTML = `
        <div class="rank-number" style="background: ${bucketColor}20; color: ${bucketColor};">#${data.rank}</div>
        <div class="rank-content">
          <div class="rank-name">${name}</div>
          <div class="rank-details">${data.total_lines.toLocaleString()} lines</div>
        </div>
      `;
      topSizeList.appendChild(item);
    });
    
    topSizeSection.appendChild(topSizeList);
    main.appendChild(topSizeSection);
    
    // Language distribution chart
    try {
      console.log("About to call addSubsystemLanguageDistribution");
      await addSubsystemLanguageDistribution(main);
      console.log("addSubsystemLanguageDistribution completed");
    } catch (error) {
      console.error("Error loading language distribution:", error);
      
      // Show an error section so user knows something went wrong
      const errorSection = document.createElement("div");
      errorSection.className = "card language-distribution-section";
      errorSection.innerHTML = `
        <h2>💻 Subsystems by Primary Language</h2>
        <div class="no-data-message">
          <p>Error loading language distribution: ${error.message}</p>
          <p>Check the browser console for more details.</p>
        </div>
      `;
      main.appendChild(errorSection);
    }
    
    // Language lines distribution
    try {
      console.log("About to call addLanguageLinesDistribution");
      await addLanguageLinesDistribution(main);
      console.log("addLanguageLinesDistribution completed");
    } catch (error) {
      console.error("Error loading language lines distribution:", error);
    }
    
    try {
      addSubsystemWorkloadTrendSection(main, overviewData.trend);
    } catch (error) {
      console.error("Error rendering subsystem workload trend:", error);
    }

    try {
      addRecentSubsystemWorkloadSection(main, overviewData.recent_trend);
    } catch (error) {
      console.error("Error rendering recent subsystem workload chart:", error);
    }
    
    // Activity section
    if (overviewData.activity) {
      const activitySection = document.createElement("div");
      activitySection.className = "card";
      activitySection.innerHTML = createTitleWithTooltip(
        `🔥 Most Active (${overviewData.activity.period})`, 
        "Subsystems and developers ranked by activity level during the specified period. Shows both commit frequency and lines changed to identify the most active areas of development.",
        "h2"
      );
      
      const activityGrid = document.createElement("div");
      activityGrid.className = "activity-grid";
      
      // Most commits
      const commitsCard = document.createElement("div");
      commitsCard.className = "activity-card";
      commitsCard.innerHTML = '<h3>Most Commits</h3>';
      
      const commitsList = document.createElement("div");
      commitsList.className = "activity-list";
      
      overviewData.activity.most_commits.slice(0, 10).forEach((subsystem, index) => {
        if (subsystem.commits > 0) {
          const item = document.createElement("div");
          item.className = "activity-item clickable";
          item.onclick = () => navigateToSubsystem(subsystem.name);
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name">${subsystem.name}</span>
            <span class="activity-value">${subsystem.commits} commits</span>
          `;
          commitsList.appendChild(item);
        }
      });
      
      commitsCard.appendChild(commitsList);
      activityGrid.appendChild(commitsCard);
      
      // Most changes
      const changesCard = document.createElement("div");
      changesCard.className = "activity-card";
      changesCard.innerHTML = '<h3>Most Code Changes</h3>';
      
      const changesList = document.createElement("div");
      changesList.className = "activity-list";
      
      overviewData.activity.most_changes.slice(0, 10).forEach((subsystem, index) => {
        if (subsystem.lines_changed > 0) {
          const item = document.createElement("div");
          item.className = "activity-item clickable";
          item.onclick = () => navigateToSubsystem(subsystem.name);
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name">${subsystem.name}</span>
            <span class="activity-value">${subsystem.lines_changed.toLocaleString()} lines</span>
          `;
          changesList.appendChild(item);
        }
      });
      
      changesCard.appendChild(changesList);
      activityGrid.appendChild(changesCard);
      
      activitySection.appendChild(activityGrid);
      main.appendChild(activitySection);
    }
    
    // Dead subsystems section
    if (overviewData.dead_subsystems && overviewData.dead_subsystems.count > 0) {
      const deadSection = document.createElement("div");
      deadSection.className = "card";
      deadSection.innerHTML = `<h2>⚠️ Potentially Dead Subsystems (${overviewData.dead_subsystems.count})</h2>`;
      
      const deadList = document.createElement("div");
      deadList.className = "dead-subsystems-list";
      
      // Sort dead subsystems by months since activity (descending)
      const sortedDeadSubsystems = overviewData.dead_subsystems.subsystems
        .slice()
        .sort((a, b) => (b.months_since_activity || 999) - (a.months_since_activity || 999))
        .slice(0, 10); // Show top 10
      
      sortedDeadSubsystems.forEach((subsystem) => {
        const item = document.createElement("div");
        item.className = "dead-subsystem-item clickable";
        item.onclick = () => navigateToSubsystem(subsystem.name);
        
        let activityInfo = "No activity found";
        if (subsystem.last_activity_date) {
          activityInfo = `Last activity: ${subsystem.last_activity_date}`;
          if (subsystem.months_since_activity) {
            activityInfo += ` (${subsystem.months_since_activity} months ago)`;
          }
        }
        
        item.innerHTML = `
          <div class="dead-icon">⚠️</div>
          <div class="dead-content">
            <div class="dead-name">${subsystem.name}</div>
            <div class="dead-details">${activityInfo}</div>
          </div>
        `;
        deadList.appendChild(item);
      });
      
      deadSection.appendChild(deadList);
      main.appendChild(deadSection);
    }
    
  } catch (error) {
    console.error("Error loading subsystems overview:", error);
    clearMain();
    setViewHeader("Subsystems Overview", "Error loading overview data", "Error");
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load subsystems overview: ' + error.message + '</div>';
  }
}

function addSubsystemWorkloadTrendSection(container, trendData) {
  if (!container || !trendData) {
    return;
  }
  const months = Array.isArray(trendData.months) ? trendData.months.filter(Boolean) : [];
  const rawSeries = Array.isArray(trendData.series) ? trendData.series : [];
  if (!months.length || !rawSeries.length) {
    return;
  }

  const filteredSeries = rawSeries
    .map((entry) => ({
      name: entry.name,
      values: Array.isArray(entry.values) ? entry.values : [],
      total: entry.total || 0,
      isAggregate: !!entry.is_aggregate
    }))
    .filter((entry) => entry.values.some((value) => value > 0));

  if (!filteredSeries.length) {
    return;
  }

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = createTitleWithTooltip(
    "📊 Workload Distribution (Last 12 Months)",
    "Stacked monthly view of the busiest subsystems measured by lines changed. Quickly shows where engineering effort concentrated over the past year.",
    "h2"
  );

  const chartWrapper = document.createElement("div");
  chartWrapper.style.marginTop = "12px";
  chartWrapper.style.height = "320px";
  const canvas = document.createElement("canvas");
  const chartId = `chart-subsystem-trend-${Date.now()}`;
  canvas.id = chartId;
  chartWrapper.appendChild(canvas);
  card.appendChild(chartWrapper);

  const ctx = canvas.getContext("2d");
  const labels = months.map((monthKey) => formatSubsystemMonthLabel(monthKey));
  const colorPalette = [
    "rgba(37, 99, 235, 0.85)",
    "rgba(34, 197, 94, 0.85)",
    "rgba(249, 115, 22, 0.85)",
    "rgba(168, 85, 247, 0.85)",
    "rgba(14, 165, 233, 0.85)",
    "rgba(244, 63, 94, 0.85)",
    "rgba(5, 150, 105, 0.85)",
    "rgba(234, 179, 8, 0.85)"
  ];
  const borderPalette = [
    "#2563eb",
    "#22c55e",
    "#f97316",
    "#a855f7",
    "#0ea5e9",
    "#f43f5e",
    "#059669",
    "#eab308"
  ];

  const datasets = filteredSeries.map((entry, idx) => {
    const paddedValues = entry.values.slice(0, months.length);
    while (paddedValues.length < months.length) {
      paddedValues.push(0);
    }
    const paletteIndex = idx % colorPalette.length;
    const backgroundColor = entry.isAggregate ? "rgba(107, 114, 128, 0.5)" : colorPalette[paletteIndex];
    const borderColor = entry.isAggregate ? "rgba(107, 114, 128, 0.9)" : borderPalette[paletteIndex];
    return {
      label: entry.name,
      data: paddedValues,
      backgroundColor,
      borderColor,
      borderWidth: entry.isAggregate ? 1 : 0,
      stack: "subsystem-workload"
    };
  });

  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
  }

  const trendChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "index",
        intersect: false
      },
      plugins: {
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            label(context) {
              const value = context.parsed.y ?? context.parsed ?? 0;
              const formatted = typeof value === "number" ? value.toLocaleString() : value;
              return `${context.dataset.label}: ${formatted} lines`;
            }
          }
        }
      },
      scales: {
        x: {
          stacked: true
        },
        y: {
          stacked: true,
          beginAtZero: true,
          title: { display: true, text: "Lines changed" },
          ticks: {
            callback(value) {
              return typeof value === "number" ? value.toLocaleString() : value;
            }
          }
        }
      }
    }
  });

  state.charts[chartId] = trendChart;

  const summary = document.createElement("div");
  summary.style.display = "flex";
  summary.style.flexWrap = "wrap";
  summary.style.gap = "8px";
  summary.style.marginTop = "14px";

  filteredSeries.forEach((entry) => {
    if ((entry.total || 0) <= 0) {
      return;
    }
    const pill = document.createElement("span");
    pill.style.backgroundColor = "var(--background-secondary)";
    pill.style.border = "1px solid var(--border)";
    pill.style.borderRadius = "999px";
    pill.style.padding = "4px 10px";
    pill.style.fontSize = "0.85em";
    pill.textContent = `${entry.name}: ${(entry.total || 0).toLocaleString()} lines`;
    summary.appendChild(pill);
  });

  if (summary.children.length > 0) {
    card.appendChild(summary);
  }

  container.appendChild(card);
}

function addRecentSubsystemWorkloadSection(container, recentTrendData) {
  if (!container || !recentTrendData) {
    return;
  }

  const months = Array.isArray(recentTrendData.months) ? recentTrendData.months.filter(Boolean) : [];
  const rawSeries = Array.isArray(recentTrendData.series) ? recentTrendData.series : [];
  if (!months.length || !rawSeries.length) {
    return;
  }

  const labels = months.map((monthKey) => formatSubsystemMonthLabel(monthKey));
  const normalizedSeries = rawSeries
    .map((entry) => {
      const values = Array.isArray(entry.values) ? entry.values : [];
      const safeValues = labels.map((_, idx) => {
        const value = values[idx];
        return typeof value === "number" ? value : 0;
      });
      const total = typeof entry.total === "number"
        ? entry.total
        : safeValues.reduce((sum, value) => sum + value, 0);
      return {
        name: entry.name,
        values: safeValues,
        total
      };
    })
    .filter((entry) => entry.total > 0)
    .sort((a, b) => b.total - a.total);

  if (!normalizedSeries.length) {
    return;
  }

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = createTitleWithTooltip(
    "🔎 Detailed Workload (Last 2 Months)",
    "Shows per-subsystem line changes for the two latest months without grouping so short-term hotspots stay visible.",
    "h2"
  );

  const helperText = document.createElement("p");
  helperText.style.margin = "4px 0 0";
  helperText.style.fontSize = "0.9rem";
  helperText.style.color = "var(--text-muted, #6b7280)";
  helperText.textContent = "Hover to inspect subsystem names and exact line counts.";
  card.appendChild(helperText);

  const chartWrapper = document.createElement("div");
  chartWrapper.style.marginTop = "12px";
  const dynamicHeight = Math.min(640, Math.max(260, normalizedSeries.length * 18));
  chartWrapper.style.height = `${dynamicHeight}px`;
  const canvas = document.createElement("canvas");
  const chartId = `chart-subsystem-recent-${Date.now()}`;
  canvas.id = chartId;
  chartWrapper.appendChild(canvas);
  card.appendChild(chartWrapper);
  container.appendChild(card);

  const ctx = canvas.getContext("2d");
  const colorForIndex = (idx, alpha = 0.75) => {
    const hue = (idx * 37) % 360;
    return `hsla(${hue}, 65%, 55%, ${alpha})`;
  };

  const datasets = normalizedSeries.map((entry, idx) => ({
    label: entry.name,
    data: entry.values,
    backgroundColor: colorForIndex(idx, 0.7),
    borderColor: colorForIndex(idx, 1),
    borderWidth: 1,
    stack: "recent-subsystem-workload"
  }));

  if (state.charts[chartId]) {
    state.charts[chartId].destroy();
  }

  const recentChart = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: {
        mode: "nearest",
        intersect: true,
        axis: "x"
      },
      plugins: {
        legend: {
          display: normalizedSeries.length <= 18,
          position: "bottom"
        },
        tooltip: {
          mode: "nearest",
          intersect: true,
          callbacks: {
            label(context) {
              const value = context.parsed.y ?? context.parsed ?? 0;
              const formatted = typeof value === "number" ? value.toLocaleString() : value;
              return `${context.dataset.label}: ${formatted} lines`;
            }
          }
        }
      },
      scales: {
        x: { stacked: true },
        y: {
          stacked: true,
          beginAtZero: true,
          title: { display: true, text: "Lines changed" },
          ticks: {
            callback(value) {
              return typeof value === "number" ? value.toLocaleString() : value;
            }
          }
        }
      }
    }
  });

  state.charts[chartId] = recentChart;
}

async function showUsersOverviewDashboard() {
  try {
    // Prevent concurrent executions
    if (state.loadingUsersOverview) {
      console.log("Users overview already loading, skipping duplicate call");
      return;
    }
    
    state.loadingUsersOverview = true;
    console.log("Starting users overview dashboard loading");
    
    setViewHeader("Developers Overview", "Development team statistics and activity", "Developers");
    
    const main = $("main-content");
    main.innerHTML = "";
    
    const overviewData = await fetchJSON('/api/users/overview');
    let capacityLeaders = [];
    try {
      const capacityResponse = await fetchJSON('/api/developers/capacity-profiles?limit=10');
      capacityLeaders = capacityResponse.developers || [];
    } catch (error) {
      console.warn("Could not load developer capacity profiles:", error);
    }
    
    clearMain();
    setViewHeader("Developers Overview", "Development team statistics and activity", "Developers");
    
    // Developer summary
    const summarySection = document.createElement("div");
    summarySection.className = "card";
    summarySection.innerHTML = '<h2>👥 Developer Summary</h2>';
    
    const summaryGrid = document.createElement("div");
    summaryGrid.className = "overview-grid";
    
    // Use aggregate stats from backend (not just top 10)
    const monthlyActive = overviewData.activity?.total_active_users || 0;
    const yearlyActive = overviewData.yearly?.total_active_users || 0;
    const totalCommitsMonthly = overviewData.activity?.total_commits || 0;
    const totalCommitsYearly = overviewData.yearly?.total_commits || 0;
    
    const teamStats = [
      { title: 'Active Developers', value: monthlyActive, subtitle: 'this month', emoji: '👨‍💻', color: '#10B981' },
      { title: 'Total Commits', value: totalCommitsMonthly, subtitle: 'this month', emoji: '📝', color: '#3B82F6' },
      { title: 'Yearly Active', value: yearlyActive, subtitle: 'developers', emoji: '📅', color: '#8B5CF6' },
      { title: 'Yearly Commits', value: totalCommitsYearly, subtitle: 'total', emoji: '🚀', color: '#F59E0B' }
    ];
    
    teamStats.forEach(stat => {
      const statCard = document.createElement("div");
      statCard.className = "overview-stat-card";
      statCard.innerHTML = `
        <div class="stat-header" style="color: ${stat.color};">
          <span class="stat-emoji">${stat.emoji}</span>
          <span class="stat-title">${stat.title}</span>
        </div>
        <div class="stat-value">${stat.value.toLocaleString()}</div>
        <div class="stat-subtitle">${stat.subtitle}</div>
      `;
      summaryGrid.appendChild(statCard);
    });
    
    summarySection.appendChild(summaryGrid);
    main.appendChild(summarySection);
    
    if (capacityLeaders.length > 0) {
      const capacityCard = document.createElement("div");
      capacityCard.className = "card";
      capacityCard.innerHTML = createTitleWithTooltip(
        "🧠 Blame Capacity Leaders",
        "Top developers ranked by their theoretical headcount based on git blame ownership. Calculated using the same language-weighted capacity rules as team analysis. Only includes developers with ≥0.9 headcount.",
        "h2"
      );

      const capacityList = document.createElement("div");
      capacityList.className = "activity-list";

      capacityLeaders.slice(0, 10).forEach((dev, index) => {
        const isActive = state.users.some(u => u.slug === dev.slug);
        const item = document.createElement("div");
        item.className = isActive ? "activity-item clickable" : "activity-item inactive";
        if (isActive) {
          item.onclick = () => navigateToUser(dev.slug);
        } else {
          item.style.cursor = "default";
          item.title = "Inactive contributor (no recent activity in analysis period)";
        }

        const nameStyle = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
        item.innerHTML = `
          <span class="activity-rank">${index + 1}.</span>
          <span class="activity-name"${nameStyle}>${dev.display_name || dev.slug}</span>
          <span class="activity-value">${dev.developer_equivalent.toFixed(1)} devs · ${(dev.total_lines || 0).toLocaleString()} lines</span>
        `;

        const topLanguages = Object.entries(dev.language_breakdown || {})
          .sort((a, b) => (b[1].lines || 0) - (a[1].lines || 0))
          .slice(0, 2)
          .map(([lang, data]) => `${lang} (${data.theoretical_devs.toFixed(1)} devs)`);

        if (topLanguages.length > 0) {
          const langLine = document.createElement("div");
          langLine.style.flexBasis = "100%";
          langLine.style.fontSize = "0.85em";
          langLine.style.color = "var(--text-secondary)";
          langLine.style.marginLeft = "2.5rem";
          langLine.textContent = `Top: ${topLanguages.join(', ')}`;
          item.appendChild(langLine);
        }

        capacityList.appendChild(item);
      });

      const capacityNote = document.createElement("div");
      capacityNote.className = "note-text";
      capacityNote.style.marginTop = "8px";
      capacityNote.textContent = "Measured using git blame ownership share × subsystem language lines; displayed only for developers worth ≥0.9 headcount.";

      capacityCard.appendChild(capacityList);
      capacityCard.appendChild(capacityNote);
      main.appendChild(capacityCard);
    }
    
    // Monthly activity
    if (overviewData.activity) {
      const monthlySection = document.createElement("div");
      monthlySection.className = "card";
      monthlySection.innerHTML = `<h2>📈 Monthly Activity (${overviewData.activity.period})</h2>`;
      
      const monthlyGrid = document.createElement("div");
      monthlyGrid.className = "activity-grid";
      
      // Most active by commits
      const commitsCard = document.createElement("div");
      commitsCard.className = "activity-card";
      commitsCard.innerHTML = createTitleWithTooltip(
        'Most Active Committers', 
        'Developers ranked by number of commits across all subsystems for the current month. Shows who is actively making changes to the codebase.',
        'h3'
      );
      
      const commitsList = document.createElement("div");
      commitsList.className = "activity-list";
      
      overviewData.activity.most_active_monthly.slice(0, 10).forEach((user, index) => {
        if (user.monthly_commits > 0) {
          const isActive = state.users.some(u => u.slug === user.slug);
          const item = document.createElement("div");
          item.className = isActive ? "activity-item clickable" : "activity-item inactive";
          
          if (isActive) {
            item.onclick = () => navigateToUser(user.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const nameClass = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name"${nameClass}>${user.display_name}</span>
            <span class="activity-value">${user.monthly_commits} commits</span>
          `;
          commitsList.appendChild(item);
        }
      });
      
      commitsCard.appendChild(commitsList);
      monthlyGrid.appendChild(commitsCard);
      
      // Most productive by lines
      const linesCard = document.createElement("div");
      linesCard.className = "activity-card";
      linesCard.innerHTML = createTitleWithTooltip(
        "Most Productive", 
        "Developers ranked by total lines added across all subsystems for the current month. This includes additions, but excludes deletions.", 
        "h3"
      );
      
      const linesList = document.createElement("div");
      linesList.className = "activity-list";
      
      overviewData.activity.most_productive_monthly.slice(0, 10).forEach((user, index) => {
        if (user.monthly_lines_added > 0) {
          const isActive = state.users.some(u => u.slug === user.slug);
          const item = document.createElement("div");
          item.className = isActive ? "activity-item clickable" : "activity-item inactive";
          
          if (isActive) {
            item.onclick = () => navigateToUser(user.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const nameClass = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name"${nameClass}>${user.display_name}</span>
            <span class="activity-value">+${user.monthly_lines_added.toLocaleString()} lines</span>
          `;
          linesList.appendChild(item);
        }
      });
      
      linesCard.appendChild(linesList);
      monthlyGrid.appendChild(linesCard);
      
      monthlySection.appendChild(monthlyGrid);
      main.appendChild(monthlySection);
    }
    
    // Yearly leaders
    if (overviewData.yearly) {
      const yearlySection = document.createElement("div");
      yearlySection.className = "card";
      yearlySection.innerHTML = `<h2>🏆 ${overviewData.yearly.year} Leaders</h2>`;
      
      const yearlyGrid = document.createElement("div");
      yearlyGrid.className = "activity-grid";
      
      // Top committers
      const yearCommitsCard = document.createElement("div");
      yearCommitsCard.className = "activity-card";
      yearCommitsCard.innerHTML = createTitleWithTooltip(
        'Top Committers', 
        'Developers ranked by total number of commits across all subsystems for the entire year. Shows the most consistently active contributors.',
        'h3'
      );
      
      const yearCommitsList = document.createElement("div");
      yearCommitsList.className = "activity-list";
      
      overviewData.yearly.most_active_yearly.slice(0, 10).forEach((user, index) => {
        if (user.yearly_commits > 0) {
          const isActive = state.users.some(u => u.slug === user.slug);
          const item = document.createElement("div");
          item.className = isActive ? "activity-item clickable" : "activity-item inactive";
          
          if (isActive) {
            item.onclick = () => navigateToUser(user.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const nameClass = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name"${nameClass}>${user.display_name}</span>
            <span class="activity-value">${user.yearly_commits.toLocaleString()} commits</span>
          `;
          yearCommitsList.appendChild(item);
        }
      });
      
      yearCommitsCard.appendChild(yearCommitsList);
      yearlyGrid.appendChild(yearCommitsCard);
      
      // Top contributors
      const yearLinesCard = document.createElement("div");
      yearLinesCard.className = "activity-card";
      yearLinesCard.innerHTML = '<h3>Top Contributors</h3>';
      
      const yearLinesList = document.createElement("div");
      yearLinesList.className = "activity-list";
      
      overviewData.yearly.most_productive_yearly.slice(0, 10).forEach((user, index) => {
        if (user.yearly_lines_added > 0) {
          const isActive = state.users.some(u => u.slug === user.slug);
          const item = document.createElement("div");
          item.className = isActive ? "activity-item clickable" : "activity-item inactive";
          
          if (isActive) {
            item.onclick = () => navigateToUser(user.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const nameClass = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          item.innerHTML = `
            <span class="activity-rank">${index + 1}.</span>
            <span class="activity-name"${nameClass}>${user.display_name}</span>
            <span class="activity-value">+${user.yearly_lines_added.toLocaleString()} lines</span>
          `;
          yearLinesList.appendChild(item);
        }
      });
      
      yearLinesCard.appendChild(yearLinesList);
      yearlyGrid.appendChild(yearLinesCard);
      
      yearlySection.appendChild(yearlyGrid);
      main.appendChild(yearlySection);
    }
    
    await addBadgeStatistics(main);
    await addOwnershipStatistics(main);
    
    state.loadingUsersOverview = false;
    console.log("Users overview dashboard loading completed");
    
  } catch (error) {
    console.error("Error loading users overview:", error);
    clearMain();
    setViewHeader("Developers Overview", "Error loading overview data", "Error");
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load developers overview: ' + error.message + '</div>';
  } finally {
    state.loadingUsersOverview = false;
    console.log("Users overview dashboard loading finished");
  }
}

async function addBadgeStatistics(container) {
  try {
    console.log("Loading badge statistics for users overview...");
    
    if (container.querySelector('.badge-statistics-section')) {
      console.log("Badge statistics section already exists, skipping");
      return;
    }
    
    const badgeSection = document.createElement("div");
    badgeSection.className = "card badge-statistics-section";
    badgeSection.innerHTML = createTitleWithTooltip(
      "🏆 Achievement Badges Overview", 
      "Summary of badges earned by developers across the team. Shows distribution of productivity awards, maintainer recognitions, and ownership achievements.",
      "h2"
    );
    container.appendChild(badgeSection);

    const badgeOverview = await fetchJSON('/api/users/badges-overview');
    if (!badgeOverview || !badgeOverview.summary) {
      const emptyMessage = document.createElement("div");
      emptyMessage.className = "note-text";
      emptyMessage.textContent = "Badge data is not available. Run the update pipeline to generate ownership analytics.";
      badgeSection.appendChild(emptyMessage);
      return;
    }

    const summary = badgeOverview.summary || {};
    const totalUsers = summary.total_users || state.users.length || 0;
    const usersWithBadgesCount = summary.users_with_badges || 0;
    const totalBadges = summary.total_badges || 0;
    const badgeTypes = summary.badge_types || {};
    const topBadgeHolders = badgeOverview.top_badge_holders || [];
    const ownershipLeaders = badgeOverview.top_ownership_holders || [];

    const statsGrid = document.createElement("div");
    statsGrid.className = "badge-stats-grid";

    const badgeStats = [
      { title: 'Users with Badges', value: usersWithBadgesCount, subtitle: `out of ${totalUsers} developers`, emoji: '🎖️', color: '#F59E0B' },
      { title: 'Total Badges', value: totalBadges, subtitle: 'across all users', emoji: '🏆', color: '#10B981' },
      { title: 'Productivity Awards', value: badgeTypes.productivity || 0, subtitle: 'most productive dev', emoji: '🚀', color: '#3B82F6' },
      { title: 'Ownership Badges', value: badgeTypes.ownership_percentage || 0, subtitle: 'significant ownership', emoji: '👑', color: '#8B5CF6' }
    ];

    badgeStats.forEach(stat => {
      const statCard = document.createElement("div");
      statCard.className = "badge-stat-card";
      statCard.innerHTML = `
        <div class="stat-icon" style="color: ${stat.color};">
          <span class="stat-emoji">${stat.emoji}</span>
        </div>
        <div class="stat-content">
          <div class="stat-title">${stat.title}</div>
          <div class="stat-value" style="color: ${stat.color};">${stat.value.toLocaleString()}</div>
          <div class="stat-subtitle">${stat.subtitle}</div>
        </div>
      `;
      statsGrid.appendChild(statCard);
    });

    badgeSection.appendChild(statsGrid);

    if (topBadgeHolders.length > 0) {
      const contentLayout = document.createElement("div");
      contentLayout.className = "badge-content-layout";
      
      const highlightHolders = topBadgeHolders.slice(0, 8);
      const topHoldersDiv = document.createElement("div");
      topHoldersDiv.className = "badge-holders-section";
      topHoldersDiv.innerHTML = '<h3>🌟 Top Badge Holders</h3>';

      const holdersList = document.createElement("div");
      holdersList.className = "badge-holders-grid";

      highlightHolders.forEach((holder, index) => {
        const holderItem = document.createElement("div");
        holderItem.className = "badge-holder-card";
        holderItem.classList.add(state.users.some(u => u.slug === holder.slug) ? "clickable" : "inactive");
        if (state.users.some(u => u.slug === holder.slug)) {
          holderItem.onclick = () => navigateToUser(holder.slug);
        } else {
          holderItem.style.cursor = "default";
          holderItem.title = "Inactive contributor (no recent activity in analysis period)";
        }

        const productivityBadges = holder.type_counts?.productivity || 0;
        const ownershipBadges = holder.type_counts?.ownership_percentage || 0;
        const maintainerBadges = holder.type_counts?.maintainer || 0;

        holderItem.innerHTML = `
          <div class="holder-rank">
            <span class="rank-number">${index + 1}</span>
          </div>
          <div class="holder-info">
            <div class="holder-name">${holder.display_name || holder.slug}</div>
            <div class="holder-badges">
              ${productivityBadges > 0 ? `<span class="mini-badge productivity">🚀 ${productivityBadges}</span>` : ''}
              ${ownershipBadges > 0 ? `<span class="mini-badge ownership">👑 ${ownershipBadges}</span>` : ''}
              ${maintainerBadges > 0 ? `<span class="mini-badge maintainer">🔧 ${maintainerBadges}</span>` : ''}
            </div>
          </div>
          <div class="holder-total">
            <span class="total-count">${holder.badge_count}</span>
            <span class="total-label">badges</span>
          </div>
        `;
        holdersList.appendChild(holderItem);
      });

      topHoldersDiv.appendChild(holdersList);
      contentLayout.appendChild(topHoldersDiv);
      badgeSection.appendChild(contentLayout);

      const rankingGrid = document.createElement("div");
      rankingGrid.className = "ranking-grid";
      rankingGrid.style.marginTop = "20px";

      const topBadgesCard = document.createElement("div");
      topBadgesCard.className = "ranking-list";
      topBadgesCard.innerHTML = `
        <div class="ranking-header">
          <span class="ranking-emoji">🏆</span>
          <div class="title-with-help">
            <div>
              <h3 style="margin: 0;">Top 20 Badge Holders</h3>
              <p class="ranking-subtitle">By total number of badges</p>
            </div>
          </div>
        </div>
      `;

      const topBadgesList = document.createElement("div");
      topBadgesList.className = "ranking-items";

      topBadgeHolders.slice(0, 20).forEach((holder, index) => {
        const item = document.createElement("div");
        const isActive = state.users.some(u => u.slug === holder.slug);
        item.className = isActive ? "ranking-item clickable" : "ranking-item inactive";
        if (isActive) {
          item.onclick = () => navigateToUser(holder.slug);
        } else {
          item.style.cursor = "default";
          item.title = "Inactive contributor (no recent activity in analysis period)";
        }

        const productivityBadges = holder.type_counts?.productivity || 0;
        const ownershipBadges = holder.type_counts?.ownership_percentage || 0;
        const maintainerBadges = holder.type_counts?.maintainer || 0;

        item.innerHTML = `
          <span class="ranking-position">#${index + 1}</span>
          <span class="ranking-name">${holder.display_name || holder.slug}</span>
          <div class="ranking-meta">
            <span class="ranking-value">${holder.badge_count} total</span>
            <span class="ranking-subtext" style="font-size: 0.85em; color: #94a3b8;">
              ${productivityBadges > 0 ? `🚀${productivityBadges} ` : ''}${ownershipBadges > 0 ? `👑${ownershipBadges} ` : ''}${maintainerBadges > 0 ? `🔧${maintainerBadges}` : ''}
            </span>
          </div>
        `;
        topBadgesList.appendChild(item);
      });

      topBadgesCard.appendChild(topBadgesList);
      rankingGrid.appendChild(topBadgesCard);

      if (ownershipLeaders.length > 0) {
        const ownershipBadgesCard = document.createElement("div");
        ownershipBadgesCard.className = "ranking-list";
        ownershipBadgesCard.innerHTML = `
          <div class="ranking-header">
            <span class="ranking-emoji">👑</span>
            <div class="title-with-help">
              <div>
                <h3 style="margin: 0;">Top 20 Ownership Badge Holders</h3>
                <p class="ranking-subtitle">By number of ownership badges</p>
              </div>
            </div>
          </div>
        `;

        const ownershipBadgesList = document.createElement("div");
        ownershipBadgesList.className = "ranking-items";

        ownershipLeaders.slice(0, 20).forEach((holder, index) => {
          const item = document.createElement("div");
          const isActive = state.users.some(u => u.slug === holder.slug);
          item.className = isActive ? "ranking-item clickable" : "ranking-item inactive";
          if (isActive) {
            item.onclick = () => navigateToUser(holder.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }

          const subsystems = holder.subsystems || [];
          const subsystemsText = subsystems.join(', ');

          item.innerHTML = `
            <span class="ranking-position">#${index + 1}</span>
            <span class="ranking-name">${holder.display_name || holder.slug}</span>
            <div class="ranking-meta">
              <span class="ranking-value">${holder.ownership_badge_count} subsystems</span>
              <span class="ranking-subtext" style="font-size: 0.85em; color: #94a3b8;" title="${subsystemsText}">
                ${subsystemsText.length > 30 ? subsystemsText.substring(0, 30) + '...' : subsystemsText}
              </span>
            </div>
          `;
          ownershipBadgesList.appendChild(item);
        });

        ownershipBadgesCard.appendChild(ownershipBadgesList);
        rankingGrid.appendChild(ownershipBadgesCard);
      }

      badgeSection.appendChild(rankingGrid);
    } else {
      const emptyMessage = document.createElement("div");
      emptyMessage.className = "note-text";
      emptyMessage.style.marginTop = "12px";
      emptyMessage.textContent = "No badge data available for the current analysis window.";
      badgeSection.appendChild(emptyMessage);
    }

  } catch (error) {
    console.error("Error loading badge statistics:", error);
  }
}

async function addOwnershipStatistics(container) {
  try {
    console.log("Loading ownership statistics for users overview...");
    
    // Check if section already exists
    if (container.querySelector('.ownership-statistics-section')) {
      console.log("Ownership statistics section already exists, skipping");
      return;
    }
    
    const ownershipSection = document.createElement("div");
    ownershipSection.className = "card ownership-statistics-section";
    ownershipSection.innerHTML = createTitleWithTooltip(
      "📊 Code Ownership Distribution", 
      "Analysis of how code ownership is distributed across developers. Shows which developers have significant ownership (>10%) of subsystems and codebases.",
      "h2"
    );

    container.appendChild(ownershipSection);

    // Collect ownership data from all subsystems
    const ownershipData = {};
    let totalOwnerships = 0;
    let processedSubsystems = 0;

    for (const subsystem of state.subsystems) {
      try {
        const ownershipResponse = await fetchJSON(`/api/subsystems/${encodeURIComponent(subsystem.name)}/significant-ownership`);
        
        if (ownershipResponse.owners && ownershipResponse.owners.length > 0) {
          processedSubsystems++;
          
          ownershipResponse.owners.forEach(owner => {
            if (!ownershipData[owner.slug]) {
              ownershipData[owner.slug] = {
                display_name: owner.display_name,
                slug: owner.slug,
                ownerships: [],
                totalPercentage: 0
              };
            }
            
            ownershipData[owner.slug].ownerships.push({
              subsystem: subsystem.name,
              percentage: owner.percentage
            });
            
            ownershipData[owner.slug].totalPercentage += owner.percentage;
            totalOwnerships++;
          });
        }
      } catch (error) {
        console.warn(`Could not get ownership data for ${subsystem.name}:`, error);
      }
    }

    // Create statistics
    const usersWithOwnership = Object.keys(ownershipData);
    const statsGrid = document.createElement("div");
    statsGrid.className = "ownership-stats-grid";

    const ownershipStats = [
      { title: 'Users with Ownership', value: usersWithOwnership.length, subtitle: `out of ${state.users.length} developers`, emoji: '👑', color: '#8B5CF6' },
      { title: 'Total Ownerships', value: totalOwnerships, subtitle: 'significant ownerships', emoji: '📊', color: '#10B981' },
      { title: 'Covered Subsystems', value: processedSubsystems, subtitle: `out of ${state.subsystems.length} subsystems`, emoji: '🏗️', color: '#3B82F6' },
      { title: 'Avg Ownerships/User', value: usersWithOwnership.length > 0 ? Math.round(totalOwnerships / usersWithOwnership.length * 10) / 10 : 0, subtitle: 'per developer', emoji: '📈', color: '#F59E0B' }
    ];

    ownershipStats.forEach(stat => {
      const statCard = document.createElement("div");
      statCard.className = "ownership-stat-card";
      statCard.innerHTML = `
        <div class="stat-icon" style="color: ${stat.color};">
          <span class="stat-emoji">${stat.emoji}</span>
        </div>
        <div class="stat-content">
          <div class="stat-title">${stat.title}</div>
          <div class="stat-value" style="color: ${stat.color};">${stat.value.toLocaleString()}</div>
          <div class="stat-subtitle">${stat.subtitle}</div>
        </div>
      `;
      statsGrid.appendChild(statCard);
    });

    ownershipSection.appendChild(statsGrid);

    // Show top code owners if we have data
    if (usersWithOwnership.length > 0) {
      const contentLayout = document.createElement("div");
      contentLayout.className = "ownership-content-layout";
      
      const topOwners = Object.values(ownershipData)
        .sort((a, b) => b.ownerships.length - a.ownerships.length)
        .slice(0, 8); // Show more owners

      const topOwnersDiv = document.createElement("div");
      topOwnersDiv.className = "code-owners-section";
      topOwnersDiv.innerHTML = '<h3>👑 Top Code Owners</h3>';

      const ownersList = document.createElement("div");
      ownersList.className = "code-owners-grid";

      topOwners.forEach((owner, index) => {
        const isActive = state.users.some(u => u.slug === owner.slug);
        const ownerItem = document.createElement("div");
        ownerItem.className = isActive ? "code-owner-card clickable" : "code-owner-card inactive";
        
        if (isActive) {
          ownerItem.onclick = () => navigateToUser(owner.slug);
        } else {
          ownerItem.style.cursor = "default";
          ownerItem.title = "Inactive contributor (no recent activity in analysis period)";
        }
        
        const avgPercentage = Math.round(owner.totalPercentage / owner.ownerships.length);
        const nameStyle = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
        
        ownerItem.innerHTML = `
          <div class="owner-rank">
            <span class="rank-number">${index + 1}</span>
          </div>
          <div class="owner-info">
            <div class="owner-name"${nameStyle}>${owner.display_name || owner.slug}</div>
            <div class="owner-stats">
              <span class="ownership-stat">${owner.ownerships.length} subsystems</span>
              <span class="ownership-stat">${avgPercentage}% avg ownership</span>
            </div>
          </div>
          <div class="owner-total">
            <span class="total-count">${Math.round(owner.totalPercentage)}%</span>
            <span class="total-label">total</span>
          </div>
        `;
        ownersList.appendChild(ownerItem);
      });

      topOwnersDiv.appendChild(ownersList);
      contentLayout.appendChild(topOwnersDiv);
      ownershipSection.appendChild(contentLayout);
    }
    
    // Add Top 20 Code Owners by Total Lines
    try {
      const totalOwnershipResponse = await fetchJSON('/api/developers/total-ownership');
      
      if (totalOwnershipResponse.developers && totalOwnershipResponse.developers.length > 0) {
        const topCodeOwnersCard = document.createElement("div");
        topCodeOwnersCard.className = "ranking-list-no-scroll";
        topCodeOwnersCard.style.marginTop = "20px";
        topCodeOwnersCard.innerHTML = createTitleWithTooltip(
          "💎 Top 20 Code Owners", 
          "Ranked by total lines owned (git blame) across all subsystems. Note: Git blame counts all lines in tracked files including blanks and comments, which is typically 2-3x more than actual code lines (from cloc). This metric shows breadth of contribution across the codebase.",
          "h3"
        );
        
        const topCodeOwnersList = document.createElement("div");
        topCodeOwnersList.className = "ranking-items";
        
        totalOwnershipResponse.developers.slice(0, 20).forEach((dev, index) => {
          const isActive = state.users.some(u => u.slug === dev.slug);
          const item = document.createElement("div");
          item.className = isActive ? "ranking-item clickable" : "ranking-item inactive";
          
          if (isActive) {
            item.onclick = () => navigateToUser(dev.slug);
          } else {
            item.style.cursor = "default";
            item.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const nameStyle = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          const subsystemsText = dev.subsystem_count === 1 ? '1 subsystem' : `${dev.subsystem_count} subsystems`;
          
          item.innerHTML = `
            <span class="ranking-position">#${index + 1}</span>
            <span class="ranking-name"${nameStyle}>${dev.display_name}</span>
            <div class="ranking-meta">
              <span class="ranking-value">${dev.total_lines.toLocaleString()} lines</span>
              <span class="ranking-subtext">${subsystemsText}</span>
            </div>
          `;
          topCodeOwnersList.appendChild(item);
        });
        
        topCodeOwnersCard.appendChild(topCodeOwnersList);
        ownershipSection.appendChild(topCodeOwnersCard);
      }
    } catch (error) {
      console.error("Error loading total code ownership:", error);
      // Don't break the section, just skip this list
    }

  } catch (error) {
    console.error("Error loading ownership statistics:", error);
    // Don't break the overview, just skip ownership section
  }
}

/* REMOVED - This function used simulated/fake ownership change data
   Real ownership trends are now available on individual developer detail pages
async function addOwnershipChangesAnalysis(container, abortSignal) {
  try {
    console.log("Analyzing ownership changes for users overview...");
    
    // Check if section already exists
    if (container.querySelector('.ownership-changes-section')) {
      console.log("Ownership changes section already exists, skipping");
      return;
    }
    
    // Check if cancelled before starting
    if (abortSignal && abortSignal.aborted) {
      throw new DOMException('Operation cancelled', 'AbortError');
    }
    
    const ownershipChangesSection = document.createElement("div");
    ownershipChangesSection.className = "card ownership-changes-section";
    ownershipChangesSection.innerHTML = createTitleWithTooltip(
      "📈 Active Contributors - Ownership Patterns", 
      "Shows current code ownership distribution for active developers. Changes shown are estimated patterns based on current ownership levels. For accurate historical ownership trends, view individual developer pages.",
      "h2"
    );

    // We'll analyze ownership changes by comparing current ownership with historical data
    // Since we don't have historical API endpoints, we'll use monthly data to estimate changes
    const ownershipHistory = {};
    const currentDate = new Date();
    const currentYear = currentDate.getFullYear();
    
    // Get ownership data for different months to track changes
    const monthsToCheck = [];
    for (let month = 1; month <= Math.min(12, currentDate.getMonth() + 1); month++) {
      monthsToCheck.push(month);
    }

    // Collect ownership data for each subsystem across different months
    const ownershipEvolution = {};
    let processedSubsystems = 0;

    for (const subsystem of state.subsystems.slice(0, 10)) { // Limit to first 10 for performance
      try {
        // Get current ownership
        const currentOwnership = await fetchJSON(`/api/subsystems/${encodeURIComponent(subsystem.name)}/significant-ownership`);
        
        if (currentOwnership.owners && currentOwnership.owners.length > 0) {
          processedSubsystems++;
          
          currentOwnership.owners.forEach(owner => {
            if (!ownershipEvolution[owner.slug]) {
              ownershipEvolution[owner.slug] = {
                display_name: owner.display_name,
                slug: owner.slug,
                subsystems: {},
                totalCurrentOwnership: 0,
                ownershipChanges: 0
              };
            }
            
            ownershipEvolution[owner.slug].subsystems[subsystem.name] = {
              current: owner.percentage,
              previous: 0, // We'll estimate this
              change: 0
            };
            
            ownershipEvolution[owner.slug].totalCurrentOwnership += owner.percentage;
          });
        }
      } catch (error) {
        console.warn(`Could not get ownership data for ${subsystem.name}:`, error);
      }
    }

    // Filter to only include active users (those with recent activity)
    // Remove inactive users from ownership evolution data
    const activeUserSlugs = new Set(state.users.map(u => u.slug));
    Object.keys(ownershipEvolution).forEach(slug => {
      if (!activeUserSlugs.has(slug)) {
        delete ownershipEvolution[slug];
      }
    });
    
    // Calculate estimated ownership changes
    // Since we don't have historical data, we'll estimate changes based on current ownership patterns
    // Note: This is a simplified estimation - actual changes would require historical tracking
    Object.values(ownershipEvolution).forEach(user => {
      let totalChange = 0;
      let changedSubsystems = 0;
      
      Object.keys(user.subsystems).forEach(subsystemName => {
        const subsystem = user.subsystems[subsystemName];
        
        // Estimate ownership changes based on current patterns
        // Higher ownership suggests recent growth, lower ownership suggests established position
        if (subsystem.current > 50) {
          // High ownership - likely stable or slight growth
          subsystem.previous = subsystem.current - Math.random() * 10;
          subsystem.change = subsystem.current - subsystem.previous;
        } else if (subsystem.current > 25) {
          // Medium ownership - could be growing
          subsystem.previous = subsystem.current - Math.random() * 20;
          subsystem.change = subsystem.current - subsystem.previous;
        } else {
          // Lower ownership - might be newer area or declining
          subsystem.previous = Math.max(0, subsystem.current - Math.random() * 15);
          subsystem.change = subsystem.current - subsystem.previous;
        }
        
        if (Math.abs(subsystem.change) > 5) { // Significant change threshold
          changedSubsystems++;
        }
        
        totalChange += subsystem.change;
      });
      
      user.ownershipChanges = totalChange;
      user.changedSubsystems = changedSubsystems;
    });

    const usersWithChanges = Object.values(ownershipEvolution);
    
    // Create statistics
    const statsGrid = document.createElement("div");
    statsGrid.className = "ownership-changes-stats-grid";

    const biggestGainer = usersWithChanges.reduce((max, user) => 
      user.ownershipChanges > (max?.ownershipChanges || 0) ? user : max, null);
    const biggestShifter = usersWithChanges.reduce((max, user) => 
      user.changedSubsystems > (max?.changedSubsystems || 0) ? user : max, null);
    const avgChange = usersWithChanges.length > 0 ? 
      usersWithChanges.reduce((sum, user) => sum + Math.abs(user.ownershipChanges), 0) / usersWithChanges.length : 0;

    const changesStats = [
      { 
        title: 'Active Contributors', 
        value: usersWithChanges.length, 
        subtitle: 'with ownership data', 
        emoji: '👥', 
        color: '#10B981' 
      },
      { 
        title: 'Highest Ownership', 
        value: biggestGainer ? `${Math.round(biggestGainer.totalCurrentOwnership)}%` : 'N/A', 
        subtitle: biggestGainer ? biggestGainer.display_name || biggestGainer.slug : 'no data', 
        emoji: '👑', 
        color: '#3B82F6' 
      },
      { 
        title: 'Most Subsystems', 
        value: biggestShifter ? biggestShifter.changedSubsystems : 0, 
        subtitle: biggestShifter ? `${biggestShifter.display_name || biggestShifter.slug}` : 'no data', 
        emoji: '🎯', 
        color: '#8B5CF6' 
      },
      { 
        title: 'Avg Ownership', 
        value: `${Math.round(avgChange)}%`, 
        subtitle: 'per active developer', 
        emoji: '📊', 
        color: '#F59E0B' 
      }
    ];

    changesStats.forEach(stat => {
      const statCard = document.createElement("div");
      statCard.className = "ownership-changes-stat-card";
      statCard.innerHTML = `
        <div class="stat-icon" style="color: ${stat.color};">
          <span class="stat-emoji">${stat.emoji}</span>
        </div>
        <div class="stat-content">
          <div class="stat-title">${stat.title}</div>
          <div class="stat-value" style="color: ${stat.color};">${stat.value}</div>
          <div class="stat-subtitle">${stat.subtitle}</div>
        </div>
      `;
      statsGrid.appendChild(statCard);
    });

    ownershipChangesSection.appendChild(statsGrid);

    // Show top ownership changers
    if (usersWithChanges.length > 0) {
      const contentLayout = document.createElement("div");
      contentLayout.className = "ownership-changes-content-layout";
      
      // Sort by absolute change amount (biggest changes first)
      const topChangers = usersWithChanges
        .filter(user => Math.abs(user.ownershipChanges) > 5) // Only significant changes
        .sort((a, b) => Math.abs(b.ownershipChanges) - Math.abs(a.ownershipChanges))
        .slice(0, 8);

      if (topChangers.length > 0) {
        const topChangersDiv = document.createElement("div");
        topChangersDiv.className = "ownership-changers-section";
        topChangersDiv.innerHTML = '<h3>👥 Active Contributors by Ownership</h3>';

        const changersList = document.createElement("div");
        changersList.className = "ownership-changers-grid";

        topChangers.forEach((changer, index) => {
          const isActive = state.users.some(u => u.slug === changer.slug);
          const changerItem = document.createElement("div");
          changerItem.className = isActive ? "ownership-changer-card clickable" : "ownership-changer-card inactive";
          
          if (isActive) {
            changerItem.onclick = () => navigateToUser(changer.slug);
          } else {
            changerItem.style.cursor = "default";
            changerItem.title = "Inactive contributor (no recent activity in analysis period)";
          }
          
          const changeDirection = changer.ownershipChanges > 0 ? 'increase' : 'decrease';
          const changeIcon = changer.ownershipChanges > 0 ? '📈' : '📉';
          const changeColor = changer.ownershipChanges > 0 ? '#10B981' : '#EF4444';
          const nameStyle = isActive ? "" : ' style="color: #dc2626; font-style: italic;"';
          
          changerItem.innerHTML = `
            <div class="changer-rank">
              <span class="rank-number">${index + 1}</span>
            </div>
            <div class="changer-info">
              <div class="changer-name"${nameStyle}>${changer.display_name || changer.slug}</div>
              <div class="changer-details">
                <span class="change-indicator ${changeDirection}">
                  ${changeIcon} ${Math.abs(Math.round(changer.ownershipChanges))}% ${changeDirection}
                </span>
                <span class="subsystem-count">${changer.changedSubsystems} subsystems affected</span>
              </div>
            </div>
            <div class="changer-total">
              <span class="total-count" style="color: ${changeColor};">${Math.round(changer.totalCurrentOwnership)}%</span>
              <span class="total-label">current total</span>
            </div>
          `;
          changersList.appendChild(changerItem);
        });

        topChangersDiv.appendChild(changersList);
        contentLayout.appendChild(topChangersDiv);
        ownershipChangesSection.appendChild(contentLayout);
      } else {
        // No active contributors message
        const noChangesDiv = document.createElement("div");
        noChangesDiv.className = "no-changes-message";
        noChangesDiv.innerHTML = `
          <div style="text-align: center; padding: 40px; color: #9ca3af;">
            <span style="font-size: 48px;">👥</span>
            <h3>No Active Contributors</h3>
            <p>No currently active developers found with code ownership in the analyzed subsystems. This section shows only contributors with recent activity in the analysis period.</p>
          </div>
        `;
        ownershipChangesSection.appendChild(noChangesDiv);
      }
    }

    container.appendChild(ownershipChangesSection);

  } catch (error) {
    console.error("Error analyzing ownership changes:", error);
    // Don't break the overview, just skip ownership changes section
  }
}
*/ // End of removed addOwnershipChangesAnalysis function

async function showTeamsOverviewDashboard() {
  try {
    // Prevent concurrent executions
    if (state.loadingTeamsOverview) {
      console.log("Teams overview already loading, skipping duplicate call");
      return;
    }
    
    state.loadingTeamsOverview = true;
    console.log("Starting teams overview dashboard loading");
    
    setViewHeader("Teams Overview", "Development teams statistics and activity", "Teams");
    
    const main = $("main-content");
    main.innerHTML = createLoadingIndicator(
      "Loading Teams Overview", 
      "Gathering team statistics and activity metrics..."
    );
    
    clearMain();
    setViewHeader("Teams Overview", "Development teams statistics and activity", "Teams");
    
    // Check if teams are configured
    if (!state.teams || state.teams.length === 0) {
      const noTeamsSection = document.createElement("div");
      noTeamsSection.className = "card";
      noTeamsSection.innerHTML = `
        <h2>📋 No Teams Configured</h2>
        <p>No development teams have been configured yet. Use the Settings menu to create teams and assign members.</p>
        <p><strong>Steps to get started:</strong></p>
        <ol>
          <li>Click the hamburger menu (☰) in the top left</li>
          <li>Select "Settings"</li>
          <li>Go to the "Teams" tab</li>
          <li>Create teams and assign team members</li>
        </ol>
      `;
      main.appendChild(noTeamsSection);
      return;
    }

    // Load team analytics data with preference for recent activity
    let teamsAnalytics = [];
    let periodLabel = "Last 3 Months";
    const preferredInitialPeriod = "last3months";
    let initialDataLoaded = false;

    try {
        const preferredData = await fetchJSON(`/api/teams/overview?period=${preferredInitialPeriod}`);
        if (preferredData && Array.isArray(preferredData.teams) && preferredData.teams.length > 0) {
            teamsAnalytics = preferredData.teams;
            periodLabel = preferredData.period || "Last 3 Months";
            initialDataLoaded = true;
        } else {
            console.warn("Preferred period data empty, falling back to overall analytics");
        }
    } catch (error) {
        console.warn("Failed to load preferred period data, falling back to overall analytics:", error);
    }

    if (!initialDataLoaded) {
        periodLabel = "Overall";
        try {
            console.log("Initial team overview load - using consistent yearly data");
            
            // Use the same logic as period toggle to ensure consistency from first load
            const currentYear = new Date().getFullYear();
            
            // Fetch yearly data for each team to ensure consistency
            const yearlyTeamData = [];
            const teamPromises = state.teams.slice(0, 8).map(async team => { // Limit to first 8 teams for performance
                try {
                    const yearlyData = await fetchJSON(`/api/teams/${encodeURIComponent(team.id)}/year/${currentYear}`);
                    
                    console.log(`Team ${team.id} yearly data structure:`, {
                        total_commits: yearlyData.total_commits,
                        per_subsystem_keys: Object.keys(yearlyData.per_subsystem || {}),
                        subsystems_keys: Object.keys(yearlyData.subsystems || {}),
                        all_keys: Object.keys(yearlyData)
                    });
                    
                    // Try different possible field names for subsystem data
                    const subsystemData = yearlyData.per_subsystem || 
                                          yearlyData.subsystems || 
                                          yearlyData.subsystem_breakdown ||
                                          yearlyData.subsystem_summary ||
                                          yearlyData.per_repo ||
                                          {};
                                          
                    // Also try counting from members' subsystem contributions if direct subsystem data isn't available
                    let activeSubsystemsCount = Object.keys(subsystemData).length;
                    
                    // If no subsystem data found, try to derive from other sources
                    if (activeSubsystemsCount === 0) {
                        // Check if there are members with per-subsystem data
                        if (yearlyData.members && Array.isArray(yearlyData.members)) {
                            const allSubsystems = new Set();
                            yearlyData.members.forEach(member => {
                                if (member.per_subsystem) {
                                    Object.keys(member.per_subsystem).forEach(sub => allSubsystems.add(sub));
                                }
                                if (member.subsystems) {
                                    Object.keys(member.subsystems).forEach(sub => allSubsystems.add(sub));
                                }
                            });
                            activeSubsystemsCount = allSubsystems.size;
                        }
                    }
                    
                    console.log(`Team ${team.id} calculated active subsystems: ${activeSubsystemsCount}`);
                    
                    return {
                        id: team.id,
                        name: team.name,
                        total_commits: yearlyData.total_commits || 0,
                        total_lines_changed: (yearlyData.total_additions || 0) + (yearlyData.total_deletions || 0),
                        total_additions: yearlyData.total_additions || 0,
                        total_deletions: yearlyData.total_deletions || 0,
                        active_subsystems_count: activeSubsystemsCount,
                        responsible_subsystems_count: yearlyData.responsible_subsystems?.length || 0,
                        responsible_lines_of_code: yearlyData.total_responsible_lines || 0,
                        member_count: team.members?.length || 0  // Add member count from original team data
                    };
                } catch (error) {
                    console.warn(`Failed to fetch yearly data for team ${team.id}:`, error);
                    return null;
                }
            });
            
            const resolvedTeamData = (await Promise.all(teamPromises)).filter(team => team !== null);
            
            if (resolvedTeamData.length > 0) {
                console.log("Successfully fetched consistent yearly data for initial load:", resolvedTeamData.length, "teams");
                teamsAnalytics = resolvedTeamData;
                periodLabel = "Overall";
                initialDataLoaded = true;
            } else {
                throw new Error("No yearly team data could be fetched");
            }
            
        } catch (error) {
            console.warn("Failed to fetch consistent yearly data for initial load, falling back to overview API:", error);
            
            // Fallback to original overview API
            try {
                const response = await fetch("/api/teams/overview");
                if (response.ok) {
                    const data = await response.json();
                    teamsAnalytics = data.teams || [];
                    periodLabel = data.period || "Overall";
                    initialDataLoaded = true;
                }
            } catch (fallbackError) {
                console.warn("Failed to load team analytics:", fallbackError);
            }
        }
    }

    // Teams summary
    const summarySection = document.createElement("div");
    summarySection.className = "card";
    summarySection.innerHTML = '<h2>🏢 Teams Summary</h2>';
    
    const summaryGrid = document.createElement("div");
    summaryGrid.className = "overview-grid";
    
    // Calculate team stats
    const totalTeams = state.teams.length;
    const totalMembers = state.teams.reduce((sum, team) => sum + (team.members?.length || 0), 0);
    const totalCommits = teamsAnalytics.reduce((sum, team) => sum + team.total_commits, 0);
    const totalLinesChanged = teamsAnalytics.reduce((sum, team) => sum + team.total_lines_changed, 0);
    const totalResponsibleSubsystems = teamsAnalytics.reduce((sum, team) => sum + (team.responsible_subsystems_count || 0), 0);
    const totalResponsibleLinesOfCode = teamsAnalytics.reduce((sum, team) => sum + (team.responsible_lines_of_code || 0), 0);
    
    const teamStats = [
      { title: 'Total Teams', value: totalTeams, subtitle: 'configured', emoji: '🏢', color: '#10B981' },
      { title: 'Team Members', value: totalMembers, subtitle: 'total developers', emoji: '👥', color: '#3B82F6' },
      { title: 'Total Commits', value: totalCommits, subtitle: 'this period', emoji: '📝', color: '#8B5CF6' },
      { title: 'Responsible Subsystems', value: totalResponsibleSubsystems, subtitle: 'managed by teams', emoji: '🎯', color: '#06B6D4' },
      { title: 'Managed Code', value: totalResponsibleLinesOfCode, subtitle: 'lines under management', emoji: '💻', color: '#F59E0B' }
    ];
    
    teamStats.forEach(stat => {
      const statCard = document.createElement("div");
      statCard.className = "overview-stat-card";
      statCard.innerHTML = `
        <div class="stat-header" style="color: ${stat.color};">
          <span class="stat-emoji">${stat.emoji}</span>
          <span class="stat-title">${stat.title}</span>
        </div>
        <div class="stat-value">${stat.value.toLocaleString()}</div>
        <div class="stat-subtitle">${stat.subtitle}</div>
      `;
      summaryGrid.appendChild(statCard);
    });
    
    summarySection.appendChild(summaryGrid);
    main.appendChild(summarySection);

    // Team Rankings Section
    if (teamsAnalytics.length > 0) {
      await addTeamRankings(main, teamsAnalytics, periodLabel);
    }
    
    // Teams list
    if (!main.querySelector('.team-details-section')) {
      const teamsSection = document.createElement("div");
      teamsSection.className = "card team-details-section";
      teamsSection.innerHTML = '<h2>👨‍💻 Team Details</h2>';
      
      const teamsGrid = document.createElement("div");
      teamsGrid.className = "teams-grid";
      
      state.teams.forEach(team => {
        // Find analytics data for this team
        const teamAnalytics = teamsAnalytics.find(t => t.id === team.id);
        
        const teamCard = document.createElement("div");
        teamCard.className = "team-overview-card";
        
        let analyticsInfo = '';
        if (teamAnalytics) {
          analyticsInfo = `
            <div class="team-stats">
              <div class="team-stat">
                <span class="stat-label">Commits:</span>
                <span class="stat-value">${(teamAnalytics.total_commits || 0).toLocaleString()}</span>
              </div>
              <div class="team-stat">
                <span class="stat-label">Lines Changed:</span>
                <span class="stat-value">${(teamAnalytics.total_lines_changed || 0).toLocaleString()}</span>
              </div>
              <div class="team-stat">
                <span class="stat-label">Subsystems:</span>
                <span class="stat-value">${teamAnalytics.active_subsystems_count}</span>
              </div>
            </div>
          `;
        }
        
        teamCard.innerHTML = `
          <div class="team-header">
            <h3>${team.name || team.id}</h3>
            <span class="team-member-count">${team.members?.length || 0} members</span>
          </div>
          ${team.description ? `<p class="team-description">${team.description}</p>` : ''}
          ${analyticsInfo}
          <div class="team-members">
            <strong>Members:</strong> ${
              team.members && team.members.length > 0
                ? team.members.map(memberSlug => {
                    const isActive = state.users.some(user => user.slug === memberSlug);
                    return isActive 
                      ? `<span class="member-name">${memberSlug}</span>`
                      : `<span class="member-name inactive" style="color: #dc2626; font-style: italic;" title="Inactive contributor">${memberSlug}</span>`;
                  }).join(', ')
                : 'No members assigned'
            }
          </div>
          <div class="team-actions">
            <button class="btn btn-primary view-team-btn" data-team-id="${team.id}">View Team Dashboard</button>
          </div>
        `;
        teamsGrid.appendChild(teamCard);
      });
      
      teamsSection.appendChild(teamsGrid);
      main.appendChild(teamsSection);
      
      // Add event listeners for team buttons
      const teamButtons = main.querySelectorAll('.view-team-btn');
      teamButtons.forEach(btn => {
        btn.addEventListener('click', () => {
          const teamId = btn.getAttribute('data-team-id');
          const team = state.teams.find(t => t.id === teamId);
          if (team) {
            selectTeam(team);
          }
        });
      });
    }
    
    state.loadingTeamsOverview = false;
    console.log("Teams overview dashboard loading completed");
    
  } catch (error) {
    console.error("Error loading teams overview:", error);
    clearMain();
    setViewHeader("Teams Overview", "Error loading overview data", "Error");
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load teams overview: ' + error.message + '</div>';
  } finally {
    state.loadingTeamsOverview = false;
    console.log("Teams overview dashboard loading finished");
  }
}

async function addTeamRankings(main, teamsAnalytics, periodLabel, insertBeforeElement = null) {
  // Check if rankings section already exists
  if (main.querySelector('.team-rankings-section:not([data-section="team-rankings"])')) {
    console.log("Team rankings section already exists, skipping");
    return;
  }
  
  const rankingsSection = document.createElement("div");
  rankingsSection.className = "card team-rankings-section";
  rankingsSection.setAttribute("data-section", "team-rankings");
  rankingsSection.innerHTML = `<h2>🏆 Team Rankings - ${periodLabel}</h2>`;

  // Add period information note only when needed
  if (periodLabel.includes("Last 3 Months") || periodLabel.includes("last3months")) {
    const periodNote = document.createElement("div");
    periodNote.className = "period-note";
    periodNote.innerHTML = `
      <p><strong>📅 Note:</strong> These rankings show data for the last 3 months. For complete yearly statistics, view individual team details.</p>
    `;
    rankingsSection.appendChild(periodNote);
  }

  // Add period toggle buttons
  const periodToggle = document.createElement("div");
  periodToggle.className = "period-toggle";
  periodToggle.innerHTML = `
    <button class="period-btn" data-period="overall">Overall</button>
    <button class="period-btn" data-period="last3months">Last 3 Months</button>
  `;
  
  // Set active button based on current period
  const isLast3Months = periodLabel === "Last 3 Months";
  periodToggle.querySelector(`[data-period="${isLast3Months ? 'last3months' : 'overall'}"]`).classList.add('active');
  
  rankingsSection.appendChild(periodToggle);

  const rankingsContainer = document.createElement("div");
  rankingsContainer.className = "rankings-container";

  // Create three ranking lists
  const rankings = [
    {
      title: "Most Active Teams",
      subtitle: "By total commits",
      emoji: "🔥",
      tooltip: "Teams ranked by total number of commits across all subsystems for the selected time period. Includes all commits made by team members.",
      data: [...teamsAnalytics].sort((a, b) => b.total_commits - a.total_commits).slice(0, 10),
      getValue: (team) => team.total_commits.toLocaleString(),
      getSubtext: (team) => `${team.total_lines_changed.toLocaleString()} lines changed`
    },
    {
      title: "Highest Impact Teams",
      subtitle: "By lines changed",
      emoji: "📈", 
      tooltip: "Teams ranked by total lines changed (added + deleted) across all subsystems. Represents the overall code impact and volume of work.",
      data: [...teamsAnalytics].sort((a, b) => b.total_lines_changed - a.total_lines_changed).slice(0, 10),
      getValue: (team) => team.total_lines_changed.toLocaleString(),
      getSubtext: (team) => `${team.total_commits.toLocaleString()} commits`
    },
    {
      title: "Most Diverse Teams",
      subtitle: "By subsystems worked on",
      emoji: "🎯",
      tooltip: "Teams ranked by the number of different subsystems they have contributed to. Shows which teams work across multiple areas of the codebase.",
      data: [...teamsAnalytics].sort((a, b) => b.active_subsystems_count - a.active_subsystems_count).slice(0, 10),
      getValue: (team) => `${team.active_subsystems_count} subsystems`,
      getSubtext: (team) => `${team.total_commits.toLocaleString()} commits`
    },
    {
      title: "Highest Ownership Teams", 
      subtitle: "By responsible codebase size",
      emoji: "🏗️",
      tooltip: "Teams ranked by the total lines of code they are responsible for maintaining. Based on designated team ownership of subsystems in settings.",
      data: [...teamsAnalytics].sort((a, b) => {
        const aLines = typeof a.responsible_lines_of_code === 'number' ? a.responsible_lines_of_code : 0;
        const bLines = typeof b.responsible_lines_of_code === 'number' ? b.responsible_lines_of_code : 0;
        return bLines - aLines;
      }).slice(0, 10),
      getValue: (team) => {
        const lines = typeof team.responsible_lines_of_code === 'number' ? team.responsible_lines_of_code : 0;
        return `${lines.toLocaleString()} lines`;
      },
      getSubtext: (team) => `${team.responsible_subsystems_count || 0} subsystems managed`
    }
  ];

  rankings.forEach(ranking => {
    const rankingCard = document.createElement("div");
    rankingCard.className = "ranking-list";
    
    rankingCard.innerHTML = `
      <div class="ranking-header">
        <span class="ranking-emoji">${ranking.emoji}</span>
        <div class="title-with-help">
          <div>
            <h3>${ranking.title}</h3>
            <p class="ranking-subtitle">${ranking.subtitle}</p>
          </div>
          <span class="help-icon">?
            <span class="tooltip">${ranking.tooltip}</span>
          </span>
        </div>
      </div>
      <div class="ranking-items"></div>
    `;

    const itemsContainer = rankingCard.querySelector('.ranking-items');
    
    ranking.data.forEach((team, index) => {
      const item = document.createElement("div");
      item.className = "ranking-item";
      
      const rankNumber = index + 1;
      
      item.innerHTML = `
        <span class="rank-number">${rankNumber}</span>
        <div class="rank-content">
          <button class="team-link" data-team-id="${team.id}" title="View ${team.name} team dashboard">
            ${team.name}
          </button>
          <div class="rank-stats">
            <span class="rank-value">${ranking.getValue(team)}</span>
            <span class="rank-subtext">${ranking.getSubtext(team)}</span>
          </div>
        </div>
        <span class="member-count">${team.member_count} members</span>
      `;
      
      itemsContainer.appendChild(item);
    });

    rankingsContainer.appendChild(rankingCard);
  });

  rankingsSection.appendChild(rankingsContainer);
  
  // Insert the section at the correct position
  if (insertBeforeElement) {
    main.insertBefore(rankingsSection, insertBeforeElement);
  } else {
    main.appendChild(rankingsSection);
  }

  // Add event listeners for team links in rankings
  const teamLinks = rankingsSection.querySelectorAll('.team-link');
  teamLinks.forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const teamId = link.getAttribute('data-team-id');
      const team = state.teams.find(t => t.id === teamId);
      if (team) {
        selectTeam(team);
      }
    });
  });

  // Add event listeners for period toggle buttons
  const periodButtons = rankingsSection.querySelectorAll('.period-btn');
  periodButtons.forEach(button => {
    button.addEventListener('click', async (e) => {
      e.preventDefault();
      const period = button.getAttribute('data-period');
      
      // Update active button
      periodButtons.forEach(btn => btn.classList.remove('active'));
      button.classList.add('active');
      
      // Reload rankings for the selected period
      try {
        console.log("Loading teams overview for period:", period);
        
        let teamsOverviewData;
        
        if (period === 'overall') {
          // For "overall", we want to ensure we get yearly data that matches team details
          console.log("Fetching yearly data to ensure consistency with team details...");
          
          try {
            // Get the current year
            const currentYear = new Date().getFullYear();
            
            // Fetch yearly data for each team to ensure consistency
            const yearlyTeamData = [];
            const teamPromises = state.teams.slice(0, 8).map(async team => { // Limit to first 8 teams for performance
              try {
                const yearlyData = await fetchJSON(`/api/teams/${encodeURIComponent(team.id)}/year/${currentYear}`);
                
                // Try different possible field names for subsystem data
                const subsystemData = yearlyData.per_subsystem || 
                                      yearlyData.subsystems || 
                                      yearlyData.subsystem_breakdown ||
                                      yearlyData.subsystem_summary ||
                                      yearlyData.per_repo ||
                                      {};
                                      
                // Also try counting from members' subsystem contributions if direct subsystem data isn't available
                let activeSubsystemsCount = Object.keys(subsystemData).length;
                
                // If no subsystem data found, try to derive from other sources
                if (activeSubsystemsCount === 0) {
                  // Check if there are members with per-subsystem data
                  if (yearlyData.members && Array.isArray(yearlyData.members)) {
                    const allSubsystems = new Set();
                    yearlyData.members.forEach(member => {
                      if (member.per_subsystem) {
                        Object.keys(member.per_subsystem).forEach(sub => allSubsystems.add(sub));
                      }
                      if (member.subsystems) {
                        Object.keys(member.subsystems).forEach(sub => allSubsystems.add(sub));
                      }
                    });
                    activeSubsystemsCount = allSubsystems.size;
                  }
                }
                
                return {
                  id: team.id,
                  name: team.name,
                  total_commits: yearlyData.total_commits || 0,
                  total_lines_changed: (yearlyData.total_additions || 0) + (yearlyData.total_deletions || 0),
                  total_additions: yearlyData.total_additions || 0,
                  total_deletions: yearlyData.total_deletions || 0,
                  active_subsystems_count: activeSubsystemsCount,
                  responsible_subsystems_count: yearlyData.responsible_subsystems?.length || 0,
                  responsible_lines_of_code: yearlyData.total_responsible_lines || 0,
                  member_count: team.members?.length || 0  // Add member count from original team data
                };
              } catch (error) {
                console.warn(`Failed to fetch yearly data for team ${team.id}:`, error);
                return null;
              }
            });
            
            const resolvedTeamData = (await Promise.all(teamPromises)).filter(team => team !== null);
            
            if (resolvedTeamData.length > 0) {
              console.log("Successfully fetched consistent yearly data for", resolvedTeamData.length, "teams");
              teamsOverviewData = {
                teams: resolvedTeamData,
                period: "Overall"
              };
            } else {
              throw new Error("No yearly team data could be fetched");
            }
            
          } catch (error) {
            console.warn("Failed to fetch consistent yearly data, falling back to overview API:", error);
            teamsOverviewData = await fetchJSON(`/api/teams/overview?period=${period}`);
          }
        } else {
          teamsOverviewData = await fetchJSON(`/api/teams/overview?period=${period}`);
        }
        
        console.log("Final teams overview data:", {
          period: teamsOverviewData.period,
          teamsCount: teamsOverviewData.teams?.length,
          sampleTeamData: teamsOverviewData.teams?.[0]
        });
        
        // Find and remove the current rankings section specifically
        const oldRankingsSection = main.querySelector('.team-rankings-section[data-section="team-rankings"]');
        let insertBeforeElement = null;
        
        if (oldRankingsSection) {
          // Remember where to insert the new section
          insertBeforeElement = oldRankingsSection.nextElementSibling;
          oldRankingsSection.remove();
        }
        
        // Create new rankings section
        await addTeamRankings(main, teamsOverviewData.teams, teamsOverviewData.period, insertBeforeElement);
        
      } catch (error) {
        console.error("Error loading teams rankings for period:", period, error);
      }
    });
  });
}

async function addSubsystemLanguageDistribution(container) {
  console.log("🔍 DEBUG: addSubsystemLanguageDistribution function called");
  console.log("🔍 DEBUG: Container element:", container);
  console.log("🔍 DEBUG: Current state.subsystems:", state.subsystems);
  
  try {
    console.log("Loading subsystem language distribution...");
    console.log("Current state.subsystems:", state.subsystems);
    
    // Check if we have subsystems
    if (!container.querySelector('.language-distribution-section')) {
      console.log("🔍 DEBUG: No existing language distribution section found, proceeding");
      
      // Get all subsystem language data
      const subsystemList = state.subsystems || [];
      console.log("Processing language data for", subsystemList.length, "subsystems:", subsystemList);
      
      if (subsystemList.length === 0) {
        console.log("🔍 DEBUG: No subsystems found in state, showing placeholder");
        
        // Show a message that no subsystems are available
        const languageSection = document.createElement("div");
        languageSection.className = "card language-distribution-section";
        languageSection.innerHTML = `
          <h2>💻 Subsystems by Primary Language</h2>
          <div class="no-data-message">
            <p>No subsystems available for language analysis.</p>
            <p>State contains ${subsystemList.length} subsystems.</p>
            <p>Debug: Check browser console for state.subsystems content.</p>
          </div>
        `;
        container.appendChild(languageSection);
        console.log("🔍 DEBUG: Added no-subsystems section to container");
        return;
      }
      
      console.log("🔍 DEBUG: Found", subsystemList.length, "subsystems, starting language processing...");
      
      const languageDistribution = {};
      let processedCount = 0;
      let errorCount = 0;
      
      for (const subsystem of subsystemList) {
        try {
          console.log(`Fetching language data for subsystem: ${subsystem.name}`);
          const languageData = await fetchJSON(`/api/subsystems/${encodeURIComponent(subsystem.name)}/languages`);
          console.log(`Language data for ${subsystem.name}:`, languageData);
          
          if (languageData.languages && Object.keys(languageData.languages).length > 0) {
            // Determine primary language (most lines of code)
            const primaryLanguage = getPrimaryLanguage(languageData.languages);
            console.log(`Primary language for ${subsystem.name}:`, primaryLanguage);
            
            if (primaryLanguage && primaryLanguage !== 'Others') {
              console.log(`🔍 DEBUG: Adding ${primaryLanguage} to distribution for ${subsystem.name}`);
              if (!languageDistribution[primaryLanguage]) {
                languageDistribution[primaryLanguage] = 0;
              }
              languageDistribution[primaryLanguage]++;
            } else {
              console.log(`🔍 DEBUG: Skipping ${subsystem.name} - primary language: ${primaryLanguage}`);
            }
            processedCount++;
          } else {
            console.warn(`No language data returned for ${subsystem.name}`);
          }
        } catch (error) {
          console.warn(`Could not get language data for ${subsystem.name}:`, error);
          errorCount++;
        }
      }
      
      console.log("Language distribution processing complete:", {
        totalSubsystems: subsystemList.length,
        processedCount,
        errorCount,
        languageDistribution
      });
      
      console.log("🔍 DEBUG: languageDistribution keys:", Object.keys(languageDistribution));
      console.log("🔍 DEBUG: languageDistribution values:", Object.values(languageDistribution));
      
      if (Object.keys(languageDistribution).length === 0) {
        console.log("No language distribution data available, showing placeholder");
        
        // Show a message that language data is not available
        const languageSection = document.createElement("div");
        languageSection.className = "card language-distribution-section";
        languageSection.innerHTML = `
          <h2>💻 Subsystems by Primary Language</h2>
          <div class="no-data-message">
            <p>Language distribution data is not available.</p>
            <p>All ${processedCount} processed subsystems returned null primary languages.</p>
            <p>This may indicate the language filter is too restrictive.</p>
            ${errorCount > 0 ? `<p><small>Failed to load data for ${errorCount} subsystems.</small></p>` : ''}
          </div>
        `;
        container.appendChild(languageSection);
        console.log("🔍 DEBUG: Added no-data section due to empty languageDistribution");
        return;
      }
      
      console.log("🔍 DEBUG: Creating chart with", Object.keys(languageDistribution).length, "languages");
      
      // Create language distribution chart
      const languageSection = document.createElement("div");
      languageSection.className = "card language-distribution-section";
      languageSection.innerHTML = '<h2>💻 Subsystems by Primary Language</h2>';
      console.log("🔍 DEBUG: Created language section element");
      
      const chartContainer = document.createElement("div");
      chartContainer.className = "chart-container";
      chartContainer.innerHTML = '<canvas id="language-distribution-chart" style="max-height: 300px;"></canvas>';
      languageSection.appendChild(chartContainer);
      console.log("🔍 DEBUG: Created chart container with canvas");
      
      container.appendChild(languageSection);
      console.log("🔍 DEBUG: Appended language section to container");
      
      // Create the chart after the element is in the DOM
      setTimeout(() => {
        try {
          console.log("🔍 DEBUG: Starting chart creation in setTimeout");
          console.log("🔍 DEBUG: Chart.js available:", typeof Chart !== 'undefined');
          
          // Sort by count and get top languages
          const sortedLanguages = Object.entries(languageDistribution)
            .sort(([,a], [,b]) => b - a)
            .slice(0, 8); // Show top 8 languages
          
          const labels = sortedLanguages.map(([lang]) => lang);
          const data = sortedLanguages.map(([, count]) => count);
          
          console.log("🔍 DEBUG: Chart data prepared - labels:", labels, "data:", data);
          
          const ctx = document.getElementById("language-distribution-chart");
          console.log("🔍 DEBUG: Canvas element found:", ctx);
          
          if (ctx && labels.length > 0) {
            console.log("🔍 DEBUG: Creating Chart.js chart...");
            
            // Destroy existing chart if it exists
            if (state.charts.languageDistribution) {
              console.log("🔍 DEBUG: Destroying existing chart");
              state.charts.languageDistribution.destroy();
            }
            state.charts.languageDistribution = new Chart(ctx, {
              type: "bar",
              data: {
                labels: labels,
                datasets: [{
                  label: "Subsystems",
                  data: data,
                  backgroundColor: [
                    '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', 
                    '#06B6D4', '#84CC16', '#F97316'
                  ],
                  borderColor: [
                    '#1D4ED8', '#059669', '#D97706', '#DC2626', '#7C3AED',
                    '#0891B2', '#65A30D', '#EA580C'
                  ],
                  borderWidth: 1
                }]
              },
              options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                  legend: {
                    display: false
                  },
                  tooltip: {
                    callbacks: {
                      label: function(context) {
                        const total = data.reduce((sum, val) => sum + val, 0);
                        const percentage = ((context.parsed.y / total) * 100).toFixed(1);
                        return `${context.parsed.y} subsystems (${percentage}%)`;
                      }
                    }
                  }
                },
                scales: {
                  y: {
                    beginAtZero: true,
                    ticks: {
                      stepSize: 1,
                      color: '#9CA3AF'
                    },
                    grid: {
                      color: '#374151'
                    }
                  },
                  x: {
                    ticks: {
                      color: '#9CA3AF'
                    },
                    grid: {
                      display: false
                    }
                  }
                }
              }
            });
            
            console.log("🔍 DEBUG: Chart.js chart created successfully");
          } else {
            console.error("🚨 ERROR: Could not create language chart - canvas element not found or no data");
            console.log("🔍 DEBUG: ctx element:", ctx);
            console.log("🔍 DEBUG: labels.length:", labels.length);
          }
          
          // Add summary text
          const summaryDiv = document.createElement("div");
          summaryDiv.className = "language-summary";
          const totalSubsystems = data.reduce((sum, val) => sum + val, 0);
          summaryDiv.innerHTML = `
            <p><strong>Distribution:</strong> ${totalSubsystems} subsystems analyzed across ${labels.length} primary languages</p>
          `;
          languageSection.appendChild(summaryDiv);
          console.log("🔍 DEBUG: Added chart summary text");
          
        } catch (error) {
          console.error("🚨 ERROR creating language distribution chart:", error);
          console.error("🚨 ERROR Stack:", error.stack);
        }
      }, 100);
    }
    
    console.log("🔍 DEBUG: addSubsystemLanguageDistribution function completed successfully");
    
  } catch (error) {
    console.error("🚨 ERROR in addSubsystemLanguageDistribution:", error);
    console.error("🚨 ERROR Stack:", error.stack);
    
    // Always show something, even if there's an error
    if (!container.querySelector('.language-distribution-section')) {
      const errorSection = document.createElement("div");
      errorSection.className = "card language-distribution-section";
      errorSection.innerHTML = `
        <h2>💻 Subsystems by Primary Language</h2>
        <div class="no-data-message">
          <p>Error loading language distribution: ${error.message}</p>
          <p>Check browser console for details.</p>
        </div>
      `;
      container.appendChild(errorSection);
      console.log("🔍 DEBUG: Added error section due to exception");
    }
  }
}

async function addLanguageLinesDistribution(container) {
  try {
    const languageData = await fetchJSON('/api/subsystems/language-lines');
    
    if (!languageData.languages || Object.keys(languageData.languages).length === 0) {
      return;
    }
    
    // Filter out markup/config languages (same as getPrimaryLanguage)
    const excludeLanguages = new Set([
      'HTML', 'CSS', 'SCSS', 'Sass', 'Less',
      'JSON', 'YAML', 'XML', 'TOML', 'INI',
      'Markdown', 'reStructuredText', 'AsciiDoc', 'LaTeX', 'TeX',
      'CSV', 'TSV', 'Properties', 'Dockerfile', 'Makefile',
      'Text', 'Binary', 'Data', 'Image', 'Video', 'Audio',
      'Protocol Buffer', 'Thrift', 'Avro', 'GraphQL',
      'Mustache', 'Handlebars', 'Jinja', 'Smarty',
      'SVG', 'PostScript', 'Rich Text Format', 'Unknown'
    ]);
    
    // Filter languages
    const filteredLanguages = Object.entries(languageData.languages)
      .filter(([lang, _]) => !excludeLanguages.has(lang));
    
    if (filteredLanguages.length === 0) {
      return;
    }
    
    const section = document.createElement("div");
    section.className = "card language-distribution-section";
    section.innerHTML = createTitleWithTooltip(
      "📊 Lines of Code by Language", 
      "Total lines of code across all subsystems, broken down by programming language. Excludes markup and configuration languages (HTML, CSS, JSON, YAML, etc.).",
      "h2"
    );
    
    const chartContainer = document.createElement("div");
    chartContainer.style.height = "400px";
    chartContainer.style.marginTop = "20px";
    
    const canvas = document.createElement("canvas");
    chartContainer.appendChild(canvas);
    section.appendChild(chartContainer);
    container.appendChild(section);
    
    // Show all filtered languages (no "Others" category)
    const labels = filteredLanguages.map(([lang, _]) => lang);
    const data = filteredLanguages.map(([_, lines]) => lines);
    const total = data.reduce((sum, val) => sum + val, 0);
    
    // Create chart
    new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labels,
        datasets: [{
          label: 'Lines of Code',
          data: data,
          backgroundColor: [
            '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
            '#06B6D4', '#84CC16', '#F97316', '#EC4899', '#14B8A6',
            '#F43F5E', '#6366F1', '#A855F7', '#22D3EE', '#6B7280'
          ],
          borderColor: [
            '#1D4ED8', '#059669', '#D97706', '#DC2626', '#7C3AED',
            '#0891B2', '#65A30D', '#EA580C', '#DB2777', '#0D9488',
            '#E11D48', '#4F46E5', '#9333EA', '#06B6D4', '#4B5563'
          ],
          borderWidth: 1
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: false
          },
          tooltip: {
            callbacks: {
              label: function(context) {
                const lines = context.parsed.y;
                const percentage = ((lines / total) * 100).toFixed(1);
                return `${lines.toLocaleString()} lines (${percentage}%)`;
              }
            }
          }
        },
        scales: {
          y: {
            beginAtZero: true,
            ticks: {
              color: '#9CA3AF',
              callback: function(value) {
                if (value >= 1000) {
                  return (value / 1000).toFixed(0) + 'K';
                }
                return value;
              }
            },
            grid: {
              color: '#374151'
            }
          },
          x: {
            ticks: {
              color: '#9CA3AF',
              maxRotation: 45,
              minRotation: 45
            },
            grid: {
              display: false
            }
          }
        }
      }
    });
  } catch (error) {
    console.error("Error loading language lines distribution:", error);
  }
}

function getPrimaryLanguage(languages) {
  // Define languages we consider "real programming languages"
  const realLanguages = new Set([
    'JavaScript', 'TypeScript', 'Python', 'Java', 'C#', 'C++', 'C', 
    'Go', 'Rust', 'Swift', 'Kotlin', 'PHP', 'Ruby', 'Scala', 'Dart',
    'Objective-C', 'R', 'MATLAB', 'Perl', 'Haskell', 'Clojure', 'F#',
    'Elixir', 'Erlang', 'Lua', 'Julia', 'Assembly', 'Groovy',
    'Vim Script', 'Vim script', 'Emacs Lisp', 'OCaml', 'Scheme', 'Common Lisp', 
    'Forth', 'Ada', 'Fortran', 'COBOL', 'Pascal', 'D', 'Nim', 
    'Crystal', 'Zig', 'V', 'Odin', 'Raku', 'Awk',
    'Shell', 'Bash', 'Bourne Again Shell', 'Bourne Shell',
    'PowerShell', 'Zsh', 'Fish', 'Tcl',
    'SQL', 'PLpgSQL', 'PL/SQL', 'T-SQL', 'PostgreSQL',
    'Nix', 'Dhall', 'HCL', 'Jsonnet', 'CUE',
    'x86 Assembly', 'ARM Assembly', 'MIPS Assembly',
    'BASIC', 'Visual Basic', 'VBScript', 'Delphi', 'ActionScript',
    'WebAssembly', 'WASM'
  ]);

  // Languages to explicitly exclude (data/markup/config formats) - same as getLanguageStats
  const excludeLanguages = new Set([
    'HTML', 'CSS', 'SCSS', 'Sass', 'Less',
    'JSON', 'YAML', 'XML', 'TOML', 'INI',
    'Markdown', 'reStructuredText', 'AsciiDoc', 'LaTeX', 'TeX',
    'CSV', 'TSV', 'Properties', 'Dockerfile', 'Makefile',
    'Text', 'Binary', 'Data', 'Image', 'Video', 'Audio',
    'Protocol Buffer', 'Thrift', 'Avro', 'GraphQL',
    'Mustache', 'Handlebars', 'Jinja', 'Smarty',
    'SVG', 'PostScript', 'Rich Text Format', 'Unknown'
  ]);

  let maxLines = 0;
  let primaryLanguage = null;
  
  // Find the programming language with the most lines (same logic as getLanguageStats)
  for (const [lang, stats] of Object.entries(languages)) {
    // Include if it's explicitly in real languages, exclude if it's in exclude list
    const shouldInclude = realLanguages.has(lang) && !excludeLanguages.has(lang);
    
    if (shouldInclude && stats.code_lines > maxLines) {
      maxLines = stats.code_lines;
      primaryLanguage = lang;
    }
  }
  
  // If no programming language found, return null (will be filtered out)
  return primaryLanguage;
}

// Function to get primary language with correct filtering (for display purposes)
function getCorrectPrimaryLanguage(languages) {
  // Define languages we consider "real programming languages"
  const realLanguages = new Set([
    'JavaScript', 'TypeScript', 'Python', 'Java', 'C#', 'C++', 'C', 
    'Go', 'Rust', 'Swift', 'Kotlin', 'PHP', 'Ruby', 'Scala', 'Dart',
    'Objective-C', 'R', 'MATLAB', 'Perl', 'Haskell', 'Clojure', 'F#',
    'Elixir', 'Erlang', 'Lua', 'Julia', 'Assembly', 'Groovy',
    'Vim Script', 'Vim script', 'Emacs Lisp', 'OCaml', 'Scheme', 'Common Lisp', 
    'Forth', 'Ada', 'Fortran', 'COBOL', 'Pascal', 'D', 'Nim', 
    'Crystal', 'Zig', 'V', 'Odin', 'Raku', 'Awk',
    'Shell', 'Bash', 'Bourne Again Shell', 'Bourne Shell',
    'PowerShell', 'Zsh', 'Fish', 'Tcl',
    'SQL', 'PLpgSQL', 'PL/SQL', 'T-SQL', 'PostgreSQL',
    'Nix', 'Dhall', 'HCL', 'Jsonnet', 'CUE',
    'x86 Assembly', 'ARM Assembly', 'MIPS Assembly',
    'BASIC', 'Visual Basic', 'VBScript', 'Delphi', 'ActionScript',
    'WebAssembly', 'WASM'
  ]);

  // Languages to explicitly exclude (data/markup/config formats)
  const excludeLanguages = new Set([
    'HTML', 'CSS', 'SCSS', 'Sass', 'Less',
    'JSON', 'YAML', 'XML', 'TOML', 'INI',
    'Markdown', 'reStructuredText', 'AsciiDoc', 'LaTeX', 'TeX',
    'CSV', 'TSV', 'Properties', 'Dockerfile', 'Makefile',
    'Text', 'Binary', 'Data', 'Image', 'Video', 'Audio',
    'Protocol Buffer', 'Thrift', 'Avro', 'GraphQL',
    'Mustache', 'Handlebars', 'Jinja', 'Smarty',
    'SVG', 'PostScript', 'Rich Text Format', 'Unknown'
  ]);

  let maxLines = 0;
  let primaryLanguage = null;
  
  // Find the programming language with the most lines (use additions + deletions as proxy for activity)
  for (const [lang, stats] of Object.entries(languages)) {
    // Include if it's explicitly in real languages, exclude if it's in exclude list
    const shouldInclude = realLanguages.has(lang) && !excludeLanguages.has(lang);
    
    if (shouldInclude) {
      // Handle both formats: object with additions/deletions or just a number
      let langActivity;
      if (typeof stats === 'object' && stats !== null) {
        langActivity = (stats.additions || 0) + (stats.deletions || 0) || stats.code_lines || 0;
      } else {
        langActivity = stats || 0;
      }
      
      if (langActivity > maxLines) {
        maxLines = langActivity;
        primaryLanguage = lang;
      }
    }
  }
  
  return primaryLanguage;
}



function createContributionHeatmap(perDateData, fromDate, toDate) {
  console.log("Creating contribution heatmap", fromDate, "to", toDate);
  
  try {
    const heatmapDiv = document.createElement('div');
    heatmapDiv.className = 'github-heatmap';
    
    // Use the actual date range provided
    const startDate = new Date(fromDate);
    const endDate = new Date(toDate);
    
    // Check if this is a year-long period (more than 11 months)
    const daysDiff = Math.ceil((endDate - startDate) / (24 * 60 * 60 * 1000));
    const isYearlyView = daysDiff > 330; // Consider it yearly if more than 11 months
    
    let displayStart, displayEnd;
    
    // Always show full year for better visual consistency - this ensures all months (Jan-Dec) are visible
    const year = startDate.getFullYear();
    displayStart = new Date(year, 0, 1); // Jan 1
    displayEnd = new Date(year, 11, 31); // Dec 31
    console.log("Showing full year:", year, "(isYearlyView:", isYearlyView + ")");
    
    // Calculate dimensions
    const oneDay = 24 * 60 * 60 * 1000;
    
    // Find max commits for color scaling
    const maxCommits = Math.max(...Object.values(perDateData).map(d => d.commits || 0), 1);
    console.log("Max commits for scaling:", maxCommits);
    
    // Find first Sunday on or before the display start
    const firstDay = new Date(displayStart);
    while (firstDay.getDay() !== 0) {
      firstDay.setTime(firstDay.getTime() - oneDay);
    }
    
    // Calculate weeks to display
    const lastDay = new Date(displayEnd);
    while (lastDay.getDay() !== 6) {
      lastDay.setTime(lastDay.getTime() + oneDay);
    }
    
    const totalWeeks = Math.ceil((lastDay.getTime() - firstDay.getTime()) / (7 * oneDay)) + 1;
    console.log("Total weeks to display:", totalWeeks);
    
    // Create month labels - always show all 12 months for visual consistency
    const monthLabels = document.createElement('div');
    monthLabels.className = 'heatmap-months';
    
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    
    // Always show all 12 months for better visual consistency and navigation
    for (let month = 0; month < 12; month++) {
      const monthStart = new Date(displayStart.getFullYear(), month, 1);
      const weeksFromStart = Math.floor((monthStart.getTime() - firstDay.getTime()) / (7 * oneDay));
      
      const monthSpan = document.createElement('span');
      monthSpan.className = 'heatmap-month';
      monthSpan.textContent = monthNames[month];
      monthSpan.style.left = (25 + weeksFromStart * 15) + 'px';
      monthSpan.style.width = '40px';
      monthSpan.style.textAlign = 'left';
      monthSpan.style.position = 'absolute';
      monthLabels.appendChild(monthSpan);
    }
    
    heatmapDiv.appendChild(monthLabels);
    
    // Create weekday labels with proper spacing
    const weekdayLabels = document.createElement('div');
    weekdayLabels.className = 'heatmap-weekdays';
    const weekdayNames = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
    
    // Only show Mon, Wed, Fri to avoid clutter
    [1, 3, 5].forEach((dayIndex) => {
      const daySpan = document.createElement('span');
      daySpan.className = 'heatmap-weekday';
      daySpan.textContent = weekdayNames[dayIndex];
      daySpan.style.top = (dayIndex * 15 + 8) + 'px'; // 15px per day + offset for months 
      daySpan.style.width = '30px';
      daySpan.style.textAlign = 'right';
      daySpan.style.position = 'absolute';
      daySpan.style.right = '5px'; // Position from the right edge
      weekdayLabels.appendChild(daySpan);
    });
    
    heatmapDiv.appendChild(weekdayLabels);
    
    // Create the grid
    const grid = document.createElement('div');
    grid.className = 'heatmap-grid';
    grid.style.width = (totalWeeks * 15) + 'px'; // Fixed width based on weeks
    
    const currentDate = new Date(firstDay);
    
    for (let week = 0; week < totalWeeks; week++) {
      const weekDiv = document.createElement('div');
      weekDiv.className = 'heatmap-week';
      
      // Create 7 days for this week
      for (let dayOfWeek = 0; dayOfWeek < 7; dayOfWeek++) {
        const dayDiv = document.createElement('div');
        dayDiv.className = 'heatmap-day';
        
        const dateStr = currentDate.toISOString().split('T')[0];
        const dayData = perDateData[dateStr];
        const commits = dayData ? (dayData.commits || 0) : 0;
        
        // Show data for all dates within the display range
        if (currentDate >= displayStart && currentDate <= displayEnd) {
          // Color intensity based on commits
          let intensity = 0;
          if (commits > 0) {
            intensity = Math.min(4, Math.ceil((commits / maxCommits) * 4));
          }
          
          // All displayed data should be normal intensity since we only include selected month's data
          dayDiv.className += ' level-' + intensity;
          
          // Tooltip
          dayDiv.title = dateStr + ': ' + commits + ' commits';
          
          // Add click functionality to get the date
          dayDiv.addEventListener('click', function() {
            // Format the date nicely for display
            const clickedDate = new Date(dateStr);
            const formattedDate = clickedDate.toLocaleDateString('en-US', {
              weekday: 'long',
              year: 'numeric',
              month: 'long', 
              day: 'numeric'
            });
            
            // Show a more elegant notification instead of alert
            showDateNotification(formattedDate, commits, dateStr);
            
            // Log to console for potential further use
            console.log('Clicked on date:', dateStr, 'with', commits, 'commits');
          });
          
          // Add visual feedback on hover
          dayDiv.style.transition = 'transform 0.1s ease';
          dayDiv.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.2)';
          });
          dayDiv.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
          });
        } else {
          dayDiv.className += ' outside-range';
        }
        
        weekDiv.appendChild(dayDiv);
        currentDate.setTime(currentDate.getTime() + oneDay);
      }
      
      grid.appendChild(weekDiv);
    }
    
    heatmapDiv.appendChild(grid);
    
    console.log("Heatmap created successfully for period", fromDate, "to", toDate);
    return heatmapDiv;
  } catch (error) {
    console.error("Error in createContributionHeatmap:", error);
    const errorDiv = document.createElement('div');
    errorDiv.className = 'error';
    errorDiv.textContent = 'Error creating contribution heatmap: ' + error.message;
    return errorDiv;
  }
}

// --------------------------
// Initialization
// --------------------------

document.addEventListener("DOMContentLoaded", () => {
  console.log("DOM loaded, starting initialization");
  
  try {
    if (isKioskMode()) {
      initializeKioskMode();
      return;
    }
    // Set up mode buttons
    $("mode-users").addEventListener("click", () => setMode("users"));
    $("mode-teams").addEventListener("click", () => setMode("teams"));
    $("mode-subsystems").addEventListener("click", () => setMode("subsystems"));
    const alertsButton = $("mode-alerts");
    if (alertsButton) {
      alertsButton.addEventListener("click", () => setMode("alerts"));
    }
    
    // Start with subsystems mode
    setMode("subsystems");
    
    // Load data
    loadUsersAndSubsystems().then(() => {
      console.log("Initial data loaded successfully");
    }).catch(error => {
      console.error("Failed to load initial data:", error);
      // Even if data loading fails, try to show the UI
      setMode("subsystems");
    });

    // Initialize hamburger menu and settings with error handling
    try {
      initializeHamburgerMenu();
      console.log("Hamburger menu initialized");
    } catch (error) {
      console.error("Failed to initialize hamburger menu:", error);
    }
    
    try {
      initializeSettings();
      console.log("Settings initialized");
    } catch (error) {
      console.error("Failed to initialize settings:", error);
    }
    
    try {
      initializeIntegrations();
      console.log("Integrations initialized");
    } catch (error) {
      console.error("Failed to initialize integrations:", error);
    }
    
    refreshLastUpdateBanner();
    scheduleLastUpdateRefresh();
    
  } catch (error) {
    console.error("Error during initialization:", error);
  }
});

// --------------------------
// Hamburger Menu & Settings
// --------------------------

function initializeHamburgerMenu() {
  const hamburgerButton = $("hamburger-button");
  const hamburgerDropdown = $("hamburger-dropdown");
  const runUpdateLink = $("run-update-link");
  const settingsLink = $("settings-link");
  const integrationsLink = $("integrations-link");
  const aboutLink = $("about-link");

  const disableMenuLink = (link, message) => {
    if (!link) return;
    link.classList.add("disabled");
    link.setAttribute("aria-disabled", "true");
    if (message) {
      link.setAttribute("title", message);
    }
    link.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
    });
  };

  hamburgerButton.addEventListener("click", (e) => {
    e.stopPropagation();
    const isActive = hamburgerButton.classList.contains("active");
    
    if (isActive) {
      closeHamburgerMenu();
    } else {
      openHamburgerMenu();
    }
  });

  // Close menu when clicking outside
  document.addEventListener("click", (e) => {
    // Don't close hamburger menu if clicking in a modal
    if (e.target.closest(".modal.show") || e.target.closest(".json-import-modal.show")) {
      return;
    }
    
    if (!e.target.closest(".hamburger-button") && !e.target.closest(".hamburger-dropdown")) {
      closeHamburgerMenu();
    }
  });

  if (runUpdateLink) {
    if (READ_ONLY_MODE) {
      disableMenuLink(runUpdateLink, "Disabled in read-only mode");
    } else {
      runUpdateLink.addEventListener("click", (e) => {
        e.preventDefault();
        closeHamburgerMenu();
        startUpdateProcess();
      });
    }
  }

  if (READ_ONLY_MODE) {
    const msg = "Disabled in read-only mode";
    disableMenuLink(settingsLink, msg);
    disableMenuLink(integrationsLink, msg);
  } else {
    if (settingsLink) {
      settingsLink.addEventListener("click", (e) => {
        e.preventDefault();
        closeHamburgerMenu();
        openSettingsModal();
      });
    }

    if (integrationsLink) {
      integrationsLink.addEventListener("click", (e) => {
        e.preventDefault();
        closeHamburgerMenu();
        openIntegrationsModal();
      });
    }
  }

  // About link
  aboutLink.addEventListener("click", (e) => {
    e.preventDefault();
    closeHamburgerMenu();
    alert("repo-squirrel v1.0\n\nA comprehensive repository analytics dashboard providing insights into team activity, subsystem metrics, and development patterns.");
  });
}

function openHamburgerMenu() {
  const hamburgerButton = $("hamburger-button");
  const dropdown = $("hamburger-dropdown");
  
  // Calculate position based on button location
  const rect = hamburgerButton.getBoundingClientRect();
  dropdown.style.top = (rect.bottom + 8) + "px";
  dropdown.style.left = rect.left + "px";
  
  hamburgerButton.classList.add("active");
  dropdown.classList.add("show");
}

function closeHamburgerMenu() {
  $("hamburger-button").classList.remove("active");
  $("hamburger-dropdown").classList.remove("show");
}

function initializeSettings() {
  const modal = $("settings-modal");
  const closeButton = $("settings-modal-close");
  const tabs = document.querySelectorAll(".settings-tab");
  const tabContents = document.querySelectorAll(".settings-tab-content");

  // Close modal
  closeButton.addEventListener("click", closeSettingsModal);

  // Tab switching
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      const targetTab = tab.dataset.tab;
      switchSettingsTab(targetTab);
    });
  });

  // Save buttons
  $("save-ignore-users").addEventListener("click", saveIgnoreUsers);
  $("save-aliases").addEventListener("click", saveAliasesUI);
  $("save-teams").addEventListener("click", saveTeams);

  // Reset buttons
  $("reset-ignore-users").addEventListener("click", resetIgnoreUsers);
  $("reset-aliases").addEventListener("click", resetAliases);
  $("reset-teams").addEventListener("click", resetTeams);

  // Alias management - new UI initializes itself when loaded
  if ($("import-export-aliases")) {
    $("import-export-aliases").addEventListener("click", openJsonModal);
  }
  if ($("close-json-modal")) {
    $("close-json-modal").addEventListener("click", closeJsonModal);
  }
  if ($("import-json")) {
    $("import-json").addEventListener("click", importJsonAliases);
  }
  if ($("export-json")) {
    $("export-json").addEventListener("click", exportJsonAliases);
  }

  // Teams management
  $("add-team").addEventListener("click", addTeam);
  $("import-export-teams").addEventListener("click", openTeamsJsonModal);
  $("close-teams-json-modal").addEventListener("click", closeTeamsJsonModal);
  $("import-teams-json").addEventListener("click", importTeamsJson);
  $("export-teams-json").addEventListener("click", exportTeamsJson);

  // Repository management
  $("add-repo").addEventListener("click", addRepository);
  $("refresh-repos").addEventListener("click", () => {
    loadRepositoriesUI();
    // Show brief feedback
    const refreshBtn = $("refresh-repos");
    const originalText = refreshBtn.textContent;
    refreshBtn.textContent = "🔄 Refreshing...";
    refreshBtn.disabled = true;
    setTimeout(() => {
      refreshBtn.textContent = originalText;
      refreshBtn.disabled = false;
    }, 1000);
  });
  
  // Auto-derive repository name from URL
  $("repo-url").addEventListener("input", deriveRepositoryName);
  
  // Track manual editing of repository name
  $("repo-name").addEventListener("input", function() {
    const nameInput = $("repo-name");
    nameInput.dataset.manuallyEdited = "true";
    // Remove any error styling when user starts typing
    nameInput.style.borderColor = "";
    nameInput.style.backgroundColor = "";
  });

  // Subsystem management
  $("add-subsystem").addEventListener("click", addSubsystem);
  $("save-subsystems").addEventListener("click", saveSubsystems);
  $("reset-subsystems").addEventListener("click", resetSubsystems);
  $("import-export-subsystems").addEventListener("click", openSubsystemsJsonModal);
  $("close-subsystems-json-modal").addEventListener("click", closeSubsystemsJsonModal);
  $("import-subsystems-json").addEventListener("click", importSubsystemsJson);
  $("export-subsystems-json").addEventListener("click", exportSubsystemsJson);

  // Team responsibilities management
  $("responsibility-team").addEventListener("change", loadTeamResponsibilitySubsystems);
  $("hide-assigned-subsystems").addEventListener("change", loadTeamResponsibilitySubsystems);
  $("update-responsibilities").addEventListener("click", updateTeamResponsibilities);

  // Team capacity configuration
  $("add-language-capacity").addEventListener("click", addLanguageCapacity);
  $("save-capacity-config").addEventListener("click", saveCapacityConfig);
  
  // Background updates
  $("save-update-settings").addEventListener("click", saveUpdateSettings);

  // Initialize management states
  window.aliasesData = {};
  window.teamsData = {};
  window.repositoriesData = [];
  window.subsystemsData = {};
  window.teamResponsibilitiesData = {};
  window.capacityConfig = { default_lines_per_dev: 20000, languages: {}, yellow_threshold: 90, red_threshold: 110 };

  initializeKioskSettingsHandlers();
}

function openSettings(defaultTab = "ignore-users") {
  if (READ_ONLY_MODE) {
    alert("Settings are disabled in read-only mode.");
    return;
  }
  const modal = $("settings-modal");
  modal.classList.add("show");
  
  // Switch to the specified tab
  switchSettingsTab(defaultTab);
  
  // Load current settings
  loadIgnoreUsers().then(() => {
    setupIgnoreUsersSearch();
  });
  loadAliasesUI();
  loadTeamsUI();
  loadRepositoriesUI();
  loadSubsystemsUI();
  loadTeamResponsibilitiesUI();
  loadUpdateSettings();
  
  // Add backdrop click prevention
  modal.addEventListener("click", handleModalBackdropClick);
  
  // Focus on the main content area to help users see the first-time setup
  if (defaultTab === "repositories") {
    setTimeout(() => {
      const repoTab = document.querySelector('[data-tab="repositories"]');
      if (repoTab) {
        repoTab.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    }, 100);
  }
}

function openSettingsModal() {
  openSettings("ignore-users");
}

async function closeSettingsModal() {
  const modal = $("settings-modal");
  
  if (!READ_ONLY_MODE) {
    // Check if stats exist, and if not, prompt user to run update
    try {
      const statsResponse = await fetch("/api/stats/check");
      if (statsResponse.ok) {
        const statsData = await statsResponse.json();
        
        // If no stats exist and there are repositories configured, suggest running update
        if (!statsData.has_data) {
          const reposResponse = await fetch("/api/settings/repositories");
          if (reposResponse.ok) {
            const reposData = await reposResponse.json();
            if (reposData.repositories && reposData.repositories.length > 0) {
              // We have repos but no stats - show custom dialog
              modal.classList.remove("show");
              modal.removeEventListener("click", handleModalBackdropClick);
              
              // Show custom confirmation dialog
              showFirstUpdateDialog();
              return;
            }
          }
        }
      }
    } catch (error) {
      console.error("Error checking stats status:", error);
      // Continue closing modal even if check fails
    }
  }
  
  modal.classList.remove("show");
  
  // Remove backdrop click prevention
  modal.removeEventListener("click", handleModalBackdropClick);
}

function showFirstUpdateDialog() {
  const dialog = $("first-update-dialog");
  const confirmBtn = $("first-update-confirm");
  const cancelBtn = $("first-update-cancel");
  
  // Show the dialog
  dialog.style.display = "block";
  setTimeout(() => {
    dialog.classList.add("show");
  }, 10);
  
  // Handle confirm
  confirmBtn.onclick = () => {
    dialog.classList.remove("show");
    setTimeout(() => {
      dialog.style.display = "none";
      startUpdateProcess();
    }, 300);
  };
  
  // Handle cancel
  cancelBtn.onclick = () => {
    dialog.classList.remove("show");
    setTimeout(() => {
      dialog.style.display = "none";
    }, 300);
  };
  
  // Close on backdrop click
  dialog.onclick = (e) => {
    if (e.target === dialog) {
      cancelBtn.click();
    }
  };
}

function handleModalBackdropClick(e) {
  // Only close modal if clicking on the backdrop (the modal itself), not its content
  if (e.target === e.currentTarget) {
    // For settings modal, we don't want to close on backdrop click to prevent accidental loss
    // Users must explicitly click the close button or save/cancel
    e.stopPropagation();
    return;
  }
}

function switchSettingsTab(tabName) {
  // Update tab buttons
  document.querySelectorAll(".settings-tab").forEach(tab => {
    tab.classList.toggle("active", tab.dataset.tab === tabName);
  });

  // Update tab content
  document.querySelectorAll(".settings-tab-content").forEach(content => {
    content.classList.toggle("active", content.id === `${tabName}-tab`);
  });
  
  // Load data for specific tabs
  if (tabName === "capacity") {
    loadCapacityConfig();
  } else if (tabName === "updates") {
    loadUpdateSettings();
  } else if (tabName === "kiosk") {
    loadKioskSettingsUI();
  }
}


// --------------------------
// Kiosk mode runtime & settings
// --------------------------

async function initializeKioskMode() {
  try {
    const stage = $("kiosk-stage");
    if (!stage) {
      console.error("kiosk-stage element not found");
      return;
    }
    kioskState.stage = stage;
    kioskState.placeholder = $("kiosk-placeholder");
    setupKioskStage();
    startKioskClock();
    kioskState.initialized = true;
    await refreshIntegrationsStatus(true);
    await loadUsersAndSubsystems();
    await refreshKioskSlides();
    document.addEventListener("keydown", handleKioskHotkeys);
  } catch (error) {
    console.error("Failed to initialize kiosk mode:", error);
    showKioskPlaceholder(error?.message || "Unable to start kiosk mode.");
  }
}

function setupKioskStage() {
  if (!kioskState.stage) return;
  kioskState.stage.innerHTML = "";
  const overlay = document.createElement("div");
  overlay.className = "kiosk-overlay";
  const titleBlock = document.createElement("div");
  const titleEl = document.createElement("h1");
  titleEl.id = "kiosk-slide-title";
  titleEl.textContent = "Kiosk Mode";
  const subtitleEl = document.createElement("p");
  subtitleEl.id = "kiosk-slide-meta";
  subtitleEl.textContent = "Preparing visualizations";
  titleBlock.appendChild(titleEl);
  titleBlock.appendChild(subtitleEl);
  const clockEl = document.createElement("div");
  clockEl.className = "kiosk-time";
  clockEl.id = "kiosk-clock";
  overlay.appendChild(titleBlock);
  overlay.appendChild(clockEl);
  const container = document.createElement("div");
  container.className = "kiosk-slide-container";
  kioskState.stage.appendChild(overlay);
  kioskState.stage.appendChild(container);
  kioskState.overlayTitle = titleEl;
  kioskState.overlayMeta = subtitleEl;
  kioskState.overlayClock = clockEl;
  kioskState.slideContainer = container;
  if (kioskState.placeholder) {
    kioskState.placeholder.classList.add("kiosk-message");
    kioskState.stage.appendChild(kioskState.placeholder);
  }
}

function startKioskClock() {
  const updateClock = () => {
    if (!kioskState.overlayClock) return;
    const now = new Date();
    kioskState.overlayClock.textContent = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };
  updateClock();
  if (kioskState.clockTimer) {
    clearInterval(kioskState.clockTimer);
  }
  kioskState.clockTimer = setInterval(updateClock, 1000);
}

async function refreshKioskSlides() {
  try {
    const config = await fetchKioskConfig();
    await buildKioskSlides(config);
  } catch (error) {
    console.error("Unable to load kiosk settings:", error);
    showKioskPlaceholder(error?.message || "Unable to load kiosk configuration.");
  }
}

async function fetchKioskConfig() {
  const response = await fetch("/api/settings/kiosk");
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return await response.json();
}

async function buildKioskSlides(config) {
  const pages = normalizeKioskPages(config);
  kioskState.rotationSeconds = Math.max(5, config?.rotation_seconds || 30);
  kioskState.refreshMinutes = Math.max(1, config?.refresh_minutes || 15);
  clearKioskTimers();
  kioskState.slides = [];
  kioskState.currentIndex = -1;
  const container = ensureKioskSlideContainer();
  container.innerHTML = "";
  if (!pages.length) {
    showKioskPlaceholder("No kiosk pages configured yet. Open Settings → Kiosk Mode to add some.");
    return;
  }
  hideKioskPlaceholder();
  for (let i = 0; i < pages.length; i += 1) {
    const page = pages[i];
    try {
      const slide = await createKioskPage(page, i, container);
      if (slide) {
        kioskState.slides.push(slide);
        if (slide.element?.parentElement !== container) {
          container.appendChild(slide.element);
        }
      }
    } catch (error) {
      console.warn("Page failed", error);
      const fallback = createKioskMessageSlide(
        error?.message || "Unable to render page.",
        page?.title || `Page ${i + 1}`,
        page?.description || ""
      );
      kioskState.slides.push(fallback);
      if (fallback.element?.parentElement !== container) {
        container.appendChild(fallback.element);
      }
    }
  }
  if (!kioskState.slides.length) {
    showKioskPlaceholder("Unable to render the selected visualizations.");
    return;
  }
  advanceKioskSlide(1);
  scheduleKioskRotation();
  scheduleKioskRefresh();
}

function ensureKioskSlideContainer() {
  if (kioskState.slideContainer) {
    return kioskState.slideContainer;
  }
  if (kioskState.stage) {
    let container = kioskState.stage.querySelector('.kiosk-slide-container');
    if (!container) {
      container = document.createElement('div');
      container.className = 'kiosk-slide-container';
      kioskState.stage.appendChild(container);
    }
    kioskState.slideContainer = container;
    return container;
  }
  throw new Error('Missing kiosk stage container.');
}

async function createKioskPage(page, index, mountContainer = null) {
  const items = Array.isArray(page?.items) ? page.items : [];
  const pageTitle = (page?.title || ``).trim() || `Page ${index + 1}`;
  const pageSubtitle = (page?.description || ``).trim() || `${items.length} visualization${items.length === 1 ? '' : 's'}`;
  if (!items.length) {
    return createKioskMessageSlide("No visualizations configured for this page.", pageTitle, page?.description || "");
  }
  const slideEl = document.createElement("div");
  slideEl.className = "kiosk-slide kiosk-page-slide";
  slideEl.dataset.title = pageTitle;
  slideEl.dataset.subtitle = page?.description || pageSubtitle;
  if (mountContainer && mountContainer.appendChild) {
    mountContainer.appendChild(slideEl);
  }
  const grid = document.createElement("div");
  const layout = normalizeKioskLayout(page?.layout);
  grid.className = `kiosk-page-grid kiosk-layout-${layout}`;
  grid.dataset.layout = layout;
  slideEl.appendChild(grid);
  let renderedCount = 0;
  const renderContext = { key: null };
  for (const item of items) {
    try {
      const panel = await createKioskVisualization(item, renderContext);
      if (panel) {
        grid.appendChild(panel.element);
        reflowChartsForElement(panel.element);
        renderedCount += 1;
      }
    } catch (error) {
      console.warn("Visualization failed", error);
      grid.appendChild(createKioskPanelMessage(error?.message || "Unable to render visualization."));
    }
  }
  if (!renderedCount) {
    return createKioskMessageSlide("No visualizations rendered for this page.", pageTitle, page?.description || "");
  }
  return { element: slideEl, definition: null, item: null, page };
}

function createKioskPanelMessage(message) {
  const panel = document.createElement("div");
  panel.className = "kiosk-panel";
  const msgEl = document.createElement("div");
  msgEl.className = "kiosk-panel-message";
  msgEl.innerHTML = `<p>${message}</p>`;
  panel.appendChild(msgEl);
  return panel;
}

function createKioskMessageSlide(message, title = "Visualization", subtitle = "") {
  const slideEl = document.createElement("div");
  slideEl.className = "kiosk-slide";
  slideEl.dataset.title = title || "Visualization";
  slideEl.dataset.subtitle = subtitle || "";
  const msgEl = document.createElement("div");
  msgEl.className = "kiosk-message";
  msgEl.innerHTML = `<p>${message || 'Slide unavailable'}</p>`;
  slideEl.appendChild(msgEl);
  return { element: slideEl, definition: null, item: null };
}

async function createKioskVisualization(item, renderContext = null) {
  const def = VISUALIZATION_REGISTRY[item.visualization_id];
  if (!def) {
    return { element: createKioskPanelMessage(`Visualization "${item.visualization_id}" is not supported.`), definition: null, item };
  }
  const scope = def.scope || item.scope;
  const requiresEntity = def.requiresEntity !== false;
  if (requiresEntity && !item.entity_id) {
    return { element: createKioskPanelMessage("No entity configured for this slide."), definition: def, item };
  }
  let entity = null;
  if (requiresEntity) {
    entity = resolveEntityForScope(scope, item.entity_id);
    if (!entity) {
      return { element: createKioskPanelMessage(`Unable to find ${scope} "${item.entity_id}".`), definition: def, item };
    }
  }
  let period = null;
  if (requiresEntity) {
    period = resolvePeriodForEntity(scope, entity, item.period_mode || "latest-year");
    if (!period) {
      return { element: createKioskPanelMessage(`No ${item.period_mode || 'latest'} period exists for ${item.entity_id}.`), definition: def, item };
    }
  }
  const entityKey = requiresEntity ? getEntityKey(scope, entity) : "";
  const periodKey = requiresEntity ? buildPeriodKey(period) : "global";
  const viewKey = def?.kioskView || def?.id || item.visualization_id;
  const renderKey = `${scope || 'global'}|${viewKey}|${entityKey}|${periodKey}`;
  const shouldRenderSource = !renderContext || renderContext.key !== renderKey;
  if (shouldRenderSource) {
    await renderSourceForKiosk(scope, entity, period, def, item);
    if (renderContext) {
      renderContext.key = renderKey;
    }
  }
  const element = await waitForVisualizationElementBySelector(item.visualization_id, entityKey, 9000);
  if (!element) {
    return { element: createKioskPanelMessage(`Could not render ${def.label}${entityKey ? ` for ${entityKey}` : ''}.`), definition: def, item };
  }
  if (element.parentElement) {
    element.parentElement.removeChild(element);
  }
  detachChartsForElement(element);
  element.style.height = "100%";
  const panel = document.createElement("div");
  panel.className = "kiosk-panel";
  panel.appendChild(element);
  scheduleChartDetachment(element);
  return { element: panel, definition: def, item, entity, period };
}

function getEntityKey(scope, entity) {
  if (!entity) return "";
  if (scope === "user") return entity.slug;
  if (scope === "team") return entity.id || entity.name;
  if (scope === "subsystem") return entity.name;
  return entity.id || entity.name || entity.slug || "";
}

function getEntityLabel(scope, entity, fallback) {
  if (!entity) return fallback;
  if (scope === "user") return entity.display_name || entity.slug || fallback;
  if (scope === "team") return entity.name || entity.id || fallback;
  if (scope === "subsystem") return entity.name || fallback;
  return fallback;
}

function buildSlideSubtitle(entityLabel, period, periodMode) {
  const label = entityLabel || "";
  const periodText = formatPeriodLabel(period, periodMode);
  if (label && periodText) {
    return `${label} • ${periodText}`;
  }
  return label || periodText || "";
}

function formatPeriodLabel(period, mode) {
  if (!period) return mode === "latest-month" ? "Latest month" : "Latest year";
  if (period.label) {
    return period.label;
  }
  if (period.from && period.to) {
    return `${period.from} → ${period.to}`;
  }
  return buildPeriodKey(period);
}

function resolveEntityForScope(scope, entityId) {
  if (scope === "user") {
    return (state.users || []).find((user) => user.slug === entityId);
  }
  if (scope === "team") {
    return (state.teams || []).find((team) => (team.id || team.name) === entityId);
  }
  if (scope === "subsystem") {
    return (state.subsystems || []).find((subsystem) => subsystem.name === entityId);
  }
  return null;
}

function resolvePeriodForEntity(scope, entity, mode) {
  if (!entity) return null;
  const periodList = scope === "user" ? (entity.months || []) : (entity.periods || []);
  if (!periodList.length) {
    return null;
  }
  const targetIsYearly = mode !== "latest-month";
  const filtered = periodList.filter((p) => !!p && !!p.is_yearly === targetIsYearly);
  if (!filtered.length) {
    return null;
  }
  const sorted = [...filtered].sort((a, b) => (b.from || "").localeCompare(a.from || ""));
  return sorted[0];
}

async function renderSourceForKiosk(scope, entity, period, definition = null) {
  if (scope === "alerts") {
    if (!isPagerDutyConfigured()) {
      throw new Error("PagerDuty integration is required for this visualization.");
    }
    if (state.mode !== "alerts") {
      suppressAlertsModeWarning = true;
      setMode("alerts", false);
    }
    const targetView = definition?.kioskView || "overview";
    if (targetView === "all-incidents") {
      await showAllPagerDutyIncidentsView(false);
    } else {
      await showAlertsOverviewDashboard(false);
    }
    return;
  }
  if (scope === "user") {
    state.selectedUser = entity;
    state.selectedUserMonth = period;
    await loadUserMonth(entity, period);
    return;
  }
  if (scope === "team") {
    state.selectedTeam = entity;
    state.selectedTeamPeriod = period;
    await loadTeamPeriod(entity, period);
    return;
  }
  if (scope === "subsystem") {
    state.selectedSubsystem = entity;
    state.selectedSubsystemPeriod = period;
    await loadSubsystemPeriod(entity, period);
    return;
  }
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForVisualizationElementBySelector(vizId, entityKey, timeoutMs = 8000) {
  const start = Date.now();
  const selectorParts = [`[data-visualization-id="${vizId}"]`];
  if (entityKey) {
    selectorParts.push(`[data-visualization-entity="${cssEscape(entityKey)}"]`);
  }
  const selector = selectorParts.join("");
  const searchRoot = document.getElementById("app") || document;
  while (Date.now() - start < timeoutMs) {
    const el = searchRoot.querySelector(selector) || searchRoot.querySelector(`[data-visualization-id="${vizId}"]`);
    if (el) {
      return el;
    }
    await wait(150);
  }
  return null;
}

function cssEscape(value) {
  if (window.CSS?.escape) {
    return window.CSS.escape(value);
  }
  return String(value).replace(/[^a-zA-Z0-9_-]/g, (char) => `\\${char.charCodeAt(0).toString(16)} `);
}

function detachChartsForElement(element) {
  if (!element || !state.charts) return;
  Object.entries(state.charts).forEach(([key, chart]) => {
    if (chart?.canvas && element.contains(chart.canvas)) {
      delete state.charts[key];
    }
  });
}

function scheduleChartDetachment(element, attempts = 5, delay = 200) {
  if (!element) return;
  let remaining = Math.max(0, attempts);
  if (remaining <= 0) return;
  const tick = () => {
    if (remaining <= 0) return;
    detachChartsForElement(element);
    remaining -= 1;
    if (remaining > 0) {
      setTimeout(tick, delay);
    }
  };
  setTimeout(tick, delay);
}

function reflowChartsForElement(element) {
  if (!element || typeof Chart === "undefined") {
    return;
  }
  const canvases = element.querySelectorAll("canvas");
  if (!canvases.length) {
    return;
  }
  const resizeCharts = () => {
    canvases.forEach((canvas) => {
      let chartInstance = null;
      if (typeof Chart.getChart === "function") {
        chartInstance = Chart.getChart(canvas);
      }
      if (!chartInstance && typeof Chart.instances === "object") {
        const candidates = Array.isArray(Chart.instances)
          ? Chart.instances
          : Object.values(Chart.instances || {});
        chartInstance = candidates.find((instance) => instance?.canvas === canvas) || null;
      }
      if (!chartInstance) {
        chartInstance = canvas._chart || canvas.__chart || null;
      }
      if (!chartInstance) {
        return;
      }
      if (typeof chartInstance.resize === "function") {
        chartInstance.resize();
      }
      if (typeof chartInstance.update === "function") {
        chartInstance.update("none");
      }
    });
  };
  if (typeof requestAnimationFrame === "function") {
    requestAnimationFrame(resizeCharts);
  } else {
    setTimeout(resizeCharts, 0);
  }
}

function showKioskPlaceholder(message) {
  if (!kioskState.placeholder) return;
  kioskState.placeholder.style.display = "flex";
  kioskState.placeholder.innerHTML = `<p>${message}</p>`;
  if (kioskState.slideContainer) {
    kioskState.slideContainer.style.display = "none";
  }
}

function hideKioskPlaceholder() {
  if (kioskState.placeholder) {
    kioskState.placeholder.style.display = "none";
  }
  if (kioskState.slideContainer) {
    kioskState.slideContainer.style.display = "block";
  }
}

function advanceKioskSlide(step = 1) {
  if (!kioskState.slides.length) {
    showKioskPlaceholder("No slides ready.");
    return;
  }
  kioskState.currentIndex = (kioskState.currentIndex + step + kioskState.slides.length) % kioskState.slides.length;
  showKioskSlide(kioskState.currentIndex);
}

function showKioskSlide(index) {
  kioskState.slides.forEach((slide, idx) => {
    slide.element.classList.toggle("active", idx === index);
  });
  const active = kioskState.slides[index];
  if (active?.element) {
    kioskState.overlayTitle.textContent = active.element.dataset.title || active.definition?.label || "Visualization";
    kioskState.overlayMeta.textContent = active.element.dataset.subtitle || "";
  }
}

function scheduleKioskRotation() {
  if (kioskState.rotationTimer) {
    clearInterval(kioskState.rotationTimer);
  }
  kioskState.rotationTimer = setInterval(() => advanceKioskSlide(1), kioskState.rotationSeconds * 1000);
}

function scheduleKioskRefresh() {
  if (kioskState.refreshTimer) {
    clearTimeout(kioskState.refreshTimer);
  }
  kioskState.refreshTimer = setTimeout(() => {
    refreshKioskSlides();
  }, kioskState.refreshMinutes * 60000);
}

function clearKioskTimers() {
  if (kioskState.rotationTimer) {
    clearInterval(kioskState.rotationTimer);
    kioskState.rotationTimer = null;
  }
  if (kioskState.refreshTimer) {
    clearTimeout(kioskState.refreshTimer);
    kioskState.refreshTimer = null;
  }
}

function handleKioskHotkeys(event) {
  if (!isKioskMode()) return;
  if (event.key === "ArrowRight") {
    advanceKioskSlide(1);
  } else if (event.key === "ArrowLeft") {
    advanceKioskSlide(-1);
  } else if (event.key?.toLowerCase() === "r") {
    refreshKioskSlides();
  }
}

// --------------------------
// Kiosk settings management
// --------------------------

let kioskSettingsConfig = null;
let kioskSettingsLoading = false;

function initializeKioskSettingsHandlers() {
  const saveBtn = $("save-kiosk-settings");
  const resetBtn = $("reset-kiosk-settings");
  const addPageBtn = $("add-kiosk-page");
  const container = $("kiosk-pages-container");
  if (saveBtn) {
    saveBtn.addEventListener("click", saveKioskSettings);
    if (READ_ONLY_MODE) {
      saveBtn.disabled = true;
      saveBtn.title = "Disabled in read-only mode";
    }
  }
  if (resetBtn) {
    resetBtn.addEventListener("click", resetKioskSettings);
  }
  if (addPageBtn) {
    addPageBtn.addEventListener("click", addKioskPage);
    if (READ_ONLY_MODE) {
      addPageBtn.disabled = true;
      addPageBtn.title = "Disabled in read-only mode";
    }
  }
  if (container) {
    container.addEventListener("change", handleKioskSettingsChange);
    container.addEventListener("click", handleKioskSettingsClick);
  }
}

async function loadKioskSettingsUI(force = false) {
  if (kioskSettingsLoading) {
    return;
  }
  const alertEl = $("kiosk-settings-alert");
  if (alertEl) {
    alertEl.classList.remove("show");
  }
  kioskSettingsLoading = true;
  try {
    const response = await fetch("/api/settings/kiosk");
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    const payload = await response.json();
    kioskSettingsConfig = normalizeKioskSettingsConfig(payload);
    renderKioskSettingsForm();
  } catch (error) {
    console.error("Failed to load kiosk settings:", error);
    showKioskSettingsMessage(error?.message || "Unable to load kiosk configuration.", "error");
  } finally {
    kioskSettingsLoading = false;
  }
}

function normalizeKioskSettingsConfig(rawConfig) {
  const normalized = {
    rotation_seconds: rawConfig?.rotation_seconds || 30,
    refresh_minutes: rawConfig?.refresh_minutes || 15
  };
  normalized.pages = normalizeKioskPages(rawConfig);
  return normalized;
}

function renderKioskSettingsForm() {
  const rotationInput = $("kiosk-rotation-seconds");
  const refreshInput = $("kiosk-refresh-minutes");
  if (rotationInput && kioskSettingsConfig) {
    rotationInput.value = kioskSettingsConfig.rotation_seconds || 30;
    rotationInput.disabled = READ_ONLY_MODE;
  }
  if (refreshInput && kioskSettingsConfig) {
    refreshInput.value = kioskSettingsConfig.refresh_minutes || 15;
    refreshInput.disabled = READ_ONLY_MODE;
  }
  renderKioskPages();
}

function renderKioskPages() {
  const container = $("kiosk-pages-container");
  if (!container) return;
  container.innerHTML = "";
  const pages = Array.isArray(kioskSettingsConfig?.pages) ? kioskSettingsConfig.pages : [];
  if (!pages.length) {
    container.innerHTML = '<div class="empty-state">No kiosk pages configured yet.</div>';
    return;
  }
  pages.forEach((page, index) => {
    container.appendChild(renderKioskPageCard(page, index));
  });
}

function renderKioskPageCard(page, index) {
  const card = document.createElement("div");
  card.className = "kiosk-page-card";
  card.dataset.pageId = page.id;

  const header = document.createElement("div");
  header.className = "kiosk-page-header";

  const titleGroup = document.createElement("div");
  titleGroup.className = "form-group";
  const titleLabel = document.createElement("label");
  titleLabel.textContent = "Page title";
  const titleInput = document.createElement("input");
  titleInput.type = "text";
  titleInput.value = page.title || `Page ${index + 1}`;
  titleInput.placeholder = "e.g., Alerts overview";
  titleInput.dataset.field = "title";
  titleInput.dataset.pageId = page.id;
  titleInput.disabled = READ_ONLY_MODE;
  titleGroup.appendChild(titleLabel);
  titleGroup.appendChild(titleInput);
  header.appendChild(titleGroup);

  const actions = document.createElement("div");
  actions.className = "kiosk-page-actions";
  const removePageBtn = document.createElement("button");
  removePageBtn.className = "btn btn-link";
  removePageBtn.type = "button";
  removePageBtn.dataset.action = "remove-page";
  removePageBtn.dataset.pageId = page.id;
  removePageBtn.textContent = "Remove page";
  removePageBtn.disabled = READ_ONLY_MODE;
  actions.appendChild(removePageBtn);
  header.appendChild(actions);
  card.appendChild(header);

  const descriptionRow = document.createElement("div");
  descriptionRow.className = "kiosk-page-description";
  const descriptionGroup = document.createElement("div");
  descriptionGroup.className = "form-group";
  const descriptionLabel = document.createElement("label");
  descriptionLabel.textContent = "Subtitle (optional)";
  const descriptionInput = document.createElement("input");
  descriptionInput.type = "text";
  descriptionInput.placeholder = "Shown under the page title in kiosk mode";
  descriptionInput.value = page.description || "";
  descriptionInput.dataset.field = "description";
  descriptionInput.dataset.pageId = page.id;
  descriptionInput.disabled = READ_ONLY_MODE;
  descriptionGroup.appendChild(descriptionLabel);
  descriptionGroup.appendChild(descriptionInput);
  descriptionRow.appendChild(descriptionGroup);

  const layoutGroup = document.createElement("div");
  layoutGroup.className = "form-group";
  const layoutLabel = document.createElement("label");
  layoutLabel.textContent = "Layout";
  const layoutSelect = document.createElement("select");
  layoutSelect.dataset.field = "layout";
  layoutSelect.dataset.pageId = page.id;
  layoutSelect.disabled = READ_ONLY_MODE;
  [
    { value: "grid", label: "Balanced grid" },
    { value: "vertical", label: "Vertical stack" },
    { value: "horizontal", label: "Horizontal row" }
  ].forEach(({ value, label }) => {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = label;
    if (normalizeKioskLayout(page.layout) === value) {
      opt.selected = true;
    }
    layoutSelect.appendChild(opt);
  });
  layoutGroup.appendChild(layoutLabel);
  layoutGroup.appendChild(layoutSelect);
  descriptionRow.appendChild(layoutGroup);

  card.appendChild(descriptionRow);

  const itemsHeader = document.createElement("div");
  itemsHeader.className = "kiosk-items-header";
  const itemsTitle = document.createElement("h4");
  itemsTitle.textContent = "Visualizations";
  itemsHeader.appendChild(itemsTitle);
  const addItemBtn = document.createElement("button");
  addItemBtn.className = "btn btn-secondary";
  addItemBtn.type = "button";
  addItemBtn.dataset.action = "add-item";
  addItemBtn.dataset.pageId = page.id;
  addItemBtn.textContent = "+ Add visualization";
  addItemBtn.disabled = READ_ONLY_MODE;
  itemsHeader.appendChild(addItemBtn);
  card.appendChild(itemsHeader);

  const itemsWrapper = document.createElement("div");
  itemsWrapper.className = "kiosk-page-items kiosk-items-container";
  if (!page.items.length) {
    const emptyState = document.createElement("div");
    emptyState.className = "kiosk-page-empty";
    emptyState.textContent = "No visualizations yet. Add one to include this page in kiosk mode.";
    itemsWrapper.appendChild(emptyState);
  } else {
    page.items.forEach((item) => {
      itemsWrapper.appendChild(renderKioskItemCard(page, item));
    });
  }
  card.appendChild(itemsWrapper);

  return card;
}

function renderKioskItemCard(page, item) {
  const card = document.createElement("div");
  card.className = "kiosk-item-card";
  card.dataset.itemId = item.id;
  card.dataset.pageId = page.id;
  const vizOptions = getVisualizationOptions();
  const definition = VISUALIZATION_REGISTRY[item.visualization_id] || {};
  const scope = definition.scope || item.scope;
  if (scope && item.scope !== scope) {
    item.scope = scope;
  }
  const requiresEntity = definition.requiresEntity !== false;
  const entityOptions = requiresEntity ? getEntityOptions(scope) : [];
  const selectVisualization = document.createElement("select");
  selectVisualization.dataset.field = "visualization_id";
  selectVisualization.dataset.itemId = item.id;
  selectVisualization.dataset.pageId = page.id;
  selectVisualization.disabled = READ_ONLY_MODE;
  vizOptions.forEach((option) => {
    const opt = document.createElement("option");
    opt.value = option.id;
    opt.textContent = `${option.label}`;
    opt.selected = option.id === item.visualization_id;
    opt.dataset.scope = option.scope;
    selectVisualization.appendChild(opt);
  });
  const vizGroup = createFormGroup("Visualization", selectVisualization);

  let entityGroup = null;
  if (requiresEntity) {
    const entitySelect = document.createElement("select");
    entitySelect.dataset.field = "entity_id";
    entitySelect.dataset.itemId = item.id;
    entitySelect.dataset.pageId = page.id;
    entitySelect.disabled = READ_ONLY_MODE || !entityOptions.length;
    entityOptions.forEach((entity) => {
      const opt = document.createElement("option");
      opt.value = entity.value;
      opt.textContent = entity.label;
      if (entity.value === item.entity_id) {
        opt.selected = true;
      }
      entitySelect.appendChild(opt);
    });
    entityGroup = createFormGroup("Entity", entitySelect);
    if (!entityOptions.length) {
      const note = document.createElement("small");
      note.textContent = "No data loaded yet";
      note.style.color = "#fcd34d";
      entityGroup.appendChild(note);
    }
  }

  let periodGroup = null;
  if (requiresEntity) {
    const periodSelect = document.createElement("select");
    periodSelect.dataset.field = "period_mode";
    periodSelect.dataset.itemId = item.id;
    periodSelect.dataset.pageId = page.id;
    periodSelect.disabled = READ_ONLY_MODE;
    [
      { value: "latest-year", label: "Latest yearly data" },
      { value: "latest-month", label: "Latest monthly data" }
    ].forEach(({ value, label }) => {
      const opt = document.createElement("option");
      opt.value = value;
      opt.textContent = label;
      opt.selected = (item.period_mode || "latest-year") === value;
      periodSelect.appendChild(opt);
    });
    periodGroup = createFormGroup("Period", periodSelect);
  }

  const row = document.createElement("div");
  row.className = "kiosk-item-row";
  row.appendChild(vizGroup);
  if (entityGroup) {
    row.appendChild(entityGroup);
  }
  if (periodGroup) {
    row.appendChild(periodGroup);
  }
  const titleInput = document.createElement("input");
  titleInput.type = "text";
  titleInput.placeholder = "Custom title (optional)";
  titleInput.dataset.field = "custom_title";
  titleInput.dataset.itemId = item.id;
  titleInput.dataset.pageId = page.id;
  titleInput.value = item.custom_title || "";
  titleInput.disabled = READ_ONLY_MODE;
  const actions = document.createElement("div");
  actions.className = "kiosk-item-actions";
  actions.appendChild(titleInput);
  const removeBtn = document.createElement("button");
  removeBtn.className = "btn-link";
  removeBtn.dataset.action = "remove-item";
  removeBtn.dataset.itemId = item.id;
  removeBtn.dataset.pageId = page.id;
  removeBtn.type = "button";
  removeBtn.textContent = "Remove";
  if (READ_ONLY_MODE) {
    removeBtn.disabled = true;
  }
  actions.appendChild(removeBtn);
  card.appendChild(row);
  card.appendChild(actions);
  return card;
}

function createFormGroup(labelText, control) {
  const wrapper = document.createElement("div");
  wrapper.className = "form-group";
  const label = document.createElement("label");
  label.textContent = labelText;
  wrapper.appendChild(label);
  wrapper.appendChild(control);
  return wrapper;
}

function getVisualizationOptions() {
  return VISUALIZATION_DEFINITIONS.map((def) => ({
    id: def.id,
    label: def.label,
    scope: def.scope
  }));
}

function getEntityOptions(scope) {
  if (scope === "user") {
    return (state.users || []).map((user) => ({ value: user.slug, label: user.display_name || user.slug }));
  }
  if (scope === "team") {
    return (state.teams || []).map((team) => ({ value: team.id || team.name, label: team.name || team.id }));
  }
  if (scope === "subsystem") {
    return (state.subsystems || []).map((subsystem) => ({ value: subsystem.name, label: subsystem.name }));
  }
  return [];
}

function addKioskItem(pageId) {
  if (READ_ONLY_MODE) return;
  const defs = getVisualizationOptions();
  if (!defs.length) {
    showKioskSettingsMessage("No visualizations are available yet.", "error");
    return;
  }
  kioskSettingsConfig = kioskSettingsConfig || { rotation_seconds: 30, refresh_minutes: 15, pages: [] };
  kioskSettingsConfig.pages = kioskSettingsConfig.pages || [];
  let page = kioskSettingsConfig.pages.find((p) => p.id === pageId);
  if (!page) {
    page = {
      id: generateKioskPageId(),
      title: `Page ${kioskSettingsConfig.pages.length + 1}`,
      description: "",
      layout: "grid",
      items: []
    };
    kioskSettingsConfig.pages.push(page);
  }
  const def = defs[0];
  const requiresEntity = def.requiresEntity !== false;
  const entityOptions = requiresEntity ? getEntityOptions(def.scope) : [];
  const firstEntity = requiresEntity ? entityOptions[0] : null;
  const newItem = {
    id: generateKioskItemId(),
    visualization_id: def.id,
    scope: def.scope,
    entity_id: requiresEntity ? (firstEntity?.value || "") : "",
    entity_label: requiresEntity ? (firstEntity?.label || "") : "",
    period_mode: requiresEntity ? "latest-year" : "",
    custom_title: "",
    options: {}
  };
  page.items.push(newItem);
  renderKioskPages();
}

function addKioskPage() {
  if (READ_ONLY_MODE) return;
  kioskSettingsConfig = kioskSettingsConfig || { rotation_seconds: 30, refresh_minutes: 15, pages: [] };
  kioskSettingsConfig.pages = kioskSettingsConfig.pages || [];
  const newPage = {
    id: generateKioskPageId(),
    title: `Page ${kioskSettingsConfig.pages.length + 1}`,
    description: "",
    layout: "grid",
    items: []
  };
  kioskSettingsConfig.pages.push(newPage);
  renderKioskPages();
}

function removeKioskPage(pageId) {
  if (READ_ONLY_MODE) return;
  if (!kioskSettingsConfig?.pages?.length) {
    return;
  }
  if (!confirm("Remove this kiosk page?")) {
    return;
  }
  kioskSettingsConfig.pages = kioskSettingsConfig.pages.filter((page) => page.id !== pageId);
  renderKioskPages();
}

function handleKioskSettingsChange(event) {
  const field = event.target?.dataset?.field;
  const pageId = event.target?.dataset?.pageId;
  if (!field || !pageId || !kioskSettingsConfig?.pages) {
    return;
  }
  const page = kioskSettingsConfig.pages.find((p) => p.id === pageId);
  if (!page) {
    return;
  }
  const itemId = event.target?.dataset?.itemId;
  if (!itemId) {
    if (field === "title") {
      page.title = event.target.value;
    } else if (field === "description") {
      page.description = event.target.value;
    } else if (field === "layout") {
      page.layout = normalizeKioskLayout(event.target.value);
    }
    return;
  }
  const item = page.items.find((i) => i.id === itemId);
  if (!item) {
    return;
  }
  if (field === "visualization_id") {
    item.visualization_id = event.target.value;
    const def = VISUALIZATION_REGISTRY[item.visualization_id];
    item.scope = def?.scope;
    const requiresEntity = def?.requiresEntity !== false;
    if (requiresEntity) {
      const options = getEntityOptions(item.scope);
      if (!options.find((opt) => opt.value === item.entity_id)) {
        item.entity_id = options[0]?.value || "";
        item.entity_label = options[0]?.label || "";
      }
      if (!item.period_mode) {
        item.period_mode = "latest-year";
      }
    } else {
      item.entity_id = "";
      item.entity_label = "";
      item.period_mode = "";
    }
    renderKioskPages();
    return;
  }
  if (field === "entity_id") {
    item.entity_id = event.target.value;
    const options = getEntityOptions(item.scope);
    const selected = options.find((opt) => opt.value === item.entity_id);
    if (selected) {
      item.entity_label = selected.label;
    }
    return;
  }
  if (field === "period_mode") {
    item.period_mode = event.target.value;
    return;
  }
  if (field === "custom_title") {
    item.custom_title = event.target.value;
  }
}

function handleKioskSettingsClick(event) {
  const action = event.target?.dataset?.action;
  if (!action) {
    return;
  }
  if (action === "remove-item") {
    const pageId = event.target.dataset.pageId;
    const itemId = event.target.dataset.itemId;
    if (!pageId || !itemId || !kioskSettingsConfig?.pages) {
      return;
    }
    const page = kioskSettingsConfig.pages.find((p) => p.id === pageId);
    if (!page) {
      return;
    }
    page.items = page.items.filter((item) => item.id !== itemId);
    renderKioskPages();
    return;
  }
  if (action === "add-item") {
    addKioskItem(event.target.dataset.pageId);
    return;
  }
  if (action === "remove-page") {
    removeKioskPage(event.target.dataset.pageId);
  }
}

async function saveKioskSettings() {
  if (READ_ONLY_MODE) {
    showKioskSettingsMessage("Cannot save in read-only mode.", "error");
    return;
  }
  if (!kioskSettingsConfig) {
    return;
  }
  const rotationInput = $("kiosk-rotation-seconds");
  const refreshInput = $("kiosk-refresh-minutes");
  if (rotationInput) {
    kioskSettingsConfig.rotation_seconds = parseInt(rotationInput.value, 10) || 30;
  }
  if (refreshInput) {
    kioskSettingsConfig.refresh_minutes = parseInt(refreshInput.value, 10) || 15;
  }
  try {
    const response = await fetch("/api/settings/kiosk", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(kioskSettingsConfig)
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    if (data?.config) {
      kioskSettingsConfig = normalizeKioskSettingsConfig(data.config);
    }
    showKioskSettingsMessage("Kiosk configuration saved.", "success");
  } catch (error) {
    console.error("Failed to save kiosk settings:", error);
    showKioskSettingsMessage(error?.message || "Unable to save kiosk configuration.", "error");
  }
}

async function resetKioskSettings() {
  await loadKioskSettingsUI(true);
  showKioskSettingsMessage("Kiosk configuration reset.");
}

function showKioskSettingsMessage(message, type = "info") {
  const alertEl = $("kiosk-settings-alert");
  if (!alertEl) return;
  alertEl.textContent = message;
  alertEl.classList.remove("success", "error", "info");
  alertEl.classList.add("show");
  if (type === "success") {
    alertEl.style.background = "rgba(34,197,94,0.15)";
    alertEl.style.borderColor = "#22c55e";
  } else if (type === "error") {
    alertEl.style.background = "rgba(248,113,113,0.15)";
    alertEl.style.borderColor = "#ef4444";
  } else {
    alertEl.style.background = "rgba(59,130,246,0.1)";
    alertEl.style.borderColor = "#3b82f6";
  }
}

function generateKioskItemId() {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  return `kiosk-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

function generateKioskPageId() {
  if (window.crypto?.randomUUID) {
    return `page-${window.crypto.randomUUID()}`;
  }
  return `page-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

// --------------------------
// Integrations management
// --------------------------

function initializeIntegrations() {
  const modal = $("integrations-modal");
  if (!modal) return;

  const closeButton = $("integrations-modal-close");
  if (closeButton) {
    closeButton.addEventListener("click", closeIntegrationsModal);
  }

  const saveButton = $("save-pagerduty-token");
  if (saveButton) {
    saveButton.addEventListener("click", savePagerDutyToken);
    if (READ_ONLY_MODE) {
      saveButton.disabled = true;
      saveButton.title = "Disabled in read-only mode";
    }
  }

  const clearButton = $("clear-pagerduty-token");
  if (clearButton) {
    clearButton.addEventListener("click", clearPagerDutyToken);
    if (READ_ONLY_MODE) {
      clearButton.disabled = true;
      clearButton.title = "Disabled in read-only mode";
    }
  }
 
  refreshIntegrationsStatus(true);
}

async function openIntegrationsModal() {
  if (READ_ONLY_MODE) {
    alert("Integrations are disabled in read-only mode.");
    return;
  }

  const modal = $("integrations-modal");
  if (!modal) return;

  const input = $("pagerduty-api-token");
  if (input) {
    input.value = "";
  }

  modal.classList.add("show");
  modal.addEventListener("click", handleModalBackdropClick);
  await loadIntegrationsSettings();
}

function closeIntegrationsModal() {
  const modal = $("integrations-modal");
  if (!modal) return;
  modal.classList.remove("show");
  modal.removeEventListener("click", handleModalBackdropClick);
}

async function loadIntegrationsSettings() {
  if (READ_ONLY_MODE) {
    refreshIntegrationsStatus(true);
    renderPagerDutyIntegration({ error: "Integrations are disabled in read-only mode." });
    return;
  }

  const statusMessage = $("pagerduty-token-message");
  if (statusMessage) {
    statusMessage.textContent = "Loading PagerDuty settings…";
  }

  try {
    const response = await fetch("/api/settings/integrations");
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.error) {
      throw new Error(data.error || `HTTP ${response.status}`);
    }
    state.integrations = data || state.integrations;
    renderPagerDutyIntegration(data?.pagerduty || {});
    updateAlertsModeVisibility();
  } catch (error) {
    console.error("Failed to load integration settings:", error);
    renderPagerDutyIntegration({ error: error.message });
  }
}

function renderPagerDutyIntegration(info = {}) {
  const pill = $("pagerduty-token-pill");
  const message = $("pagerduty-token-message");
  const hasToken = !!info.has_token;

  if (pill) {
    pill.textContent = hasToken ? "Configured" : "Not configured";
    pill.classList.toggle("active", hasToken);
    pill.classList.toggle("idle", !hasToken);
  }

  if (message) {
    if (info.error) {
      message.textContent = `Unable to load PagerDuty settings: ${info.error}`;
    } else if (hasToken) {
      const preview = info.token_preview ? ` (${info.token_preview})` : "";
      const updated = info.updated_at ? ` · updated ${formatDateTime(info.updated_at)}` : "";
      message.textContent = `Token saved${preview}${updated}`;
    } else {
      message.textContent = "No token configured yet.";
    }
  }
}

async function savePagerDutyToken() {
  if (READ_ONLY_MODE) {
    alert("Integrations are disabled in read-only mode.");
    return;
  }

  const input = $("pagerduty-api-token");
  const token = (input?.value || "").trim();
  if (!token) {
    alert("Enter a PagerDuty API token before saving.");
    input?.focus();
    return;
  }

  const saveBtn = $("save-pagerduty-token");
  setIntegrationButtonState(saveBtn, true, "Saving…");

  try {
    const response = await fetch("/api/settings/integrations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pagerduty: { api_token: token } })
    });
    const data = await response.json();
    if (!response.ok || data.error) {
      throw new Error(data.error || "Failed to save PagerDuty token");
    }

    if (input) {
      input.value = "";
    }

    state.integrations = data || state.integrations;
    renderPagerDutyIntegration(data?.pagerduty || {});
    updateAlertsModeVisibility();
    alert("PagerDuty token saved.");
  } catch (error) {
    console.error("Failed to save PagerDuty token:", error);
    alert(error.message || "Failed to save PagerDuty token.");
  } finally {
    setIntegrationButtonState(saveBtn, false, "Save Token");
  }
}

async function clearPagerDutyToken() {
  if (READ_ONLY_MODE) {
    alert("Integrations are disabled in read-only mode.");
    return;
  }

  if (!confirm("Remove the saved PagerDuty token?")) {
    return;
  }

  const clearBtn = $("clear-pagerduty-token");
  setIntegrationButtonState(clearBtn, true, "Clearing…");

  try {
    const response = await fetch("/api/settings/integrations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pagerduty: { api_token: "" } })
    });
    const data = await response.json();
    if (!response.ok || data.error) {
      throw new Error(data.error || "Failed to remove PagerDuty token");
    }

    state.integrations = data || state.integrations;
    renderPagerDutyIntegration(data?.pagerduty || {});
    state.alerts.overview = null;
    state.alerts.error = null;
    updateAlertsModeVisibility();
    alert("PagerDuty token removed.");
  } catch (error) {
    console.error("Failed to clear PagerDuty token:", error);
    alert(error.message || "Failed to clear PagerDuty token.");
  } finally {
    setIntegrationButtonState(clearBtn, false, "Clear Token");
  }
}

function setIntegrationButtonState(button, loading, label) {
  if (!button) return;
  button.disabled = !!loading;
  if (label) {
    button.textContent = label;
  }
}

async function loadIgnoreUsers() {
  try {
    // Load the list of ignored users
    const ignoreResponse = await fetchJSON("/api/settings/ignore-users");
    const ignoredContent = ignoreResponse.content || "";
    const ignoredUsers = ignoredContent.split('\n').map(u => u.trim()).filter(u => u.length > 0);
    
    // Load available users
    const usersResponse = await fetchJSON("/api/settings/available-users");
    const availableUsers = usersResponse.users || [];
    
    // Store data globally
    window.ignoredUsersData = ignoredUsers;
    window.availableUsersData = availableUsers;
    
    // Render the UI
    renderIgnoreUsersUI();
    
  } catch (error) {
    console.error("Error loading ignore users:", error);
    window.ignoredUsersData = [];
    window.availableUsersData = [];
    renderIgnoreUsersUI();
  }
}

function renderIgnoreUsersUI() {
  const usersList = $("ignore-users-list");
  const ignoredSummary = $("ignored-users-summary");
  
  // Clear existing content
  usersList.innerHTML = '';
  ignoredSummary.innerHTML = '';
  
  const ignoredUsers = window.ignoredUsersData || [];
  const availableUsers = window.availableUsersData || [];
  
  // Render available users checkboxes
  if (availableUsers.length === 0) {
    usersList.innerHTML = '<div style="text-align: center; color: #9ca3af; padding: 20px;">No users found</div>';
  } else {
    availableUsers.forEach(user => {
      const isIgnored = ignoredUsers.includes(user.slug);
      
      const userItem = document.createElement("div");
      userItem.className = "user-checkbox-item";
      userItem.innerHTML = `
        <label>
          <input type="checkbox" value="${user.slug}" ${isIgnored ? 'checked' : ''} onchange="toggleIgnoreUser('${user.slug}')">
          <span>${user.display_name}</span>
        </label>
      `;
      usersList.appendChild(userItem);
    });
  }
  
  // Render currently ignored users
  if (ignoredUsers.length === 0) {
    ignoredSummary.innerHTML = '<div style="text-align: center; color: #9ca3af; padding: 20px;">No users are currently ignored</div>';
  } else {
    ignoredUsers.forEach(userSlug => {
      // Find display name if available
      const user = availableUsers.find(u => u.slug === userSlug);
      const displayName = user ? user.display_name : userSlug;
      
      const ignoredItem = document.createElement("div");
      ignoredItem.className = "ignored-user-item";
      ignoredItem.innerHTML = `
        <span class="ignored-user-name">${displayName}</span>
        <button class="unignore-btn" onclick="unignoreUser('${userSlug}')">Remove</button>
      `;
      ignoredSummary.appendChild(ignoredItem);
    });
  }
}

function toggleIgnoreUser(userSlug) {
  const ignoredUsers = window.ignoredUsersData || [];
  const index = ignoredUsers.indexOf(userSlug);
  
  if (index === -1) {
    // Add to ignored list
    ignoredUsers.push(userSlug);
  } else {
    // Remove from ignored list
    ignoredUsers.splice(index, 1);
  }
  
  window.ignoredUsersData = ignoredUsers;
  renderIgnoreUsersUI();
}

function unignoreUser(userSlug) {
  const ignoredUsers = window.ignoredUsersData || [];
  const index = ignoredUsers.indexOf(userSlug);
  
  if (index !== -1) {
    ignoredUsers.splice(index, 1);
    window.ignoredUsersData = ignoredUsers;
    renderIgnoreUsersUI();
  }
}

function setupIgnoreUsersSearch() {
  const searchInput = $("ignore-users-search");
  const usersList = $("ignore-users-list");
  
  searchInput.addEventListener('input', function() {
    const searchTerm = this.value.toLowerCase();
    const userItems = usersList.querySelectorAll('.user-checkbox-item');
    
    userItems.forEach(item => {
      const label = item.querySelector('label span');
      const userName = label.textContent.toLowerCase();
      const userSlug = item.querySelector('input').value.toLowerCase();
      
      if (userName.includes(searchTerm) || userSlug.includes(searchTerm)) {
        item.style.display = '';
      } else {
        item.style.display = 'none';
      }
    });
  });
}

// New improved alias UI state
window.aliasUIState = {
  availableUsers: [],
  selectedUserSlugs: [],
  aliasesData: {}
};

async function loadAliasesUI() {
  try {
    // Load existing aliases
    const response = await fetchJSON("/api/settings/aliases");
    const content = response.content || "{}";
    
    try {
      window.aliasUIState.aliasesData = JSON.parse(content);
      window.aliasesData = window.aliasUIState.aliasesData; // Keep for compatibility
    } catch (e) {
      console.error("Error parsing aliases JSON:", e);
      window.aliasUIState.aliasesData = {};
      window.aliasesData = {};
    }
    
    // Load available users
    const usersResponse = await fetchJSON("/api/settings/available-users");
    window.aliasUIState.availableUsers = usersResponse.users || [];
    
    // Initialize the new UI
    initializeAliasUIv2();
    renderAvailableUsers();
    renderAliasesList();
  } catch (error) {
    console.error("Error loading aliases:", error);
    window.aliasUIState.aliasesData = {};
    window.aliasesData = {};
    renderAliasesList();
  }
}

function initializeAliasUIv2() {
  // Set up event listeners for new UI
  const clearBtn = $("clear-selection");
  const createBtn = $("create-alias-group");
  const searchInput = $("user-search");
  const showAliasedCheckbox = $("show-aliased");
  
  if (clearBtn) {
    clearBtn.onclick = () => {
      window.aliasUIState.selectedUserSlugs = [];
      updateSelectedUsersList();
      renderAvailableUsers();
    };
  }
  
  if (createBtn) {
    createBtn.onclick = createAliasGroup;
  }
  
  if (searchInput) {
    searchInput.oninput = () => renderAvailableUsers();
  }
  
  if (showAliasedCheckbox) {
    showAliasedCheckbox.onchange = () => renderAvailableUsers();
  }
}

function renderAvailableUsers() {
  const container = $("available-users-grid");
  if (!container) return;
  
  const searchTerm = ($("user-search")?.value || "").toLowerCase();
  const showAliased = $("show-aliased")?.checked || false;
  const aliases = window.aliasUIState.aliasesData;
  
  // Get set of all slugs that are already aliased
  const aliasedSlugs = new Set();
  Object.entries(aliases).forEach(([canonical, slugs]) => {
    aliasedSlugs.add(canonical);
    slugs.forEach(s => aliasedSlugs.add(s));
  });
  
  // Filter users
  const filteredUsers = window.aliasUIState.availableUsers.filter(user => {
    const matchesSearch = user.display_name.toLowerCase().includes(searchTerm) || 
                         user.slug.toLowerCase().includes(searchTerm);
    const isAliased = aliasedSlugs.has(user.slug);
    const showUser = showAliased || !isAliased;
    
    return matchesSearch && showUser;
  });
  
  container.innerHTML = "";
  
  if (filteredUsers.length === 0) {
    container.innerHTML = '<div class="empty-state">No users found</div>';
    return;
  }
  
  filteredUsers.forEach(user => {
    const isSelected = window.aliasUIState.selectedUserSlugs.includes(user.slug);
    const isAliased = aliasedSlugs.has(user.slug);
    const isInactive = user.active === false;
    
    const userCard = document.createElement("div");
    userCard.className = `user-card ${isSelected ? 'selected' : ''} ${isAliased ? 'aliased' : ''} ${isInactive ? 'inactive-user' : ''}`;
    userCard.onclick = () => toggleUserSelection(user.slug);
    
    const inactiveBadge = isInactive ? '<div class="inactive-badge" title="No recent commits, but has code ownership">Inactive</div>' : '';
    
    userCard.innerHTML = `
      <div class="user-card-content">
        <div class="user-display-name">${user.display_name}</div>
        <div class="user-slug">${user.slug}</div>
        ${isAliased ? '<div class="aliased-badge">Grouped</div>' : ''}
        ${inactiveBadge}
      </div>
      <div class="user-card-check">${isSelected ? '✓' : ''}</div>
    `;
    
    container.appendChild(userCard);
  });
}

function toggleUserSelection(slug) {
  const index = window.aliasUIState.selectedUserSlugs.indexOf(slug);
  
  if (index >= 0) {
    window.aliasUIState.selectedUserSlugs.splice(index, 1);
  } else {
    window.aliasUIState.selectedUserSlugs.push(slug);
  }
  
  updateSelectedUsersList();
  renderAvailableUsers();
}

function updateSelectedUsersList() {
  const container = $("selected-users-list");
  const primarySelect = $("primary-user-select");
  const createBtn = $("create-alias-group");
  
  if (!container) return;
  
  const selected = window.aliasUIState.selectedUserSlugs;
  
  if (selected.length === 0) {
    container.innerHTML = '<div class="empty-state">No users selected</div>';
    primarySelect.disabled = true;
    primarySelect.innerHTML = '<option value="">Select primary user...</option>';
    createBtn.disabled = true;
    return;
  }
  
  // Show selected users
  container.innerHTML = "";
  selected.forEach(slug => {
    const user = window.aliasUIState.availableUsers.find(u => u.slug === slug);
    if (!user) return;
    
    const userTag = document.createElement("div");
    userTag.className = "selected-user-tag";
    userTag.innerHTML = `
      <span>${user.display_name} <small>(${slug})</small></span>
      <button class="remove-btn" onclick="event.stopPropagation(); toggleUserSelection('${slug}')">&times;</button>
    `;
    container.appendChild(userTag);
  });
  
  // Update primary select
  primarySelect.disabled = selected.length < 2;
  primarySelect.innerHTML = '<option value="">Select primary user...</option>';
  
  selected.forEach(slug => {
    const user = window.aliasUIState.availableUsers.find(u => u.slug === slug);
    if (user) {
      const option = document.createElement("option");
      option.value = slug;
      option.textContent = `${user.display_name} (${slug})`;
      primarySelect.appendChild(option);
    }
  });
  
  // Enable create button if we have 2+ users
  createBtn.disabled = selected.length < 2;
}

function createAliasGroup() {
  const primarySelect = $("primary-user-select");
  const primarySlug = primarySelect.value;
  
  if (!primarySlug) {
    alert("Please select a primary user identity");
    return;
  }
  
  const selected = window.aliasUIState.selectedUserSlugs;
  if (selected.length < 2) {
    alert("Please select at least 2 users to group");
    return;
  }
  
  // Create the alias: primary -> [other slugs]
  const otherSlugs = selected.filter(s => s !== primarySlug);
  
  window.aliasUIState.aliasesData[primarySlug] = otherSlugs;
  window.aliasesData = window.aliasUIState.aliasesData;
  
  // Clear selection and editing state
  window.aliasUIState.selectedUserSlugs = [];
  window.aliasUIState.editingGroup = null;
  updateSelectedUsersList();
  
  // Reset UI
  resetAliasCreationUI();
  
  // Re-render
  renderAvailableUsers();
  renderAliasesList();
}

function renderAliasesList() {
  const container = $("aliases-list");
  if (!container) return;
  
  container.innerHTML = "";
  
  const aliases = window.aliasUIState.aliasesData || window.aliasesData || {};
  
  if (Object.keys(aliases).length === 0) {
    container.innerHTML = '<div class="empty-state">No user groups configured yet</div>';
    return;
  }
  
  const getPrimaryUser = (slug) => {
    return window.aliasUIState.availableUsers.find(u => u.slug === slug) || { display_name: slug, slug };
  };
  
  // Sort aliases alphabetically by display name
  const sortedAliases = Object.entries(aliases).sort(([canonicalA], [canonicalB]) => {
    const userA = getPrimaryUser(canonicalA);
    const userB = getPrimaryUser(canonicalB);
    return userA.display_name.localeCompare(userB.display_name);
  });
  
  sortedAliases.forEach(([canonical, slugs]) => {
    const primaryUser = getPrimaryUser(canonical);
    
    // Sort the merged identities alphabetically too
    const sortedSlugs = [...slugs].sort((a, b) => {
      const userA = getPrimaryUser(a);
      const userB = getPrimaryUser(b);
      return userA.display_name.localeCompare(userB.display_name);
    });
    
    const aliasGroup = document.createElement("div");
    aliasGroup.className = "alias-group-card";
    
    aliasGroup.innerHTML = `
      <div class="alias-group-header">
        <div class="primary-user">
          <span class="primary-badge">PRIMARY</span>
          <strong>${primaryUser.display_name}</strong>
          <small>(${canonical})</small>
        </div>
        <div class="alias-group-actions">
          <button class="btn-secondary btn-small" onclick="editAliasGroup('${canonical}')">✏️ Edit</button>
          <button class="btn-danger btn-small" onclick="removeAliasGroup('${canonical}')">Delete Group</button>
        </div>
      </div>
      <div class="alias-group-members">
        <div class="members-label">Merged identities (${sortedSlugs.length}):</div>
        ${sortedSlugs.map(slug => {
          const user = getPrimaryUser(slug);
          return `<div class="alias-member">
            ${user.display_name} <small>(${slug})</small>
            <button class="btn-link btn-tiny" onclick="removeMemberFromGroup('${canonical}', '${slug}')" title="Remove this identity">×</button>
          </div>`;
        }).join('')}
      </div>
    `;
    
    container.appendChild(aliasGroup);
  });
}

function removeAliasGroup(canonical) {
  if (!confirm(`Remove this user group? Statistics will be separated again.`)) {
    return;
  }
  
  delete window.aliasUIState.aliasesData[canonical];
  window.aliasesData = window.aliasUIState.aliasesData;
  
  renderAliasesList();
  renderAvailableUsers();
}

function editAliasGroup(canonical) {
  // Load the existing group into the editor
  const currentAliases = window.aliasUIState.aliasesData[canonical] || [];
  
  // Store that we're editing (not creating new) - save the original state
  window.aliasUIState.editingGroup = {
    canonical: canonical,
    aliases: [...currentAliases]
  };
  
  // Select all users in the group (canonical + aliases)
  window.aliasUIState.selectedUserSlugs = [canonical, ...currentAliases];
  
  // Update the selected users display
  updateSelectedUsersList();
  
  // Set the primary user dropdown to the canonical
  const primarySelect = $("primary-user-select");
  if (primarySelect) {
    primarySelect.value = canonical;
  }
  
  // Update help text
  const helpText = document.querySelector(".alias-creation-section .help-text");
  if (helpText) {
    const primaryUser = window.aliasUIState.availableUsers.find(u => u.slug === canonical);
    const displayName = primaryUser ? primaryUser.display_name : canonical;
    helpText.innerHTML = `<strong>✏️ Editing group: "${displayName}"</strong><br>Select/deselect users, change primary identity if needed, then click "Update Group".`;
    helpText.style.background = "rgba(251, 191, 36, 0.1)";
    helpText.style.padding = "10px";
    helpText.style.borderRadius = "4px";
  }
  
  // Update buttons
  const createBtn = $("create-alias-group");
  if (createBtn) {
    createBtn.textContent = "Update Group";
  }
  
  const cancelBtn = $("cancel-edit-group");
  if (cancelBtn) {
    cancelBtn.style.display = "block";
  }
  
  // Temporarily remove the group from the list (will be re-added on save)
  delete window.aliasUIState.aliasesData[canonical];
  window.aliasesData = window.aliasUIState.aliasesData;
  
  // Re-render to show the group is being edited
  renderAliasesList();
  renderAvailableUsers();
  
  // Scroll to top
  document.querySelector(".alias-creation-section").scrollIntoView({ behavior: "smooth" });
}

function cancelEditAliasGroup() {
  // Restore the original group
  if (window.aliasUIState.editingGroup) {
    const { canonical, aliases } = window.aliasUIState.editingGroup;
    window.aliasUIState.aliasesData[canonical] = aliases;
    window.aliasesData = window.aliasUIState.aliasesData;
  }
  
  // Clear selection and editing state
  window.aliasUIState.selectedUserSlugs = [];
  window.aliasUIState.editingGroup = null;
  updateSelectedUsersList();
  
  // Reset UI
  resetAliasCreationUI();
  
  // Re-render
  renderAvailableUsers();
  renderAliasesList();
}

function removeMemberFromGroup(canonical, memberSlug) {
  if (!confirm(`Remove this identity from the group?`)) {
    return;
  }
  
  const aliases = window.aliasUIState.aliasesData[canonical] || [];
  const updated = aliases.filter(slug => slug !== memberSlug);
  
  if (updated.length === 0) {
    // If no aliases left, remove the whole group
    delete window.aliasUIState.aliasesData[canonical];
  } else {
    window.aliasUIState.aliasesData[canonical] = updated;
  }
  
  window.aliasesData = window.aliasUIState.aliasesData;
  
  renderAliasesList();
  renderAvailableUsers();
}

function resetAliasCreationUI() {
  const helpText = document.querySelector(".alias-creation-section .help-text");
  if (helpText) {
    helpText.innerHTML = '<strong>To create a new group:</strong> Select 2+ users from the list below, choose which identity to keep (primary), then click "Create Group".<br><strong>To edit existing group:</strong> Click "✏️ Edit" on any group below.';
    helpText.style.background = "";
    helpText.style.padding = "";
    helpText.style.borderRadius = "";
  }
  
  const createBtn = $("create-alias-group");
  if (createBtn) {
    createBtn.textContent = "Create Group";
    createBtn.onclick = createAliasGroup;
  }
  
  const cancelBtn = $("cancel-edit-group");
  if (cancelBtn) {
    cancelBtn.style.display = "none";
  }
  
  const primarySelection = document.querySelector(".primary-user-selection");
  if (primarySelection) {
    primarySelection.style.display = "block";
  }
}

function addAliasMapping() {
  const canonicalInput = $("canonical-name");
  const slugsInput = $("alias-slugs");
  
  const canonical = canonicalInput.value.trim();
  const slugsText = slugsInput.value.trim();
  
  if (!canonical) {
    alert("Please enter a canonical username");
    return;
  }
  
  if (!slugsText) {
    alert("Please enter at least one alternative slug");
    return;
  }
  
  const slugs = slugsText.split('\n').map(s => s.trim()).filter(s => s.length > 0);
  
  if (slugs.length === 0) {
    alert("Please enter at least one valid alternative slug");
    return;
  }
  
  // Add to aliases data
  if (!window.aliasesData) {
    window.aliasesData = {};
  }
  
  window.aliasesData[canonical] = slugs;
  
  // Clear form
  canonicalInput.value = "";
  slugsInput.value = "";
  
  // Re-render list
  renderAliasesList();
}

function removeSlugFromAlias(canonical, slugIndex) {
  if (!window.aliasesData[canonical]) return;
  
  window.aliasesData[canonical].splice(slugIndex, 1);
  
  // Remove the mapping entirely if no slugs left
  if (window.aliasesData[canonical].length === 0) {
    delete window.aliasesData[canonical];
  }
  
  renderAliasesList();
}

function removeAlias(canonical) {
  if (confirm(`Are you sure you want to remove the alias mapping for "${canonical}"?`)) {
    delete window.aliasesData[canonical];
    renderAliasesList();
  }
}

function editAlias(canonical) {
  const aliasData = window.aliasesData[canonical];
  if (!aliasData) return;
  
  // Fill the form with existing data
  $("canonical-name").value = canonical;
  $("alias-slugs").value = aliasData.join('\n');
  
  // Remove the existing mapping (will be re-added when form is submitted)
  delete window.aliasesData[canonical];
  renderAliasesList();
  
  // Focus the form
  $("canonical-name").focus();
}

async function saveAliasesUI() {
  const button = $("save-aliases");
  const originalText = button.textContent;
  
  try {
    button.textContent = "Saving...";
    button.disabled = true;
    
    const content = JSON.stringify(window.aliasesData || {}, null, 2);
    
    const response = await fetch("/api/settings/aliases", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ content })
    });
    
    const result = await response.json();
    
    if (response.ok) {
      button.textContent = "Saved!";
      setTimeout(() => {
        button.textContent = originalText;
      }, 2000);
    } else {
      throw new Error(result.error || "Failed to save");
    }
  } catch (error) {
    console.error("Error saving aliases:", error);
    alert("Error saving aliases: " + error.message);
    button.textContent = originalText;
  } finally {
    button.disabled = false;
  }
}

function openJsonModal() {
  const modal = $("json-modal");
  const textarea = $("json-content");
  
  // Pre-populate with current data
  textarea.value = JSON.stringify(window.aliasesData || {}, null, 2);
  
  modal.classList.add("show");
}

function closeJsonModal() {
  $("json-modal").classList.remove("show");
}

function importJsonAliases() {
  const textarea = $("json-content");
  const content = textarea.value.trim();
  
  if (!content) {
    alert("Please enter JSON content to import");
    return;
  }
  
  try {
    const parsed = JSON.parse(content);
    
    if (typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error("JSON must be an object");
    }
    
    // Validate structure
    for (const [key, value] of Object.entries(parsed)) {
      if (!Array.isArray(value)) {
        throw new Error(`Value for "${key}" must be an array`);
      }
      for (const item of value) {
        if (typeof item !== 'string') {
          throw new Error(`All items in "${key}" array must be strings`);
        }
      }
    }
    
    // If validation passes, update data
    window.aliasesData = parsed;
    renderAliasesList();
    closeJsonModal();
    
    alert("Aliases imported successfully!");
    
  } catch (error) {
    alert("Error importing JSON: " + error.message);
  }
}

function exportJsonAliases() {
  const textarea = $("json-content");
  textarea.value = JSON.stringify(window.aliasesData || {}, null, 2);
  textarea.select();
  document.execCommand('copy');
  alert("JSON copied to clipboard!");
}

async function saveIgnoreUsers() {
  const button = $("save-ignore-users");
  const originalText = button.textContent;
  
  try {
    button.textContent = "Saving...";
    button.disabled = true;
    
    const ignoredUsers = window.ignoredUsersData || [];
    const content = ignoredUsers.join('\n');
    
    const response = await fetch("/api/settings/ignore-users", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ content })
    });
    
    const result = await response.json();
    
    if (response.ok) {
      button.textContent = "Saved!";
      setTimeout(() => {
        button.textContent = originalText;
      }, 2000);
    } else {
      throw new Error(result.error || "Failed to save");
    }
  } catch (error) {
    console.error("Error saving ignore users:", error);
    alert("Error saving ignore users: " + error.message);
    button.textContent = originalText;
  } finally {
    button.disabled = false;
  }
}

function resetIgnoreUsers() {
  if (confirm("Are you sure you want to reset the ignore users list? This will reload from the file.")) {
    loadIgnoreUsers();
  }
}

function resetAliases() {
  if (confirm("Are you sure you want to reset the aliases? This will reload from the file and discard any unsaved changes.")) {
    loadAliasesUI();
  }
}

// Teams Management Functions

async function loadTeamsUI() {
  try {
    const response = await fetchJSON("/api/settings/teams");
    const teamsContent = JSON.parse(response.content || '{}');
    window.teamsData = teamsContent;
    
    // Load available users for team member selection
    const usersResponse = await fetchJSON("/api/settings/available-users");
    const availableUsers = usersResponse.users || [];
    
    // Store for use in rendering team member display names
    window.availableUsersData = availableUsers;
    
    // Set up filter checkbox handler
    const hideAssignedCheckbox = $("hide-assigned-users");
    if (hideAssignedCheckbox) {
      hideAssignedCheckbox.onchange = renderTeamMemberSelector;
    }
    
    // Initial render
    renderTeamMemberSelector();
    renderTeamsList();
  } catch (error) {
    console.error("Error loading teams:", error);
    const teamsList = $("teams-list");
    if (teamsList) {
      teamsList.innerHTML = '<div class="error">Failed to load teams: ' + error.message + '</div>';
    }
  }
}

function renderTeamMemberSelector(editingTeamId = null) {
  const memberSelector = $("team-member-selector");
  if (!memberSelector) return;
  
  const availableUsers = window.availableUsersData || [];
  const hideAssigned = $("hide-assigned-users")?.checked || false;
  
  // Get all users already in teams (excluding the team being edited)
  const usersInTeams = new Set();
  if (hideAssigned && window.teamsData) {
    Object.entries(window.teamsData).forEach(([teamId, team]) => {
      // Skip the team being edited
      if (editingTeamId && teamId === editingTeamId) {
        return;
      }
      if (team.members) {
        team.members.forEach(member => usersInTeams.add(member));
      }
    });
  }
  
  memberSelector.innerHTML = '';
  
  availableUsers.forEach(user => {
    // Skip if user is already in a team and filter is enabled
    if (hideAssigned && usersInTeams.has(user.slug)) {
      return;
    }
    
    const checkbox = document.createElement("div");
    checkbox.className = "member-checkbox";
    const isInactive = user.active === false;
    const inactiveClass = isInactive ? ' inactive-member' : '';
    const inactiveBadge = isInactive ? ' <span class="inactive-badge-inline" title="No recent commits, but has code ownership">Inactive</span>' : '';
    
    checkbox.innerHTML = `
      <label class="${inactiveClass}">
        <input type="checkbox" value="${user.slug}" name="team-member">
        <span>${user.display_name}${inactiveBadge}</span>
      </label>
    `;
    memberSelector.appendChild(checkbox);
  });
}

function renderTeamsList() {
  const teamsList = $("teams-list");
  teamsList.innerHTML = '';
  
  if (!window.teamsData || Object.keys(window.teamsData).length === 0) {
    teamsList.innerHTML = '<div class="no-data">No teams configured</div>';
    return;
  }
  
  // Create a map of slug -> display name from available users
  const userDisplayMap = {};
  if (window.availableUsersData) {
    window.availableUsersData.forEach(user => {
      userDisplayMap[user.slug] = user.display_name;
    });
  }
  
  Object.entries(window.teamsData).forEach(([teamId, teamData]) => {
    const teamItem = document.createElement("div");
    teamItem.className = "team-item";
    
    // Convert member slugs to display names
    const memberDisplayNames = (teamData.members || [])
      .map(slug => userDisplayMap[slug] || slug)
      .join(', ') || 'No members';
    
    teamItem.innerHTML = `
      <div class="team-info">
        <div class="team-header">
          <strong>${teamData.name || teamId}</strong>
          <div class="team-actions">
            <button class="btn btn-small edit-team-btn" data-team-id="${teamId}">Edit</button>
            <button class="btn btn-small btn-danger delete-team-btn" data-team-id="${teamId}">Delete</button>
          </div>
        </div>
        <div class="team-description">${teamData.description || 'No description'}</div>
        <div class="team-members">
          <strong>Members (${teamData.members?.length || 0}):</strong> 
          ${memberDisplayNames}
        </div>
      </div>
    `;
    teamsList.appendChild(teamItem);
  });
  
  // Add event listeners
  teamsList.querySelectorAll('.edit-team-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const teamId = e.target.getAttribute('data-team-id');
      editTeam(teamId);
    });
  });
  
  teamsList.querySelectorAll('.delete-team-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const teamId = e.target.getAttribute('data-team-id');
      deleteTeam(teamId);
    });
  });
}

function editTeam(teamId) {
  const teamData = window.teamsData[teamId];
  if (!teamData) return;
  
  // Populate the form with existing data
  $("team-id").value = teamId;
  $("team-name").value = teamData.name || '';
  $("team-description").value = teamData.description || '';
  
  // Re-render member selector with editing context (to show all members of this team)
  renderTeamMemberSelector(teamId);
  
  // Check the appropriate members
  const memberCheckboxes = document.querySelectorAll('input[name="team-member"]');
  memberCheckboxes.forEach(checkbox => {
    checkbox.checked = (teamData.members || []).includes(checkbox.value);
  });
  
  // Update the button text
  $("add-team").textContent = "Update Team";
  $("add-team").setAttribute('data-editing', teamId);
}

function deleteTeam(teamId) {
  if (confirm(`Are you sure you want to delete the team "${window.teamsData[teamId]?.name || teamId}"?`)) {
    delete window.teamsData[teamId];
    renderTeamsList();
    renderTeamMemberSelector();
  }
}

async function saveTeams() {
  try {
    const content = JSON.stringify(window.teamsData, null, 2);
    const response = await fetch("/api/settings/teams", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content })
    });
    
    const result = await response.json();
    if (response.ok) {
      alert("Teams saved successfully!");
      // Reload teams data in the main app
      const teamsResponse = await fetchJSON("/api/teams");
      state.teams = teamsResponse.teams || [];
      renderTeamList();
    } else {
      alert("Error saving teams: " + result.error);
    }
  } catch (error) {
    console.error("Error saving teams:", error);
    alert("Error saving teams: " + error.message);
  }
}

function resetTeams() {
  if (confirm("Are you sure you want to reset the teams? This will reload from the file and discard any unsaved changes.")) {
    loadTeamsUI();
  }
}

function resetTeams() {
  if (confirm("Are you sure you want to reset the teams? This will reload from the file and discard any unsaved changes.")) {
    loadTeamsUI();
  }
}

function addTeam() {
  const teamName = $("team-name").value.trim();
  const teamDescription = $("team-description").value.trim();
  
  if (!teamName) {
    alert("Please enter a team name");
    return;
  }
  
  // Auto-generate team ID from team name
  function generateTeamId(name) {
    return name.toLowerCase()
      .replace(/[^a-z0-9\s]/g, '') // Remove special characters
      .replace(/\s+/g, '-')        // Replace spaces with hyphens
      .replace(/-+/g, '-')         // Remove multiple consecutive hyphens
      .replace(/^-|-$/g, '');      // Remove leading/trailing hyphens
  }
  
  const baseTeamId = generateTeamId(teamName);
  let teamId = baseTeamId;
  
  // Get selected members
  const memberCheckboxes = document.querySelectorAll('input[name="team-member"]:checked');
  const members = Array.from(memberCheckboxes).map(cb => cb.value);
  
  // Check if editing existing team
  const editingTeamId = $("add-team").getAttribute('data-editing');
  
  if (editingTeamId) {
    // If editing, use the existing team ID
    teamId = editingTeamId;
  } else {
    // If creating new team, ensure unique ID
    let counter = 1;
    while (window.teamsData[teamId]) {
      teamId = `${baseTeamId}-${counter}`;
      counter++;
    }
  }
  
  // Add/update team
  window.teamsData[teamId] = {
    name: teamName,
    description: teamDescription,
    members: members
  };
  
  // Clear form
  $("team-id").value = '';
  $("team-name").value = '';
  $("team-description").value = '';
  memberCheckboxes.forEach(cb => cb.checked = false);
  
  // Reset button
  $("add-team").textContent = "Create Team";
  $("add-team").removeAttribute('data-editing');
  
  // Re-render list and member selector (to update filter)
  renderTeamsList();
  renderTeamMemberSelector();
}

function openTeamsJsonModal() {
  const modal = $("teams-json-modal");
  const content = $("teams-json-content");
  content.value = JSON.stringify(window.teamsData, null, 2);
  modal.style.display = "block";
}

function closeTeamsJsonModal() {
  $("teams-json-modal").style.display = "none";
}

function importTeamsJson() {
  try {
    const content = $("teams-json-content").value;
    const data = JSON.parse(content);
    window.teamsData = data;
    renderTeamsList();
    renderTeamMemberSelector();
    closeTeamsJsonModal();
    alert("Teams imported successfully!");
  } catch (error) {
    alert("Invalid JSON format: " + error.message);
  }
}

function exportTeamsJson() {
  const content = $("teams-json-content");
  content.value = JSON.stringify(window.teamsData, null, 2);
  content.select();
  document.execCommand("copy");
  alert("Teams JSON copied to clipboard!");
}

// Repository Management Functions

async function loadRepositoriesUI() {
  try {
    const response = await fetchJSON("/api/settings/repositories");
    window.repositoriesData = response.repositories || [];
    
    // Remove the 'exists' property since all listed repos should be cloned
    window.repositoriesData = window.repositoriesData.map(repo => ({
      name: repo.name,
      url: repo.url
    }));
    
    renderRepositoriesList();
    
    // Initialize manual editing flag
    const nameInput = $("repo-name");
    if (nameInput) {
      nameInput.dataset.manuallyEdited = "false";
    }
  } catch (error) {
    console.error("Error loading repositories:", error);
    window.repositoriesData = [];
    renderRepositoriesList();
  }
}

function renderRepositoriesList() {
  const container = $("repos-list");
  container.innerHTML = "";
  
  const repos = window.repositoriesData || [];
  
  if (repos.length === 0) {
    container.innerHTML = `
      <div style="text-align: center; padding: 30px; background: var(--background-secondary); border-radius: 8px; margin: 10px 0;">
        <h3 style="margin: 0 0 15px 0; color: var(--text-primary);">🚀 Welcome to repo-squirrel!</h3>
        <p style="margin: 0 0 15px 0; color: var(--text-secondary); font-size: 14px;">
          To get started, add your first Git repository above. You can add repositories from GitHub, GitLab, or any other Git hosting service.
        </p>
        <p style="margin: 0; color: var(--text-secondary); font-size: 12px;">
          <strong>Tip:</strong> After adding repositories, click "Run Update" from the hamburger menu to analyze your code and generate insights.
        </p>
      </div>
    `;
    return;
  }
  
  repos.forEach(repo => {
    const repoItem = document.createElement("div");
    repoItem.className = "repo-item";
    repoItem.id = `repo-${repo.name.replace(/[^a-zA-Z0-9]/g, '-')}`;
    
    repoItem.innerHTML = `
      <div class="repo-name">${repo.name}</div>
      <div class="repo-url">${repo.url}</div>
      <div class="repo-actions">
        <button class="btn-small btn-danger" onclick="removeRepository('${repo.name}')">Remove</button>
      </div>
    `;
    
    container.appendChild(repoItem);
  });
}

function deriveRepositoryName() {
  const urlInput = $("repo-url");
  const nameInput = $("repo-name");
  
  const url = urlInput.value.trim();
  
  if (!url) {
    // Only clear if not manually edited
    if (!nameInput.dataset.manuallyEdited) {
      nameInput.value = "";
      nameInput.placeholder = "Repository name will be auto-filled from URL";
    }
    return;
  }
  
  // Only auto-derive if the field is empty or hasn't been manually edited
  if (nameInput.value.trim() && nameInput.dataset.manuallyEdited === "true") {
    return; // User has manually edited, don't override
  }
  
  try {
    // Parse different Git URL formats:
    // https://github.com/owner/repo.git
    // https://github.com/owner/repo
    // https://github.com/owner/repo/
    // git@github.com:owner/repo.git
    // git@github.com:owner/repo
    
    let repoName = "";
    
    if (url.startsWith("https://") || url.startsWith("http://")) {
      // HTTP(S) URL format
      // Remove trailing slash if present
      const cleanUrl = url.replace(/\/$/, "");
      const pathMatch = cleanUrl.match(/^https?:\/\/[^\/]+\/(.+?)(?:\.git)?$/);
      if (pathMatch) {
        repoName = pathMatch[1];
      }
    } else if (url.includes("@") && url.includes(":")) {
      // SSH URL format: git@host:owner/repo.git
      const sshMatch = url.match(/^[^@]+@[^:]+:(.+?)(?:\.git)?$/);
      if (sshMatch) {
        repoName = sshMatch[1];
      }
    } else if (url.includes("/") && !url.includes("://")) {
      // Simple format: owner/repo
      repoName = url.replace(/\.git$/, "");
    }
    
    if (repoName) {
      // Validate format (should be owner/repo)
      if (/^[^\/]+\/[^\/]+$/.test(repoName)) {
        nameInput.value = repoName;
        nameInput.placeholder = "Auto-derived from URL (editable)";
        nameInput.dataset.manuallyEdited = "false"; // Mark as auto-derived
        
        // Remove any error styling
        nameInput.style.borderColor = "";
        nameInput.style.backgroundColor = "";
      } else {
        if (!nameInput.dataset.manuallyEdited) {
          nameInput.value = "";
          nameInput.placeholder = "Could not parse owner/repo from URL";
          nameInput.style.borderColor = "#dc3545";
          nameInput.style.backgroundColor = "#fff5f5";
        }
      }
    } else {
      if (!nameInput.dataset.manuallyEdited) {
        nameInput.value = "";
        nameInput.placeholder = "Invalid repository URL format";
        nameInput.style.borderColor = "#dc3545";
        nameInput.style.backgroundColor = "#fff5f5";
      }
    }
  } catch (error) {
    console.error("Error parsing repository URL:", error);
    if (!nameInput.dataset.manuallyEdited) {
      nameInput.value = "";
      nameInput.placeholder = "Error parsing URL";
      nameInput.style.borderColor = "#dc3545";
      nameInput.style.backgroundColor = "#fff5f5";
    }
  }
}

async function addRepository() {
  const urlInput = $("repo-url");
  const nameInput = $("repo-name");
  const addButton = $("add-repo");
  
  const url = urlInput.value.trim();
  
  if (!url) {
    alert("Please enter a repository URL");
    urlInput.focus();
    return;
  }
  
  // Auto-derive the name if it's not already set
  if (!nameInput.value.trim()) {
    deriveRepositoryName();
  }
  
  const name = nameInput.value.trim();
  
  if (!name) {
    alert("Could not derive repository name from URL. Please check the URL format.\n\nSupported formats:\n- https://github.com/owner/repo.git\n- https://github.com/owner/repo\n- git@github.com:owner/repo.git");
    urlInput.focus();
    urlInput.select();
    return;
  }
  
  if (!/^[^\/]+\/[^\/]+$/.test(name)) {
    alert("Repository name must be in format 'owner/repo'");
    urlInput.focus();
    urlInput.select();
    return;
  }
  
  // Check if already exists
  if (window.repositoriesData.some(repo => repo.name === name)) {
    alert("Repository already exists");
    return;
  }
  
  const originalText = addButton.textContent;
  
  try {
    // Update button to show starting
    addButton.textContent = "Starting clone...";
    addButton.disabled = true;
    addButton.classList.add('btn-loading');
    
    // Add progress indicator to form
    let progressIndicator = document.querySelector('.add-repo-progress');
    if (!progressIndicator) {
      progressIndicator = document.createElement('div');
      progressIndicator.className = 'add-repo-progress clone-progress';
      addButton.parentElement.appendChild(progressIndicator);
    }
    progressIndicator.innerHTML = `⏳ Starting clone of ${name}...`;
    
    // Start the clone
    const response = await fetch("/api/settings/repositories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ 
        action: "clone", 
        name: name,
        url: url 
      })
    });
    
    const result = await response.json();
    
    console.log('Clone start response:', result); // Debug
    
    if (response.ok && result.progress_id) {
      console.log(`Starting progress polling for ID: ${result.progress_id}`); // Debug
      
      // Start polling for progress
      const success = await pollCloneProgress(result.progress_id, progressIndicator, addButton, name);
      
      if (success) {
        // Clear the form
        urlInput.value = "";
        nameInput.value = "";
        nameInput.dataset.manuallyEdited = "false"; // Reset manual edit flag
        nameInput.placeholder = "Repository name will be auto-filled from URL";
        
        // Remove progress indicator
        progressIndicator.remove();
        
        // Show success state briefly
        addButton.textContent = "✅ Added & Cloned!";
        addButton.style.backgroundColor = "#10B981";
        
        setTimeout(() => {
          addButton.textContent = originalText;
          addButton.style.backgroundColor = "";
          addButton.disabled = false;
          addButton.classList.remove('btn-loading');
        }, 2000);
        
        // Reload repository list from backend to avoid duplicates
        await loadRepositoriesUI();
        
        // Update repository dropdown in subsystems tab
        populateRepositorySelect();
        
      } else {
        // Progress polling indicated failure - error already shown
        throw new Error("Clone failed");
      }
      
    } else {
      throw new Error(result.error || "Failed to start clone");
    }
    
  } catch (error) {
    console.error("Error adding repository:", error);
    
    // Remove progress indicator
    const progressIndicator = document.querySelector('.add-repo-progress');
    if (progressIndicator) {
      progressIndicator.remove();
    }
    
    // Restore button - but keep form data!
    addButton.textContent = originalText;
    addButton.disabled = false;
    addButton.classList.remove('btn-loading');
    
    // Show error but keep form filled so user can fix typos
    if (error.message !== "Clone failed") { // Don't show generic error if detailed error was already shown
      alert("❌ Error adding repository: " + error.message + "\n\nPlease check the URL and repository name, then try again.");
    }
    
    // Focus back to the likely problematic field
    if (error.message.includes("clone") || error.message.includes("repository")) {
      urlInput.focus();
      urlInput.select();
    } else {
      nameInput.focus();
      nameInput.select();
    }
  }
}

async function pollCloneProgress(progressId, progressIndicator, button, repoName) {
  return new Promise((resolve) => {
    let allMessages = [];
    
    const pollInterval = setInterval(async () => {
      try {
        console.log(`Polling progress for ${progressId}`); // Debug
        
        const response = await fetch(`/api/settings/repositories/clone-progress/${progressId}`);
        
        if (!response.ok) {
          console.error(`Progress poll failed: ${response.status}`); // Debug
          clearInterval(pollInterval);
          
          // If we get 404, the server may have restarted - check if clone completed
          if (response.status === 404) {
            console.log("Progress ID not found - checking if repository was cloned successfully");
            try {
              await loadRepositoriesUI();
              const currentRepos = window.repositoriesData || [];
              const repoExists = currentRepos.some(repo => repo.name === repoName);
              
              if (repoExists) {
                progressIndicator.innerHTML = `✅ ${repoName} cloned successfully!`;
                resolve(true);
                return;
              }
            } catch (checkError) {
              console.error("Error checking repository status:", checkError);
            }
          }
          
          progressIndicator.innerHTML = "❌ Error monitoring progress";
          resolve(false);
          return;
        }
        
        const progress = await response.json();
        console.log(`Progress response:`, progress); // Debug
        
        // Add new messages to our collection
        if (progress.progress_messages && progress.progress_messages.length > 0) {
          allMessages.push(...progress.progress_messages);
          
          console.log(`🔥 NEW MESSAGES (${progress.progress_messages.length}):`, progress.progress_messages); // Enhanced debug
          
          // Show the latest message or a summary
          const latestMessage = progress.progress_messages[progress.progress_messages.length - 1];
          const elapsed = progress.elapsed_time;
          
          console.log(`📝 Latest message: "${latestMessage}" (${elapsed}s elapsed)`); // Enhanced debug
          
          // Parse git progress for better display
          const displayMessage = parseGitProgress(latestMessage, elapsed);
          console.log(`🎨 Parsed display: "${displayMessage}"`); // Enhanced debug
          progressIndicator.innerHTML = displayMessage;
        } else {
          console.log(`No new messages, elapsed: ${progress.elapsed_time}s`); // Debug
          // No new messages, but update time
          const elapsed = progress.elapsed_time;
          if (elapsed > 5) { // Only show time update after 5 seconds
            progressIndicator.innerHTML = `⌛ Clone in progress... (${Math.floor(elapsed / 60) > 0 ? Math.floor(elapsed / 60) + 'm ' : ''}${elapsed % 60}s)`;
          }
        }
        
        // Update button text based on status
        if (progress.status === "cloning") {
          button.textContent = "Cloning...";
        } else if (progress.status === "starting") {
          button.textContent = "Starting...";
        }
        
        // Check if completed
        if (progress.status === "completed") {
          clearInterval(pollInterval);
          progressIndicator.innerHTML = "✅ Clone completed successfully!";
          resolve(true);
        } else if (progress.status === "failed") {
          clearInterval(pollInterval);
          progressIndicator.innerHTML = `❌ Clone failed: ${progress.error || "Unknown error"}`;
          alert(`❌ Failed to clone ${repoName}: ${progress.error || "Unknown error"}`);
          resolve(false);
        }
        
      } catch (error) {
        console.error("Error polling progress:", error);
        clearInterval(pollInterval);
        
        // If we lose connection, refresh the repository list to check if clone completed
        console.log("Lost connection during polling - refreshing repository list");
        setTimeout(async () => {
          try {
            await loadRepositoriesUI();
            const currentRepos = window.repositoriesData || [];
            const repoExists = currentRepos.some(repo => repo.name === repoName);
            
            if (repoExists) {
              progressIndicator.innerHTML = `✅ ${repoName} cloned successfully (verified after connection loss)`;
              button.disabled = false;
              button.classList.remove('btn-loading');
              resolve(true);
            } else {
              progressIndicator.innerHTML = "❌ Lost connection to progress";
              resolve(false);
            }
          } catch (refreshError) {
            console.error("Error refreshing repository list:", refreshError);
            progressIndicator.innerHTML = "❌ Lost connection to progress";
            resolve(false);
          }
        }, 2000);
      }
    }, 1000); // Poll every second
    
    // Timeout after 60 minutes
    setTimeout(() => {
      clearInterval(pollInterval);
      progressIndicator.innerHTML = "❌ Clone timed out";
      alert(`❌ Clone of ${repoName} timed out after 60 minutes`);
      resolve(false);
    }, 3600000);
  });
}

function parseGitProgress(message, elapsed) {
  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;
  const timeStr = minutes > 0 ? `${minutes}m ${seconds}s` : `${seconds}s`;
  
  // Parse common git progress patterns
  if (message.includes("Cloning into") || message.includes("Starting git clone")) {
    return `🔄 Starting clone... (${timeStr})`;
  } else if (message.includes("Receiving objects") || message.includes("remote: Counting objects")) {
    // Extract percentage and speed if available
    const percentMatch = message.match(/(\d+)%/);
    const speedMatch = message.match(/(\d+\.\d+\s*[KMGT]?i?B\/s)/);
    
    if (percentMatch) {
      const percent = percentMatch[1];
      const speed = speedMatch ? speedMatch[1] : "";
      return `⬇️ Receiving objects: ${percent}% ${speed} (${timeStr})`;
    }
    return `⬇️ Downloading repository data... (${timeStr})`;
  } else if (message.includes("Resolving deltas")) {
    const percentMatch = message.match(/(\d+)%/);
    if (percentMatch) {
      const percent = percentMatch[1];
      return `🔧 Resolving deltas: ${percent}% (${timeStr})`;
    }
    return `🔧 Processing repository structure... (${timeStr})`;
  } else if (message.includes("Checking out files") || message.includes("Updating files")) {
    const percentMatch = message.match(/(\d+)%/);
    if (percentMatch) {
      const percent = percentMatch[1];
      return `📁 Checking out files: ${percent}% (${timeStr})`;
    }
    return `📁 Setting up working directory... (${timeStr})`;
  } else if (message.includes("Clone in progress")) {
    return `⚡ ${message} (${timeStr})`;
  } else if (message.startsWith("✅") || message.startsWith("❌")) {
    return message; // Already formatted
  } else if (message.includes("remote:") || message.includes("Enumerating")) {
    return `🔍 Preparing download... (${timeStr})`;
  } else if (message.includes("Total")) {
    return `📦 Repository prepared for download (${timeStr})`;
  } else if (message.trim().length > 0) {
    // Generic progress with operation counter
    return `⏳ Processing... (${timeStr})`;
  } else {
    return `⌛ Clone in progress... (${timeStr})`;
  }
}

async function removeRepository(repoName) {
  if (!confirm(`Are you sure you want to remove repository "${repoName}"? This will delete the local files.`)) {
    return;
  }
  
  try {
    console.log(`Removing repository: ${repoName}`);
    
    // Show loading indicator
    const main = $("main-content");
    if (main) {
      const loadingDiv = document.createElement('div');
      loadingDiv.id = 'removal-loading';
      loadingDiv.style.cssText = 'position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%); background: white; padding: 30px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); z-index: 10000; text-align: center;';
      loadingDiv.innerHTML = `
        <div style="font-size: 18px; margin-bottom: 15px;">🗑️ Removing repository...</div>
        <div style="font-size: 14px; color: #666;">This may take a while for large repositories</div>
        <div style="margin-top: 15px; font-size: 12px; color: #999;">${repoName}</div>
      `;
      document.body.appendChild(loadingDiv);
    }
    
    // Use AbortController with 10 minute timeout for large repositories
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 600000); // 10 minutes
    
    const response = await fetch("/api/settings/repositories", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "remove", name: repoName }),
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      const errorText = await response.text();
      throw new Error(`HTTP ${response.status}: ${errorText}`);
    }
    
    const result = await response.json();
    console.log(`Remove result:`, result);
    
    if (result.success) {
      // If async removal, poll to check when it's done
      if (result.async) {
        console.log("Removal started in background, polling for completion...");
        const loadingDiv = document.getElementById('removal-loading');
        const startTime = Date.now();
        let removalTimeout;
        
        // Poll every 2 seconds to check if repository is removed
        const pollInterval = setInterval(async () => {
          try {
            const repoCheckResponse = await fetch("/api/settings/repositories");
            if (repoCheckResponse.ok) {
              const repoData = await repoCheckResponse.json();
              const stillExists = repoData.repositories.some(repo => repo.name === repoName);
              
              if (!stillExists) {
                // Repository removed!
                clearInterval(pollInterval);
                clearTimeout(removalTimeout);
                console.log("Repository successfully removed");
                
                // Update local data
                window.repositoriesData = window.repositoriesData.filter(repo => repo.name !== repoName);
                renderRepositoriesList();
                
                // Reload subsystems data
                try {
                  const subsystemResponse = await fetch("/api/subsystems");
                  if (subsystemResponse.ok) {
                    const subsystemData = await subsystemResponse.json();
                    state.subsystems = subsystemData.subsystems || [];
                    renderSubsystemList();
                  }
                } catch (subsystemError) {
                  console.error("Failed to reload subsystems:", subsystemError);
                }
                
                // Update repository dropdown
                populateRepositorySelect();
                
                // Remove loading indicator
                if (loadingDiv) {
                  loadingDiv.remove();
                }
              } else {
                // Still exists, update loading message
                if (loadingDiv) {
                  const elapsed = Math.floor((Date.now() - startTime) / 1000);
                  loadingDiv.innerHTML = `
                    <div style="font-size: 18px; margin-bottom: 15px;">🗑️ Removing repository...</div>
                    <div style="font-size: 14px; color: #666;">Large repository - this may take several minutes</div>
                    <div style="margin-top: 15px; font-size: 12px; color: #999;">${repoName}</div>
                    <div style="margin-top: 10px; font-size: 12px; color: #999;">${elapsed}s elapsed</div>
                  `;
                }
              }
            }
          } catch (pollError) {
            console.error("Error polling for removal completion:", pollError);
          }
        }, 2000);
        
        // Set a max timeout of 10 minutes
        removalTimeout = setTimeout(() => {
          clearInterval(pollInterval);
          const loadingDiv = document.getElementById('removal-loading');
          if (loadingDiv) {
            loadingDiv.remove();
          }
          alert("Repository removal is taking longer than expected. Please refresh the page to check if it completed.");
        }, 600000);
        
      } else {
        // Synchronous removal (small repos)
        window.repositoriesData = window.repositoriesData.filter(repo => repo.name !== repoName);
        renderRepositoriesList();
        
        // Reload subsystems data
        try {
          const subsystemResponse = await fetch("/api/subsystems");
          if (subsystemResponse.ok) {
            const subsystemData = await subsystemResponse.json();
            state.subsystems = subsystemData.subsystems || [];
            renderSubsystemList();
          }
        } catch (subsystemError) {
          console.error("Failed to reload subsystems:", subsystemError);
        }
        
        populateRepositorySelect();
        
        // Remove loading indicator
        const loadingDiv = document.getElementById('removal-loading');
        if (loadingDiv) {
          loadingDiv.remove();
        }
      }
    } else {
      alert("Error: " + (result.error || "Unknown error"));
      
      // Remove loading indicator
      const loadingDiv = document.getElementById('removal-loading');
      if (loadingDiv) {
        loadingDiv.remove();
      }
    }
  } catch (error) {
    console.error("Error removing repository:", error);
    
    // Remove loading indicator
    const loadingDiv = document.getElementById('removal-loading');
    if (loadingDiv) {
      loadingDiv.remove();
    }
    
    // Check if the repository was actually removed despite the error
    try {
      console.log("Checking if repository was removed despite error...");
      await loadRepositoriesUI();
      
      const stillExists = window.repositoriesData.some(repo => repo.name === repoName);
      if (!stillExists) {
        alert(`✅ Repository "${repoName}" was successfully removed (connection issue during confirmation)`);
      } else {
        alert(`❌ Error removing repository: ${error.message}`);
      }
    } catch (checkError) {
      alert(`❌ Error removing repository: ${error.message}`);
    }
  }
}

// Subsystem Management Functions

async function loadSubsystemsUI() {
  try {
    const response = await fetchJSON("/api/settings/subsystems");
    const content = response.content || "{}";
    
    try {
      window.subsystemsData = JSON.parse(content);
    } catch (e) {
      console.error("Error parsing subsystems JSON:", e);
      window.subsystemsData = {};
    }
    
    populateRepositorySelect();
    renderSubsystemsList();
  } catch (error) {
    console.error("Error loading subsystems:", error);
    window.subsystemsData = {};
    renderSubsystemsList();
  }
}

function populateRepositorySelect() {
  const select = $("subsystem-repo");
  select.innerHTML = '<option value="">Select a repository...</option>';
  
  const repos = window.repositoriesData || [];
  repos.forEach(repo => {
    const option = document.createElement("option");
    option.value = repo.name;
    option.textContent = repo.name;
    select.appendChild(option);
  });
}

function renderSubsystemsList() {
  const container = $("subsystems-list");
  container.innerHTML = "";
  
  const subsystems = window.subsystemsData || {};
  
  if (Object.keys(subsystems).length === 0) {
    container.innerHTML = '<div style="text-align: center; color: #9ca3af; padding: 20px;">No subsystem mappings configured</div>';
    return;
  }
  
  Object.entries(subsystems).forEach(([repoName, services]) => {
    Object.entries(services).forEach(([serviceName, paths]) => {
      const subsystemItem = document.createElement("div");
      subsystemItem.className = "subsystem-item";
      
      const pathTags = paths.map(path => {
        const isEntireRepo = path === "";
        return `<span class="path-tag ${isEntireRepo ? 'entire-repo' : ''}">${isEntireRepo ? '(entire repo)' : path}</span>`;
      }).join('');
      
      subsystemItem.innerHTML = `
        <div class="subsystem-name">${serviceName}</div>
        <div class="subsystem-repo">Repository: ${repoName}</div>
        <div class="subsystem-paths">${pathTags}</div>
        <div class="subsystem-actions">
          <button class="btn-small btn-edit" onclick="editSubsystem('${repoName}', '${serviceName}')">Edit</button>
          <button class="btn-small btn-danger" onclick="removeSubsystem('${repoName}', '${serviceName}')">Remove</button>
        </div>
      `;
      
      container.appendChild(subsystemItem);
    });
  });
}

function addSubsystem() {
  const repoSelect = $("subsystem-repo");
  const nameInput = $("subsystem-name");
  const pathsInput = $("subsystem-paths");
  
  const repoName = repoSelect.value.trim();
  const serviceName = nameInput.value.trim();
  const pathsText = pathsInput.value.trim();
  
  if (!repoName) {
    alert("Please select a repository");
    return;
  }
  
  if (!serviceName) {
    alert("Please enter a subsystem name");
    return;
  }
  
  const paths = pathsText ? pathsText.split('\n').map(p => p.trim()).filter(p => p.length > 0) : [""];
  
  // Initialize repo if it doesn't exist
  if (!window.subsystemsData[repoName]) {
    window.subsystemsData[repoName] = {};
  }
  
  // Add the subsystem
  window.subsystemsData[repoName][serviceName] = paths;
  
  // Clear form
  nameInput.value = "";
  pathsInput.value = "";
  repoSelect.value = "";
  
  renderSubsystemsList();
}

function removeSubsystem(repoName, serviceName) {
  if (!confirm(`Are you sure you want to remove subsystem "${serviceName}" from repository "${repoName}"?`)) {
    return;
  }
  
  if (window.subsystemsData[repoName]) {
    delete window.subsystemsData[repoName][serviceName];
    
    // Remove repo entry if no services left
    if (Object.keys(window.subsystemsData[repoName]).length === 0) {
      delete window.subsystemsData[repoName];
    }
  }
  
  renderSubsystemsList();
}

function editSubsystem(repoName, serviceName) {
  const subsystemData = window.subsystemsData[repoName]?.[serviceName];
  if (!subsystemData) return;
  
  // Fill the form
  $("subsystem-repo").value = repoName;
  $("subsystem-name").value = serviceName;
  $("subsystem-paths").value = subsystemData.join('\n');
  
  // Remove the existing mapping from data only (no confirmation dialog)
  if (window.subsystemsData[repoName]) {
    delete window.subsystemsData[repoName][serviceName];
    
    // Remove repo entry if no services left
    if (Object.keys(window.subsystemsData[repoName]).length === 0) {
      delete window.subsystemsData[repoName];
    }
  }
  
  // Re-render the list
  renderSubsystemsList();
  
  // Focus the form
  $("subsystem-name").focus();
}

async function saveSubsystems() {
  const button = $("save-subsystems");
  const originalText = button.textContent;
  
  try {
    button.textContent = "Saving...";
    button.disabled = true;
    
    const content = JSON.stringify(window.subsystemsData || {}, null, 2);
    
    const response = await fetch("/api/settings/subsystems", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ content })
    });
    
    const result = await response.json();
    
    if (response.ok) {
      button.textContent = "Saved!";
      setTimeout(() => {
        button.textContent = originalText;
      }, 2000);
    } else {
      throw new Error(result.error || "Failed to save");
    }
  } catch (error) {
    console.error("Error saving subsystems:", error);
    alert("Error saving subsystems: " + error.message);
    button.textContent = originalText;
  } finally {
    button.disabled = false;
  }
}

function resetSubsystems() {
  if (confirm("Are you sure you want to reset the subsystems? This will reload from the file and discard any unsaved changes.")) {
    loadSubsystemsUI();
  }
}

function openSubsystemsJsonModal() {
  const modal = $("subsystems-json-modal");
  const textarea = $("subsystems-json-content");
  
  textarea.value = JSON.stringify(window.subsystemsData || {}, null, 2);
  modal.classList.add("show");
}

function closeSubsystemsJsonModal() {
  $("subsystems-json-modal").classList.remove("show");
}

function importSubsystemsJson() {
  const textarea = $("subsystems-json-content");
  const content = textarea.value.trim();
  
  if (!content) {
    alert("Please enter JSON content to import");
    return;
  }
  
  try {
    const parsed = JSON.parse(content);
    
    if (typeof parsed !== 'object' || Array.isArray(parsed)) {
      throw new Error("JSON must be an object");
    }
    
    // Validate structure
    for (const [repoName, services] of Object.entries(parsed)) {
      if (typeof services !== 'object' || Array.isArray(services)) {
        throw new Error(`Services for "${repoName}" must be an object`);
      }
      for (const [serviceName, paths] of Object.entries(services)) {
        if (!Array.isArray(paths)) {
          throw new Error(`Paths for "${serviceName}" must be an array`);
        }
        for (const path of paths) {
          if (typeof path !== 'string') {
            throw new Error(`All paths in "${serviceName}" must be strings`);
          }
        }
      }
    }
    
    // If validation passes, update data
    window.subsystemsData = parsed;
    renderSubsystemsList();
    closeSubsystemsJsonModal();
    
    alert("Subsystems imported successfully!");
    
  } catch (error) {
    alert("Error importing JSON: " + error.message);
  }
}

function exportSubsystemsJson() {
  const textarea = $("subsystems-json-content");
  textarea.value = JSON.stringify(window.subsystemsData || {}, null, 2);
  textarea.select();
  document.execCommand('copy');
  alert("JSON copied to clipboard!");
}

// --------------------------
// Team Responsibilities Management
// --------------------------

async function loadTeamResponsibilitiesUI() {
  try {
    const response = await fetch("/api/settings/team-subsystem-responsibilities");
    const data = await response.json();
    
    if (data.error) {
      console.error("Error loading team responsibilities:", data.error);
      return;
    }
    
    window.teamResponsibilitiesData = data.responsibilities || {};
    
    // Populate team dropdown
    const teamSelect = $("responsibility-team");
    teamSelect.innerHTML = '<option value="">Select a team...</option>';
    
    for (const [teamId, teamInfo] of Object.entries(data.teams || {})) {
      const option = document.createElement("option");
      option.value = teamId;
      option.textContent = teamInfo.name || teamId;
      teamSelect.appendChild(option);
    }
    
    // Store available subsystems for later use
    window.availableSubsystems = data.available_subsystems || [];
    
    updateResponsibilitiesOverview();
    
  } catch (error) {
    console.error("Failed to load team responsibilities:", error);
  }
}

function loadTeamResponsibilitySubsystems(editingTeamId = null) {
  const teamId = $("responsibility-team").value;
  const subsystemsContainer = $("responsibility-subsystems");
  
  if (!teamId) {
    subsystemsContainer.innerHTML = '<p class="text-gray-400">Please select a team first.</p>';
    return;
  }
  
  const currentResponsibilities = window.teamResponsibilitiesData[teamId] || [];
  const hideAssigned = $("hide-assigned-subsystems").checked;
  
  // Build set of subsystems assigned to other teams (excluding current team)
  const assignedToOthers = new Set();
  if (hideAssigned) {
    for (const [otherTeamId, subsystems] of Object.entries(window.teamResponsibilitiesData)) {
      // Skip the team we're currently editing
      if (otherTeamId === (editingTeamId || teamId)) continue;
      
      for (const subsystem of subsystems) {
        assignedToOthers.add(subsystem);
      }
    }
  }
  
  subsystemsContainer.innerHTML = '';
  
  for (const subsystem of window.availableSubsystems || []) {
    // Skip if assigned to another team and we're hiding those
    if (hideAssigned && assignedToOthers.has(subsystem) && !currentResponsibilities.includes(subsystem)) {
      continue;
    }
    
    const item = document.createElement("div");
    item.className = "subsystem-checkbox-item";
    
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.id = `subsystem-${subsystem}`;
    checkbox.value = subsystem;
    checkbox.checked = currentResponsibilities.includes(subsystem);
    
    const label = document.createElement("label");
    label.htmlFor = `subsystem-${subsystem}`;
    label.textContent = subsystem;
    
    item.appendChild(checkbox);
    item.appendChild(label);
    subsystemsContainer.appendChild(item);
  }
}

async function updateTeamResponsibilities() {
  const teamId = $("responsibility-team").value;
  
  if (!teamId) {
    alert("Please select a team first.");
    return;
  }
  
  // Collect selected subsystems
  const checkboxes = document.querySelectorAll("#responsibility-subsystems input[type='checkbox']:checked");
  const selectedSubsystems = Array.from(checkboxes).map(cb => cb.value);
  
  // Update local data
  window.teamResponsibilitiesData[teamId] = selectedSubsystems;
  
  try {
    const response = await fetch("/api/settings/team-subsystem-responsibilities", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ responsibilities: window.teamResponsibilitiesData })
    });
    
    const result = await response.json();
    
    if (result.error) {
      alert(`Error: ${result.error}`);
      return;
    }
    
    alert("Team responsibilities updated successfully!");
    updateResponsibilitiesOverview();
    
  } catch (error) {
    console.error("Failed to update team responsibilities:", error);
    alert("Failed to update team responsibilities. Please check the console for details.");
  }
}

function updateResponsibilitiesOverview() {
  const container = $("responsibilities-overview");
  
  if (!window.teamResponsibilitiesData || Object.keys(window.teamResponsibilitiesData).length === 0) {
    container.innerHTML = '<p class="text-gray-400">No team responsibilities configured yet.</p>';
    return;
  }
  
  container.innerHTML = '';
  
  for (const [teamId, subsystems] of Object.entries(window.teamResponsibilitiesData)) {
    if (subsystems.length === 0) continue;
    
    const teamItem = document.createElement("div");
    teamItem.className = "team-responsibility-item";
    
    teamItem.innerHTML = `
      <div class="team-responsibility-header">
        <span class="team-responsibility-name">${teamId}</span>
        <span class="team-responsibility-count">${subsystems.length} subsystems</span>
      </div>
      <div class="responsibility-subsystems">
        ${subsystems.map(subsystem => `<span class="responsibility-subsystem-tag">${subsystem}</span>`).join('')}
      </div>
    `;
    
    container.appendChild(teamItem);
  }
}

// --------------------------
// Team Capacity Configuration
// --------------------------

async function loadCapacityConfig() {
  try {
    const response = await fetch("/api/settings/capacity-config");
    const data = await response.json();
    
    if (data.error) {
      console.error("Error loading capacity config:", data.error);
      return;
    }
    
    window.capacityConfig = data;
    
    // Update UI
    $("default-lines-per-dev").value = data.default_lines_per_dev || 20000;
    $("yellow-threshold").value = data.yellow_threshold || 95;
    $("red-threshold").value = data.red_threshold || 110;
    
    // Render language list
    renderLanguageCapacityList();
    
  } catch (error) {
    console.error("Failed to load capacity config:", error);
  }
}

function renderLanguageCapacityList() {
  const list = $("language-capacity-list");
  list.innerHTML = "";
  
  const languages = window.capacityConfig.languages || {};
  
  Object.keys(languages).sort().forEach(lang => {
    const item = document.createElement("div");
    item.className = "capacity-language-item";
    item.style.display = "flex";
    item.style.alignItems = "center";
    item.style.gap = "10px";
    item.style.marginBottom = "10px";
    item.style.padding = "10px";
    item.style.backgroundColor = "var(--background-tertiary)";
    item.style.borderRadius = "4px";
    
    item.innerHTML = `
      <span style="flex: 1; color: var(--text-primary);">${lang}</span>
      <span style="color: var(--text-secondary);">${languages[lang].toLocaleString()} lines/dev</span>
      <button class="btn btn-danger btn-sm" onclick="removeLanguageCapacity('${lang}')">Remove</button>
    `;
    
    list.appendChild(item);
  });
  
  if (Object.keys(languages).length === 0) {
    list.innerHTML = '<p style="color: var(--text-secondary); font-style: italic;">No language-specific configurations yet.</p>';
  }
}

function addLanguageCapacity() {
  const language = prompt("Enter language name (e.g., Python, Java, JavaScript):");
  if (!language) return;
  
  const linesPerDev = prompt(`Enter lines per developer for ${language}:`, "20000");
  if (!linesPerDev) return;
  
  const lines = parseInt(linesPerDev);
  if (isNaN(lines) || lines <= 0) {
    alert("Please enter a valid positive number.");
    return;
  }
  
  if (!window.capacityConfig.languages) {
    window.capacityConfig.languages = {};
  }
  
  window.capacityConfig.languages[language] = lines;
  renderLanguageCapacityList();
  
  // Auto-save after adding
  saveCapacityConfig();
}

function removeLanguageCapacity(language) {
  if (confirm(`Remove capacity configuration for ${language}?`)) {
    delete window.capacityConfig.languages[language];
    renderLanguageCapacityList();
    
    // Auto-save after removing
    saveCapacityConfig();
  }
}

async function saveCapacityConfig() {
  // Get values from inputs
  const defaultLines = parseInt($("default-lines-per-dev").value);
  const yellowThreshold = parseInt($("yellow-threshold").value);
  const redThreshold = parseInt($("red-threshold").value);
  
  if (isNaN(defaultLines) || defaultLines <= 0) {
    alert("Please enter a valid default lines per developer.");
    return;
  }
  
  if (isNaN(yellowThreshold) || yellowThreshold <= 0) {
    alert("Please enter a valid yellow threshold.");
    return;
  }
  
  if (isNaN(redThreshold) || redThreshold <= 0) {
    alert("Please enter a valid red threshold.");
    return;
  }
  
  window.capacityConfig.default_lines_per_dev = defaultLines;
  window.capacityConfig.yellow_threshold = yellowThreshold;
  window.capacityConfig.red_threshold = redThreshold;
  
  try {
    const response = await fetch("/api/settings/capacity-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(window.capacityConfig)
    });
    
    const result = await response.json();
    
    if (result.error) {
      alert(`Error: ${result.error}`);
      return;
    }
    
    alert("Capacity configuration saved successfully!");
    
  } catch (error) {
    console.error("Failed to save capacity config:", error);
    alert("Failed to save capacity configuration. Please check the console for details.");
  }
}

// --------------------------
// Update Process Management
// --------------------------

const MAX_DETAIL_LINES = 500;

let updateState = {
  isRunning: false,
  steps: [
    { id: 'git-pull', name: 'Updating Repositories', description: 'Running git pull on all repositories' },
    { id: 'master-script', name: 'Running Analysis', description: 'Executing master.py script' },
    { id: 'complete', name: 'Complete', description: 'Update process finished' }
  ],
  currentStep: 0,
  progress: 0
};

function startUpdateProcess() {
  if (READ_ONLY_MODE) {
    alert("Manual updates are disabled in read-only mode.");
    return;
  }
  if (updateState.isRunning) {
    alert("Update is already running. Please wait for it to complete.");
    return;
  }

  // Reset state
  updateState.isRunning = true;
  updateState.currentStep = 0;
  updateState.progress = 0;

  const logContent = $("update-log-content");
  if (logContent) {
    logContent.innerHTML = "";
  }
  const detailContent = $("update-detailed-content");
  if (detailContent) {
    detailContent.innerHTML = "";
  }

  // Show modal
  const modal = $("update-modal");
  modal.classList.add("show");
  
  // Initialize UI
  updateProgressUI();
  
  // Start the update process
  runUpdate();
}

function updateProgressUI() {
  const step = updateState.steps[updateState.currentStep];
  const statusTitle = $("update-status-title");
  const progressBar = $("update-progress-bar");
  const progressText = $("update-progress-text");
  
  if (step) {
    statusTitle.textContent = step.name;
    progressBar.style.width = updateState.progress + "%";
    progressText.textContent = Math.round(updateState.progress) + "%";
  }
}

function addUpdateLogMessage(message, type = 'info') {
  const logContent = $("update-log-content");
  if (!logContent) return;
  const messageDiv = document.createElement("div");
  messageDiv.className = `log-message ${type}`;
  messageDiv.textContent = message;
  logContent.appendChild(messageDiv);
  
  // Auto-scroll to bottom
  logContent.scrollTop = logContent.scrollHeight;
}

function addDetailedProgressMessage(message) {
  const detailContent = $("update-detailed-content");
  if (!detailContent || !message) return;
  const shouldStick = detailContent.scrollTop + detailContent.clientHeight >= detailContent.scrollHeight - 8;
  const line = document.createElement("div");
  line.className = "detail-line";
  line.textContent = message;
  detailContent.appendChild(line);
  while (detailContent.children.length > MAX_DETAIL_LINES) {
    detailContent.removeChild(detailContent.firstChild);
  }
  if (shouldStick) {
    detailContent.scrollTop = detailContent.scrollHeight;
  }
}

async function runUpdate() {
  try {
    addUpdateLogMessage("🚀 Starting update process...", "step");
    
    // Start the unified update process - 12-month rolling window 
    const startResponse = await fetch("/api/update/run-analysis", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({})
    });
    
    const startResult = await startResponse.json();
    
    if (!startResponse.ok) {
      throw new Error("Failed to start update process: " + (startResult.error || "Unknown error"));
    }
    
    // Connect to Server-Sent Events for real-time progress
    return new Promise((resolve, reject) => {
      const eventSource = new EventSource("/api/update/progress");
      
      eventSource.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          
          switch (data.type) {
            case 'info':
              addUpdateLogMessage(data.message, "info");
              // Update progress if provided
              if (data.progress !== undefined) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              break;
            case 'warning':
              addUpdateLogMessage(data.message, "warning");
              if (data.progress !== undefined) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              break;
            case 'success':
              addUpdateLogMessage(data.message, "success");
              if (data.progress !== undefined) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              break;
            case 'detail':
              addDetailedProgressMessage(data.message);
              if (typeof data.progress === "number" && data.progress > updateState.progress) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              break;
            case 'error':
              addUpdateLogMessage(data.message, "error");
              if (data.progress !== undefined) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              eventSource.close();
              refreshLastUpdateBanner();
              reject(new Error(data.message));
              return;
            case 'complete':
              eventSource.close();
              updateState.progress = 100;
              updateProgressUI();
              addUpdateLogMessage("🎉 Update process completed successfully!", "success");
              refreshLastUpdateBanner();
              scheduleLastUpdateRefresh();
              
              // Show completion actions
              const actions = $("update-actions");
              actions.style.display = "flex";
              
              // Set up action handlers
              $("update-close").onclick = () => {
                closeUpdateModal();
              };
              
              $("refresh-page").onclick = () => {
                window.location.reload();
              };
              
              // Add download logs handler
              if ($("download-update-logs")) {
                $("download-update-logs").onclick = () => {
                  window.open('/api/update/logs/download', '_blank');
                };
              }
              
              resolve({ success: true });
              break;
            case 'heartbeat':
              // Ignore heartbeat messages
              break;
          }
        } catch (e) {
          console.error("Error parsing SSE message:", e);
        }
      };
      
      eventSource.onerror = function(event) {
        console.error("SSE error:", event);
        eventSource.close();
        reject(new Error("Connection to update progress lost"));
      };
      
      // Set a timeout to prevent hanging forever - increased for enterprise-scale batch operations
      setTimeout(() => {
        if (eventSource.readyState !== EventSource.CLOSED) {
          eventSource.close();
          reject(new Error("Update process timed out"));
        }
      }, 432000000); // 120 hours timeout (5 days for massive enterprise operations)
    });
    
  } catch (error) {
    console.error("Update process failed:", error);
    addUpdateLogMessage("❌ Update failed: " + error.message, "error");
    
    const statusTitle = $("update-status-title");
    statusTitle.textContent = "Update Failed";
    
    // Show close button
    const actions = $("update-actions");
    actions.style.display = "flex";
    
    $("update-close").onclick = () => {
      closeUpdateModal();
    };
    
    // Add download logs handler
    if ($("download-update-logs")) {
      $("download-update-logs").onclick = () => {
        window.open('/api/update/logs/download', '_blank');
      };
    }
    
    $("refresh-page").style.display = "none"; // Hide refresh on error
    throw error;
    
  } finally {
    updateState.isRunning = false;
  }
}

async function runGitPull() {
  try {
    addUpdateLogMessage("Getting repository list...", "info");
    
    const response = await fetch("/api/update/git-pull", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      }
    });
    
    const result = await response.json();
    
    if (!response.ok) {
      return { success: false, error: result.error || "Unknown error" };
    }
    
    // Process results
    if (result.results) {
      result.results.forEach(repoResult => {
        if (repoResult.success) {
          addUpdateLogMessage(`📦 ${repoResult.repo}: ${repoResult.message || "Updated successfully"}`, "success");
        } else {
          addUpdateLogMessage(`⚠️ ${repoResult.repo}: ${repoResult.error}`, "error");
        }
      });
    }
    
    return { success: true };
    
  } catch (error) {
    console.error("Git pull error:", error);
    return { success: false, error: error.message };
  }
}

async function runAnalysisScript() {
  try {
    addUpdateLogMessage("🔄 Starting analysis script...", "info");
    
    // Start the analysis process
    const startResponse = await fetch("/api/update/run-analysis", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      }
    });
    
    const startResult = await startResponse.json();
    
    if (!startResponse.ok) {
      return { success: false, error: startResult.error || "Unknown error" };
    }
    
    // Connect to Server-Sent Events for real-time progress
    return new Promise((resolve, reject) => {
      const eventSource = new EventSource("/api/update/progress");
      
      eventSource.onmessage = function(event) {
        try {
          const data = JSON.parse(event.data);
          
          switch (data.type) {
            case 'info':
              addUpdateLogMessage(data.message, "info");
              break;
            case 'success':
              addUpdateLogMessage(data.message, "success");
              break;
            case 'detail':
              addDetailedProgressMessage(data.message);
              break;
            case 'error':
              addUpdateLogMessage(data.message, "error");
              break;
            case 'complete':
              eventSource.close();
              resolve({ success: true });
              break;
            case 'heartbeat':
              // Ignore heartbeat messages
              break;
          }
        } catch (e) {
          console.error("Error parsing SSE message:", e);
        }
      };
      
      eventSource.onerror = function(event) {
        console.error("SSE error:", event);
        eventSource.close();
        reject(new Error("Connection to update progress lost"));
      };
      
      // Set a timeout to prevent hanging forever - increased for enterprise-scale batch operations
      setTimeout(() => {
        if (eventSource.readyState !== EventSource.CLOSED) {
          eventSource.close();
          reject(new Error("Analysis script timed out"));
        }
      }, 432000000); // 120 hours timeout (5 days for massive enterprise operations)
    });
    
  } catch (error) {
    console.error("Analysis script error:", error);
    return { success: false, error: error.message };
  }
}

function closeUpdateModal() {
  const modal = $("update-modal");
  modal.classList.remove("show");
  
  // Reset state
  updateState.isRunning = false;
  updateState.currentStep = 0;
  updateState.progress = 0;
  
  // Clear logs
  const logContent = $("update-log-content");
  if (logContent) {
    logContent.innerHTML = "";
  }
  const detailContent = $("update-detailed-content");
  if (detailContent) {
    detailContent.innerHTML = "";
  }
  
  // Hide actions
  const actions = $("update-actions");
  actions.style.display = "none";
  
  // Reset status
  const statusTitle = $("update-status-title");
  statusTitle.textContent = "Initializing...";
  
  const progressBar = $("update-progress-bar");
  const progressText = $("update-progress-text");
  progressBar.style.width = "0%";
  progressText.textContent = "0%";
}

// --------------------------
// Date notification functionality
// --------------------------

function showDateNotification(formattedDate, commits, dateStr) {
  // Remove any existing notifications
  const existingNotification = document.querySelector('.date-notification');
  if (existingNotification) {
    existingNotification.remove();
  }
  
  // Create notification element
  const notification = document.createElement('div');
  notification.className = 'date-notification';
  
  // Format commit text
  const commitText = commits === 0 ? 'No commits' : 
                     commits === 1 ? '1 commit' : 
                     `${commits} commits`;
  
  notification.innerHTML = `
    <div class="date-notification-content">
      <div class="date-notification-date">${formattedDate}</div>
      <div class="date-notification-commits">${commitText}</div>
      <div class="date-notification-iso">${dateStr}</div>
    </div>
    <button class="date-notification-close">&times;</button>
  `;
  
  // Add click handler for close button
  notification.querySelector('.date-notification-close').addEventListener('click', function() {
    notification.remove();
  });
  
  // Add to page
  document.body.appendChild(notification);
  
  // Auto-remove after 5 seconds
  setTimeout(() => {
    if (notification.parentElement) {
      notification.remove();
    }
  }, 5000);
  
  // Add click-outside to close
  setTimeout(() => {
    const clickOutsideHandler = function(event) {
      if (!notification.contains(event.target)) {
        notification.remove();
        document.removeEventListener('click', clickOutsideHandler);
      }
    };
    document.addEventListener('click', clickOutsideHandler);
  }, 100);
}
async function loadUserOwnershipTimeline(userSlug) {
  try {
    const response = await fetchJSON(`/api/users/${encodeURIComponent(userSlug)}/ownership-timeline`);
    return response.timelines || {};
  } catch (err) {
    console.error("Failed to load ownership timeline for", userSlug, ":", err);
    return {};
  }
}

function renderUserOwnershipTimelines(userSlug, timelines, container) {
  if (!timelines || Object.keys(timelines).length === 0) {
    return;
  }
  
  const timelineCard = document.createElement("div");
  timelineCard.className = "card";
  timelineCard.innerHTML = '<h2>📈 Ownership Evolution</h2><p style="margin-bottom: 16px; color: #94a3b8;">Your ownership trends in subsystems where you are a top maintainer</p>';
  
  const timelinesContainer = document.createElement("div");
  timelinesContainer.style.display = "grid";
  timelinesContainer.style.gap = "20px";
  
  Object.entries(timelines).forEach(([subsystemName, timelineData], index) => {
    const subsystemContainer = document.createElement("div");
    subsystemContainer.style.marginBottom = "10px";
    
    // Subsystem title with current ownership
    const titleDiv = document.createElement("div");
    titleDiv.style.marginBottom = "8px";
    titleDiv.innerHTML = `<strong style="color: #e2e8f0;">${subsystemName}</strong> <span style="color: #94a3b8; font-size: 0.9em;">(Current: ${timelineData.current_ownership}%)</span>`;
    subsystemContainer.appendChild(titleDiv);
    
    // Chart container
    const chartContainer = document.createElement("div");
    chartContainer.className = "maintainer-timeline-chart";
    chartContainer.style.height = "200px";
    
    const canvas = document.createElement("canvas");
    canvas.id = `user-ownership-timeline-${userSlug}-${index}`;
    chartContainer.appendChild(canvas);
    subsystemContainer.appendChild(chartContainer);
    
    timelinesContainer.appendChild(subsystemContainer);
    
    // Create chart
    setTimeout(() => {
      createUserOwnershipChart(canvas.id, subsystemName, timelineData);
    }, 100);
  });
  
  timelineCard.appendChild(timelinesContainer);
  container.appendChild(timelineCard);
}

async function loadUserSubsystemActivity(userSlug, year) {
  try {
    return await fetchJSON(`/api/users/${encodeURIComponent(userSlug)}/subsystem-activity/${year}`);
  } catch (error) {
    console.error("Failed to load subsystem activity timeline for", userSlug, ":", error);
    return null;
  }
}

async function loadTeamSubsystemActivity(teamId, year) {
  try {
    return await fetchJSON(`/api/teams/${encodeURIComponent(teamId)}/subsystem-activity/${year}`);
  } catch (error) {
    console.error("Failed to load team subsystem activity for", teamId, ":", error);
    return null;
  }
}

function renderUserSubsystemTimeline(userSlug, activityData, mountPoint) {
  if (!mountPoint) {
    mountPoint = document.createElement("div");
    const main = $("main-content");
    if (main) {
      main.appendChild(mountPoint);
    }
  }

  if (!activityData || !Array.isArray(activityData.timeline)) {
    if (mountPoint && mountPoint.parentElement) {
      mountPoint.remove();
    }
    return;
  }

  const timeline = activityData.timeline || [];
  const summary = activityData.summary || {};

  mountPoint.innerHTML = "";

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = createTitleWithTooltip(
    "🗺️ Subsystem Activity Timeline",
    "Monthly breakdown of which subsystems this developer touched, highlighting the dominant subsystem for each month.",
    "h2"
  );

  const wrapper = document.createElement("div");
  wrapper.className = "developer-subsystem-timeline";

  const statsRow = document.createElement("div");
  statsRow.className = "subsystem-timeline-stats";

  const subsystemRankInfo = summary?.peer_rankings?.subsystems_touched;
  const statsConfig = [
    {
      label: "Active Months",
      value: summary.months_active || 0,
      description: "Months with subsystem activity",
    },
    {
      label: "Subsystems Touched",
      value: summary.subsystems_touched || 0,
      description: "Unique subsystems this year",
      rankInfo: subsystemRankInfo,
    },
    {
      label: "Lines Changed",
      value: (summary.total_changed_lines || 0).toLocaleString(),
      description: "Lines added + deleted",
    },
  ];

  if (summary.most_active_subsystem && summary.most_active_subsystem.name) {
    const most = summary.most_active_subsystem;
    statsConfig.push({
      label: "Most Active Subsystem",
      value: most.name,
      description: `${(most.changed_lines || 0).toLocaleString()} lines`,
    });
  }

  statsConfig.forEach((stat) => {
    const statCard = document.createElement("div");
    statCard.className = "subsystem-timeline-stat";

    const statLabel = document.createElement("div");
    statLabel.className = "stat-label";
    statLabel.textContent = stat.label;

    const statValue = document.createElement("div");
    statValue.className = "stat-value";
    statValue.textContent = typeof stat.value === "number" ? stat.value.toLocaleString() : stat.value;

    const statDescription = document.createElement("div");
    statDescription.className = "stat-description";
    statDescription.textContent = stat.description || "";

    statCard.appendChild(statLabel);
    statCard.appendChild(statValue);
    statCard.appendChild(statDescription);

    if (stat.rankInfo) {
      const rankText = formatRankSummary(stat.rankInfo);
      if (rankText) {
        const rankEl = document.createElement("div");
        rankEl.className = "stat-rank";
        rankEl.textContent = rankText;
        statCard.appendChild(rankEl);
      }
    }

    statsRow.appendChild(statCard);
  });

  wrapper.appendChild(statsRow);

  const timelineRow = document.createElement("div");
  timelineRow.className = "subsystem-timeline-row";

  timeline.forEach((monthEntry) => {
    const monthBlock = document.createElement("div");
    monthBlock.className = "timeline-month" + (monthEntry.has_activity ? " active" : " inactive");

    const monthLabel = document.createElement("div");
    monthLabel.className = "timeline-month-label";
    monthLabel.textContent = monthEntry.short_label || monthEntry.display_label || monthEntry.month;
    monthBlock.appendChild(monthLabel);

    const monthMeta = document.createElement("div");
    monthMeta.className = "timeline-month-meta";
    if (monthEntry.has_activity) {
      monthMeta.textContent = `${(monthEntry.total_changed_lines || 0).toLocaleString()} lines · ${(monthEntry.total_commits || 0).toLocaleString()} commits`;
    } else {
      monthMeta.textContent = "No subsystem activity";
    }
    monthBlock.appendChild(monthMeta);

    if (monthEntry.has_activity && Array.isArray(monthEntry.subsystems) && monthEntry.subsystems.length > 0) {
      const subsList = document.createElement("div");
      subsList.className = "timeline-subsystem-list";

      const subsToShow = monthEntry.subsystems.slice(0, 3);
      subsToShow.forEach((sub) => {
        const item = document.createElement("div");
        item.className = "timeline-subsystem-item";

        const nameEl = document.createElement("div");
        nameEl.className = "timeline-subsystem-name";
        nameEl.textContent = sub.name;

        const linesEl = document.createElement("div");
        linesEl.className = "timeline-subsystem-lines";
        const lines = (sub.changed_lines || 0).toLocaleString();
        const share = monthEntry.total_changed_lines > 0
          ? Math.round((sub.changed_lines || 0) / monthEntry.total_changed_lines * 100)
          : 0;
        linesEl.textContent = share ? `${lines} lines (${share}%)` : `${lines} lines`;

        const bar = document.createElement("div");
        bar.className = "timeline-subsystem-bar";
        const barFill = document.createElement("span");
        const widthPercent = monthEntry.total_changed_lines > 0
          ? Math.max(8, Math.min(100, ((sub.changed_lines || 0) / monthEntry.total_changed_lines) * 100))
          : 100;
        barFill.style.width = `${widthPercent}%`;
        bar.appendChild(barFill);

        item.appendChild(nameEl);
        item.appendChild(linesEl);
        item.appendChild(bar);
        item.addEventListener("click", () => {
          const stubPeriod = { label: monthEntry.label };
          navigateToSubsystem(sub.name, stubPeriod);
        });
        subsList.appendChild(item);
      });

      if (monthEntry.subsystems.length > subsToShow.length) {
        const more = document.createElement("div");
        more.className = "timeline-more";
        more.textContent = `+${monthEntry.subsystems.length - subsToShow.length} more`;
        subsList.appendChild(more);
      }

      monthBlock.appendChild(subsList);
    } else {
      const emptyState = document.createElement("div");
      emptyState.className = "timeline-month-empty";
      emptyState.textContent = "Idle month";
      monthBlock.appendChild(emptyState);
    }

    timelineRow.appendChild(monthBlock);
  });

  wrapper.appendChild(timelineRow);

  if (Array.isArray(summary.top_subsystems) && summary.top_subsystems.length > 0) {
    const topList = document.createElement("div");
    topList.className = "timeline-top-subsystems";
    summary.top_subsystems.forEach((sub) => {
      const entry = document.createElement("div");
      entry.className = "timeline-top-entry";

      const name = document.createElement("div");
      name.className = "name";
      name.textContent = sub.name;

      const stats = document.createElement("div");
      stats.className = "lines";
      const lines = (sub.changed_lines || 0).toLocaleString();
      const months = sub.months_active || 0;
      stats.textContent = `${lines} lines · ${months} mos`;

      entry.appendChild(name);
      entry.appendChild(stats);
      entry.addEventListener("click", () => navigateToSubsystem(sub.name));
      topList.appendChild(entry);
    });
    wrapper.appendChild(topList);
  }

  card.appendChild(wrapper);
  mountPoint.appendChild(card);
}

function renderTeamSubsystemTimeline(team, activityData, mountPoint) {
  if (!mountPoint) {
    mountPoint = document.createElement("div");
    const main = $("main-content");
    if (main) {
      main.appendChild(mountPoint);
    }
  }

  const timeline = Array.isArray(activityData?.timeline) ? activityData.timeline : [];
  const summary = activityData?.summary || {};

  if (!timeline.length) {
    if (mountPoint && mountPoint.parentElement) {
      mountPoint.remove();
    }
    return;
  }

  mountPoint.innerHTML = "";

  const card = document.createElement("div");
  card.className = "card";
  card.innerHTML = createTitleWithTooltip(
    "🧭 Team Focus Timeline",
    "Monthly breakdown highlighting which subsystems this team invested in, based on combined member line changes.",
    "h2"
  );

  const wrapper = document.createElement("div");
  wrapper.className = "developer-subsystem-timeline";

  const statsRow = document.createElement("div");
  statsRow.className = "subsystem-timeline-stats";

  const statsConfig = [
    {
      label: "Active Months",
      value: summary.months_active || 0,
      description: "Months with subsystem activity",
    },
    {
      label: "Subsystems Touched",
      value: summary.subsystems_touched || 0,
      description: "Unique subsystems this year",
    },
    {
      label: "Lines Changed",
      value: (summary.total_changed_lines || 0).toLocaleString(),
      description: "Lines added + deleted",
    },
    {
      label: "Commits",
      value: (summary.total_commits || 0).toLocaleString(),
      description: "Team commits",
    },
  ];

  if (typeof summary.team_members_count === "number") {
    statsConfig.push({
      label: "Team Members",
      value: summary.team_members_count,
      description: "Contributors in this team",
    });
  }

  if (summary.most_active_subsystem && summary.most_active_subsystem.name) {
    const most = summary.most_active_subsystem;
    statsConfig.push({
      label: "Most Active Subsystem",
      value: most.name,
      description: `${(most.changed_lines || 0).toLocaleString()} lines`,
    });
  }

  statsConfig.forEach((stat) => {
    const statCard = document.createElement("div");
    statCard.className = "subsystem-timeline-stat";

    const statLabel = document.createElement("div");
    statLabel.className = "stat-label";
    statLabel.textContent = stat.label;

    const statValue = document.createElement("div");
    statValue.className = "stat-value";
    statValue.textContent = typeof stat.value === "number" ? stat.value.toLocaleString() : stat.value;

    const statDescription = document.createElement("div");
    statDescription.className = "stat-description";
    statDescription.textContent = stat.description || "";

    statCard.appendChild(statLabel);
    statCard.appendChild(statValue);
    statCard.appendChild(statDescription);
    statsRow.appendChild(statCard);
  });

  wrapper.appendChild(statsRow);

  const timelineRow = document.createElement("div");
  timelineRow.className = "subsystem-timeline-row";

  timeline.forEach((monthEntry) => {
    const monthBlock = document.createElement("div");
    monthBlock.className = "timeline-month" + (monthEntry.has_activity ? " active" : " inactive");

    const monthLabel = document.createElement("div");
    monthLabel.className = "timeline-month-label";
    monthLabel.textContent = monthEntry.short_label || monthEntry.display_label || monthEntry.month;
    monthBlock.appendChild(monthLabel);

    const monthMeta = document.createElement("div");
    monthMeta.className = "timeline-month-meta";
    if (monthEntry.has_activity) {
      monthMeta.textContent = `${(monthEntry.total_changed_lines || 0).toLocaleString()} lines · ${(monthEntry.total_commits || 0).toLocaleString()} commits`;
    } else {
      monthMeta.textContent = "No subsystem activity";
    }
    monthBlock.appendChild(monthMeta);

    if (monthEntry.has_activity && Array.isArray(monthEntry.subsystems) && monthEntry.subsystems.length > 0) {
      const subsList = document.createElement("div");
      subsList.className = "timeline-subsystem-list";

      const subsToShow = monthEntry.subsystems.slice(0, 4);
      subsToShow.forEach((sub) => {
        const item = document.createElement("div");
        item.className = "timeline-subsystem-item";

        const nameEl = document.createElement("div");
        nameEl.className = "timeline-subsystem-name";
        nameEl.textContent = sub.name;

        const linesEl = document.createElement("div");
        linesEl.className = "timeline-subsystem-lines";
        const lines = (sub.changed_lines || 0).toLocaleString();
        const share = monthEntry.total_changed_lines > 0
          ? Math.round((sub.changed_lines || 0) / monthEntry.total_changed_lines * 100)
          : 0;
        linesEl.textContent = share ? `${lines} lines (${share}%)` : `${lines} lines`;

        const bar = document.createElement("div");
        bar.className = "timeline-subsystem-bar";
        const barFill = document.createElement("span");
        const widthPercent = monthEntry.total_changed_lines > 0
          ? Math.max(8, Math.min(100, ((sub.changed_lines || 0) / monthEntry.total_changed_lines) * 100))
          : 100;
        barFill.style.width = `${widthPercent}%`;
        bar.appendChild(barFill);

        item.appendChild(nameEl);
        item.appendChild(linesEl);
        item.appendChild(bar);
        item.addEventListener("click", () => navigateToSubsystem(sub.name, { label: monthEntry.label }));
        subsList.appendChild(item);
      });

      if (monthEntry.subsystems.length > subsToShow.length) {
        const more = document.createElement("div");
        more.className = "timeline-more";
        more.textContent = `+${monthEntry.subsystems.length - subsToShow.length} more`;
        subsList.appendChild(more);
      }

      monthBlock.appendChild(subsList);
    } else {
      const emptyState = document.createElement("div");
      emptyState.className = "timeline-month-empty";
      emptyState.textContent = "Idle month";
      monthBlock.appendChild(emptyState);
    }

    timelineRow.appendChild(monthBlock);
  });

  wrapper.appendChild(timelineRow);

  if (Array.isArray(summary.top_subsystems) && summary.top_subsystems.length > 0) {
    const topList = document.createElement("div");
    topList.className = "timeline-top-subsystems";
    summary.top_subsystems.forEach((sub) => {
      const entry = document.createElement("div");
      entry.className = "timeline-top-entry";

      const name = document.createElement("div");
      name.className = "name";
      name.textContent = sub.name;

      const stats = document.createElement("div");
      stats.className = "lines";
      const lines = (sub.changed_lines || 0).toLocaleString();
      const months = sub.months_active || 0;
      stats.textContent = `${lines} lines · ${months} mos`;

      entry.appendChild(name);
      entry.appendChild(stats);
      entry.addEventListener("click", () => navigateToSubsystem(sub.name));
      topList.appendChild(entry);
    });
    wrapper.appendChild(topList);
  }

  card.appendChild(wrapper);
  mountPoint.appendChild(card);
}

function createUserOwnershipChart(canvasId, subsystemName, timelineData) {
  const ctx = document.getElementById(canvasId);
  if (!ctx) {
    console.error("Canvas not found:", canvasId);
    return;
  }
  
  const values = Array.isArray(timelineData.ownership) ? timelineData.ownership : [];
  if (!values.length) {
    console.warn("No ownership data for", subsystemName);
    return;
  }
  
  // Calculate dynamic Y-axis range with safe padding for flat datasets
  let minValue = Math.min(...values);
  let maxValue = Math.max(...values);
  if (!isFinite(minValue) || !isFinite(maxValue)) {
    minValue = 0;
    maxValue = 0;
  }
  if (maxValue === minValue) {
    const delta = Math.max(1, maxValue * 0.05 || 1);
    minValue = Math.max(0, minValue - delta);
    maxValue = Math.min(100, maxValue + delta);
  }
  const range = maxValue - minValue;
  const padding = Math.max(0.5, range * 0.1);
  const yMin = Math.max(0, minValue - padding);
  const yMax = Math.min(100, maxValue + padding);
  
  new Chart(ctx, {
    type: "line",
    data: {
      labels: timelineData.months,
      datasets: [{
        label: "Ownership %",
        data: timelineData.ownership,
        backgroundColor: "rgba(75, 192, 192, 0.1)",
        borderColor: "rgba(75, 192, 192, 1)",
        borderWidth: 2,
        fill: true,
        tension: 0.4,
        pointRadius: 4,
        pointBackgroundColor: "rgba(75, 192, 192, 1)",
        pointBorderColor: "#fff",
        pointBorderWidth: 2,
        pointHoverRadius: 6
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false
        },
        title: {
          display: false
        },
        tooltip: {
          callbacks: {
            label: function(context) {
              return context.parsed.y.toFixed(1) + '% ownership';
            }
          }
        }
      },
      scales: {
        y: {
          min: yMin,
          max: yMax,
          ticks: {
            callback: function(value) {
              return value.toFixed(1) + '%';
            },
            font: {
              size: 11
            }
          },
          grid: {
            color: 'rgba(0, 0, 0, 0.05)'
          }
        },
        x: {
          ticks: {
            font: {
              size: 10
            },
            maxRotation: 45,
            minRotation: 45
          },
          grid: {
            display: false
          }
        }
      }
    }
  });
}
