// static/app.js - Fixed version
console.log("app.js loaded");

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
  loadingTeamsOverview: false // flag to prevent concurrent teams overview loads
};

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
    
    // Update summary
    this.container.querySelector('.progress-completed').textContent = completed;
    this.container.querySelector('.progress-total').textContent = total;
    
    // Update task list
    const list = this.container.querySelector('.progress-list');
    list.innerHTML = '';
    
    this.tasks.forEach((task, id) => {
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
      
      const duration = task.endTime ? 
        ` (${((task.endTime - task.startTime) / 1000).toFixed(1)}s)` : '';
      
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
        // Auto-open settings focused on repositories tab for first-time users
        openSettings("repositories");
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
    setMode(state.mode);
    
    // Clear loading indicator
    if (main) {
      main.innerHTML = '<div class="empty-state"><p>Use the selector on the left to pick a user/month, team/period, or subsystem/period.</p></div>';
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
  state.mode = mode;
  
  // Update button states
  const userBtn = $("mode-users");
  const teamsBtn = $("mode-teams");
  const subsystemBtn = $("mode-subsystems");
  
  userBtn.classList.toggle("active", mode === "users");
  teamsBtn.classList.toggle("active", mode === "teams");
  subsystemBtn.classList.toggle("active", mode === "subsystems");
  
  // Update sidebar visibility
  const userSidebar = $("sidebar-users");
  const teamsSidebar = $("sidebar-teams");
  const subsystemSidebar = $("sidebar-subsystems");
  
  if (userSidebar && teamsSidebar && subsystemSidebar) {
    userSidebar.style.display = mode === "users" ? "block" : "none";
    teamsSidebar.style.display = mode === "teams" ? "block" : "none";
    subsystemSidebar.style.display = mode === "subsystems" ? "block" : "none";
  }
  
  // Clear main content when switching modes
  if (showOverview) {
    clearMain();
    
    // Show overview dashboard for the selected mode
    if (mode === "users") {
      showUsersOverviewDashboard();
    } else if (mode === "teams") {
      showTeamsOverviewDashboard();
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
    await renderUserDashboard(user, month, data);
  } catch (err) {
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
    'SVG', 'PostScript', 'Rich Text Format'
  ]);

  const labels = [];
  const values = [];
  
  for (const [lang, stats] of Object.entries(langs)) {
    // Include if it's explicitly in real languages, exclude if it's in exclude list
    const shouldInclude = realLanguages.has(lang) && !excludeLanguages.has(lang);
    
    if (shouldInclude) {
      const added = stats.additions || 0;
      const deleted = stats.deletions || 0;
      labels.push(lang);
      values.push(added + deleted);
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

async function renderUserDashboard(user, month, summary) {
  clearMain();

  const periodType = month.is_yearly ? "Yearly" : "Monthly";
  const periodLabel = month.is_yearly ? month.label : month.label + " (" + summary.from + " → " + summary.to + ")";

  setViewHeader(
    "User: " + (summary.author_name || user.slug),
    periodLabel,
    "User · " + periodType
  );

  const main = $("main-content");

  // KPIs
  const kpiContainer = document.createElement("div");
  kpiContainer.className = "kpi-grid";

  const kpis = [
    { label: "Total commits", value: summary.total_commits || 0 },
    { label: "Lines added", value: summary.total_lines_added || 0 },
    { label: "Lines deleted", value: summary.total_lines_deleted || 0 },
    {
      label: "Net lines",
      value: summary.net_lines || 0
    }
  ];

  kpis.forEach((k) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    card.innerHTML = '<div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>';
    kpiContainer.appendChild(card);
  });

  main.appendChild(kpiContainer);

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
      try {
        console.log("Rendering badges for user", user.slug, ":", badges?.length || 0, "badges");
        if (badges && badges.length > 0) {
          renderUserBadges(badges, main);
        } else {
          console.log("No badges to render for user", user.slug);
        }
      } catch (error) {
        console.error("Error rendering user badges:", error);
        // Create error element to show to user
        const errorDiv = document.createElement("div");
        errorDiv.className = "error";
        errorDiv.textContent = "Error loading badges: " + error.message;
        main.appendChild(errorDiv);
      }
    }).catch(error => {
      console.error("Error loading user badges:", error);
      // Don't break the UI, just log the error
    });
    
    // Load and render ownership timelines for subsystems where user is top maintainer
    loadUserOwnershipTimeline(user.slug).then(timelines => {
      try {
        if (timelines && Object.keys(timelines).length > 0) {
          renderUserOwnershipTimelines(user.slug, timelines, main);
        }
      } catch (error) {
        console.error("Error rendering ownership timelines:", error);
      }
    }).catch(error => {
      console.error("Error loading ownership timelines:", error);
    });
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

  // Add contribution heatmap if we have daily data
  if (summary.per_date && Object.keys(summary.per_date).length > 0) {
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
        // Show all yearly data
        heatmapData = summary.per_date || {};
        heatmapFromDate = summary.from || month.from;
        heatmapToDate = summary.to || month.to;
        console.log("Using full yearly data for heatmap:", Object.keys(heatmapData).length, "days");
      } else {
        // For monthly view, show only selected month's data but display full year layout
        const monthStart = summary.from;
        const monthEnd = summary.to;
        const year = monthStart.split('-')[0];
        
        // Only include commits from the selected month, but prepare for full year display
        heatmapData = {};
        if (summary.per_date) {
          for (const [date, data] of Object.entries(summary.per_date)) {
            // Only include dates that fall within the selected month
            if (date >= monthStart && date <= monthEnd) {
              heatmapData[date] = {
                ...data,
                isHighlighted: true // This month's data is always highlighted
              };
            }
          }
        }
        
        // Display full year range so all months are visible, but only selected month has data
        heatmapFromDate = `${year}-01-01`;
        heatmapToDate = `${year}-12-31`;
        console.log(`Using selected month data only (${monthStart} to ${monthEnd}):`, Object.keys(heatmapData).length, "days");
      }
      
      console.log("Creating heatmap for period:", heatmapFromDate, "to", heatmapToDate, "with", Object.keys(heatmapData).length, "data points");
      const heatmapElement = createContributionHeatmap(heatmapData, heatmapFromDate, heatmapToDate);
      heatmapContainer.appendChild(heatmapElement);
      
      heatmapCard.appendChild(heatmapContainer);
      main.appendChild(heatmapCard);
    } catch (error) {
      console.error("Error creating contribution heatmap:", error);
      // Don't add the heatmap if there's an error
    }
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
    
    // Create the monthly chart asynchronously
    const year = parseInt(month.label);
    setTimeout(() => createMonthlyChart("chart-monthly", user.slug, year, false), 100);
  }

  // Monthly Statistics Card (different behavior for monthly vs yearly view)
  if (month.is_yearly) {
    // For yearly view: show last month statistics
    const lastMonthCard = await createLastMonthStatsCard(user.slug, false);
    if (lastMonthCard) {
      main.appendChild(lastMonthCard);
    }
  } else {
    // For monthly view: show selected month statistics using summary data
    const selectedMonthCard = await createSelectedMonthStatsCard(user.slug, month, summary, false);
    if (selectedMonthCard) {
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
  setTimeout(() => createDailyChart("chart-daily-activity", user.slug, chartYear, chartMonth, false), 100);

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
      await addSubsystemContributionHeatmap(main, subsystem.name, period);
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
      const details = summary.responsible_subsystem_details?.[subsystemName] || { name: subsystemName, lines_of_code: 0 };
      return details;
    }).sort((a, b) => b.lines_of_code - a.lines_of_code);
    
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
      lineCount.textContent = `${subsystemDetail.lines_of_code.toLocaleString()} lines`;
      
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

  // KPIs
  const kpiContainer = document.createElement("div");
  kpiContainer.className = "kpi-grid";

  // Calculate correct primary language if language data is available
  let primaryLanguage = "Not available";
  if (summary.languages && Object.keys(summary.languages).length > 0) {
    const correctPrimary = getCorrectPrimaryLanguage(summary.languages);
    primaryLanguage = correctPrimary || "None detected";
  }

  const kpis = [
    { label: "Team Members", value: summary.members?.length || 0 },
    { label: "Total Commits", value: summary.total_commits || 0 },
    { label: "Lines Added", value: summary.total_additions || 0 },
    { label: "Lines Deleted", value: summary.total_deletions || 0 },
    { label: "Primary Language", value: primaryLanguage }
  ];

  kpis.forEach((k) => {
    const card = document.createElement("div");
    card.className = "kpi-card";
    card.innerHTML = '<div class="kpi-label">' + k.label + '</div><div class="kpi-value">' + k.value + '</div>';
    kpiContainer.appendChild(card);
  });

  main.appendChild(kpiContainer);

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
    membersSection.innerHTML = createTitleWithTooltip(
      "Team Members Contributions", 
      "Individual contribution statistics for each team member during the selected period. Shows commits, lines added, and lines deleted per member.",
      "h3"
    );

    const membersContainer = document.createElement("div");
    membersContainer.className = "chart-grid";

    const membersList = Object.entries(summary.member_contributions)
      .sort((a, b) => b[1].commits - a[1].commits)
      .slice(0, 10); // Top 10 contributors

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
              <td>${contrib.commits}</td>
              <td style="color: #22c55e;">${contrib.additions}</td>
              <td style="color: #ef4444;">${contrib.deletions}</td>
            </tr>
          `;
        }).join('')}
      </tbody>
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
    subsystemsSection.innerHTML = createTitleWithTooltip(
      "Subsystems Contributions", 
      "Team's contributions broken down by subsystem. Shows which subsystems the team is actively working on and their level of contribution to each.",
      "h3"
    );

    const chartContainer = document.createElement("div");
    chartContainer.className = "chart-grid";

    // Create subsystems table
    const subsystemsList = Object.entries(summary.subsystems)
      .sort((a, b) => b[1].commits - a[1].commits)
      .slice(0, 15); // Top 15 subsystems

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
            <td>${data.commits}</td>
            <td style="color: #22c55e;">${data.additions || 0}</td>
            <td style="color: #ef4444;">${data.deletions || 0}</td>
          </tr>
        `).join('')}
      </tbody>
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

async function addSubsystemContributionHeatmap(container, subsystemName, period) {
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
    
    // Collect daily commit data for the subsystem
    const dailyCommits = {};
    
    if (period.is_yearly) {
      // For yearly view, get all monthly summaries for the year
      const monthlyData = await collectSubsystemMonthlyData(subsystemName, dataCollectionYear);
      
      // Process each monthly summary to extract developers and get their real daily data
      for (const monthSummary of monthlyData) {
        if (monthSummary.repositories) {
          // Collect all developers who worked in this subsystem this month
          const subsystemDevelopers = new Set();
          
          for (const [repoName, repoData] of Object.entries(monthSummary.repositories)) {
            const developers = repoData.developers || {};
            for (const devSlug of Object.keys(developers)) {
              subsystemDevelopers.add(devSlug);
            }
          }
          
          // For each developer, fetch their actual daily commit data for this month
          const fromDate = monthSummary.from;
          const toDate = monthSummary.to;
          
          for (const devSlug of subsystemDevelopers) {
            try {
              const userMonthData = await fetchJSON(`/api/users/${encodeURIComponent(devSlug)}/month/${encodeURIComponent(fromDate)}/${encodeURIComponent(toDate)}`);
              
              // Get per_date data and filter for commits to this subsystem's repos
              const perDate = userMonthData.per_date || {};
              const perRepo = userMonthData.per_repo || {};
              
              // Get list of repos in this subsystem
              const subsystemRepos = Object.keys(monthSummary.repositories);
              
              // Check if user has commits in any of the subsystem repos
              const hasSubsystemWork = subsystemRepos.some(repo => perRepo[repo] && perRepo[repo].commits > 0);
              
              if (hasSubsystemWork) {
                // Add this user's daily commits (approximation: count all their daily commits for this month)
                for (const [dateStr, dateData] of Object.entries(perDate)) {
                  if (dateData.commits > 0) {
                    if (!dailyCommits[dateStr]) {
                      dailyCommits[dateStr] = { commits: 0 };
                    }
                    // Add a proportional share based on subsystem repos vs total repos
                    const subsystemCommits = subsystemRepos.reduce((sum, repo) => {
                      return sum + (perRepo[repo]?.commits || 0);
                    }, 0);
                    const totalMonthCommits = userMonthData.total_commits || 1;
                    const proportion = subsystemCommits / totalMonthCommits;
                    dailyCommits[dateStr].commits += Math.round(dateData.commits * proportion);
                  }
                }
              }
            } catch (error) {
              console.warn(`Could not fetch daily data for ${devSlug}:`, error);
            }
          }
        }
      }
    } else {
      // For monthly view, get real daily data from users
      const monthlyData = await collectSubsystemMonthlyData(subsystemName, dataCollectionYear);
      
      // Process only the month that matches our selected period
      for (const monthSummary of monthlyData) {
        if (monthSummary.repositories && monthSummary.from >= period.from && monthSummary.to <= period.to) {
          // Collect all developers who worked in this subsystem this month
          const subsystemDevelopers = new Set();
          
          for (const [repoName, repoData] of Object.entries(monthSummary.repositories)) {
            const developers = repoData.developers || {};
            for (const devSlug of Object.keys(developers)) {
              subsystemDevelopers.add(devSlug);
            }
          }
          
          // For each developer, fetch their actual daily commit data
          const fromDate = monthSummary.from;
          const toDate = monthSummary.to;
          
          for (const devSlug of subsystemDevelopers) {
            try {
              const userMonthData = await fetchJSON(`/api/users/${encodeURIComponent(devSlug)}/month/${encodeURIComponent(fromDate)}/${encodeURIComponent(toDate)}`);
              
              const perDate = userMonthData.per_date || {};
              const perRepo = userMonthData.per_repo || {};
              
              // Get list of repos in this subsystem
              const subsystemRepos = Object.keys(monthSummary.repositories);
              
              // Check if user has commits in any of the subsystem repos
              const hasSubsystemWork = subsystemRepos.some(repo => perRepo[repo] && perRepo[repo].commits > 0);
              
              if (hasSubsystemWork) {
                // Add this user's daily commits (proportional to subsystem work)
                for (const [dateStr, dateData] of Object.entries(perDate)) {
                  if (dateData.commits > 0) {
                    if (!dailyCommits[dateStr]) {
                      dailyCommits[dateStr] = { commits: 0, isHighlighted: true };
                    }
                    // Calculate proportion of work in this subsystem
                    const subsystemCommits = subsystemRepos.reduce((sum, repo) => {
                      return sum + (perRepo[repo]?.commits || 0);
                    }, 0);
                    const totalMonthCommits = userMonthData.total_commits || 1;
                    const proportion = subsystemCommits / totalMonthCommits;
                    dailyCommits[dateStr].commits += Math.round(dateData.commits * proportion);
                  }
                }
              }
            } catch (error) {
              console.warn(`Could not fetch daily data for ${devSlug}:`, error);
            }
          }
        }
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
      
    } catch (error) {
      console.error("Error creating contribution heatmap:", error);
      // Don't add the heatmap if there's an error
    }
    
  } catch (error) {
    console.error("Failed to load contribution activity for", subsystemName, ":", error);
    // Don't show error to user, just skip this section
  }
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
    main.innerHTML = createLoadingIndicator(
      "Loading Developers Overview", 
      "Analyzing user statistics and team metrics across all repositories..."
    );
    
    const overviewData = await fetchJSON('/api/users/overview');
    
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
    
    // Initialize progress tracker for async sections
    progressTracker.init();
    progressTracker.show();
    progressTracker.addTask('badge-stats', 'Achievement Badge Analysis');
    progressTracker.addTask('ownership-stats', 'Code Ownership Statistics');
    // 'ownership-changes' task removed - feature disabled due to fake data
    
    // Badge Statistics
    try {
      await addBadgeStatistics(main, progressTracker.getAbortSignal());
      progressTracker.completeTask('badge-stats');
    } catch (error) {
      if (error.name === 'AbortError') {
        console.log("Badge statistics cancelled by user");
        progressTracker.completeTask('badge-stats', false);
        return; // Stop further processing if cancelled
      }
      console.error("Badge statistics failed:", error);
      progressTracker.completeTask('badge-stats', false);
    }
    
    // Ownership Statistics
    try {
      await addOwnershipStatistics(main, progressTracker.getAbortSignal());
      progressTracker.completeTask('ownership-stats');
    } catch (error) {
      if (error.name === 'AbortError') {
        console.log("Ownership statistics cancelled by user");
        progressTracker.completeTask('ownership-stats', false);
        return; // Stop further processing if cancelled
      }
      console.error("Ownership statistics failed:", error);
      progressTracker.completeTask('ownership-stats', false);
    }
    
    // Ownership Changes Analysis - REMOVED (was using simulated/fake data)
    // Real ownership trends are available on individual developer pages
    /*
    try {
      await addOwnershipChangesAnalysis(main, progressTracker.getAbortSignal());
      progressTracker.completeTask('ownership-changes');
    } catch (error) {
      if (error.name === 'AbortError') {
        console.log("Ownership changes cancelled by user");
        progressTracker.completeTask('ownership-changes', false);
        return; // Stop further processing if cancelled
      }
      console.error("Ownership changes failed:", error);
      progressTracker.completeTask('ownership-changes', false);
    }
    */
    
    state.loadingUsersOverview = false;
    console.log("Users overview dashboard loading completed");
    
  } catch (error) {
    console.error("Error loading users overview:", error);
    clearMain();
    setViewHeader("Developers Overview", "Error loading overview data", "Error");
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load developers overview: ' + error.message + '</div>';
    progressTracker.hide();
  } finally {
    state.loadingUsersOverview = false;
    console.log("Users overview dashboard loading finished");
  }
}

async function addBadgeStatistics(container, abortSignal) {
  try {
    console.log("Loading badge statistics for users overview...");
    
    // Check if section already exists
    if (container.querySelector('.badge-statistics-section')) {
      console.log("Badge statistics section already exists, skipping");
      return;
    }
    
    // Check if cancelled before starting
    if (abortSignal && abortSignal.aborted) {
      throw new DOMException('Operation cancelled', 'AbortError');
    }
    
    const badgeSection = document.createElement("div");
    badgeSection.className = "card badge-statistics-section";
    badgeSection.innerHTML = createTitleWithTooltip(
      "🏆 Achievement Badges Overview", 
      "Summary of badges earned by developers across the team. Shows distribution of productivity awards, maintainer recognitions, and ownership achievements.",
      "h2"
    );
    
    container.appendChild(badgeSection);

    // Get all users and load their badges
    const usersWithBadges = [];
    let totalBadges = 0;
    const badgeTypes = {
      productivity: 0,
      ownership: 0,
      maintainer: 0,
      ownership_percentage: 0
    };

    // Load badges for all users (with reasonable timeout)
    const badgePromises = state.users.map(async user => {
      try {
        const badges = await loadUserBadges(user.slug);
        if (badges && badges.length > 0) {
          usersWithBadges.push({
            user: user,
            badges: badges,
            badgeCount: badges.length
          });
          
          totalBadges += badges.length;
          
          // Count by type
          badges.forEach(badge => {
            if (badgeTypes.hasOwnProperty(badge.type)) {
              badgeTypes[badge.type]++;
            }
          });
        }
      } catch (error) {
        console.warn(`Failed to load badges for ${user.slug}:`, error);
      }
    });

    await Promise.all(badgePromises);

    // Create statistics grid
    const statsGrid = document.createElement("div");
    statsGrid.className = "badge-stats-grid";

    const badgeStats = [
      { title: 'Users with Badges', value: usersWithBadges.length, subtitle: `out of ${state.users.length} developers`, emoji: '🎖️', color: '#F59E0B' },
      { title: 'Total Badges', value: totalBadges, subtitle: 'across all users', emoji: '🏆', color: '#10B981' },
      { title: 'Productivity Awards', value: badgeTypes.productivity, subtitle: 'most productive dev', emoji: '🚀', color: '#3B82F6' },
      { title: 'Ownership Badges', value: badgeTypes.ownership_percentage, subtitle: 'significant ownership', emoji: '👑', color: '#8B5CF6' }
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

    // Create content layout for badge holders
    if (usersWithBadges.length > 0) {
      const contentLayout = document.createElement("div");
      contentLayout.className = "badge-content-layout";
      
      const topBadgeHolders = usersWithBadges
        .sort((a, b) => b.badgeCount - a.badgeCount)
        .slice(0, 8); // Show more badge holders

      const topHoldersDiv = document.createElement("div");
      topHoldersDiv.className = "badge-holders-section";
      topHoldersDiv.innerHTML = '<h3>🌟 Top Badge Holders</h3>';

      const holdersList = document.createElement("div");
      holdersList.className = "badge-holders-grid";

      topBadgeHolders.forEach((holder, index) => {
        const holderItem = document.createElement("div");
        holderItem.className = "badge-holder-card clickable";
        holderItem.onclick = () => navigateToUser(holder.user.slug);
        
        const badges = holder.badges;
        const productivityBadges = badges.filter(b => b.type === 'productivity').length;
        const ownershipBadges = badges.filter(b => b.type === 'ownership_percentage').length;
        const maintainerBadges = badges.filter(b => b.type === 'maintainer').length;
        
        holderItem.innerHTML = `
          <div class="holder-rank">
            <span class="rank-number">${index + 1}</span>
          </div>
          <div class="holder-info">
            <div class="holder-name">${holder.user.display_name || holder.user.slug}</div>
            <div class="holder-badges">
              ${productivityBadges > 0 ? `<span class="mini-badge productivity">🚀 ${productivityBadges}</span>` : ''}
              ${ownershipBadges > 0 ? `<span class="mini-badge ownership">👑 ${ownershipBadges}</span>` : ''}
              ${maintainerBadges > 0 ? `<span class="mini-badge maintainer">🔧 ${maintainerBadges}</span>` : ''}
            </div>
          </div>
          <div class="holder-total">
            <span class="total-count">${holder.badgeCount}</span>
            <span class="total-label">badges</span>
          </div>
        `;
        holdersList.appendChild(holderItem);
      });

      topHoldersDiv.appendChild(holdersList);
      contentLayout.appendChild(topHoldersDiv);
      badgeSection.appendChild(contentLayout);
      
      // Add Top 10 Badge Holders ranking list
      const rankingGrid = document.createElement("div");
      rankingGrid.className = "ranking-grid";
      rankingGrid.style.marginTop = "20px";
      
      // Top 10 Total Badge Holders
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
      
      usersWithBadges
        .sort((a, b) => b.badgeCount - a.badgeCount)
        .slice(0, 20)
        .forEach((holder, index) => {
          const item = document.createElement("div");
          item.className = "ranking-item clickable";
          item.onclick = () => navigateToUser(holder.user.slug);
          
          const badges = holder.badges;
          const productivityBadges = badges.filter(b => b.type === 'productivity').length;
          const ownershipBadges = badges.filter(b => b.type === 'ownership_percentage').length;
          const maintainerBadges = badges.filter(b => b.type === 'maintainer').length;
          
          item.innerHTML = `
            <span class="ranking-position">#${index + 1}</span>
            <span class="ranking-name">${holder.user.display_name || holder.user.slug}</span>
            <div class="ranking-meta">
              <span class="ranking-value">${holder.badgeCount} total</span>
              <span class="ranking-subtext" style="font-size: 0.85em; color: #94a3b8;">
                ${productivityBadges > 0 ? `🚀${productivityBadges} ` : ''}${ownershipBadges > 0 ? `👑${ownershipBadges} ` : ''}${maintainerBadges > 0 ? `🔧${maintainerBadges}` : ''}
              </span>
            </div>
          `;
          topBadgesList.appendChild(item);
        });
      
      topBadgesCard.appendChild(topBadgesList);
      rankingGrid.appendChild(topBadgesCard);
      
      // Top 10 Ownership Badge Holders
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
      
      const usersWithOwnershipBadges = usersWithBadges
        .map(holder => ({
          ...holder,
          ownershipBadgeCount: holder.badges.filter(b => b.type === 'ownership_percentage').length
        }))
        .filter(holder => holder.ownershipBadgeCount > 0)
        .sort((a, b) => b.ownershipBadgeCount - a.ownershipBadgeCount)
        .slice(0, 20);
      
      usersWithOwnershipBadges.forEach((holder, index) => {
        const item = document.createElement("div");
        item.className = "ranking-item clickable";
        item.onclick = () => navigateToUser(holder.user.slug);
        
        // Get ownership badge details
        const ownershipBadges = holder.badges.filter(b => b.type === 'ownership_percentage');
        const subsystems = ownershipBadges.map(b => b.subsystem).join(', ');
        
        item.innerHTML = `
          <span class="ranking-position">#${index + 1}</span>
          <span class="ranking-name">${holder.user.display_name || holder.user.slug}</span>
          <div class="ranking-meta">
            <span class="ranking-value">${holder.ownershipBadgeCount} subsystems</span>
            <span class="ranking-subtext" style="font-size: 0.85em; color: #94a3b8;" title="${subsystems}">
              ${subsystems.length > 30 ? subsystems.substring(0, 30) + '...' : subsystems}
            </span>
          </div>
        `;
        ownershipBadgesList.appendChild(item);
      });
      
      ownershipBadgesCard.appendChild(ownershipBadgesList);
      rankingGrid.appendChild(ownershipBadgesCard);
      
      badgeSection.appendChild(rankingGrid);
    }

  } catch (error) {
    console.error("Error loading badge statistics:", error);
    // Don't break the overview, just skip badges section
  }
}

async function addOwnershipStatistics(container, abortSignal) {
  try {
    console.log("Loading ownership statistics for users overview...");
    
    // Check if section already exists
    if (container.querySelector('.ownership-statistics-section')) {
      console.log("Ownership statistics section already exists, skipping");
      return;
    }
    
    // Check if cancelled before starting
    if (abortSignal && abortSignal.aborted) {
      throw new DOMException('Operation cancelled', 'AbortError');
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

    // Load team analytics data with consistency fix
    let teamsAnalytics = [];
    let periodLabel = "Overall";
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
        }
      } catch (fallbackError) {
        console.warn("Failed to load team analytics:", fallbackError);
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
                <span class="stat-value">${teamAnalytics.total_commits.toLocaleString()}</span>
              </div>
              <div class="team-stat">
                <span class="stat-label">Lines Changed:</span>
                <span class="stat-value">${teamAnalytics.total_lines_changed.toLocaleString()}</span>
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
      data: [...teamsAnalytics].sort((a, b) => (b.responsible_lines_of_code || 0) - (a.responsible_lines_of_code || 0)).slice(0, 10),
      getValue: (team) => `${(team.responsible_lines_of_code || 0).toLocaleString()} lines`,
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
      'SVG', 'PostScript', 'Rich Text Format'
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
    'SVG', 'PostScript', 'Rich Text Format'
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
    'SVG', 'PostScript', 'Rich Text Format'
  ]);

  let maxLines = 0;
  let primaryLanguage = null;
  
  // Find the programming language with the most lines (use additions + deletions as proxy for activity)
  for (const [lang, stats] of Object.entries(languages)) {
    // Include if it's explicitly in real languages, exclude if it's in exclude list
    const shouldInclude = realLanguages.has(lang) && !excludeLanguages.has(lang);
    
    if (shouldInclude) {
      // Use additions + deletions as a measure of activity, or code_lines if available
      const langActivity = (stats.additions || 0) + (stats.deletions || 0) || stats.code_lines || 0;
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
    // Set up mode buttons
    $("mode-users").addEventListener("click", () => setMode("users"));
    $("mode-teams").addEventListener("click", () => setMode("teams"));
    $("mode-subsystems").addEventListener("click", () => setMode("subsystems"));
    
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
  const aboutLink = $("about-link");

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

  // Run Update link
  runUpdateLink.addEventListener("click", (e) => {
    e.preventDefault();
    closeHamburgerMenu();
    startUpdateProcess();
  });

  // Settings link
  settingsLink.addEventListener("click", (e) => {
    e.preventDefault();
    closeHamburgerMenu();
    openSettingsModal();
  });

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
  $("update-responsibilities").addEventListener("click", updateTeamResponsibilities);

  // Initialize management states
  window.aliasesData = {};
  window.teamsData = {};
  window.repositoriesData = [];
  window.subsystemsData = {};
  window.teamResponsibilitiesData = {};
}

function openSettings(defaultTab = "ignore-users") {
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
  
  // Clear selection
  window.aliasUIState.selectedUserSlugs = [];
  updateSelectedUsersList();
  
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
  
  Object.entries(aliases).forEach(([canonical, slugs]) => {
    const getPrimaryUser = (slug) => {
      return window.aliasUIState.availableUsers.find(u => u.slug === slug) || { display_name: slug, slug };
    };
    
    const primaryUser = getPrimaryUser(canonical);
    
    const aliasGroup = document.createElement("div");
    aliasGroup.className = "alias-group-card";
    
    aliasGroup.innerHTML = `
      <div class="alias-group-header">
        <div class="primary-user">
          <span class="primary-badge">PRIMARY</span>
          <strong>${primaryUser.display_name}</strong>
          <small>(${canonical})</small>
        </div>
        <button class="btn-danger btn-small" onclick="removeAliasGroup('${canonical}')">Delete Group</button>
      </div>
      <div class="alias-group-members">
        <div class="members-label">Merged identities:</div>
        ${slugs.map(slug => {
          const user = getPrimaryUser(slug);
          return `<div class="alias-member">${user.display_name} <small>(${slug})</small></div>`;
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
    
    // Populate member selector
    const memberSelector = $("team-member-selector");
    memberSelector.innerHTML = '';
    
    availableUsers.forEach(user => {
      const checkbox = document.createElement("div");
      checkbox.className = "member-checkbox";
      checkbox.innerHTML = `
        <label>
          <input type="checkbox" value="${user.slug}" name="team-member">
          <span>${user.display_name}</span>
        </label>
      `;
      memberSelector.appendChild(checkbox);
    });
    
    renderTeamsList();
  } catch (error) {
    console.error("Error loading teams:", error);
    const teamsList = $("teams-list");
    if (teamsList) {
      teamsList.innerHTML = '<div class="error">Failed to load teams: ' + error.message + '</div>';
    }
  }
}

function renderTeamsList() {
  const teamsList = $("teams-list");
  teamsList.innerHTML = '';
  
  if (!window.teamsData || Object.keys(window.teamsData).length === 0) {
    teamsList.innerHTML = '<div class="no-data">No teams configured</div>';
    return;
  }
  
  Object.entries(window.teamsData).forEach(([teamId, teamData]) => {
    const teamItem = document.createElement("div");
    teamItem.className = "team-item";
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
          ${(teamData.members || []).join(', ') || 'No members'}
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
  
  // Re-render list
  renderTeamsList();
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
        const startTime = Date.now();
        setTimeout(() => {
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

function loadTeamResponsibilitySubsystems() {
  const teamId = $("responsibility-team").value;
  const subsystemsContainer = $("responsibility-subsystems");
  
  if (!teamId) {
    subsystemsContainer.innerHTML = '<p class="text-gray-400">Please select a team first.</p>';
    return;
  }
  
  const currentResponsibilities = window.teamResponsibilitiesData[teamId] || [];
  
  subsystemsContainer.innerHTML = '';
  
  for (const subsystem of window.availableSubsystems || []) {
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
// Update Process Management
// --------------------------

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
  if (updateState.isRunning) {
    alert("Update is already running. Please wait for it to complete.");
    return;
  }

  // Reset state
  updateState.isRunning = true;
  updateState.currentStep = 0;
  updateState.progress = 0;

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
  const messageDiv = document.createElement("div");
  messageDiv.className = `log-message ${type}`;
  messageDiv.textContent = message;
  logContent.appendChild(messageDiv);
  
  // Auto-scroll to bottom
  logContent.scrollTop = logContent.scrollHeight;
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
            case 'error':
              addUpdateLogMessage(data.message, "error");
              if (data.progress !== undefined) {
                updateState.progress = data.progress;
                updateProgressUI();
              }
              eventSource.close();
              reject(new Error(data.message));
              return;
            case 'complete':
              eventSource.close();
              updateState.progress = 100;
              updateProgressUI();
              addUpdateLogMessage("🎉 Update process completed successfully!", "success");
              
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
  
  // Clear log
  const logContent = $("update-log-content");
  logContent.innerHTML = "";
  
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

function createUserOwnershipChart(canvasId, subsystemName, timelineData) {
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
