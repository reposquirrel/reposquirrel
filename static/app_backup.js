// static/app.js - Fixed version
console.log("app.js loaded");

let state = {
  mode: "subsystems", // "users" or "subsystems"
  users: [],
  subsystems: [], // Unified subsystems (services and standalone repos)
  selectedUser: null,
  selectedUserMonth: null, // {from, to, label, is_yearly}
  selectedSubsystem: null,
  selectedSubsystemPeriod: null,
  charts: {} // to keep references to Chart.js instances
};

function $(id) {
  return document.getElementById(id);
}

function clearMain() {
  const main = $("main-content");
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

// --------------------------
// API helpers
// --------------------------

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    throw new Error("Request failed: " + res.status + " " + res.statusText);
  }
  return res.json();
}

async function loadUsersAndSubsystems() {
  try {
    console.log("Loading users and subsystems...");
    const [userData, subsystemData] = await Promise.all([
      fetchJSON("/api/users"),
      fetchJSON("/api/subsystems")
    ]);
    console.log("Loaded users:", userData.users?.length || 0);
    console.log("Loaded subsystems:", subsystemData.subsystems?.length || 0);
    
    state.users = userData.users || [];
    state.subsystems = subsystemData.subsystems || [];
    
    console.log("State updated, rendering lists...");
    renderUserList();
    renderSubsystemList();
    console.log("Finished rendering lists");
    
    // Force update of current mode visibility
    setMode(state.mode);
    
  } catch (error) {
    console.error("Error loading data:", error);
    // Show error to user
    const main = $("main-content");
    if (main) {
      main.innerHTML = '<div class="error">Failed to load data from backend: ' + error.message + '<br>Check console for details.</div>';
    }
    throw error;
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
  
  sortedUsers.forEach((u, index) => {
    const item = document.createElement("div");
    item.className = "sidebar-item";
    if (state.selectedUser && state.selectedUser.slug === u.slug) {
      item.classList.add("selected");
    }
    item.textContent = u.display_name || u.slug;
    item.onclick = () => {
      console.log("User clicked:", u.slug, u.display_name);
      state.selectedUser = u;
      state.selectedUserMonth = null;
      renderUserMonthList();
      
      // Refresh the user list to show selection
      renderUserList();
      
      // Check if we have yearly data and show it by default
      const yearlyData = (u.months || []).find(m => m.is_yearly);
      if (yearlyData) {
        state.selectedUserMonth = yearlyData;
        loadUserMonth(u, yearlyData);
      } else {
        clearMain();
        setViewHeader(
          "User: " + (u.display_name || u.slug),
          "Select a time period",
          "User"
        );
      }
    };
    container.appendChild(item);
  });
  console.log("Added", sortedUsers.length, "user items to container");
}

function renderUserMonthList() {
  const container = $("user-month-list");
  container.innerHTML = "";
  const u = state.selectedUser;
  if (!u) return;
  
  // Separate yearly and monthly data
  const yearlyData = (u.months || []).filter(m => m.is_yearly);
  const monthlyData = (u.months || []).filter(m => !m.is_yearly);
  
  // Add yearly summaries first
  yearlyData.forEach((m) => {
    const item = document.createElement("div");
    item.className = "sidebar-item small yearly";
    if (state.selectedUserMonth && state.selectedUserMonth.from === m.from && state.selectedUserMonth.to === m.to) {
      item.classList.add("selected");
    }
    item.textContent = m.label + " (Year)";
    item.onclick = () => {
      state.selectedUserMonth = m;
      loadUserMonth(u, m);
      renderUserMonthList(); // Refresh to show selection
    };
    container.appendChild(item);
  });
  
  // Add monthly data
  monthlyData.forEach((m) => {
    const item = document.createElement("div");
    item.className = "sidebar-item small";
    if (state.selectedUserMonth && state.selectedUserMonth.from === m.from && state.selectedUserMonth.to === m.to) {
      item.classList.add("selected");
    }
    item.textContent = m.label;
    item.onclick = () => {
      state.selectedUserMonth = m;
      loadUserMonth(u, m);
      renderUserMonthList(); // Refresh to show selection
    };
    container.appendChild(item);
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
  
  // Sort subsystems alphabetically by name
  const sortedSubsystems = [...state.subsystems].sort((a, b) => {
    return a.name.toLowerCase().localeCompare(b.name.toLowerCase());
  });
  
  sortedSubsystems.forEach((s, index) => {
    const item = document.createElement("div");
    item.className = "sidebar-item";
    if (state.selectedSubsystem && state.selectedSubsystem.name === s.name) {
      item.classList.add("selected");
    }
    
    // All are now subsystems, no need for type indicators
    item.textContent = s.name;
    
    item.onclick = () => {
      console.log("Subsystem clicked:", s.name);
      state.selectedSubsystem = s;
      state.selectedSubsystemPeriod = null;
      renderSubsystemPeriodList();
      
      // Refresh the subsystem list to show selection
      renderSubsystemList();
      
      // Check if we have yearly data and show it by default
      const yearlyData = (s.periods || []).find(p => p.is_yearly);
      if (yearlyData) {
        state.selectedSubsystemPeriod = yearlyData;
        loadSubsystemPeriod(s, yearlyData);
      } else {
        clearMain();
        setViewHeader(
          "Subsystem: " + s.name,
          "Select a time period",
          "Subsystem"
        );
      }
    };
    container.appendChild(item);
  });
  console.log("Added", sortedSubsystems.length, "subsystem items to container");
}

function renderSubsystemPeriodList() {
  const container = $("subsystem-period-list");
  container.innerHTML = "";
  const s = state.selectedSubsystem;
  if (!s) return;
  
  // Separate yearly and period data
  const yearlyData = (s.periods || []).filter(p => p.is_yearly);
  const periodData = (s.periods || []).filter(p => !p.is_yearly);
  
  // Add yearly summaries first
  yearlyData.forEach((p) => {
    const item = document.createElement("div");
    item.className = "sidebar-item small yearly";
    if (state.selectedSubsystemPeriod && state.selectedSubsystemPeriod.from === p.from && state.selectedSubsystemPeriod.to === p.to) {
      item.classList.add("selected");
    }
    item.textContent = p.label + " (Year)";
    item.onclick = () => {
      state.selectedSubsystemPeriod = p;
      loadSubsystemPeriod(s, p);
      renderSubsystemPeriodList(); // Refresh to show selection
    };
    container.appendChild(item);
  });
  
  // Add period data
  periodData.forEach((p) => {
    const item = document.createElement("div");
    item.className = "sidebar-item small";
    if (state.selectedSubsystemPeriod && state.selectedSubsystemPeriod.from === p.from && state.selectedSubsystemPeriod.to === p.to) {
      item.classList.add("selected");
    }
    item.textContent = p.label;
    item.onclick = () => {
      state.selectedSubsystemPeriod = p;
      loadSubsystemPeriod(s, p);
      renderSubsystemPeriodList(); // Refresh to show selection
    };
    container.appendChild(item);
  });
}

// --------------------------
// Mode switching
// --------------------------

function setMode(mode) {
  console.log("setMode called with:", mode);
  state.mode = mode;
  const btnUsers = $("mode-users");
  const btnSubsystems = $("mode-subsystems");
  const sectionUsers = $("sidebar-users");
  const sectionSubsystems = $("sidebar-subsystems");

  if (!btnUsers || !btnSubsystems || !sectionUsers || !sectionSubsystems) {
    console.error("Missing mode elements:", {
      btnUsers: !!btnUsers,
      btnSubsystems: !!btnSubsystems,
      sectionUsers: !!sectionUsers,
      sectionSubsystems: !!sectionSubsystems
    });
    return;
  }

  // Remove active class from all buttons
  btnUsers.classList.remove("active");
  btnSubsystems.classList.remove("active");
  
  // Hide all sections
  sectionUsers.classList.add("hidden");
  sectionSubsystems.classList.add("hidden");
  
  clearMain();

  if (mode === "users") {
    btnUsers.classList.add("active");
    sectionUsers.classList.remove("hidden");
    setViewHeader("User view", "Select a user to view yearly or monthly stats", "User");
    console.log("Switched to users mode");
  } else if (mode === "subsystems") {
    btnSubsystems.classList.add("active");
    sectionSubsystems.classList.remove("hidden");
    setViewHeader("Subsystem view", "Select a subsystem to view yearly or monthly stats", "Subsystem");
    console.log("Switched to subsystems mode");
  } else {
    console.error("Unknown mode:", mode);
  }
}

// --------------------------
// Navigation helpers
// --------------------------

function findUserBySlug(slug) {
  return state.users.find(user => user.slug === slug);
}

function findSubsystemByName(name) {
  return state.subsystems.find(subsystem => subsystem.name === name);
}

function findSubsystemByRepoName(repoName) {
  console.log('Looking for subsystem matching repo:', repoName);
  
  // Try to find by exact name match first
  let subsystem = findSubsystemByName(repoName);
  if (subsystem) {
    console.log('Found exact match:', subsystem.name);
    return subsystem;
  }
  
  // Try to find by the last part of repo path
  const shortName = repoName.split("/").pop();
  console.log('Trying short name:', shortName);
  subsystem = findSubsystemByName(shortName);
  if (subsystem) {
    console.log('Found by short name:', subsystem.name);
    return subsystem;
  }
  
  // Try to find with different variations (remove org prefix)
  const cleanName = repoName.replace(/^[^\/]+\//, "");
  console.log('Trying clean name:', cleanName);
  subsystem = findSubsystemByName(cleanName);
  if (subsystem) {
    console.log('Found by clean name:', subsystem.name);
    return subsystem;
  }
  
  // Try partial matches
  console.log('Trying partial matches...');
  for (const sub of state.subsystems) {
    if (repoName.toLowerCase().includes(sub.name.toLowerCase()) ||
        sub.name.toLowerCase().includes(shortName.toLowerCase())) {
      console.log('Found partial match:', sub.name, 'for repo:', repoName);
      return sub;
    }
  }
  
  console.warn('No subsystem found for repo: ' + repoName + '. Available subsystems:', state.subsystems.map(s => s.name));
  return null;
}

function navigateToUser(userSlug) {
  const user = findUserBySlug(userSlug);
  if (!user) {
    console.warn('User with slug ' + userSlug + ' not found');
    return;
  }
  
  console.log('Navigating to user:', user.display_name || user.slug);
  
  // Switch to users mode
  setMode("users");
  
  // Select the user
  state.selectedUser = user;
  state.selectedUserMonth = null;
  renderUserMonthList();
  
  // Check if we have yearly data and show it by default (like regular user click)
  const yearlyData = (user.months || []).find(m => m.is_yearly);
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

function navigateToSubsystem(subsystemName, currentPeriod = null) {
  console.log('Attempting to navigate to subsystem:', subsystemName, 'with period:', currentPeriod);
  console.log('Available subsystems:', state.subsystems.map(s => s.name));
  
  const subsystem = findSubsystemByRepoName(subsystemName);
  if (!subsystem) {
    console.warn('Subsystem matching ' + subsystemName + ' not found');
    // Show error to user
    clearMain();
    const main = $("main-content");
    main.innerHTML = '<div class="error">Could not find subsystem matching "' + subsystemName + '". Available subsystems: ' + state.subsystems.map(s => s.name).join(', ') + '</div>';
    return;
  }
  
  console.log('Found subsystem:', subsystem.name);
  
  // Switch to subsystems mode
  setMode("subsystems");
  
  // Select the subsystem
  state.selectedSubsystem = subsystem;
  state.selectedSubsystemPeriod = null;
  renderSubsystemPeriodList();
  
  // Try to find matching period
  let targetPeriod = null;
  if (currentPeriod) {
    targetPeriod = (subsystem.periods || []).find(p => p.label === currentPeriod.label);
  }
  
  // Default to yearly if available, otherwise first period
  if (!targetPeriod) {
    targetPeriod = (subsystem.periods || []).find(p => p.is_yearly) || (subsystem.periods || [])[0];
  }
  
  if (targetPeriod) {
    state.selectedSubsystemPeriod = targetPeriod;
    loadSubsystemPeriod(subsystem, targetPeriod);
  } else {
    clearMain();
    setViewHeader(
      "Subsystem: " + subsystem.name,
      "No data available",
      "Subsystem"
    );
  }
}

function createClickableDeveloperName(developerSlug, displayName, style = "block") {
  const nameElement = document.createElement("span");
  nameElement.className = "developer-name clickable" + (style === "inline" ? " inline" : "");
  nameElement.textContent = displayName || developerSlug;
  nameElement.style.cursor = "pointer";
  nameElement.onclick = (e) => {
    e.preventDefault();
    e.stopPropagation();
    console.log('Navigating to user:', developerSlug);
    navigateToUser(developerSlug);
  };
  return nameElement;
}

// Badge functions
async function loadUserBadges(userSlug) {
  try {
    const response = await fetchJSON("/api/users/" + encodeURIComponent(userSlug) + "/badges");
    return response.badges || [];
  } catch (err) {
    console.error("Failed to load user badges:", err);
    return [];
  }
}

function renderUserBadges(badges, container) {
  if (!badges || badges.length === 0) {
    return;
  }
  
  // Separate badges by type
  const ownershipBadges = badges.filter(b => b.type === "ownership");
  const maintainerBadges = badges.filter(b => b.type === "maintainer");
  const productivityBadges = badges.filter(b => b.type === "productivity");
  
function renderUserBadges(badges, container) {
  if (!badges || badges.length === 0) {
    return;
  }
  
  // Separate badges by type
  const ownershipBadges = badges.filter(b => b.type === "ownership");
  const maintainerBadges = badges.filter(b => b.type === "maintainer");
  const productivityBadges = badges.filter(b => b.type === "productivity");
  const ownershipPercentageBadges = badges.filter(b => b.type === "ownership_percentage");
  
  // Render productivity badges section first (most prestigious)
  if (productivityBadges.length > 0) {
    const productivitySection = document.createElement("div");
    productivitySection.className = "card badges-section";
    productivitySection.innerHTML = '<h2>🏆 Achievement Badges</h2>';
    
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
}

function createBadgeElement(badge) {
  const badgeElement = document.createElement("div");
  badgeElement.className = "badge-item";
  
  const titleElement = document.createElement("div");
  titleElement.className = "badge-title";
  titleElement.textContent = badge.title;
  
  const subtitleElement = document.createElement("div");
  subtitleElement.className = "badge-subtitle";
  subtitleElement.textContent = badge.subtitle;
  
  badgeElement.appendChild(titleElement);
  badgeElement.appendChild(subtitleElement);
  
  return badgeElement;
}

function createOwnershipBadgeElement(badge) {
  const badgeElement = document.createElement("div");
  badgeElement.className = "ownership-badge-item";
  
  const subsystemElement = document.createElement("div");
  subsystemElement.className = "ownership-subsystem";
  subsystemElement.textContent = badge.subsystem;
  
  const percentageElement = document.createElement("div");
  percentageElement.className = "ownership-percentage";
  percentageElement.textContent = (badge.share * 100).toFixed(1) + "%";
  
  badgeElement.appendChild(subsystemElement);
  badgeElement.appendChild(percentageElement);
  
  return badgeElement;
}
    productivitySection.className = "card badge-card productivity-badges";
    productivitySection.innerHTML = "<h2>🚀 Excellence Badges (" + productivityBadges.length + ")</h2>";
    
    const productivityGrid = document.createElement("div");
    productivityGrid.className = "badge-grid";
    
    productivityBadges.forEach(badge => {
      const badgeEl = createBadgeElement(badge);
      productivityGrid.appendChild(badgeEl);
    });
    
    productivitySection.appendChild(productivityGrid);
    container.appendChild(productivitySection);
  }
  
  // Render ownership badges section
  if (ownershipBadges.length > 0) {
    const ownershipSection = document.createElement("div");
    ownershipSection.className = "card badge-card ownership-badges";
    ownershipSection.innerHTML = "<h2>🏆 Code Ownership Badges (" + ownershipBadges.length + ")</h2>";
    
    const ownershipGrid = document.createElement("div");
    ownershipGrid.className = "badge-grid";
    
    ownershipBadges.forEach(badge => {
      const badgeEl = createBadgeElement(badge);
      ownershipGrid.appendChild(badgeEl);
    });
    
    ownershipSection.appendChild(ownershipGrid);
    container.appendChild(ownershipSection);
  }
  
  // Render maintainer badges section  
  if (maintainerBadges.length > 0) {
    const maintainerSection = document.createElement("div");
    maintainerSection.className = "card badge-card maintainer-badges";
    maintainerSection.innerHTML = "<h2>⚡ Recent Activity Badges (" + maintainerBadges.length + ")</h2>";
    
    const maintainerGrid = document.createElement("div");
    maintainerGrid.className = "badge-grid";
    
    maintainerBadges.forEach(badge => {
      const badgeEl = createBadgeElement(badge);
      maintainerGrid.appendChild(badgeEl);
    });
    
    maintainerSection.appendChild(maintainerGrid);
    container.appendChild(maintainerSection);
  }
}

function createBadgeElement(badge) {
  const badgeEl = document.createElement("div");
  
  // Add base badge class and specific badge type class
  badgeEl.className = "badge";
  if (badge.badge_type) {
    badgeEl.classList.add("badge-" + badge.badge_type);
  }
  
  const titleEl = document.createElement("div");
  titleEl.className = "badge-title";
  titleEl.textContent = badge.title;
  
  const subtitleEl = document.createElement("div");
  subtitleEl.className = "badge-subtitle";
  subtitleEl.textContent = badge.subtitle;
  
  badgeEl.appendChild(titleEl);
  badgeEl.appendChild(subtitleEl);
  
  if (badge.subsystem) {
    badgeEl.onclick = () => {
      console.log('Badge clicked, navigating to subsystem:', badge.subsystem);
      navigateToSubsystem(badge.subsystem);
    };
    badgeEl.style.cursor = "pointer";
  }
  
  return badgeEl;
}

// --------------------------
// User monthly dashboard
// --------------------------

async function loadUserMonth(user, month) {
  try {
    let url;
    if (month.is_yearly) {
      const year = parseInt(month.label);
      url = "/api/users/" + encodeURIComponent(user.slug) + "/year/" + year;
    } else {
      url = "/api/users/" + encodeURIComponent(user.slug) + "/month/" + month.from + "/" + month.to;
    }
    
    const data = await fetchJSON(url);
    renderUserDashboard(user, month, data);
  } catch (err) {
    console.error(err);
    clearMain();
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load user stats.</div>';
  }
}

function getLanguageStats(summary) {
  const langs = summary.languages || {};
  
  // Define languages we consider "real programming languages"
  const realLanguages = new Set([
    // Major languages
    'JavaScript', 'TypeScript', 'Python', 'Java', 'C#', 'C++', 'C', 
    'Go', 'Rust', 'Swift', 'Kotlin', 'PHP', 'Ruby', 'Scala', 'Dart',
    'Objective-C', 'R', 'MATLAB', 'Perl', 'Haskell', 'Clojure', 'F#',
    'Elixir', 'Erlang', 'Lua', 'Julia', 'Assembly', 'Groovy',
    'Vim Script', 'Emacs Lisp', 'OCaml', 'Scheme', 'Common Lisp', 
    'Forth', 'Ada', 'Fortran', 'COBOL', 'Pascal', 'D', 'Nim', 
    'Crystal', 'Zig', 'V', 'Odin',
    // Shell/Scripting languages
    'Shell', 'Bash', 'Bourne Again Shell', 'Bourne Shell',
    'PowerShell', 'Zsh', 'Fish',
    // SQL variants
    'SQL', 'PLpgSQL', 'PL/SQL', 'T-SQL'
  ]);

  const labels = [];
  const values = [];
  
  for (const [lang, stats] of Object.entries(langs)) {
    // Only include real programming languages
    if (realLanguages.has(lang)) {
      const added = stats.additions || 0;
      const deleted = stats.deletions || 0;
      labels.push(lang);
      values.push(added + deleted);
    }
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
    values.push((weekdays[dayName] || {}).commits || 0);
  }
  return { labels, values };
}

function getHourStats(summary) {
  const hours = summary.per_hour || {};
  const labels = [];
  const values = [];
  for (let h = 0; h < 24; h++) {
    labels.push(h.toString().padStart(2, "0") + ":00");
    values.push((hours[h.toString()] || {}).commits || 0);
  }
  return { labels, values };
}

function renderUserDashboard(user, month, summary) {
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

  // Load and render badges (async)
  loadUserBadges(user.slug).then(badges => {
    renderUserBadges(badges, main);
  });

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
        navigateToSubsystem(repoName, month);
      };
      
      repoList.appendChild(li);
    });

    repoBox.appendChild(repoList);
    main.appendChild(repoBox);
  }

  // Add contribution heatmap if we have daily data
  if (summary.per_date && Object.keys(summary.per_date).length > 0) {
    const heatmapCard = document.createElement("div");
    heatmapCard.className = "card heatmap-card";
    heatmapCard.innerHTML = '<h2>Contribution activity</h2>';
    
    const heatmapContainer = document.createElement("div");
    heatmapContainer.className = "contribution-heatmap";
    
    // Always show a full year for consistency, but filter data based on selection
    let heatmapFromDate, heatmapToDate;
    let filteredPerDateData = {};
    
    if (month.is_yearly) {
      // Show the full year
      const year = parseInt(month.label);
      heatmapFromDate = year + "-01-01";
      heatmapToDate = year + "-12-31";
      filteredPerDateData = summary.per_date; // Use all data for yearly view
    } else {
      // Show the full year but highlight only the selected month's data
      const monthDate = new Date(summary.from);
      const year = monthDate.getFullYear();
      heatmapFromDate = year + "-01-01";
      heatmapToDate = year + "-12-31";
      
      // Filter data to only include the selected month
      const monthStart = summary.from;
      const monthEnd = summary.to;
      
      for (const [date, data] of Object.entries(summary.per_date)) {
        if (date >= monthStart && date <= monthEnd) {
          filteredPerDateData[date] = data;
        }
      }
    }
    
    heatmapContainer.appendChild(createContributionHeatmap(filteredPerDateData, heatmapFromDate, heatmapToDate));
    
    heatmapCard.appendChild(heatmapContainer);
    main.appendChild(heatmapCard);
  }

  // Chart containers
  const chartRow = document.createElement("div");
  chartRow.className = "chart-grid";

  // Languages
  const langCard = document.createElement("div");
  langCard.className = "card";
  langCard.innerHTML = '<h2>Lines changed per language</h2><canvas id="chart-languages"></canvas>';
  chartRow.appendChild(langCard);

  // Weekday
  const weekdayCard = document.createElement("div");
  weekdayCard.className = "card";
  weekdayCard.innerHTML = '<h2>Commits by weekday</h2><canvas id="chart-weekday"></canvas>';
  chartRow.appendChild(weekdayCard);

  // Hour
  const hourCard = document.createElement("div");
  hourCard.className = "card";
  hourCard.innerHTML = '<h2>Commits by hour</h2><canvas id="chart-hour"></canvas>';
  chartRow.appendChild(hourCard);

  main.appendChild(chartRow);

  // Build charts
  const langStats = getLanguageStats(summary);
  if (langStats.labels.length > 0) {
    const ctx = document.getElementById("chart-languages");
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

  const weekdayStats = getWeekdayStats(summary);
  const ctxWeekday = document.getElementById("chart-weekday");
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
      scales: { y: { beginAtZero: true } }
    }
  });

  const hourStats = getHourStats(summary);
  const ctxHour = document.getElementById("chart-hour");
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
      scales: { y: { beginAtZero: true } }
    }
  });
}

// --------------------------
// Subsystem dashboard
// --------------------------

async function loadSubsystemPeriod(subsystem, period) {
  try {
    let url;
    if (period.is_yearly) {
      const year = parseInt(period.label);
      url = "/api/subsystems/" + encodeURIComponent(subsystem.name) + "/year/" + year;
    } else {
      url = "/api/subsystems/" + encodeURIComponent(subsystem.name) + "/month/" + period.from + "/" + period.to;
    }
    
    const data = await fetchJSON(url);
    renderSubsystemDashboard(subsystem, period, data);
  } catch (err) {
    console.error("Failed to load subsystem stats for " + subsystem.name + ":", err);
    clearMain();
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load subsystem stats: ' + err.message + '</div>';
  }
}

function renderSubsystemDashboard(subsystem, period, summary) {
  try {
    clearMain();

    const periodType = period.is_yearly ? "Yearly" : "Monthly";
    const periodLabel = period.is_yearly ? period.label : period.label + " (" + summary.from + " → " + summary.to + ")";

    setViewHeader(
      "Subsystem: " + (summary.service || subsystem.name),
      periodLabel,
      "Subsystem · " + periodType
    );

    const main = $("main-content");

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
      topDevCard.innerHTML = '<h2>Top Developer</h2>';
      
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

    // Add top maintainers section (from recent activity)
    addTopMaintainersSection(main, subsystem.name);

    // Show all developers if we have the data
    const developers = summary.developers || {};
    if (Object.keys(developers).length > 0) {
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

  } catch (error) {
    console.error("Error rendering subsystem dashboard:", error);
    clearMain();
    const main = $("main-content");
    main.innerHTML = '<div class="error">Error rendering dashboard: ' + error.message + '</div>';
  }
}

// Add function to show top maintainers in subsystem view
async function addTopMaintainersSection(container, subsystemName) {
  try {
    // Load all user badges to find maintainers for this subsystem
    const allBadges = {};
    
    // We'll need to query each user's badges to find who has maintainer badges for this subsystem
    for (const user of state.users) {
      try {
        const userBadges = await loadUserBadges(user.slug);
        const maintainerBadges = userBadges.filter(b => 
          b.type === "maintainer" && b.subsystem === subsystemName
        );
        
        if (maintainerBadges.length > 0) {
          allBadges[user.slug] = {
            display_name: user.display_name || user.slug,
            badges: maintainerBadges
          };
        }
      } catch (err) {
        console.warn("Failed to load badges for user:", user.slug, err);
      }
    }
    
    // If we found maintainers, show them
    if (Object.keys(allBadges).length > 0) {
      const maintainerCard = document.createElement("div");
      maintainerCard.className = "card";
      maintainerCard.innerHTML = '<h2>🔥 Top Maintainers (Last 3 Months)</h2>';
      
      const maintainerList = document.createElement("div");
      maintainerList.className = "top-maintainer-list";
      
      Object.entries(allBadges).forEach(([userSlug, userData]) => {
        const maintainerInfo = document.createElement("div");
        maintainerInfo.className = "top-developer-info";
        
        // Create clickable developer name
        const nameElement = createClickableDeveloperName(userSlug, userData.display_name);
        
        const badgeInfo = userData.badges[0]; // Take the first (should be only one for this subsystem)
        const statsElement = document.createElement("div");
        statsElement.className = "developer-stats";
        statsElement.innerHTML = badgeInfo.subtitle;
        
        maintainerInfo.appendChild(nameElement);
        maintainerInfo.appendChild(statsElement);
        maintainerList.appendChild(maintainerInfo);
      });
      
      maintainerCard.appendChild(maintainerList);
      container.appendChild(maintainerCard);
    }
  } catch (error) {
    console.warn("Failed to load top maintainers:", error);
  }
}

// --------------------------
// Contribution heatmap functions
// --------------------------

function createContributionHeatmap(perDateData, fromDate, toDate) {
  const container = document.createElement("div");
  container.className = "heatmap-container";
  
  const startDate = new Date(fromDate);
  const endDate = new Date(toDate);
  
  // Check if this spans a full year or just a month
  const isYearlyView = (endDate.getTime() - startDate.getTime()) > (300 * 24 * 60 * 60 * 1000);
  
  let heatmapStart, heatmapEnd;
  
  if (isYearlyView) {
    heatmapStart = new Date(startDate.getFullYear(), 0, 1);
    heatmapEnd = new Date(startDate.getFullYear(), 11, 31);
  } else {
    heatmapStart = new Date(startDate.getFullYear(), 0, 1);
    heatmapEnd = new Date(startDate.getFullYear(), 11, 31);
  }
  
  // Find the Sunday before start date for proper alignment
  const gridStart = new Date(heatmapStart);
  gridStart.setDate(gridStart.getDate() - gridStart.getDay());
  
  // Find the Saturday after end date for proper alignment  
  const gridEnd = new Date(heatmapEnd);
  gridEnd.setDate(gridEnd.getDate() + (6 - gridEnd.getDay()));
  
  // Calculate max commits for color scaling
  const commitCounts = Object.values(perDateData).map(data => data.commits || 0);
  const maxCommits = Math.max(...commitCounts, 1);
  
  // Create week columns
  const weeks = [];
  const currentDate = new Date(gridStart);
  
  while (currentDate <= gridEnd) {
    const week = [];
    for (let day = 0; day < 7; day++) {
      const dateStr = currentDate.toISOString().split('T')[0];
      const dayData = perDateData[dateStr] || { commits: 0 };
      const isInRange = currentDate >= heatmapStart && currentDate <= heatmapEnd;
      
      // For monthly view, only highlight days within the selected month
      const isInSelectedPeriod = isYearlyView || 
        (currentDate >= startDate && currentDate <= endDate);
      
      week.push({
        date: dateStr,
        commits: dayData.commits || 0,
        isInRange: isInRange,
        isInSelectedPeriod: isInSelectedPeriod,
        intensity: Math.min(Math.ceil((dayData.commits || 0) / maxCommits * 4), 4)
      });
      
      currentDate.setDate(currentDate.getDate() + 1);
    }
    weeks.push(week);
  }
  
  // Create the heatmap grid with proper layout
  const grid = document.createElement("div");
  grid.className = "heatmap-grid";
  
  const weekCount = weeks.length;
  
  // Set CSS grid properties directly
  grid.style.display = "grid";
  grid.style.gridTemplateColumns = "60px repeat(" + weekCount + ", 14px)";
  grid.style.gridTemplateRows = "30px repeat(7, 14px)";
  grid.style.gap = "3px";
  grid.style.alignItems = "center";
  grid.style.justifyItems = "center";
  grid.style.overflowX = "auto";
  grid.style.minWidth = "max-content";
  
  // Add month labels in the first row
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  
  // Find month start and end positions for spanning
  const monthRanges = new Map();
  const currentMonth = new Date(gridStart);
  let weekIndex = 0;
  const weekDate = new Date(gridStart);
  
  while (weekIndex < weekCount) {
    const monthKey = currentMonth.getMonth();
    if (!monthRanges.has(monthKey)) {
      monthRanges.set(monthKey, { start: weekIndex + 2, end: weekIndex + 2 });
    } else {
      monthRanges.get(monthKey).end = weekIndex + 2;
    }
    
    weekDate.setDate(weekDate.getDate() + 7);
    currentMonth.setTime(weekDate.getTime());
    weekIndex++;
  }
  
  // Add month labels with proper spanning
  monthRanges.forEach((range, monthIndex) => {
    const monthLabel = document.createElement("div");
    monthLabel.textContent = monthNames[monthIndex];
    monthLabel.style.gridColumn = range.start + " / " + (range.end + 1);
    monthLabel.style.gridRow = "1";
    monthLabel.style.fontSize = "11px";
    monthLabel.style.color = "#9ca3af";
    monthLabel.style.fontWeight = "500";
    monthLabel.style.whiteSpace = "nowrap";
    monthLabel.style.textAlign = "center";
    monthLabel.style.display = "flex";
    monthLabel.style.alignItems = "center";
    monthLabel.style.justifyContent = "center";
    grid.appendChild(monthLabel);
  });
  
  // Add day labels
  const dayLabels = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
  
  dayLabels.forEach((dayLabel, index) => {
    const dayDiv = document.createElement("div");
    dayDiv.textContent = dayLabel;
    dayDiv.style.gridColumn = "1";
    dayDiv.style.gridRow = "" + (index + 2);
    dayDiv.style.fontSize = "10px";
    dayDiv.style.color = "#9ca3af";
    dayDiv.style.textAlign = "right";
    dayDiv.style.paddingRight = "6px";
    dayDiv.style.whiteSpace = "nowrap";
    dayDiv.style.display = "flex";
    dayDiv.style.alignItems = "center";
    dayDiv.style.justifyContent = "flex-end";
    grid.appendChild(dayDiv);
  });
  
  // Add week columns
  weeks.forEach((week, weekIndex) => {
    week.forEach((day, dayIndex) => {
      const daySquare = document.createElement("div");
      daySquare.className = "heatmap-day intensity-" + day.intensity;
      daySquare.style.gridColumn = "" + (weekIndex + 2);
      daySquare.style.gridRow = "" + (dayIndex + 2);
      daySquare.style.width = "12px";
      daySquare.style.height = "12px";
      daySquare.style.borderRadius = "2px";
      daySquare.style.border = "1px solid #1f2937";
      
      if (!day.isInRange) {
        daySquare.classList.add("out-of-range");
      }
      
      if (!isYearlyView && !day.isInSelectedPeriod) {
        daySquare.style.opacity = "0.3";
      }
      
      // Tooltip
      const date = new Date(day.date);
      const formattedDate = date.toLocaleDateString('en-US', { 
        weekday: 'short', 
        month: 'short', 
        day: 'numeric'
      });
      
      daySquare.title = day.commits + " commits on " + formattedDate;
      
      grid.appendChild(daySquare);
    });
  });
  
  // Add legend
  const legend = document.createElement("div");
  legend.className = "heatmap-legend";
  legend.style.marginTop = "12px";
  legend.style.display = "flex";
  legend.style.alignItems = "center";
  legend.style.justifyContent = "flex-end";
  legend.style.gap = "4px";
  legend.style.fontSize = "11px";
  legend.style.color = "#9ca3af";
  
  legend.innerHTML = `
    <span>Less</span>
    <div class="heatmap-day intensity-0" style="width: 12px; height: 12px; margin: 0 2px; border-radius: 2px; border: 1px solid #1f2937;"></div>
    <div class="heatmap-day intensity-1" style="width: 12px; height: 12px; margin: 0 2px; border-radius: 2px; border: 1px solid #1f2937;"></div>
    <div class="heatmap-day intensity-2" style="width: 12px; height: 12px; margin: 0 2px; border-radius: 2px; border: 1px solid #1f2937;"></div>
    <div class="heatmap-day intensity-3" style="width: 12px; height: 12px; margin: 0 2px; border-radius: 2px; border: 1px solid #1f2937;"></div>
    <div class="heatmap-day intensity-4" style="width: 12px; height: 12px; margin: 0 2px; border-radius: 2px; border: 1px solid #1f2937;"></div>
    <span>More</span>
  `;
  
  container.appendChild(grid);
  container.appendChild(legend);
  
  return container;
}

function createMonthLabels(startDate, endDate) {
  const months = [];
  const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", 
                     "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
  
  // Determine which months are represented in the date range
  const currentDate = new Date(startDate);
  const seenMonths = new Set();
  
  while (currentDate <= endDate) {
    const monthKey = "" + currentDate.getFullYear() + "-" + currentDate.getMonth();
    if (!seenMonths.has(monthKey)) {
      seenMonths.add(monthKey);
      months.push({
        name: monthNames[currentDate.getMonth()],
        month: currentDate.getMonth()
      });
    }
    
    // Move to next month
    currentDate.setMonth(currentDate.getMonth() + 1);
    currentDate.setDate(1);
  }
  
  return months;
}

// --------------------------
// Init
// --------------------------

window.addEventListener("DOMContentLoaded", async () => {
  console.log("DOM loaded");
  $("mode-users").addEventListener("click", () => setMode("users"));
  $("mode-subsystems").addEventListener("click", () => setMode("subsystems"));

  setMode("subsystems"); // Default to subsystems view

  try {
    await loadUsersAndSubsystems();
  } catch (err) {
    console.error(err);
    const main = $("main-content");
    main.innerHTML = '<div class="error">Failed to load data from backend. Check console/logs.</div>';
  }
});