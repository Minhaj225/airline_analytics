// Render one validated, prebuilt payload without making network requests.
(() => {
  "use strict";

  // Stop with a visible explanation if the generated data file was not loaded.
  const data = window.DASHBOARD_DATA;
  if (!data) {
    document.body.innerHTML = '<main class="load-error"><h1>Dashboard data is missing.</h1><p>Run the dashboard builder, then reload this page.</p></main>';
    return;
  }

  // Formatting helpers keep labels and metrics consistent across sections.
  const el = (id) => document.getElementById(id);
  const formatInt = (value) => new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(value);
  const formatPct = (value, digits = 1) => new Intl.NumberFormat("en-US", { style: "percent", maximumFractionDigits: digits }).format(value);
  const formatFloat = (value, digits = 2) => new Intl.NumberFormat("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits }).format(value);
  const setText = (id, value) => { el(id).textContent = value; };

  // Fill fixed run metadata and headline metrics first.
  setText("run-status", `${data.run.status.toUpperCase()} / ${data.run.id}`);
  setText("sidebar-run", data.run.id);
  setText("dataset-id", data.run.dataset_id);
  setText("storage", data.run.storage.toUpperCase());
  setText("spark-master", data.run.spark_master);
  setText("run-mode", data.run.mode.toUpperCase());
  setText("intro-summary", `${formatInt(data.summary.retained_records)} retained flights became ${formatInt(data.summary.airports)} airport vertices and ${formatInt(data.summary.routes)} directed route edges.`);
  setText("raw-records", formatInt(data.summary.raw_records));
  setText("retained-records", formatInt(data.summary.retained_records));
  setText("retention", `${formatPct(data.summary.retained_fraction)} after cleaning`);
  setText("airports", formatInt(data.summary.airports));
  setText("routes", formatInt(data.summary.routes));
  setText("components", formatInt(data.summary.weak_components));
  setText("triangles", formatInt(data.summary.triangles));
  setText("rank-comparison", `Uniform and traffic-weighted PageRank have Spearman correlation ${formatFloat(data.rank_comparison.spearman, 3)}, with ${data.rank_comparison.overlap} shared airports in the top ${data.rank_comparison.top_n}.`);
  setText("delay-summary", `The airport-level Spearman association is ${formatFloat(data.summary.delay_correlation, 3)}. Mean observed arrival delay is ${formatFloat(data.summary.mean_arrival_delay, 2)} minutes across ${formatPct(data.delay.coverage)} of retained flights.`);
  setText("route-table-title", `Highest means among routes with at least ${formatInt(data.delay.threshold)} flights`);
  setText("validation-summary", `${data.validation.saved_output_checks} Scala saved-output checks and ${data.validation.export_checks} export integrity checks passed for this run.`);
  setText("generated-at", `Payload generated ${new Date(data.generated_at).toLocaleString()}`);

  // Build the mutually exclusive cleaning funnel.
  const cleaningList = el("cleaning-list");
  data.cleaning.filter((row) => row.records > 0).forEach((row) => {
    const line = document.createElement("div");
    const retained = row.reason === "retained";
    line.className = `funnel-row${retained ? " retained" : row.records > 100000 ? " major" : ""}`;
    line.innerHTML = `<span>${row.reason}</span><strong>${formatInt(row.records)} ${retained ? "kept" : "removed"}</strong>`;
    cleaningList.appendChild(line);
  });

  // Compare raw and retained coverage month by month.
  const monthlyChart = el("monthly-chart");
  const maxMonthly = Math.max(...data.monthly.map((month) => month.raw_records));
  data.monthly.forEach((month) => {
    const item = document.createElement("div");
    item.className = `month${month.retained_records === 0 ? " excluded" : ""}`;
    item.title = `${month.year}/${month.month}: ${formatInt(month.retained_records)} of ${formatInt(month.raw_records)} retained`;
    item.innerHTML = `<div class="month-bars"><span class="bar raw" style="height:${(month.raw_records / maxMonthly) * 100}%"></span><span class="bar retained" style="height:${Math.max(1, (month.retained_records / maxMonthly) * 100)}%"></span></div><span class="month-label">${String(month.month).padStart(2, "0")}</span>`;
    monthlyChart.appendChild(item);
  });

  // Scale centrality bars relative to the highest-ranked airport.
  const rankingChart = el("ranking-chart");
  const maxScore = data.top_airports[0].score;
  data.top_airports.forEach((airport) => {
    const row = document.createElement("div");
    row.className = "rank-row";
    row.title = `${airport.name}, ${airport.city}: ${formatInt(airport.incoming_flights)} incoming retained flights`;
    row.innerHTML = `<span class="rank-number">${String(airport.rank).padStart(2, "0")}</span><span class="rank-airport">${airport.iata}</span><div class="rank-bar-track"><div class="rank-bar-fill" style="width:${(airport.score / maxScore) * 100}%"></div></div><span class="rank-value">${formatFloat(airport.score, 4)}</span>`;
    rankingChart.appendChild(row);
  });

  // Summarize directed reachability toward each selected hub.
  const hubGrid = el("hub-grid");
  Object.entries(data.reachability).forEach(([hub, reach]) => {
    const card = document.createElement("article");
    card.className = "hub-card";
    card.innerHTML = `<h3>${hub}</h3><dl><div><dt>Reachable</dt><dd>${formatInt(reach.reachable)} / ${formatInt(data.summary.airports)}</dd></div><div><dt>Unreachable</dt><dd>${formatInt(reach.unreachable)}</dd></div><div><dt>Maximum legs</dt><dd>${reach.maximum_hops}</dd></div></dl>`;
    hubGrid.appendChild(card);
  });

  // Populate delay, validation, timing, figure, and limitation sections.
  const delayRoutes = el("delay-routes");
  data.delay.highest_routes.forEach((route) => {
    const row = document.createElement("tr");
    row.innerHTML = `<td>${route.origin} to ${route.destination}</td><td>${formatInt(route.flights)}</td><td>${formatFloat(route.avg_delay, 1)} min</td><td>${formatPct(route.delayed_fraction)}</td>`;
    delayRoutes.appendChild(row);
  });

  const checks = el("validation-checks");
  data.validation.checks.forEach((check) => {
    const item = document.createElement("div");
    item.className = "check-item";
    item.textContent = check.check.replaceAll("_", " ");
    checks.appendChild(item);
  });

  const stageChart = el("stage-chart");
  const maxStage = Math.max(...data.stages.map((stage) => stage.wall_seconds));
  data.stages.forEach((stage) => {
    const row = document.createElement("div");
    row.className = "stage-row";
    row.innerHTML = `<span>${stage.name}</span><div class="stage-bar"><div style="width:${(stage.wall_seconds / maxStage) * 100}%"></div></div><strong>${formatInt(stage.wall_seconds)}s</strong>`;
    stageChart.appendChild(row);
  });

  const gallery = el("figure-gallery");
  data.figures.forEach((figure) => {
    const item = document.createElement("figure");
    item.className = "figure";
    item.innerHTML = `<img src="${figure.src}" alt="${figure.title}" loading="lazy"><figcaption>${figure.title}</figcaption>`;
    gallery.appendChild(item);
  });

  const limitations = el("limitations-list");
  data.limitations.forEach((text) => {
    const item = document.createElement("li");
    item.textContent = text;
    limitations.appendChild(item);
  });

  const links = [...document.querySelectorAll(".sidebar nav a")];
  const sections = links.map((link) => document.querySelector(link.getAttribute("href"))).filter(Boolean);

  // Keep navigation state synchronized with the reader's current section.
  const setActiveLink = (sectionId) => {
    links.forEach((link) => link.classList.toggle("active", link.getAttribute("href") === `#${sectionId}`));
  };

  const updateActiveSection = () => {
    const readingLine = Math.min(220, window.innerHeight * .3);
    let active = sections[0];

    for (const section of sections) {
      const bounds = section.getBoundingClientRect();
      if (bounds.top <= readingLine) active = section;
      if (bounds.top <= readingLine && bounds.bottom > readingLine) break;
    }

    const atPageEnd = window.scrollY + window.innerHeight >= document.documentElement.scrollHeight - 4;
    setActiveLink(atPageEnd ? sections[sections.length - 1].id : active.id);
  };

  const observer = new IntersectionObserver(updateActiveSection, {
    rootMargin: "-15% 0px -70% 0px",
    threshold: [0, 1],
  });
  sections.forEach((section) => observer.observe(section));
  links.forEach((link) => link.addEventListener("click", () => setActiveLink(link.getAttribute("href").slice(1))));
  window.addEventListener("hashchange", updateActiveSection);
  requestAnimationFrame(updateActiveSection);
})();
