(function(){
  const $ = (id) => document.getElementById(id);
  const fmt = (n) => typeof n === "number" ? n.toLocaleString() : n;
  const esc = (s) => String(s)
    .replaceAll("&","&amp;")
    .replaceAll("<","&lt;")
    .replaceAll(">","&gt;")
    .replaceAll('"',"&quot;")
    .replaceAll("'","&#039;");

  async function fetchJSON(url){
    const r = await fetch(url, { credentials:"include" });
    const txt = await r.text();
    try{
      return JSON.parse(txt);
    }catch{
      return { status:r.status, text:txt };
    }
  }

  function renderKV(user){
    const userName = user?.userName || user?.emails?.[0]?.value || "—";
    const display = user?.displayName
      || [user?.name?.givenName, user?.name?.familyName].filter(Boolean).join(" ")
      || "—";
    const active = (user?.active === true || user?.active === false) ? String(user.active) : "—";
    $("meUser").textContent = userName;
    $("meDisplay").textContent = display;
    $("meActive").textContent = active;
  }

  function renderPreviewTable(el, rows, maxRows = 10){
    if(!Array.isArray(rows) || rows.length === 0){
      el.innerHTML = '<em class="empty">No rows available.</em>';
      return;
    }
    const sample = rows.slice(0, maxRows);
    const cols = Array.from(sample.reduce((set, row) => {
      Object.keys(row).forEach((key) => set.add(key));
      return set;
    }, new Set()));

    const thead = `<thead><tr>${cols.map((c) => `<th>${esc(c)}</th>`).join("")}</tr></thead>`;
    const tbody = `<tbody>${
      sample.map((row) =>
        `<tr>${cols.map((c) => `<td>${esc(row[c] ?? "—")}</td>`).join("")}</tr>`
      ).join("")
    }</tbody>`;

    el.innerHTML = `<table>${thead}${tbody}</table>`;
  }

  function progress(elBar, pct){
    if(!elBar) return;
    const next = Math.max(0, Math.min(100, pct));
    elBar.style.width = `${next}%`;
  }

  async function loadUser(){
    const data = await fetchJSON("/api/me");
    console.log("[me]", data);
    $("meMode").textContent = data?.mode ?? "—";
    $("meRaw").textContent = JSON.stringify(data, null, 2);
    renderKV(data?.current_user);
  }

  async function loadPing(){
    progress($("pingBar"), 12);
    const data = await fetchJSON("/api/sql/ping");
    console.log("[ping]", data);
    $("pingOk").textContent = data?.ok ? "OK" : "Not OK";
    $("pingRaw").textContent = JSON.stringify(data, null, 2);
    progress($("pingBar"), 100);
    setTimeout(() => progress($("pingBar"), 0), 400);
  }

  async function loadEmails(){
    const subject = $("subjectInput").value.trim();
    const fromEmail = $("fromEmailInput").value.trim();
    const isRead = $("isReadSelect").value;
    const isStarred = $("isStarredSelect").value;
    const qs = new URLSearchParams();
    
    if(subject) qs.set("subject", subject);
    if(fromEmail) qs.set("from_email", fromEmail);
    if(isRead !== "") qs.set("is_read", isRead);
    if(isStarred !== "") qs.set("is_starred", isStarred);
    qs.set("limit", "100");

    progress($("emailBar"), 18);
    const url = `/api/emails${qs.toString() ? `?${qs.toString()}` : ""}`;
    const data = await fetchJSON(url);
    console.log("[emails]", data);

    $("emailRaw").textContent = JSON.stringify(data, null, 2);
    $("emailCount").textContent = `rows: ${fmt(data?.rows?.length ?? 0)}`;

    const t = data?.timing || {};
    $("emailTiming").textContent = `query ${t.query_ms ?? "—"} ms · json ${t.serialize_ms ?? "—"} ms · total ${t.total_ms ?? "—"} ms`;

    renderPreviewTable($("emailTable"), data?.rows || []);
    progress($("emailBar"), 100);
    setTimeout(() => progress($("emailBar"), 0), 400);
  }

  function wireEvents(){
    $("runEmails")?.addEventListener("click", loadEmails);

    $("clearFilters")?.addEventListener("click", () => {
      $("subjectInput").value = "";
      $("fromEmailInput").value = "";
      $("isReadSelect").value = "";
      $("isStarredSelect").value = "";
      $("emailTable").innerHTML = "—";
      $("emailRaw").textContent = "—";
      $("emailCount").textContent = "rows: —";
      $("emailTiming").textContent = "—";
      progress($("emailBar"), 0);
    });

    $("refreshPing")?.addEventListener("click", loadPing);
  }

  async function init(){
    wireEvents();
    await Promise.all([loadUser(), loadPing(), loadEmails()]);
  }

  if(document.readyState === "loading"){
    document.addEventListener("DOMContentLoaded", init);
  }else{
    init();
  }
})();

