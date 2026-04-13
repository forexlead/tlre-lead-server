/**
 * TLRE — The Leading Real Estate
 * Production Backend  |  Node 18+  |  Deploy to Railway
 *
 * Sources:
 *   People Data Labs  → Pre-arrival corporate relo leads
 *   Wake County       → Tax delinquent seller leads  (confirmed URL)
 *   NC eCourts        → Divorce + probate filings
 *   SpeedeonData      → Post-arrival new movers
 *   Zapier webhooks   → Zillow PA, Opcity, BoldLeads, REDX
 *   BatchSkipTracing  → Auto-trace all leads without contact info
 */

const express = require("express");
const cors    = require("cors");
const XLSX    = require("xlsx");
const app     = express();

app.use(cors());
app.use(express.json({ limit: "5mb" }));

// ── RTP constants ─────────────────────────────────────────────
const RTP_ZIPS = [
  "27511","27513","27518","27519",
  "27560",
  "27701","27703","27705","27707",
  "27514","27516",
  "27502","27539",
  "27540","27526",
  "27609","27612","27615",
];

const RTP_EMPLOYERS = [
  "cisco","cisco systems","sas institute","sas","apple","ibm","red hat",
  "lenovo","barclays","gsk","glaxosmithkline","bayer","rti international",
  "fujifilm diosynth","wolfspeed","blue cross blue shield","bcbsnc",
  "duke university","unc health","university of north carolina",
  "nc state university","north carolina state university","genesys",
  "biogen","iqvia","fidelity","abbott","syneos health","pfizer",
];

// ── In-memory store ───────────────────────────────────────────
const leads = new Map();
let seq = 1;
const nid = p => `${p}_${Date.now()}_${seq++}`;

function dedup(inc) {
  const ph = (inc.phone||"").replace(/\D/g,"");
  for (const e of leads.values()) {
    if (inc.email && inc.email === e.email) return true;
    if (ph.length >= 10 && ph === (e.phone||"").replace(/\D/g,"")) return true;
    if (inc.extId && inc.extId === e.extId) return true;
  }
  return false;
}

function save(lead) {
  if (dedup(lead)) return false;
  leads.set(lead.id, lead);
  return true;
}

// ── Utilities ─────────────────────────────────────────────────
const tc = s => (s||"").toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
const fmtPhone = r => {
  const d = (r||"").replace(/\D/g,"");
  if (d.length === 10) return `(${d.slice(0,3)}) ${d.slice(3,6)}-${d.slice(6)}`;
  if (d.length === 11 && d[0]==="1") return `(${d.slice(1,4)}) ${d.slice(4,7)}-${d.slice(7)}`;
  return r;
};

const getKey = (req, k) =>
  req.body?.[k] || req.headers?.[`x-${k.toLowerCase().replace(/_/g,"-")}`] || process.env[k] || "";

// ════════════════════════════════════════════════════════════
// INTEGRATION 1 — People Data Labs
// https://docs.peopledatalabs.com/docs/person-search-api
// Auth: X-Api-Key header  |  Cost: ~$0.04–0.28/record
// Finds professionals who recently joined RTP employers
// ════════════════════════════════════════════════════════════
async function fetchPdlLeads(apiKey, daysSince = 90, size = 25) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - daysSince);

  const payload = {
    query: {
      bool: {
        must: [
          { terms: { job_company_name: RTP_EMPLOYERS } },
          { bool: {
            should: [
              { term: { location_metro: "durham, north carolina" } },
              { term: { location_metro: "raleigh, north carolina" } },
              { term: { location_metro: "cary, north carolina"   } },
            ],
            minimum_should_match: 1,
          }},
          { range: { job_start_date: { gte: cutoff.toISOString().slice(0,10) } } },
        ],
      },
    },
    size,
    required: "job_company_name AND location_metro",
    dataset:  "resume,contact,social",
  };

  const res = await fetch("https://api.peopledatalabs.com/v5/person/search", {
    method:  "POST",
    headers: { "Content-Type": "application/json", "X-Api-Key": apiKey },
    body:    JSON.stringify(payload),
  });
  if (res.status === 401 || res.status === 403) throw new Error("invalid_pdl_key");
  if (!res.ok) throw new Error(`PDL HTTP ${res.status}`);

  const data = await res.json();
  let added = 0;
  const newLeads = [];

  (data.data || []).forEach(p => {
    const exp  = (p.experience||[])[0] || {};
    const prev = (p.experience||[])[1] || {};
    const emails  = (p.emails||[]).map(e=>e.address||e).filter(Boolean);
    const phones  = (p.phone_numbers||[]).map(ph=>fmtPhone(ph.number||ph)).filter(Boolean);
    const lead = {
      id:          nid("pdl"),
      name:        p.full_name || `${p.first_name||""} ${p.last_name||""}`.trim() || "Unknown",
      email:       emails[0] || "",
      phone:       phones[0] || "",
      allEmails:   emails,
      allPhones:   phones,
      linkedinUrl: p.linkedin_url || "",
      source:      "pdl",
      sourceLabel: "People Data Labs",
      leadPhase:   "pre_arrival",
      type:        "buyer",
      employer:    exp.company?.name || p.job_company_name || "",
      jobTitle:    exp.title?.name   || p.job_title        || "",
      seniority:   (p.job_title_levels||[])[0] || "",
      budget:      calcBudget((p.job_title_levels||[])[0]),
      area:        p.location_metro || p.location_name || "RTP Area, NC",
      prevLocation:(prev.location?.name || prev.location?.metro || ""),
      timeline:    "30–90 days",
      signals: [
        `Joined ${exp.company?.name||p.job_company_name} — ${exp.start_date||"recently"}`,
        exp.title?.name  ? `Role: ${exp.title.name}`       : null,
        p.job_title_role ? `Function: ${p.job_title_role}` : null,
        prev.location?.name ? `From: ${prev.location.name}` : null,
      ].filter(Boolean),
      notes: `Corporate relo detected via PDL. ${emails.length?"Contact available.":"Needs skip trace."}`,
      heatScore:   null, aiInsight:  null,
      skipTraced:  emails.length > 0 || phones.length > 0,
      skipTraceMatched: emails.length > 0 || phones.length > 0,
      extId:       p.id || null,
      origin:      "pdl",
      receivedAt:  new Date().toISOString(),
    };
    if (save(lead)) { newLeads.push(lead); added++; }
  });

  console.log(`[PDL] ${data.data?.length||0} records → ${added} new leads`);
  return { fetched: data.data?.length||0, added };
}

function calcBudget(level="") {
  const l = level.toLowerCase();
  if (l.includes("vp")||l.includes("director")||l.includes("c-level")) return "$850K–1.4M";
  if (l.includes("senior")||l.includes("manager"))                       return "$500K–900K";
  return "$350K–650K";
}

// ════════════════════════════════════════════════════════════
// INTEGRATION 2 — Wake County Tax Delinquent
// Source: Wake County Tax Administration (free, no auth)
// File:   https://services.wake.gov/collection_extracts/Real_Estate_Delq853_MMDDYYYY.xlsx
// Updated daily. Delinquent accounts = distressed seller leads.
// ════════════════════════════════════════════════════════════
async function fetchWakeDelinquent() {
  // Try today and the previous 4 days (file published on business days)
  const dates = [];
  for (let i = 0; i < 5; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const mm   = String(d.getMonth()+1).padStart(2,"0");
    const dd   = String(d.getDate()).padStart(2,"0");
    const yyyy = d.getFullYear();
    dates.push(`${mm}${dd}${yyyy}`);
  }

  let rows = null;
  for (const dateStr of dates) {
    const url = `https://services.wake.gov/collection_extracts/Real_Estate_Delq853_${dateStr}.xlsx`;
    try {
      console.log(`[Wake] Trying ${url}`);
      const res = await fetch(url, {
        headers: { "User-Agent": "TLRE-LeadGen/1.0 contact@tlre.app" },
        signal: AbortSignal.timeout(20000),
      });
      if (!res.ok) { console.log(`[Wake] ${res.status} for ${dateStr}`); continue; }
      const buf  = await res.arrayBuffer();
      const wb   = XLSX.read(buf, { type: "array" });
      const ws   = wb.Sheets[wb.SheetNames[0]];
      rows       = XLSX.utils.sheet_to_json(ws, { defval: "" });
      console.log(`[Wake] Loaded ${rows.length} rows from ${dateStr}`);
      break;
    } catch (err) { console.warn(`[Wake] ${dateStr}: ${err.message}`); }
  }

  if (!rows) throw new Error("Wake County delinquent file unavailable — try again tomorrow or on a weekday");

  // The Tax Bill Layout defines columns — map common variants
  const col = (row, ...keys) => {
    for (const k of keys) {
      const v = row[k] || row[k.toUpperCase()] || row[k.toLowerCase()] || "";
      if (v !== "") return String(v).trim();
    }
    return "";
  };

  let added = 0;
  rows.forEach(row => {
    const ownerName = col(row,"OWNER_NAME","Owner Name","OWNERNAME","owner");
    if (!ownerName || ownerName.length < 3) return;

    const amtRaw  = col(row,"AMOUNT_DUE","Amount Due","AMTDUE","AMT_DUE","TAX_DUE");
    const amount  = parseFloat(amtRaw.replace(/[^0-9.]/g,"")) || 0;
    const taxYear = col(row,"TAX_YEAR","Tax Year","TAXYEAR","YEAR");
    const propAddr= col(row,"PROPERTY_ADDR","Property Address","SITUS","LOCATION_ADDR","LOC_ADDR");
    const city    = col(row,"CITY","PROP_CITY","City");
    const zip     = col(row,"ZIP","ZIP_CODE","Zip");
    const acct    = col(row,"ACCOUNT_NUM","Account","ACCT_NUM","REID");

    const lead = {
      id:          nid("wak"),
      name:        tc(ownerName),
      email:       "",
      phone:       "",
      source:      "wake_county",
      sourceLabel: "Wake County — Tax Delinquent",
      leadPhase:   "distressed_seller",
      type:        "seller",
      employer:    "",
      budget:      "TBD — needs AVM",
      area:        `${city||"Wake County"}, NC${zip?" "+zip:""}`,
      propertyStreet: propAddr,
      propertyCity:   city,
      propertyZip:    zip,
      timeline:    "30–90 days",
      signals: [
        "Tax delinquent — Wake County",
        taxYear  ? `Delinquent since: ${taxYear}`      : null,
        amount   ? `Amount owed: $${amount.toLocaleString()}` : null,
        propAddr ? `Property: ${propAddr}`             : null,
        amount > 5000 ? "High delinquency — motivated to sell" : null,
      ].filter(Boolean),
      notes: `Tax delinquent via Wake County daily file. Acct: ${acct}. ${propAddr||""}. $${amount.toLocaleString()} owed. No contact info — needs skip trace.`,
      heatScore:   null, aiInsight: null,
      skipTraced:  false, skipTraceMatched: false,
      extId:       acct || null,
      origin:      "wake_county",
      receivedAt:  new Date().toISOString(),
    };
    if (save(lead)) added++;
  });

  console.log(`[Wake] Delinquent: ${rows.length} rows → ${added} new leads`);
  return { fetched: rows.length, added };
}

// Recent sales — identify possible absentee owners / investors
// URL: https://services.wake.gov/realdata_extracts/Qualified_Sales_Past_24Months.xlsx
async function fetchWakeRecentSales(filterZips = RTP_ZIPS) {
  const URL = "https://services.wake.gov/realdata_extracts/Qualified_Sales_Past_24Months.xlsx";
  console.log("[Wake] Fetching recent sales…");
  const res = await fetch(URL, {
    headers: { "User-Agent": "TLRE-LeadGen/1.0" },
    signal: AbortSignal.timeout(30000),
  });
  if (!res.ok) throw new Error(`Wake recent sales HTTP ${res.status}`);
  const buf  = await res.arrayBuffer();
  const wb   = XLSX.read(buf, { type: "array" });
  const ws   = wb.Sheets[wb.SheetNames[0]];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: "" });

  const col = (row, ...keys) => {
    for (const k of keys) {
      const v = row[k]||row[k.toUpperCase()]||row[k.toLowerCase()]||"";
      if (v !== "") return String(v).trim();
    }
    return "";
  };

  // Filter: only RTP zips, recent (< 90 days), non-arm's-length = potential distressed
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 90);

  let added = 0;
  rows.forEach(row => {
    const zip = col(row,"ZIP","ZIPCODE","zip_code");
    if (filterZips.length && !filterZips.includes(zip)) return;

    const saleDateStr = col(row,"SALE_DATE","Sale Date","SALEDATE","DATE_OF_SALE");
    if (saleDateStr) {
      const saleDate = new Date(saleDateStr);
      if (saleDate < cutoff) return; // skip older than 90 days
    }

    const owner    = col(row,"OWNER_NAME","Owner","GRANTEE","NEW_OWNER");
    if (!owner) return;
    const price    = parseFloat(col(row,"SALE_PRICE","Price","SALEPRICE","SALE_AMT").replace(/[^0-9.]/g,"")) || 0;
    const addr     = col(row,"SITE_ADDR","Property Address","ADDRESS","LOCATION");
    const city     = col(row,"CITY","PROP_CITY");
    const acct     = col(row,"REID","ACCOUNT","ACCT");

    const lead = {
      id:          nid("wks"),
      name:        tc(owner),
      email:       "", phone: "",
      source:      "wake_county",
      sourceLabel: "Wake County — Recent Sale",
      leadPhase:   "distressed_seller",
      type:        "seller",
      employer:    "",
      budget:      price ? `Est. $${(price/1000).toFixed(0)}K` : "TBD",
      area:        `${city||"Wake County"}, NC ${zip}`,
      propertyStreet: addr, propertyCity: city, propertyZip: zip,
      timeline:    "3–12 months",
      signals: [
        "Recent property acquisition in RTP zone",
        saleDateStr ? `Sale date: ${saleDateStr}` : null,
        price ? `Sale price: $${price.toLocaleString()}` : null,
        addr ? `Property: ${addr}` : null,
        "New owner may be investor — potential future seller",
      ].filter(Boolean),
      notes: `Recent sale via Wake County qualified sales file. REID: ${acct}. New owner may be an investor or corporate buyer — potential listing lead within 12 months.`,
      heatScore:   null, aiInsight: null,
      skipTraced:  false, skipTraceMatched: false,
      extId:       acct || null,
      origin:      "wake_county",
      receivedAt:  new Date().toISOString(),
    };
    if (save(lead)) added++;
  });

  console.log(`[Wake] Recent sales: ${rows.length} rows → ${added} new leads`);
  return { fetched: rows.length, added };
}

// ════════════════════════════════════════════════════════════
// INTEGRATION 3 — NC eCourts
// Public portal: https://portal-nc.tylerhost.net/Portal/
// Full statewide since Oct 2025 (Tyler Technologies Odyssey)
// Searches for Domestic (divorce) and Special Proceedings (probate)
// RPA licensed access: contact NCAOC at nccourts.gov (~$500–2000/yr)
// ════════════════════════════════════════════════════════════
async function fetchNcEcourts(counties = ["Wake","Durham"], daysSince = 30) {
  let totalFetched = 0, added = 0;

  // NC eCourts Odyssey portal REST endpoints discovered from network inspection
  // These are the internal API calls the portal browser app makes
  for (const county of counties) {
    for (const caseCategory of ["Domestic","Estate"]) {
      try {
        const cases = await searchOdysseyPortal(county, caseCategory, daysSince);
        totalFetched += cases.length;
        cases.forEach(c => { const l = odysseyToLead(c, county); if (l && save(l)) added++; });
        await new Promise(r => setTimeout(r, 800));
      } catch (err) {
        console.warn(`[NCeCourts] ${county} ${caseCategory}: ${err.message}`);
      }
    }
  }

  console.log(`[NCeCourts] ${totalFetched} filings → ${added} new leads`);
  return { fetched: totalFetched, added };
}

async function searchOdysseyPortal(county, caseCategory, daysSince) {
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - daysSince);
  const fmt = d => `${d.getMonth()+1}/${d.getDate()}/${d.getFullYear()}`;

  // Try Tyler Tech Odyssey public search API
  // NC portal: https://portal-nc.tylerhost.net/Portal/
  const baseUrl = "https://portal-nc.tylerhost.net/Portal";

  // First get session cookies
  const initRes = await fetch(`${baseUrl}/Home/WorkspaceMode?p=0`, {
    headers: { "User-Agent": "Mozilla/5.0 TLRE-LeadGen/1.0", "Accept": "text/html" },
    signal: AbortSignal.timeout(10000),
  });

  if (!initRes.ok) throw new Error(`Portal unavailable: ${initRes.status}`);

  const cookies = initRes.headers.get("set-cookie") || "";
  const cookieStr = cookies.split(",").map(c => c.split(";")[0].trim()).filter(Boolean).join("; ");

  // Search for civil cases by county and category
  const searchBody = JSON.stringify({
    nodeDesc:     county,
    nodeId:       countyNodeId(county),
    caseCategory: caseCategory,
    startDate:    fmt(startDate),
    endDate:      fmt(new Date()),
    lastName:     "",
    firstName:    "",
  });

  const searchRes = await fetch(`${baseUrl}/Case/SearchByParty`, {
    method: "POST",
    headers: {
      "Content-Type":     "application/json",
      "Accept":           "application/json",
      "Cookie":           cookieStr,
      "X-Requested-With": "XMLHttpRequest",
      "Referer":          `${baseUrl}/`,
      "User-Agent":       "Mozilla/5.0 TLRE-LeadGen/1.0",
    },
    body: searchBody,
    signal: AbortSignal.timeout(15000),
  });

  if (!searchRes.ok) throw new Error(`Search returned ${searchRes.status}`);

  const data = await searchRes.json();
  return (data.cases || data.CaseList || data.results || []).slice(0, 30);
}

function countyNodeId(county) {
  const map = { Wake:"100", Durham:"300", Orange:"200", Chatham:"600", Johnston:"400" };
  return map[county] || "100";
}

function odysseyToLead(c, county) {
  if (!c) return null;
  const caseNum  = c.caseNumber || c.CaseNumber || c.caseNo || "";
  const parties  = (c.partyList || c.parties || [])
    .filter(p => !(p.partyType||"").match(/attorney|defendant|plaintiff|state/i))
    .map(p => tc(p.partyName || p.name || ""))
    .filter(Boolean);

  if (!parties.length && !caseNum) return null;

  const isDivorce = caseNum.match(/CVD|FV|10-D/i) ||
    (c.caseType||c.caseCategory||"").match(/domestic|divorce|family/i);
  const isProbate = caseNum.match(/-SP|-E\d/i) ||
    (c.caseType||c.caseCategory||"").match(/estate|probate|special/i);

  if (!isDivorce && !isProbate) return null;

  const label    = isDivorce ? "Divorce Filing" : "Probate / Estate";
  const filedStr = c.filedDate || c.FiledDate || c.fileDate || "";

  return {
    id:          nid("ncc"),
    name:        parties[0] || "Unknown Party",
    email:       "", phone: "",
    source:      "nccourts",
    sourceLabel: `NC eCourts — ${label}`,
    leadPhase:   "distressed_seller",
    type:        "seller",
    employer:    "",
    budget:      "TBD — needs property lookup",
    area:        `${county} County, NC`,
    propertyStreet: "", propertyCity: county, propertyZip: "",
    timeline:    isDivorce ? "3–9 months" : "3–12 months",
    signals: [
      `${label} — ${county} County`,
      caseNum    ? `Case: ${caseNum}`                    : null,
      filedStr   ? `Filed: ${filedStr}`                  : null,
      parties.length > 1 ? `Parties: ${parties.slice(0,2).join(" / ")}` : null,
      isDivorce  ? "Marital property typically must sell or transfer" : "Executor may need to liquidate estate property",
    ].filter(Boolean),
    notes: `${label} in ${county} County. Case: ${caseNum}. ${isDivorce?"Contact as neighborhood specialist, not 'divorce lead'.":"Contact executor directly."}`,
    heatScore:   null, aiInsight: null,
    skipTraced:  false, skipTraceMatched: false,
    caseNumber:  caseNum,
    extId:       caseNum || null,
    origin:      "nccourts",
    receivedAt:  new Date().toISOString(),
  };
}

// Manual import fallback for NC eCourts (CSV from RPA program or manual export)
// POST /api/nccourts/import  { cases: [...] }

// ════════════════════════════════════════════════════════════
// INTEGRATION 4 — SpeedeonData New Mover API
// Sign up: speedeondata.com  |  Cost: ~$150–300/mo
// Base URL provided by Speedeon on signup (unique per account)
// ════════════════════════════════════════════════════════════
async function fetchSpeedeonLeads(apiKey, baseUrl) {
  const cutoff = new Date();
  cutoff.setDate(cutoff.getDate() - 30);

  const res = await fetch(`${baseUrl.replace(/\/$/,"")}/v1/newmovers/search`, {
    method:  "POST",
    headers: { "Content-Type": "application/json", "x-api-key": apiKey },
    body: JSON.stringify({
      zipCodes:      RTP_ZIPS,
      moveAfterDate: cutoff.toISOString().slice(0,10),
      moveType:      "new_mover",
      includeRenters: true,
      pageSize:      100, page: 1,
      fields: ["first_name","last_name","full_name","new_address","new_city","new_state",
               "new_zip","prev_city","prev_state","move_date","email","phone",
               "estimated_income","household_size","age_range"],
    }),
    signal: AbortSignal.timeout(15000),
  });

  if (res.status === 401 || res.status === 403) throw new Error("invalid_speedeon_key");
  if (!res.ok) throw new Error(`Speedeon HTTP ${res.status}`);

  const data = await res.json();
  const records = data.records || data.data || data.results || [];
  let added = 0;

  records.forEach(r => {
    const isOOS = r.prev_state && r.prev_state.toUpperCase() !== "NC";
    const income = parseFloat((r.estimated_income||"").replace(/\D/g,"")) || 0;
    const lead = {
      id:          nid("spd"),
      name:        r.full_name || `${r.first_name||""} ${r.last_name||""}`.trim() || "New Mover",
      email:       r.email || "",
      phone:       fmtPhone(r.phone || ""),
      source:      "speedeon",
      sourceLabel: "SpeedeonData — New Mover",
      leadPhase:   "post_arrival",
      type:        "buyer",
      employer:    "",
      budget:      income ? `$${Math.round(income*3/1000)}K–$${Math.round(income*4.5/1000)}K` : "TBD",
      area:        `${r.new_city||""}, NC ${r.new_zip||""}`.trim(),
      prevLocation:`${r.prev_city||""}, ${r.prev_state||""}`.trim(),
      timeline:    "6–18 months",
      signals: [
        `Arrived ${r.move_date||"recently"} → ${r.new_city||r.new_zip}, NC`,
        isOOS ? `Relocated from: ${r.prev_city||""}, ${r.prev_state}` : "In-state move",
        income ? `Est. income: $${income.toLocaleString()}` : null,
        r.household_size ? `Household: ${r.household_size}` : null,
        "Currently renting — buyer within 6–18 months",
      ].filter(Boolean),
      notes: `New mover via SpeedeonData. ${isOOS?"Out-of-state relo.":"In-state move."} ${income?`Income ~$${income.toLocaleString()}.`:""}`,
      heatScore:   null, aiInsight: null,
      skipTraced:  !!(r.email||r.phone),
      skipTraceMatched: !!(r.email||r.phone),
      origin:      "speedeon",
      receivedAt:  new Date().toISOString(),
    };
    if (save(lead)) added++;
  });

  console.log(`[Speedeon] ${records.length} records → ${added} new leads`);
  return { fetched: records.length, added };
}

// ════════════════════════════════════════════════════════════
// ZAPIER WEBHOOKS
// Wire each lead source to: POST /webhook/zapier?source=SOURCE
// Source values: zillow | opcity | boldleads | redx | generic
// ════════════════════════════════════════════════════════════
const WEBHOOK_SOURCES = {
  zillow:    { label: "Zillow Premier Agent",  type: "paid", leadPhase: "active_buyer" },
  opcity:    { label: "Realtor.com / Opcity",  type: "paid", leadPhase: "active_buyer" },
  boldleads: { label: "BoldLeads",             type: "paid", leadPhase: "active_buyer" },
  redx:      { label: "REDX",                  type: "paid", leadPhase: "active_seller"},
  generic:   { label: "Zapier Webhook",         type: "paid", leadPhase: "active_buyer" },
};

function pick(raw, ...keys) {
  for (const k of keys) if (raw[k]) return String(raw[k]).trim();
  return "";
}

function normalizeWebhook(raw, sourceKey) {
  const meta    = WEBHOOK_SOURCES[sourceKey] || WEBHOOK_SOURCES.generic;
  const name    = pick(raw,"name","full_name","buyer_name","contact_name") ||
    [pick(raw,"first_name","firstName"), pick(raw,"last_name","lastName")].filter(Boolean).join(" ") || "Unknown";
  const notes   = pick(raw,"message","notes","comment","description","inquiry");
  const budget  = pick(raw,"price","price_range","budget","home_price","max_price");
  const timeline= pick(raw,"move_in_timeframe","timeline","timeframe","when");
  const signals = [`Inbound via ${meta.label}`];
  if (notes)    signals.push(`Message: "${notes.slice(0,80)}${notes.length>80?"…":""}"`);
  if (budget)   signals.push(`Budget: ${budget}`);
  if (timeline) signals.push(`Timeline: ${timeline}`);
  if (pick(raw,"pre_approved") === "true") signals.push("Claims pre-approved");

  return {
    id:          nid("zap"),
    name,
    email:       pick(raw,"email","Email","buyer_email","contact_email"),
    phone:       fmtPhone(pick(raw,"phone","Phone","buyer_phone","mobile","phone_number")),
    source:      sourceKey,
    sourceLabel: meta.label,
    leadPhase:   meta.leadPhase,
    type:        sourceKey === "redx" ? "seller" : "buyer",
    employer:    "",
    budget:      budget || "TBD",
    area:        pick(raw,"address","property_address","city","location","zip","area") || "RTP Area, NC",
    propertyStreet: pick(raw,"address","street","property_address"),
    propertyCity:   pick(raw,"city","property_city"),
    propertyZip:    pick(raw,"zip","postal_code","property_zip"),
    timeline:    timeline || "Unknown",
    signals, notes,
    heatScore:   null, aiInsight:  null,
    skipTraced:  false, skipTraceMatched: false,
    origin:      "webhook",
    receivedAt:  new Date().toISOString(),
  };
}

// ════════════════════════════════════════════════════════════
// SKIP TRACING — BatchSkipTracing
// https://batchskiptracing.com  |  ~$0.10–0.15/match
// ════════════════════════════════════════════════════════════
function parseName(full="") {
  const parts = full.replace(/[^a-zA-Z\s]/g,"").trim().split(/\s+/);
  if (!parts.length) return { firstName:"", lastName:"" };
  return parts.length === 1
    ? { firstName: parts[0], lastName: "" }
    : { firstName: parts[1]||"", lastName: parts[0]||"" };
}

async function skipTraceOne(lead, bstKey) {
  const name = parseName(lead.name);
  const res  = await fetch("https://api.batchskiptracing.com/api/batch", {
    method:  "POST",
    headers: { "Content-Type": "application/json", "x-api-key": bstKey },
    body: JSON.stringify([{
      first_name:       name.firstName,
      last_name:        name.lastName,
      property_address: lead.propertyStreet || "",
      property_city:    lead.propertyCity   || "",
      property_state:   "NC",
      property_zip:     lead.propertyZip    || "",
    }]),
    signal: AbortSignal.timeout(15000),
  });
  if (!res.ok) throw new Error(`BST HTTP ${res.status}`);
  const data = await res.json();
  const out  = (data.output||data.data||[])[0]?.output || {};
  return {
    phones: (out.phones||[]).map(p=>fmtPhone(p.phone||p.phoneNumber||"")).filter(Boolean).slice(0,3),
    emails: (out.emails||[]).map(e=>(e.email||e).toLowerCase()).filter(e=>e.includes("@")).slice(0,2),
  };
}

async function autoSkipTrace(leadList, bstKey) {
  const BATCH = 8;
  let matched = 0;
  for (let i = 0; i < leadList.length; i += BATCH) {
    for (const lead of leadList.slice(i, i+BATCH)) {
      try {
        const r = await skipTraceOne(lead, bstKey);
        const stored = leads.get(lead.id);
        if (!stored) continue;
        Object.assign(stored, {
          phone: r.phones[0]||stored.phone, email: r.emails[0]||stored.email,
          allPhones: r.phones, allEmails: r.emails,
          skipTraced: true, skipTraceMatched: r.phones.length>0||r.emails.length>0,
          skipTracedAt: new Date().toISOString(),
        });
        if (r.phones.length||r.emails.length) matched++;
      } catch (_) {}
    }
    if (i+BATCH < leadList.length) await new Promise(r=>setTimeout(r,600));
  }
  console.log(`[BST] Batch done: ${matched}/${leadList.length} matched`);
}

// ════════════════════════════════════════════════════════════
// ROUTES
// ════════════════════════════════════════════════════════════

app.get("/health", (req, res) => res.json({
  ok: true, leads: leads.size, uptime: Math.round(process.uptime()),
  keys: {
    pdl:      !!process.env.PDL_API_KEY,
    speedeon: !!process.env.SPEEDEON_API_KEY,
    bst:      !!process.env.BST_API_KEY,
    wake:     "free — no key",
    ncCourts: "free — no key",
  },
}));

app.get("/api/leads", (req, res) => {
  let all = [...leads.values()];
  if (req.query.phase)  all = all.filter(l => l.leadPhase === req.query.phase);
  if (req.query.source) all = all.filter(l => l.source   === req.query.source);
  all.sort((a,b) => new Date(b.receivedAt) - new Date(a.receivedAt));
  res.json({ leads: all, count: all.length, total: leads.size });
});

app.put("/api/leads/:id", (req, res) => {
  const l = leads.get(req.params.id);
  if (!l) return res.status(404).json({ error: "Not found" });
  leads.set(req.params.id, { ...l, ...req.body });
  res.json({ ok: true });
});

app.delete("/api/leads", (req, res) => {
  if (req.query.source) {
    for (const [id,l] of leads) if (l.source===req.query.source) leads.delete(id);
  } else { leads.clear(); }
  res.json({ ok: true });
});

// ── PDL ───────────────────────────────────────────────────────
app.post("/api/pdl/fetch", async (req, res) => {
  const apiKey = getKey(req, "PDL_API_KEY");
  if (!apiKey) return res.status(400).json({ error: "PDL_API_KEY required" });
  const bstKey = getKey(req, "BST_API_KEY");
  try {
    const result = await fetchPdlLeads(apiKey, req.body.daysSince||90, req.body.size||25);
    if (bstKey && result.added) {
      const nl = [...leads.values()].filter(l=>l.origin==="pdl"&&!l.skipTraced).slice(0,50);
      if (nl.length) setImmediate(()=>autoSkipTrace(nl,bstKey));
    }
    res.json({ ok:true, ...result });
  } catch (err) {
    console.error("[PDL]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Wake County ───────────────────────────────────────────────
app.post("/api/wake/delinquent", async (req, res) => {
  const bstKey = getKey(req, "BST_API_KEY");
  try {
    const result = await fetchWakeDelinquent();
    if (bstKey && result.added) {
      const nl = [...leads.values()].filter(l=>l.origin==="wake_county"&&!l.skipTraced).slice(0,200);
      if (nl.length) setImmediate(()=>autoSkipTrace(nl,bstKey));
    }
    res.json({ ok:true, ...result });
  } catch (err) {
    console.error("[Wake]", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/wake/sales", async (req, res) => {
  try {
    const result = await fetchWakeRecentSales(req.body.zips||RTP_ZIPS);
    res.json({ ok:true, ...result });
  } catch (err) {
    console.error("[Wake Sales]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── NC eCourts ────────────────────────────────────────────────
app.post("/api/nccourts/fetch", async (req, res) => {
  const counties  = req.body.counties  || ["Wake","Durham"];
  const daysSince = req.body.daysSince || 30;
  try {
    const result = await fetchNcEcourts(counties, daysSince);
    res.json({ ok:true, ...result });
  } catch (err) {
    console.error("[NCeCourts]", err.message);
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/nccourts/import", (req, res) => {
  const { cases } = req.body;
  if (!Array.isArray(cases)) return res.status(400).json({ error: "Expected { cases: [...] }" });
  let added = 0;
  cases.forEach(c => { const l = odysseyToLead(c, c.county||"Wake"); if (l&&save(l)) added++; });
  res.json({ ok:true, imported: cases.length, added });
});

// ── SpeedeonData ──────────────────────────────────────────────
app.post("/api/speedeon/fetch", async (req, res) => {
  const apiKey = getKey(req, "SPEEDEON_API_KEY");
  const baseUrl= req.body.baseUrl || process.env.SPEEDEON_BASE_URL;
  if (!apiKey)  return res.status(400).json({ error: "SPEEDEON_API_KEY required" });
  if (!baseUrl) return res.status(400).json({ error: "SPEEDEON_BASE_URL required — from your Speedeon onboarding email" });
  try {
    const result = await fetchSpeedeonLeads(apiKey, baseUrl);
    res.json({ ok:true, ...result });
  } catch (err) {
    console.error("[Speedeon]", err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── Zapier webhook ────────────────────────────────────────────
app.post("/webhook/zapier", (req, res) => {
  const src  = (req.query.source||"generic").toLowerCase();
  const body = req.body;
  if (!body || !Object.keys(body).length) return res.status(400).json({ error: "Empty payload" });
  const lead = normalizeWebhook(body, src);
  if (!save(lead)) return res.json({ ok:true, duplicate:true });
  console.log(`[Webhook][${src}] ${lead.name} | ${lead.email||lead.phone||"no contact"}`);
  res.json({ ok:true, id: lead.id });
});

app.post("/webhook/zapier/test", (req, res) => {
  const src = (req.query.source||"generic").toLowerCase();
  res.json({ ok:true, normalised: normalizeWebhook(req.body, src), raw: req.body });
});

// ── Skip trace ────────────────────────────────────────────────
app.post("/api/leads/:id/skiptrace", async (req, res) => {
  const lead   = leads.get(req.params.id);
  if (!lead) return res.status(404).json({ error: "Not found" });
  const bstKey = getKey(req, "BST_API_KEY");
  if (!bstKey) return res.status(400).json({ error: "BST_API_KEY required" });
  try {
    const r = await skipTraceOne(lead, bstKey);
    leads.set(lead.id, {
      ...lead,
      phone: r.phones[0]||lead.phone, email: r.emails[0]||lead.email,
      allPhones: r.phones, allEmails: r.emails,
      skipTraced: true, skipTraceMatched: r.phones.length>0||r.emails.length>0,
      skipTracedAt: new Date().toISOString(),
    });
    res.json({ ok:true, matched: r.phones.length>0||r.emails.length>0, phones: r.phones, emails: r.emails });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

app.post("/api/leads/skiptrace/batch", async (req, res) => {
  const bstKey = getKey(req, "BST_API_KEY");
  if (!bstKey) return res.status(400).json({ error: "BST_API_KEY required" });
  let untraced = [...leads.values()].filter(l=>!l.skipTraced&&!l.phone&&!l.email);
  if (req.body.source) untraced = untraced.filter(l=>l.source===req.body.source);
  if (!untraced.length) return res.json({ ok:true, queued:0 });
  setImmediate(()=>autoSkipTrace(untraced,bstKey));
  res.json({ ok:true, queued: untraced.length });
});

// ── Start ─────────────────────────────────────────────────────
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => {
  console.log(`\n  TLRE Lead Server  |  port ${PORT}`);
  console.log(`  PDL:      ${process.env.PDL_API_KEY?"✓ set":"✗ not set"}`);
  console.log(`  Speedeon: ${process.env.SPEEDEON_API_KEY?"✓ set":"✗ not set"}`);
  console.log(`  BST:      ${process.env.BST_API_KEY?"✓ set (auto skip-trace on)":"✗ not set"}`);
  console.log(`  Wake County + NC eCourts: free, no key needed\n`);
  console.log(`  Test: GET http://localhost:${PORT}/health\n`);
});
