/**
 * CMA case ingestion crawler.
 *
 * Scrapes gov.uk/cma-cases — competition enforcement decisions, merger control,
 * and market investigations — and populates the SQLite database.
 *
 * Usage:
 *   npx tsx scripts/ingest-cma.ts                     # full crawl
 *   npx tsx scripts/ingest-cma.ts --resume             # skip already-ingested cases
 *   npx tsx scripts/ingest-cma.ts --dry-run            # parse + log, no DB writes
 *   npx tsx scripts/ingest-cma.ts --force              # wipe DB first, then crawl
 *   npx tsx scripts/ingest-cma.ts --max-pages 3        # limit listing pages per case type
 *   npx tsx scripts/ingest-cma.ts --case-type mergers  # only crawl one case type
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync, writeFileSync, readFileSync } from "node:fs";
import { dirname } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["CMA_DB_PATH"] ?? "data/cma.db";
const BASE_URL = "https://www.gov.uk";
const LISTING_URL = `${BASE_URL}/cma-cases`;
const USER_AGENT =
  "AnsvarCMAIngester/1.0 (+https://ansvar.eu; compliance research crawler)";

/** Minimum delay between HTTP requests (ms). */
const RATE_LIMIT_MS = 1500;

/** Maximum retries per request. */
const MAX_RETRIES = 3;

/** Back-off multiplier (ms) after a failed request. */
const RETRY_BACKOFF_MS = 3000;

/** Request timeout (ms). */
const REQUEST_TIMEOUT_MS = 30_000;

/** File to persist resume state between runs. */
const RESUME_STATE_FILE = "data/.ingest-resume-state.json";

// Case type filter values as they appear in the gov.uk query string.
const CASE_TYPE_FILTERS: Record<string, string> = {
  "ca98":     "ca98-and-civil-cartels",
  "mergers":  "mergers",
  "markets":  "markets",
  "consumer": "consumer-enforcement",
  "criminal": "criminal-cartels",
  "regulatory": "regulatory-references-and-appeals",
  "competition-disqualification": "competition-disqualification",
  "dmu":      "digital-markets-unit",
  "sau":      "sau-referral",
};

// Map gov.uk case_type labels → our DB `type` values.
const TYPE_MAP: Record<string, string> = {
  "ca98 and civil cartels":          "ca98",
  "mergers":                         "merger",
  "markets":                         "market_investigation",
  "consumer enforcement":            "consumer_enforcement",
  "criminal cartels":                "criminal_cartel",
  "regulatory references and appeals": "regulatory",
  "competition disqualification":    "competition_disqualification",
  "digital markets unit":            "dmu",
  "sau referral":                    "sau_referral",
};

// Map gov.uk outcome labels → normalised outcome values.
const OUTCOME_MAP: Record<string, string> = {
  // Mergers
  "mergers - phase 1 clearance":                        "cleared_phase1",
  "mergers - phase 1 found not to qualify":             "not_qualifying",
  "mergers - phase 1 referral":                         "referred_phase2",
  "mergers - phase 2 clearance":                        "cleared_phase2",
  "mergers - phase 2 clearance with remedies":          "cleared_with_conditions",
  "mergers - phase 2 prohibition":                      "prohibited",
  "mergers - phase 1 clearance with undertakings in lieu": "cleared_with_conditions",
  // CA98
  "ca98 - infringement chapter i":                      "infringement",
  "ca98 - infringement chapter ii":                     "infringement",
  "ca98 - infringement chapter i and chapter ii":       "infringement",
  "ca98 - no grounds for action":                       "no_action",
  "ca98 - commitments":                                 "cleared_with_conditions",
  "ca98 - administrative priorities":                   "administrative_priorities",
  // Markets
  "phase 1 recommendations to government":              "recommendations",
  "phase 2 adverse effect on competition leading to remedies": "remedies",
  "phase 1 no reference":                               "no_reference",
};

// Map gov.uk sector labels → normalised sector IDs.
const SECTOR_MAP: Record<string, string> = {
  "energy":                           "energy",
  "financial services":               "financial_services",
  "healthcare and medical equipment": "healthcare",
  "pharmaceuticals":                  "pharmaceuticals",
  "communications":                   "communications",
  "telecommunications":               "telecommunications",
  "electronics":                      "electronics",
  "transport":                        "transport",
  "building and construction":        "building_construction",
  "retail and wholesale":             "retail",
  "food manufacturing":               "food_manufacturing",
  "recreation and leisure":           "recreation_leisure",
  "motor industry":                   "motor_industry",
  "chemicals":                        "chemicals",
  "distribution and service industries": "distribution_services",
  "clothing, footwear and fashion":   "clothing_fashion",
  "utilities":                        "utilities",
  "public markets":                   "public_markets",
  "agriculture, environment and natural resources": "agriculture_environment",
  "mining":                           "mining",
  "aerospace":                        "aerospace",
  "fire, police and security":        "fire_police_security",
  "defence":                          "defence",
  "oil and gas refining and petrochemicals": "oil_gas",
  "paper, printing and packaging":    "paper_printing",
  "not applicable":                   "other",
};

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME   = args.includes("--resume");
const FLAG_DRY_RUN  = args.includes("--dry-run");
const FLAG_FORCE    = args.includes("--force");

function flagValue(name: string): string | undefined {
  const idx = args.indexOf(name);
  if (idx === -1 || idx + 1 >= args.length) return undefined;
  return args[idx + 1];
}

const MAX_PAGES  = Number(flagValue("--max-pages")) || Infinity;
const ONLY_TYPE  = flagValue("--case-type") ?? undefined;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<Response> {
  const now = Date.now();
  const wait = RATE_LIMIT_MS - (now - lastRequestTime);
  if (wait > 0) {
    await sleep(wait);
  }
  lastRequestTime = Date.now();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);

  try {
    const res = await fetch(url, {
      headers: { "User-Agent": USER_AGENT },
      signal: controller.signal,
    });
    return res;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchWithRetry(url: string): Promise<string> {
  let lastError: Error | null = null;
  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      const res = await rateLimitedFetch(url);
      if (res.status === 429) {
        const retryAfter = Number(res.headers.get("retry-after") || "10");
        log(`  Rate-limited (429). Waiting ${retryAfter}s before retry…`);
        await sleep(retryAfter * 1000);
        continue;
      }
      if (!res.ok) {
        throw new Error(`HTTP ${res.status} for ${url}`);
      }
      return await res.text();
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(String(err));
      if (attempt < MAX_RETRIES) {
        const backoff = RETRY_BACKOFF_MS * attempt;
        log(`  Attempt ${attempt} failed: ${lastError.message}. Retrying in ${backoff}ms…`);
        await sleep(backoff);
      }
    }
  }
  throw lastError ?? new Error(`Failed to fetch ${url}`);
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19);
  console.log(`[${ts}] ${msg}`);
}

function logError(msg: string): void {
  const ts = new Date().toISOString().slice(0, 19);
  console.error(`[${ts}] ERROR: ${msg}`);
}

// ---------------------------------------------------------------------------
// Resume state
// ---------------------------------------------------------------------------

interface ResumeState {
  ingestedSlugs: string[];
}

function loadResumeState(): Set<string> {
  if (!FLAG_RESUME) return new Set();
  try {
    if (existsSync(RESUME_STATE_FILE)) {
      const data = JSON.parse(readFileSync(RESUME_STATE_FILE, "utf-8")) as ResumeState;
      log(`Loaded resume state: ${data.ingestedSlugs.length} previously ingested slugs`);
      return new Set(data.ingestedSlugs);
    }
  } catch {
    log("Could not load resume state — starting fresh");
  }
  return new Set();
}

function saveResumeState(slugs: Set<string>): void {
  const dir = dirname(RESUME_STATE_FILE);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const state: ResumeState = { ingestedSlugs: [...slugs] };
  writeFileSync(RESUME_STATE_FILE, JSON.stringify(state, null, 2));
}

// ---------------------------------------------------------------------------
// Parsing: listing pages
// ---------------------------------------------------------------------------

interface CaseListEntry {
  /** URL slug, e.g. "investigation-into-amazons-marketplace" */
  slug: string;
  title: string;
  caseType: string | null;
  state: string | null;
  sector: string | null;
  outcome: string | null;
  openedDate: string | null;
  closedDate: string | null;
}

function parseListingPage(html: string): CaseListEntry[] {
  const $ = cheerio.load(html);
  const entries: CaseListEntry[] = [];

  // gov.uk renders case entries as <li> inside a results list.
  // Each entry has a link and metadata items.
  // We look for links matching /cma-cases/<slug>.
  $("a[href^='/cma-cases/']").each((_i, el) => {
    const href = $(el).attr("href");
    if (!href || href === "/cma-cases" || href === "/cma-cases/") return;

    const slug = href.replace(/^\/cma-cases\//, "").replace(/\/$/, "");
    if (!slug) return;

    const title = $(el).text().trim();
    if (!title) return;

    // Walk upward to the containing <li> to extract metadata.
    const container = $(el).closest("li");
    const metaText = container.text();

    entries.push({
      slug,
      title,
      caseType: extractMeta(metaText, "Case type"),
      state: extractMeta(metaText, "Case state"),
      sector: extractMeta(metaText, "Market sector"),
      outcome: extractMeta(metaText, "Outcome"),
      openedDate: parseGovDate(extractMeta(metaText, "Opened")),
      closedDate: parseGovDate(extractMeta(metaText, "Closed")),
    });
  });

  return entries;
}

/** Extract a metadata value from the text block: "Case type: Mergers" → "Mergers". */
function extractMeta(text: string, label: string): string | null {
  const regex = new RegExp(`${label}:\\s*(.+?)(?:\\n|$)`, "i");
  const match = text.match(regex);
  return match?.[1]?.trim() ?? null;
}

/** Parse "27 January 2026" → "2026-01-27". Returns null on failure. */
function parseGovDate(dateStr: string | null): string | null {
  if (!dateStr) return null;
  const cleaned = dateStr.trim();
  const parsed = new Date(cleaned);
  if (isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
}

/** Check if there is a next page link, return its page number or null. */
function findNextPage(html: string): number | null {
  const $ = cheerio.load(html);
  // gov.uk pagination uses "Next page" text in links.
  const nextLink = $("a").filter((_i, el) => {
    const text = $(el).text().toLowerCase();
    return text.includes("next page") || text.includes("next");
  });

  if (nextLink.length === 0) return null;

  const href = nextLink.first().attr("href");
  if (!href) return null;

  const pageMatch = href.match(/[?&]page=(\d+)/);
  if (pageMatch?.[1]) return Number(pageMatch[1]);

  // Sometimes the text itself contains the page number: "Next page: 2 of 40"
  const textMatch = nextLink.first().text().match(/(\d+)\s+of\s+\d+/);
  if (textMatch?.[1]) return Number(textMatch[1]);

  return null;
}

// ---------------------------------------------------------------------------
// Parsing: individual case pages
// ---------------------------------------------------------------------------

interface CaseDetail {
  /** Full body text extracted from the case page. */
  bodyText: string;
  /** Summary — first meaningful paragraph. */
  summary: string | null;
  /** Parties mentioned (best-effort extraction). */
  parties: string[];
  /** Case reference number if found on page. */
  caseRef: string | null;
  /** Fine amount if mentioned. */
  fineAmount: number | null;
  /** Acquiring party (mergers). */
  acquiringParty: string | null;
  /** Target (mergers). */
  target: string | null;
  /** Turnover figure (mergers). */
  turnover: number | null;
  /** Relevant legislation articles. */
  articles: string[];
}

function parseCasePage(html: string, listEntry: CaseListEntry): CaseDetail {
  const $ = cheerio.load(html);

  // Remove navigation, breadcrumbs, footers, related links to isolate body.
  $("nav, footer, .gem-c-breadcrumbs, .related-content, .app-c-back-to-top, script, style, .gem-c-print-link").remove();

  // The main content is inside a .govspeak or article element.
  const mainContent = $(".govspeak, article, .publication-html, #contents, main").first();
  const contentRoot = mainContent.length > 0 ? mainContent : $("body");

  // Extract section headings and their paragraphs.
  const bodyParagraphs: string[] = [];
  contentRoot.find("p, h2, h3, li").each((_i, el) => {
    const text = $(el).text().trim();
    if (text && text.length > 5) {
      bodyParagraphs.push(text);
    }
  });

  const bodyText = bodyParagraphs.join("\n\n");

  // Summary: first substantial paragraph (>80 chars).
  const summary = bodyParagraphs.find((p) => p.length > 80) ?? null;

  // Case reference number: look for patterns like ME/1234/56, CE-1234/56, 50123, CC/01/23.
  const caseRef = extractCaseRef(bodyText, listEntry.title);

  // Fine amount: look for "£X million" or "£X,XXX" patterns.
  const fineAmount = extractFineAmount(bodyText);

  // Parties: extract from title (common pattern: "X / Y merger inquiry").
  const { acquiringParty, target, parties } = extractParties(listEntry.title, listEntry.caseType);

  // Turnover: look for turnover/revenue mentions.
  const turnover = extractTurnover(bodyText);

  // Legislation articles: look for Competition Act, Enterprise Act references.
  const articles = extractArticles(bodyText);

  return {
    bodyText,
    summary,
    parties,
    caseRef,
    fineAmount,
    acquiringParty,
    target,
    turnover,
    articles,
  };
}

function extractCaseRef(text: string, title: string): string | null {
  // Try explicit patterns: ME/NNNN/NN, CE-NNNN/NN, CC/NN/NN, MR-NN/NN, 50NNN, 51NNN
  const patterns = [
    /\b(ME\/\d{4,5}\/\d{2})\b/,
    /\b(CE[\-\/]\d{4,5}\/\d{2})\b/,
    /\b(CC\/\d{2,4}\/\d{2})\b/,
    /\b(MR[\-\/]\d{2,4}\/\d{2})\b/,
    /\b(5[01]\d{3}(?:[-\/]\d+)?)\b/,
  ];
  for (const p of patterns) {
    const match = text.match(p) ?? title.match(p);
    if (match?.[1]) return match[1];
  }
  return null;
}

function extractFineAmount(text: string): number | null {
  // "£84.2 million" → 84200000
  const millionMatch = text.match(/£([\d,.]+)\s*million/i);
  if (millionMatch?.[1]) {
    const num = parseFloat(millionMatch[1].replace(/,/g, ""));
    if (!isNaN(num)) return num * 1_000_000;
  }
  // "£1.2 billion" → 1200000000
  const billionMatch = text.match(/£([\d,.]+)\s*billion/i);
  if (billionMatch?.[1]) {
    const num = parseFloat(billionMatch[1].replace(/,/g, ""));
    if (!isNaN(num)) return num * 1_000_000_000;
  }
  // "penalty of £123,456"
  const rawMatch = text.match(/(?:penalty|fine|penalt(?:y|ies))\s+(?:of\s+)?£([\d,]+(?:\.\d+)?)/i);
  if (rawMatch?.[1]) {
    const num = parseFloat(rawMatch[1].replace(/,/g, ""));
    if (!isNaN(num)) return num;
  }
  return null;
}

function extractParties(
  title: string,
  caseType: string | null,
): { acquiringParty: string | null; target: string | null; parties: string[] } {
  // Merger titles follow "X / Y merger inquiry" or "X / Y" pattern.
  const isMerger = caseType?.toLowerCase().includes("merger");

  // Try splitting on " / " (the gov.uk convention for party separation in titles).
  const slashParts = title.split(/\s+\/\s+/);
  if (slashParts.length >= 2) {
    const acquiring = slashParts[0]!.trim();
    // Target may have " merger inquiry" suffix — strip it.
    const targetRaw = slashParts.slice(1).join(" / ").trim();
    const targetClean = targetRaw
      .replace(/\s+merger\s+inquir(?:y|ies)$/i, "")
      .replace(/\s+merger$/i, "")
      .trim();

    return {
      acquiringParty: isMerger ? acquiring : null,
      target: isMerger ? targetClean : null,
      parties: [acquiring, targetClean],
    };
  }

  return { acquiringParty: null, target: null, parties: [] };
}

function extractTurnover(text: string): number | null {
  const turnoverMatch = text.match(
    /(?:turnover|revenue|combined\s+(?:annual\s+)?turnover)\s+(?:of\s+)?(?:approximately\s+)?£([\d,.]+)\s*(million|billion)?/i,
  );
  if (!turnoverMatch?.[1]) return null;
  const num = parseFloat(turnoverMatch[1].replace(/,/g, ""));
  if (isNaN(num)) return null;
  const unit = turnoverMatch[2]?.toLowerCase();
  if (unit === "billion") return num * 1_000_000_000;
  if (unit === "million") return num * 1_000_000;
  return num;
}

function extractArticles(text: string): string[] {
  const articles: Set<string> = new Set();
  if (/Competition\s+Act\s+1998/i.test(text) || /\bCA98\b/.test(text) || /\bCA\s*1998\b/i.test(text)) {
    if (/Chapter\s+I/i.test(text))  articles.add("CA98 Chapter I");
    if (/Chapter\s+II/i.test(text)) articles.add("CA98 Chapter II");
    if (articles.size === 0) articles.add("CA98");
  }
  if (/Enterprise\s+Act\s+2002/i.test(text)) {
    if (/Part\s+3/i.test(text) || /merger/i.test(text)) articles.add("Enterprise Act 2002 Part 3");
    else if (/Part\s+4/i.test(text) || /market\s+investigation/i.test(text)) articles.add("Enterprise Act 2002 Part 4");
    else articles.add("Enterprise Act 2002");
  }
  if (/Article\s+101\s+TFEU/i.test(text)) articles.add("Article 101 TFEU");
  if (/Article\s+102\s+TFEU/i.test(text)) articles.add("Article 102 TFEU");
  if (/Consumer\s+Rights\s+Act\s+2015/i.test(text)) articles.add("Consumer Rights Act 2015");
  if (/Digital\s+Markets,?\s+Competition\s+and\s+Consumer(?:s)?\s+Act\s+2024/i.test(text)) {
    articles.add("DMCC Act 2024");
  }
  return [...articles];
}

// ---------------------------------------------------------------------------
// Normalisation helpers
// ---------------------------------------------------------------------------

function normaliseCaseType(raw: string | null): string | null {
  if (!raw) return null;
  return TYPE_MAP[raw.toLowerCase()] ?? raw.toLowerCase().replace(/\s+/g, "_");
}

function normaliseOutcome(raw: string | null): string | null {
  if (!raw) return null;
  return OUTCOME_MAP[raw.toLowerCase()] ?? raw.toLowerCase().replace(/\s+/g, "_");
}

function normaliseSector(raw: string | null): string {
  if (!raw) return "other";
  // gov.uk sometimes appends "(+ N other)" — strip it.
  const cleaned = raw.replace(/\s*\(.*?\)\s*$/, "").trim();
  return SECTOR_MAP[cleaned.toLowerCase()] ?? cleaned.toLowerCase().replace(/[\s,]+/g, "_");
}

function generateCaseNumber(slug: string, listEntry: CaseListEntry, detail: CaseDetail): string {
  // Prefer explicit case reference found in the page text.
  if (detail.caseRef) return detail.caseRef;

  // Generate a deterministic case number from the slug.
  // Format: CMA/<slug-hash> — keeps it unique and reproducible.
  const prefix = listEntry.caseType?.toLowerCase().includes("merger") ? "ME" : "CMA";
  // Use a short hash of the slug for uniqueness.
  const hash = simpleHash(slug);
  return `${prefix}/${hash}`;
}

/** Simple deterministic hash from a string → 6-char hex. */
function simpleHash(input: string): string {
  let h = 0;
  for (let i = 0; i < input.length; i++) {
    h = ((h << 5) - h + input.charCodeAt(i)) | 0;
  }
  return Math.abs(h).toString(16).padStart(6, "0").slice(0, 6).toUpperCase();
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    log(`Created data directory: ${dir}`);
  }

  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  log(`Database ready at ${DB_PATH}`);
  return db;
}

interface DbStatements {
  insertDecision: Database.Statement;
  insertMerger: Database.Statement;
  upsertSector: Database.Statement;
  checkDecision: Database.Statement;
  checkMerger: Database.Statement;
}

function prepareStatements(db: Database.Database): DbStatements {
  return {
    insertDecision: db.prepare(`
      INSERT OR IGNORE INTO decisions
        (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, ca98_articles, status)
      VALUES
        (@case_number, @title, @date, @type, @sector, @parties, @summary, @full_text, @outcome, @fine_amount, @ca98_articles, @status)
    `),
    insertMerger: db.prepare(`
      INSERT OR IGNORE INTO mergers
        (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
      VALUES
        (@case_number, @title, @date, @sector, @acquiring_party, @target, @summary, @full_text, @outcome, @turnover)
    `),
    upsertSector: db.prepare(`
      INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
      VALUES (@id, @name, @name_en, @description, @decision_count, @merger_count)
      ON CONFLICT(id) DO UPDATE SET
        decision_count = decision_count + @decision_count,
        merger_count = merger_count + @merger_count
    `),
    checkDecision: db.prepare("SELECT 1 FROM decisions WHERE case_number = ?"),
    checkMerger: db.prepare("SELECT 1 FROM mergers WHERE case_number = ?"),
  };
}

// ---------------------------------------------------------------------------
// Main crawl logic
// ---------------------------------------------------------------------------

interface CrawlStats {
  pagesScanned: number;
  casesFound: number;
  casesSkipped: number;
  decisionsInserted: number;
  mergersInserted: number;
  sectorsUpserted: number;
  errors: number;
}

async function crawlListingPages(
  caseTypeKey: string,
  caseTypeFilter: string,
): Promise<CaseListEntry[]> {
  const allEntries: CaseListEntry[] = [];
  let page = 1;

  while (page <= MAX_PAGES) {
    const url = `${LISTING_URL}?case_type%5B%5D=${caseTypeFilter}&page=${page}`;
    log(`Fetching listing page ${page} for ${caseTypeKey}: ${url}`);

    let html: string;
    try {
      html = await fetchWithRetry(url);
    } catch (err) {
      logError(`Failed to fetch listing page ${page}: ${err instanceof Error ? err.message : err}`);
      break;
    }

    const entries = parseListingPage(html);
    if (entries.length === 0) {
      log(`  No entries found on page ${page} — stopping pagination`);
      break;
    }

    log(`  Found ${entries.length} entries on page ${page}`);
    allEntries.push(...entries);

    const nextPage = findNextPage(html);
    if (nextPage === null || nextPage <= page) {
      log(`  No next page — reached end of listings`);
      break;
    }

    page = nextPage;
  }

  return allEntries;
}

async function ingestCase(
  entry: CaseListEntry,
  db: Database.Database | null,
  stmts: DbStatements | null,
  stats: CrawlStats,
  seenSectors: Map<string, { decisions: number; mergers: number }>,
): Promise<void> {
  const caseUrl = `${BASE_URL}/cma-cases/${entry.slug}`;
  log(`  Fetching case: ${entry.title}`);

  let html: string;
  try {
    html = await fetchWithRetry(caseUrl);
  } catch (err) {
    logError(`  Failed to fetch case ${entry.slug}: ${err instanceof Error ? err.message : err}`);
    stats.errors++;
    return;
  }

  const detail = parseCasePage(html, entry);

  if (!detail.bodyText || detail.bodyText.length < 20) {
    log(`  Skipping ${entry.slug}: no meaningful content extracted`);
    stats.casesSkipped++;
    return;
  }

  const caseNumber = generateCaseNumber(entry.slug, entry, detail);
  const caseType = normaliseCaseType(entry.caseType);
  const outcome = normaliseOutcome(entry.outcome);
  const sectorId = normaliseSector(entry.sector);
  const date = entry.closedDate ?? entry.openedDate;
  const status = entry.state?.toLowerCase() === "closed" ? "final" : "open";
  const isMerger = caseType === "merger";

  // Track sector counts.
  const sectorCounts = seenSectors.get(sectorId) ?? { decisions: 0, mergers: 0 };
  if (isMerger) sectorCounts.mergers++;
  else sectorCounts.decisions++;
  seenSectors.set(sectorId, sectorCounts);

  if (FLAG_DRY_RUN) {
    log(`  [DRY RUN] Would insert ${isMerger ? "merger" : "decision"}: ${caseNumber} — ${entry.title}`);
    if (isMerger) stats.mergersInserted++;
    else stats.decisionsInserted++;
    return;
  }

  if (!db || !stmts) return;

  try {
    if (isMerger) {
      // Check for existing record (resume support).
      if (stmts.checkMerger.get(caseNumber)) {
        log(`  Already exists: merger ${caseNumber} — skipping`);
        stats.casesSkipped++;
        return;
      }

      stmts.insertMerger.run({
        case_number: caseNumber,
        title: entry.title.replace(/\s+merger\s+inquir(?:y|ies)$/i, "").trim(),
        date,
        sector: sectorId,
        acquiring_party: detail.acquiringParty,
        target: detail.target,
        summary: detail.summary,
        full_text: detail.bodyText,
        outcome,
        turnover: detail.turnover,
      });
      stats.mergersInserted++;
      log(`  Inserted merger: ${caseNumber}`);
    } else {
      if (stmts.checkDecision.get(caseNumber)) {
        log(`  Already exists: decision ${caseNumber} — skipping`);
        stats.casesSkipped++;
        return;
      }

      stmts.insertDecision.run({
        case_number: caseNumber,
        title: entry.title,
        date,
        type: caseType,
        sector: sectorId,
        parties: detail.parties.length > 0 ? JSON.stringify(detail.parties) : null,
        summary: detail.summary,
        full_text: detail.bodyText,
        outcome,
        fine_amount: detail.fineAmount,
        ca98_articles: detail.articles.length > 0 ? JSON.stringify(detail.articles) : null,
        status,
      });
      stats.decisionsInserted++;
      log(`  Inserted decision: ${caseNumber}`);
    }
  } catch (err) {
    logError(`  DB insert failed for ${caseNumber}: ${err instanceof Error ? err.message : err}`);
    stats.errors++;
  }
}

function upsertSectors(
  db: Database.Database | null,
  stmts: DbStatements | null,
  seenSectors: Map<string, { decisions: number; mergers: number }>,
  stats: CrawlStats,
): void {
  if (FLAG_DRY_RUN || !db || !stmts) {
    log(`[DRY RUN] Would upsert ${seenSectors.size} sectors`);
    stats.sectorsUpserted = seenSectors.size;
    return;
  }

  for (const [sectorId, counts] of seenSectors) {
    const displayName = sectorId.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    try {
      stmts.upsertSector.run({
        id: sectorId,
        name: displayName,
        name_en: displayName,
        description: null,
        decision_count: counts.decisions,
        merger_count: counts.mergers,
      });
      stats.sectorsUpserted++;
    } catch (err) {
      logError(`Failed to upsert sector ${sectorId}: ${err instanceof Error ? err.message : err}`);
    }
  }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("=== CMA Case Ingestion Crawler ===");
  log(`Flags: resume=${FLAG_RESUME}, dry-run=${FLAG_DRY_RUN}, force=${FLAG_FORCE}`);
  if (ONLY_TYPE) log(`Filtering to case type: ${ONLY_TYPE}`);
  if (MAX_PAGES !== Infinity) log(`Max listing pages per type: ${MAX_PAGES}`);

  const db = FLAG_DRY_RUN ? null : initDb();
  const stmts = db ? prepareStatements(db) : null;
  const resumedSlugs = loadResumeState();
  // Track all ingested slugs (resume state + new).
  const allIngestedSlugs = new Set(resumedSlugs);

  // Also load existing case numbers from DB for resume mode.
  if (FLAG_RESUME && db) {
    const existingDecisions = db.prepare("SELECT case_number FROM decisions").all() as { case_number: string }[];
    const existingMergers = db.prepare("SELECT case_number FROM mergers").all() as { case_number: string }[];
    log(`DB contains ${existingDecisions.length} decisions, ${existingMergers.length} mergers`);
  }

  const stats: CrawlStats = {
    pagesScanned: 0,
    casesFound: 0,
    casesSkipped: 0,
    decisionsInserted: 0,
    mergersInserted: 0,
    sectorsUpserted: 0,
    errors: 0,
  };

  const seenSectors = new Map<string, { decisions: number; mergers: number }>();

  // Determine which case types to crawl.
  const typesToCrawl: [string, string][] = [];
  if (ONLY_TYPE) {
    const filter = CASE_TYPE_FILTERS[ONLY_TYPE];
    if (!filter) {
      logError(`Unknown case type: ${ONLY_TYPE}. Valid types: ${Object.keys(CASE_TYPE_FILTERS).join(", ")}`);
      process.exit(1);
    }
    typesToCrawl.push([ONLY_TYPE, filter]);
  } else {
    // Crawl the three primary case types that yield decision/merger data.
    typesToCrawl.push(
      ["ca98", CASE_TYPE_FILTERS["ca98"]!],
      ["mergers", CASE_TYPE_FILTERS["mergers"]!],
      ["markets", CASE_TYPE_FILTERS["markets"]!],
      ["consumer", CASE_TYPE_FILTERS["consumer"]!],
      ["criminal", CASE_TYPE_FILTERS["criminal"]!],
      ["regulatory", CASE_TYPE_FILTERS["regulatory"]!],
    );
  }

  for (const [typeKey, typeFilter] of typesToCrawl) {
    log(`\n--- Crawling case type: ${typeKey} ---`);

    const entries = await crawlListingPages(typeKey, typeFilter);
    stats.pagesScanned += Math.ceil(entries.length / 50) || 1;
    stats.casesFound += entries.length;

    // Deduplicate by slug (some cases appear in multiple types).
    const uniqueEntries = entries.filter((e) => {
      if (allIngestedSlugs.has(e.slug)) {
        stats.casesSkipped++;
        return false;
      }
      return true;
    });

    log(`Processing ${uniqueEntries.length} new cases (${entries.length - uniqueEntries.length} already ingested)`);

    for (const entry of uniqueEntries) {
      await ingestCase(entry, db, stmts, stats, seenSectors);
      allIngestedSlugs.add(entry.slug);

      // Save resume state periodically (every 25 cases).
      if (!FLAG_DRY_RUN && allIngestedSlugs.size % 25 === 0) {
        saveResumeState(allIngestedSlugs);
      }
    }
  }

  // Upsert sectors from aggregated counts.
  upsertSectors(db, stmts, seenSectors, stats);

  // Final resume state save.
  if (!FLAG_DRY_RUN) {
    saveResumeState(allIngestedSlugs);
  }

  // Summary.
  if (db) {
    const totalDecisions = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
    const totalMergers = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
    const totalSectors = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

    log(`\n=== Database totals ===`);
    log(`  Decisions: ${totalDecisions}`);
    log(`  Mergers:   ${totalMergers}`);
    log(`  Sectors:   ${totalSectors}`);

    db.close();
  }

  log(`\n=== Crawl complete ===`);
  log(`  Pages scanned:      ${stats.pagesScanned}`);
  log(`  Cases found:         ${stats.casesFound}`);
  log(`  Cases skipped:       ${stats.casesSkipped}`);
  log(`  Decisions inserted:  ${stats.decisionsInserted}`);
  log(`  Mergers inserted:    ${stats.mergersInserted}`);
  log(`  Sectors upserted:    ${stats.sectorsUpserted}`);
  log(`  Errors:              ${stats.errors}`);

  if (stats.errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  logError(`Fatal: ${err instanceof Error ? err.message : err}`);
  process.exit(1);
});
