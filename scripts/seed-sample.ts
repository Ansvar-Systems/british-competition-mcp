/**
 * Seed the CMA database with sample decisions, mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CMA_DB_PATH"] ?? "data/cma.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors -----------------------------------------------------------------

interface SectorRow {
  id: string;
  name: string;
  name_en: string;
  description: string;
  decision_count: number;
  merger_count: number;
}

const sectors: SectorRow[] = [
  { id: "digital_economy", name: "Digital Economy", name_en: "Digital Economy", description: "Online platforms, social networks, app stores, and digital marketplaces.", decision_count: 2, merger_count: 1 },
  { id: "energy", name: "Energy", name_en: "Energy", description: "Electricity and gas supply, renewable energy, and energy networks.", decision_count: 1, merger_count: 1 },
  { id: "retail", name: "Retail", name_en: "Retail", description: "Supermarkets, grocery retail, and consumer goods supply.", decision_count: 1, merger_count: 1 },
  { id: "financial_services", name: "Financial Services", name_en: "Financial Services", description: "Banking, insurance, payment systems, and financial market infrastructure.", decision_count: 1, merger_count: 0 },
  { id: "healthcare", name: "Healthcare", name_en: "Healthcare", description: "Hospitals, pharmaceuticals, medical devices, and private healthcare.", decision_count: 0, merger_count: 1 },
  { id: "media", name: "Media", name_en: "Media", description: "Broadcasting, newspapers, streaming services, and news agencies.", decision_count: 1, merger_count: 1 },
];

const insertSector = db.prepare(
  "INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)",
);

for (const s of sectors) {
  insertSector.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
}

console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---------------------------------------------------------------

interface DecisionRow {
  case_number: string;
  title: string;
  date: string;
  type: string;
  sector: string;
  parties: string;
  summary: string;
  full_text: string;
  outcome: string;
  fine_amount: number | null;
  gb_law_articles: string;
  status: string;
}

const decisions: DecisionRow[] = [
  {
    case_number: "CE-9742/14",
    title: "Amazon — Resale Price Maintenance Investigation",
    date: "2018-06-28",
    type: "abuse_of_dominance",
    sector: "digital_economy",
    parties: JSON.stringify(["Amazon EU SARL", "Amazon Services Europe SARL"]),
    summary: "The CMA investigated suspected breaches of competition law by Amazon relating to resale price maintenance (RPM) provisions in contracts with third-party sellers on its UK marketplace. Amazon provided commitments to remove the RPM provisions.",
    full_text: "The CMA opened a formal investigation under Chapter I of the Competition Act 1998 into suspected anti-competitive agreements between Amazon and third-party sellers on the Amazon UK marketplace. The investigation concerned provisions in Amazon's agreements with third-party sellers that may have restricted sellers from setting their own retail prices below a certain level (resale price maintenance). The CMA was concerned that these provisions may have prevented, restricted or distorted competition in the supply of goods to UK consumers. Amazon cooperated with the investigation and provided commitments to remove the relevant provisions from its seller agreements. The CMA accepted these commitments and closed the case without making an infringement decision. The commitments required Amazon to: (1) remove contractual terms that restricted third-party sellers from setting their own retail prices; (2) not re-introduce similar restrictions for a period of five years. The case illustrated the CMA's focus on ensuring that online platforms do not impose restrictions that harm competition in digital markets.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gb_law_articles: JSON.stringify(["CA98 Chapter I", "Article 101 TFEU"]),
    status: "final",
  },
  {
    case_number: "ME/6821/19",
    title: "Viagogo / StubHub — Merger Inquiry",
    date: "2021-07-16",
    type: "merger",
    sector: "digital_economy",
    parties: JSON.stringify(["Viagogo Entertainment Inc.", "StubHub Holdings LLC"]),
    summary: "The CMA found that Viagogo's acquisition of StubHub was anti-competitive, as it combined the two largest secondary ticketing platforms in the UK. Viagogo was required to divest StubHub's international business outside North America.",
    full_text: "The CMA conducted an in-depth Phase 2 inquiry into Viagogo's completed acquisition of StubHub. Viagogo and StubHub were the two largest platforms for the secondary resale of tickets for live events in the UK. The CMA found that the merger had resulted in a substantial lessening of competition (SLC) in the supply of secondary ticketing services to sellers and buyers of tickets in the UK. Prior to the merger, Viagogo and StubHub were close competitors and there was a significant degree of switching between the two platforms. The loss of competition between the two platforms was expected to lead to harm for both sellers and buyers of secondary tickets, including higher fees and reduced innovation. The CMA required Viagogo to divest StubHub's international business (excluding North America) as a remedy to restore competition. The divestiture created an independent competitor able to compete effectively with Viagogo in the UK and European secondary ticketing markets.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gb_law_articles: JSON.stringify(["Enterprise Act 2002 Part 3"]),
    status: "final",
  },
  {
    case_number: "CE-9536/12",
    title: "Online Hotel Booking — Price Parity Investigation",
    date: "2014-01-31",
    type: "cartel",
    sector: "digital_economy",
    parties: JSON.stringify(["Booking.com", "Expedia", "Hotels.com", "InterContinental Hotels Group plc", "Starwood Hotels and Resorts Worldwide Inc."]),
    summary: "The CMA investigated suspected anti-competitive agreements between hotel booking websites and hotel chains relating to price parity (best rate guarantee) clauses. Commitments were accepted from Booking.com, Expedia, and hotel chains to remove narrow MFN clauses.",
    full_text: "The CMA investigated suspected infringements of Chapter I of the Competition Act 1998 and Article 101 TFEU in online hotel booking. The investigation focused on best rate guarantee (BRG) or price parity clauses in agreements between online travel agencies (OTAs) and hotels. These clauses required hotels not to offer lower prices on other online channels, including their own websites. Following investigation, the parties agreed to narrow their price parity obligations to cover only the room types offered on the OTA platforms. The OTAs agreed that hotels could offer lower prices through their own direct channels, other OTAs operating under narrow MFN clauses, and offline channels. The CMA concluded the case by accepting commitments from Booking.com, Expedia/Hotels.com, and hotel groups under the Competition Act 1998. The commitments removed the wide parity obligations that had prevented hotels from competing on price.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gb_law_articles: JSON.stringify(["CA98 Chapter I", "Article 101 TFEU"]),
    status: "final",
  },
  {
    case_number: "CE-9950/14",
    title: "Pharmaceutical Sector — Hydrocortisone Tablets Cartel",
    date: "2021-05-24",
    type: "cartel",
    sector: "healthcare",
    parties: JSON.stringify(["Allergan plc", "Auden McKenzie Ltd", "Actavis UK Ltd"]),
    summary: "The CMA fined pharmaceutical companies a total of GBP 260 million for illegally colluding on the supply of hydrocortisone tablets to the NHS. Auden McKenzie and a generic company shared sensitive pricing information and agreed not to compete.",
    full_text: "The CMA issued infringement decisions against multiple pharmaceutical companies for participating in illegal cartels in the supply of hydrocortisone tablets to the NHS. The investigation found that Auden McKenzie (part of Actavis UK / Allergan) had entered into arrangements with potential generic entrants to delay their entry into the market and share sensitive pricing information. These arrangements allowed Auden McKenzie to maintain high prices for hydrocortisone tablets — a critical medication used by patients with adrenal insufficiency. The NHS was significantly overcharged as a result of the cartel arrangements. The CMA found that the arrangements constituted serious infringements of the Chapter I prohibition of the Competition Act 1998. Total fines imposed: approximately GBP 260 million. The case was one of the largest cartel cases in the UK pharmaceutical sector and led to the CMA developing specific guidance on competition in the pharmaceutical sector.",
    outcome: "fine",
    fine_amount: 260000000,
    gb_law_articles: JSON.stringify(["CA98 Chapter I", "Article 101 TFEU"]),
    status: "appealed",
  },
  {
    case_number: "MR-20/18",
    title: "Investment Consultants Market Investigation",
    date: "2019-12-10",
    type: "sector_inquiry",
    sector: "financial_services",
    parties: JSON.stringify(["Aon Hewitt", "Willis Towers Watson", "Mercer", "Hymans Robertson", "LCP"]),
    summary: "The CMA concluded a market investigation into investment consultants and fiduciary managers. The CMA found features of the market preventing effective competition and imposed a package of remedies including mandatory competitive tendering and disclosure requirements.",
    full_text: "The CMA conducted a market investigation under the Enterprise Act 2002 into investment consultants and fiduciary managers. The investigation found that competition was not working effectively in these markets due to: (1) low levels of engagement by pension scheme trustees when appointing and managing investment consultants and fiduciary managers; (2) conflicts of interest where investment consultants recommended their own fiduciary management services; (3) insufficient information to allow trustees to compare the performance and value for money of different providers. The CMA imposed a package of remedies: mandatory competitive tendering requirements for pension schemes that move to fiduciary management without running a tender process, requirements for investment consultants to disclose potential conflicts of interest, requirements for all providers to report performance in a standardised format, and requirements to provide clear fee information. The Financial Conduct Authority (FCA) was also required to take regulatory action to implement some of the remedies.",
    outcome: "cleared_with_conditions",
    fine_amount: null,
    gb_law_articles: JSON.stringify(["Enterprise Act 2002 Part 4"]),
    status: "final",
  },
];

const insertDecision = db.prepare(`
  INSERT OR IGNORE INTO decisions
    (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, ca98_articles, status)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertDecisionsAll = db.transaction(() => {
  for (const d of decisions) {
    insertDecision.run(
      d.case_number, d.title, d.date, d.type, d.sector,
      d.parties, d.summary, d.full_text, d.outcome,
      d.fine_amount, d.gb_law_articles, d.status,
    );
  }
});

insertDecisionsAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers -----------------------------------------------------------------

interface MergerRow {
  case_number: string;
  title: string;
  date: string;
  sector: string;
  acquiring_party: string;
  target: string;
  summary: string;
  full_text: string;
  outcome: string;
  turnover: number | null;
}

const mergers: MergerRow[] = [
  {
    case_number: "ME/6996/19",
    title: "JD Sports Fashion plc / Footasylum plc",
    date: "2020-11-04",
    sector: "retail",
    acquiring_party: "JD Sports Fashion plc",
    target: "Footasylum plc",
    summary: "The CMA ordered JD Sports to sell Footasylum after finding the merger substantially lessened competition in the sale of sports and casual footwear and apparel in the UK. The CMA found that JD Sports and Footasylum were close competitors and the merger removed a meaningful competitive constraint.",
    full_text: "The CMA conducted a Phase 2 inquiry into JD Sports Fashion plc's acquisition of Footasylum plc. JD Sports and Footasylum were both significant retailers of sports and casual footwear and clothing in the UK, with a combined estate of several hundred stores. The CMA found that the merger resulted in a substantial lessening of competition (SLC) in the sale of sports and casual footwear and apparel at retail level in the UK. JD Sports and Footasylum were close competitors — they sold similar products, targeted similar customers, and had overlapping store estates in many areas. The loss of Footasylum as an independent competitive constraint on JD Sports was expected to lead to higher prices and reduced choice for consumers. The CMA ordered JD Sports to divest Footasylum in its entirety. JD Sports appealed the remedies decision to the Competition Appeal Tribunal (CAT), which partially upheld the appeal on procedural grounds and remitted the case to the CMA. Following a fresh assessment, the CMA again concluded that divestiture of Footasylum was the appropriate remedy.",
    outcome: "prohibited",
    turnover: 500000000,
  },
  {
    case_number: "ME/6899/18",
    title: "Vodafone Group plc / Liberty Global plc (UK assets)",
    date: "2019-05-16",
    sector: "digital_economy",
    acquiring_party: "Vodafone Group plc",
    target: "Liberty Global plc (UK cable assets)",
    summary: "The CMA cleared Vodafone's acquisition of Liberty Global's UK cable network (Virgin Media) after a Phase 2 investigation. The CMA found that although the parties overlapped as providers of broadband and mobile services, the merger was not expected to substantially lessen competition.",
    full_text: "The CMA investigated Vodafone's proposed acquisition of Liberty Global's cable television, broadband and telephony operations in Germany, Czech Republic, Hungary and Romania. In the UK, Vodafone and Liberty Global (via Virgin Media) both provided broadband, TV, and telephony services. The CMA conducted a detailed Phase 2 investigation into the UK aspects of the transaction. The investigation considered whether the merger would substantially lessen competition in: broadband services (Virgin Media's network vs. other ISPs), pay-TV services, mobile services, and fixed-mobile bundles. The CMA concluded that, while there were some areas of competitive overlap, the merger was not expected to result in an SLC. Key factors included: the continued presence of BT/EE and Sky as strong competitors in broadband and pay-TV, the limited geographic overlap between Virgin Media's cable network footprint and areas where the transaction would change the competitive dynamics, and the limited competitive constraint that Vodafone and Virgin Media exerted on each other in practice. The CMA cleared the transaction without conditions.",
    outcome: "cleared_phase1",
    turnover: 10000000000,
  },
  {
    case_number: "ME/6892/18",
    title: "Amazon.com / Deliveroo Holdings",
    date: "2020-04-30",
    sector: "digital_economy",
    acquiring_party: "Amazon.com Inc.",
    target: "Deliveroo Holdings Ltd",
    summary: "The CMA initially found competition concerns with Amazon's investment in Deliveroo but ultimately cleared the deal after concluding that Deliveroo faced a real risk of exiting the market without the investment. The CMA applied the 'failing firm' exiting counterfactual.",
    full_text: "The CMA reviewed Amazon's proposed minority investment in Deliveroo, the food delivery platform. The CMA was initially concerned that the investment could reduce competition in online restaurant food delivery in the UK and in food delivery services more broadly. Amazon had previously operated its own food delivery service in the UK (Amazon Restaurants) and remained a potential entrant in the market. In its Phase 2 investigation, the CMA considered whether Amazon would, absent the investment, re-enter the food delivery market as a significant competitor to Deliveroo. The CMA also considered the failing firm/exiting counterfactual — whether Deliveroo would have been able to survive as an independent business without Amazon's investment during the COVID-19 pandemic. The CMA concluded that, given the severe financial constraints faced by Deliveroo during the pandemic, there was a real risk that the company would exit the market if the investment did not proceed. In these circumstances, the CMA found that clearing the merger was appropriate even if Amazon would otherwise be a potential competitor, since the alternative was Deliveroo's exit from the market.",
    outcome: "cleared_phase1",
    turnover: 1000000000,
  },
];

const insertMerger = db.prepare(`
  INSERT OR IGNORE INTO mergers
    (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
  VALUES
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const insertMergersAll = db.transaction(() => {
  for (const m of mergers) {
    insertMerger.run(
      m.case_number, m.title, m.date, m.sector,
      m.acquiring_party, m.target, m.summary, m.full_text,
      m.outcome, m.turnover,
    );
  }
});

insertMergersAll();
console.log(`Inserted ${mergers.length} mergers`);

const decisionCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mergerCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sectorCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;

console.log(`\nDatabase summary:`);
console.log(`  Sectors:   ${sectorCount}`);
console.log(`  Decisions: ${decisionCount}`);
console.log(`  Mergers:   ${mergerCount}`);
console.log(`\nDone. Database ready at ${DB_PATH}`);

db.close();
