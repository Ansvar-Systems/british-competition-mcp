#!/usr/bin/env node

/**
 * CMA Competition MCP — stdio entry point.
 *
 * Provides MCP tools for querying Competition and Markets Authority decisions, merger control
 * cases, and sector enforcement activity under UK Competition Act 1998 (CA98).
 *
 * Tool prefix: gb_comp_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchMergers,
  getMerger,
  listSectors,
} from "./db.js";
import { buildCitation } from './citation.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "british-competition-mcp";

// --- Tool definitions ---------------------------------------------------------

const TOOLS = [
  {
    name: "gb_comp_search_decisions",
    description:
      "Full-text search across CMA enforcement decisions (abuse of dominance, cartel, sector inquiries). Returns matching decisions with case number, parties, outcome, fine amount, and CA98 provisions cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'price-fixing', 'abuse of dominance', 'Amazon marketplace')",
        },
        type: {
          type: "string",
          enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"],
          description: "Filter by decision type. Optional.",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'digital_economy', 'energy', 'food_retail'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"],
          description: "Filter by outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "gb_comp_get_decision",
    description:
      "Get a specific CMA decision by case number (e.g., 'CE-9742/14', 'ME/6996/19').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "CMA case number (e.g., 'CE-9742/14', 'ME/6996/19')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "gb_comp_search_mergers",
    description:
      "Search CMA merger control decisions. Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., 'Sky / Fox', 'JD Sports / Footasylum')",
        },
        sector: {
          type: "string",
          description: "Filter by sector ID (e.g., 'energy', 'food_retail', 'real_estate'). Optional.",
        },
        outcome: {
          type: "string",
          enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"],
          description: "Filter by merger outcome. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "gb_comp_get_merger",
    description:
      "Get a specific CMA merger control decision by case number (e.g., 'CE-9742/14', 'ME/6996/19').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: {
          type: "string",
          description: "CMA merger case number (e.g., 'CE-9742/14', 'ME/6996/19')",
        },
      },
      required: ["case_number"],
    },
  },
  {
    name: "gb_comp_list_sectors",
    description:
      "List all sectors with CMA enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "gb_comp_list_sources",
    description: "List all data sources used by this MCP server, with URLs and descriptions.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "gb_comp_about",
    description:
      "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation --------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  case_number: z.string().min(1),
});

const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetMergerArgs = z.object({
  case_number: z.string().min(1),
});

// --- Helper ------------------------------------------------------------------

function textContent(data: unknown) {
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(data, null, 2) },
    ],
  };
}

function errorContent(message: string) {
  return {
    content: [{ type: "text" as const, text: message }],
    isError: true as const,
  };
}

// --- Server setup ------------------------------------------------------------

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "gb_comp_search_decisions": {
        const parsed = SearchDecisionsArgs.parse(args);
        const results = searchDecisions({
          query: parsed.query,
          type: parsed.type,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "gb_comp_get_decision": {
        const parsed = GetDecisionArgs.parse(args);
        const decision = getDecision(parsed.case_number);
        if (!decision) {
          return errorContent(`Decision not found: ${parsed.case_number}`);
        }
        return textContent({
          ...(typeof decision === 'object' ? decision : { data: decision }),
          _citation: buildCitation(
            decision.case_number || parsed.case_number,
            decision.title || decision.subject || parsed.case_number,
            'gb_comp_get_decision',
            { case_number: parsed.case_number },
            decision.url || decision.source_url || null,
          ),
        });
      }

      case "gb_comp_search_mergers": {
        const parsed = SearchMergersArgs.parse(args);
        const results = searchMergers({
          query: parsed.query,
          sector: parsed.sector,
          outcome: parsed.outcome,
          limit: parsed.limit,
        });
        return textContent({ results, count: results.length });
      }

      case "gb_comp_get_merger": {
        const parsed = GetMergerArgs.parse(args);
        const merger = getMerger(parsed.case_number);
        if (!merger) {
          return errorContent(`Merger case not found: ${parsed.case_number}`);
        }
        return textContent({
          ...(typeof merger === 'object' ? merger : { data: merger }),
          _citation: buildCitation(
            merger.case_number || parsed.case_number,
            merger.title || merger.subject || parsed.case_number,
            'gb_comp_get_merger',
            { case_number: parsed.case_number },
            merger.url || merger.source_url || null,
          ),
        });
      }

      case "gb_comp_list_sectors": {
        const sectors = listSectors();
        return textContent({ sectors, count: sectors.length });
      }

      case "gb_comp_list_sources": {
        return textContent({
          sources: [
            { name: "CMA (Competition and Markets Authority)", url: "https://www.gov.uk/cma", description: "Enforcement decisions, market studies" },
            { name: "CMA Merger Control", url: "https://www.gov.uk/cma-cases", description: "Merger reviews, phase 1 and 2 decisions" },
            { name: "Competition Act 1998", url: "https://www.legislation.gov.uk/", description: "Chapter I (anti-competitive agreements), Chapter II (abuse of dominance)" },
            { name: "Enterprise Act 2002", url: "https://www.legislation.gov.uk/", description: "Merger control, market investigation references" },
            { name: "Consumer Rights Act 2015", url: "https://www.legislation.gov.uk/", description: "Consumer enforcement powers" },
            { name: "OIM (Office for the Internal Market)", url: "https://www.gov.uk/", description: "Internal market assessments" },
          ],
        });
      }

      case "gb_comp_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "CMA (Competition and Markets Authority) MCP server. Provides access to UK competition law enforcement decisions, merger control cases under the Enterprise Act 2002 and Competition Act 1998.",
          data_source: "CMA (https://www.gov.uk/cma)",
          coverage: {
            decisions: "Abuse of dominance, cartel enforcement, and sector inquiries",
            mergers: "Merger control decisions — Phase I and Phase II",
            sectors: "digital economy, energy, financial services, retail, healthcare, media, transport, telecommunications",
          },
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`);
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error executing ${name}: ${message}`);
  }
});

// --- Main --------------------------------------------------------------------

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
