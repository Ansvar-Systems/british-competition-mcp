# ─────────────────────────────────────────────────────────────────────────────
# CMA Competition MCP — multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Build:  docker build -t british-competition-mcp .
# Run:    docker run --rm -p 3000:3000 british-competition-mcp
#
# The image expects a pre-built database at /app/data/cma.db.
# CI provisions data/database.db from the GitHub Release asset before build.
# ─────────────────────────────────────────────────────────────────────────────

# --- Stage 1: Build TypeScript + native deps ---
FROM node:20-slim AS builder

WORKDIR /app

# Install build toolchain for better-sqlite3 native binding
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3 make g++ \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
# Full install (postinstall runs => better-sqlite3 native binding fetched/built)
RUN npm ci
COPY tsconfig.json ./
COPY src/ src/
RUN npm run build

# --- Stage 2: Production ---
FROM node:20-slim AS production

WORKDIR /app
ENV NODE_ENV=production
ENV CMA_DB_PATH=/app/data/cma.db

# Carry over node_modules with prebuilt better-sqlite3 binding from builder.
# Do NOT re-run `npm ci` here — it would strip the native binding via --ignore-scripts.
COPY --from=builder /app/node_modules /app/node_modules
COPY package.json package-lock.json* ./
COPY --from=builder /app/dist/ dist/

# Database asset (provisioned by CI from GitHub Release `database.db.gz` → data/database.db)
COPY data/database.db data/cma.db

# Non-root user for security
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
