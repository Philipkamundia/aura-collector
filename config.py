# =============================================================================
# AURA — 1xBet FIFA Esports Dataset Ingestion Engine
# Configuration
# =============================================================================

import os

# ── Database ──────────────────────────────────────────────────────────────────
# Set DATABASE_URL in your environment (Railway / .env)
# Format: postgresql://user:password@host:port/dbname
# Supabase example:
#   postgresql://postgres.[ref]:[password]@aws-0-[region].pooler.supabase.com:6543/postgres
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# ── API ───────────────────────────────────────────────────────────────────────
API_BASE_URL = "https://1x-bet.mobi"
API_TIMEOUT  = 10       # seconds per request
MAX_RETRIES  = 3        # retries before giving up
RETRY_DELAY  = 1.5      # seconds between retries

# ── API query parameters ──────────────────────────────────────────────────────
SPORTS_ID  = 85
COUNTRY_ID = 169
PARTNER_ID = 36
GAMES_COUNT = 40

# ── Polling intervals ──────────────────────────────────────────────────��──────
POLL_INTERVAL  = 5.0    # seconds between per-match polls
SCAN_INTERVAL  = 30     # seconds between full game-list scans

# ── League filtering ──────────────────────────────────────────────────────────
# Standard 11v11 FIFA only — drop high-scoring unpredictable formats
EXCLUDED_LEAGUE_TERMS = [
    "Rush",
    "3x3",
    "4x4",
    "5x5",
    "6x6",
    "7x7",
    "Penalty",
]

MAX_PREMATCH_WAIT_MINUTES = 5

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL  = os.environ.get("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
