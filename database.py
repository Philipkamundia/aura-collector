# =============================================================================
# AURA — Database Layer (PostgreSQL / Supabase)
# =============================================================================
#
# Uses psycopg2 with a simple connection-pool of size 1 per process.
# All queries use %s positional placeholders (psycopg2 style).
# goal_data is stored as JSONB — Postgres handles atomic array appends natively.
# =============================================================================

import json
import logging
import sys
import threading
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
import psycopg2.pool

import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

CREATE_MATCHES = """
CREATE TABLE IF NOT EXISTS matches (
    -- identity & sequencing
    id               BIGINT      PRIMARY KEY,
    tourney_id       INTEGER     NOT NULL DEFAULT 0,
    iteration        INTEGER     NOT NULL DEFAULT 0,
    -- slot_index: position within the tourney's parallel schedule (0-based,
    -- ordered by kickoff_time). Stable across iterations for the same team pairing.
    slot_index       SMALLINT    NOT NULL DEFAULT 0,
    game_version     TEXT        NOT NULL DEFAULT '',
    league           TEXT        NOT NULL DEFAULT '',
    league_id        INTEGER     NOT NULL DEFAULT 0,
    kickoff_time     BIGINT      NOT NULL DEFAULT 0,

    -- teams
    team1_id         INTEGER     NOT NULL DEFAULT 0,
    team2_id         INTEGER     NOT NULL DEFAULT 0,
    team1_name       TEXT        NOT NULL DEFAULT '',
    team2_name       TEXT        NOT NULL DEFAULT '',

    -- result (populated on finish)
    team1_score      SMALLINT    NOT NULL DEFAULT 0,
    team2_score      SMALLINT    NOT NULL DEFAULT 0,
    total_goals      SMALLINT    GENERATED ALWAYS AS (team1_score + team2_score) STORED,
    result           CHAR(1)     DEFAULT NULL,          -- 'H' | 'D' | 'A'

    -- goal sequence: [{T,M,H}, ...]
    goal_data        JSONB       NOT NULL DEFAULT '[]',
    first_goal_team  SMALLINT    DEFAULT NULL,
    first_goal_min   SMALLINT    DEFAULT NULL,

    -- pre-match odds snapshot
    odds_home        NUMERIC(7,4) DEFAULT NULL,
    odds_draw        NUMERIC(7,4) DEFAULT NULL,
    odds_away        NUMERIC(7,4) DEFAULT NULL,

    -- totals — all O/U lines stored as JSONB: [{line, over, under}, ...]
    odds_total_lines JSONB        DEFAULT '[]',
    -- main line extracted flat for fast querying
    odds_total_line  NUMERIC(5,2) DEFAULT NULL,
    odds_over        NUMERIC(7,4) DEFAULT NULL,
    odds_under       NUMERIC(7,4) DEFAULT NULL,

    odds_hcap_line   NUMERIC(5,2) DEFAULT NULL,
    odds_hcap_home   NUMERIC(7,4) DEFAULT NULL,
    odds_hcap_away   NUMERIC(7,4) DEFAULT NULL,
    odds_btts_yes    NUMERIC(7,4) DEFAULT NULL,
    odds_btts_no     NUMERIC(7,4) DEFAULT NULL,

    -- half-time result (G=5): T=184 home, T=185 draw, T=186 away, T=187 no goal
    odds_ht_home     NUMERIC(7,4) DEFAULT NULL,
    odds_ht_draw     NUMERIC(7,4) DEFAULT NULL,
    odds_ht_away     NUMERIC(7,4) DEFAULT NULL,
    odds_ht_nogoal   NUMERIC(7,4) DEFAULT NULL,

    -- half-time / full-time (G=18): T=188 HH, T=189 DD, T=190 AA
    odds_htft_hh     NUMERIC(7,4) DEFAULT NULL,
    odds_htft_dd     NUMERIC(7,4) DEFAULT NULL,
    odds_htft_aa     NUMERIC(7,4) DEFAULT NULL,

    -- team to score first (G=43): T=504 team1, T=505 team2
    odds_first_team1 NUMERIC(7,4) DEFAULT NULL,
    odds_first_team2 NUMERIC(7,4) DEFAULT NULL,

    -- draw at half-time (G=44): T=506 yes, T=507 no
    odds_ht_draw_yes NUMERIC(7,4) DEFAULT NULL,
    odds_ht_draw_no  NUMERIC(7,4) DEFAULT NULL,

    -- win to nil (G=20): T=388 team1 P=1, T=389 team2 P=1, T=390 0-0 P=1
    odds_win_nil_1   NUMERIC(7,4) DEFAULT NULL,
    odds_win_nil_2   NUMERIC(7,4) DEFAULT NULL,
    odds_no_goal     NUMERIC(7,4) DEFAULT NULL,

    -- correct score (G=11) — stored as JSONB: {T: odds, ...}
    -- T=15 1-0, T=16 2-0, T=17 3-0, T=18 0-1, T=19 1-1,
    -- T=20 0-2, T=21 3-1+, T=22 2-1, T=23 0-0
    odds_cs          JSONB        DEFAULT '{}',

    -- first goal market (G=19): T=180 team1, T=181 team2
    odds_fg_team1    NUMERIC(7,4) DEFAULT NULL,
    odds_fg_team2    NUMERIC(7,4) DEFAULT NULL,

    -- derived (margin-removed implied probabilities)
    overround        NUMERIC(8,6) DEFAULT NULL,
    implied_home     NUMERIC(8,6) DEFAULT NULL,
    implied_draw     NUMERIC(8,6) DEFAULT NULL,
    implied_away     NUMERIC(8,6) DEFAULT NULL,

    -- in-play signal
    max_odd_locks    SMALLINT    NOT NULL DEFAULT 0,

    -- timing
    -- elapsed_seconds: SC.TS at the moment "Match finished" was detected
    -- 5400 = exactly 90 min; higher = extra time / stoppage
    elapsed_seconds  INTEGER     DEFAULT NULL,

    -- housekeeping
    status           SMALLINT    NOT NULL DEFAULT 0,    -- 0=live 1=finished
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated     TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_status    ON matches (status)",
    "CREATE INDEX IF NOT EXISTS idx_league    ON matches (league)",
    "CREATE INDEX IF NOT EXISTS idx_tourney   ON matches (tourney_id, iteration)",
    "CREATE INDEX IF NOT EXISTS idx_slot      ON matches (tourney_id, slot_index, iteration)",
    "CREATE INDEX IF NOT EXISTS idx_teams     ON matches (team1_id, team2_id)",
    "CREATE INDEX IF NOT EXISTS idx_kickoff   ON matches (kickoff_time)",
    "CREATE INDEX IF NOT EXISTS idx_iteration ON matches (iteration)",
    "CREATE INDEX IF NOT EXISTS idx_result    ON matches (result) WHERE status = 1",
]

# Auto-update last_updated on every row change
CREATE_FUNCTION = """
CREATE OR REPLACE FUNCTION set_last_updated()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.last_updated = NOW();
    RETURN NEW;
END;
$$
"""

CREATE_TRIGGER = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_last_updated'
        AND tgrelid = 'matches'::regclass
    ) THEN
        CREATE TRIGGER trg_last_updated
        BEFORE UPDATE ON matches
        FOR EACH ROW EXECUTE FUNCTION set_last_updated();
    END IF;
END;
$$
"""

# Schema migrations — applied automatically to existing databases
MIGRATIONS = [
    ("odds_total_lines", "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_total_lines JSONB DEFAULT '[]'"),
    ("odds_ht_home",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_home     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_ht_draw",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_draw     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_ht_away",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_away     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_ht_nogoal",   "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_nogoal   NUMERIC(7,4) DEFAULT NULL"),
    ("odds_htft_hh",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_htft_hh     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_htft_dd",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_htft_dd     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_htft_aa",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_htft_aa     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_first_team1", "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_first_team1 NUMERIC(7,4) DEFAULT NULL"),
    ("odds_first_team2", "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_first_team2 NUMERIC(7,4) DEFAULT NULL"),
    ("odds_ht_draw_yes", "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_draw_yes NUMERIC(7,4) DEFAULT NULL"),
    ("odds_ht_draw_no",  "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_ht_draw_no  NUMERIC(7,4) DEFAULT NULL"),
    ("odds_win_nil_1",   "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_win_nil_1   NUMERIC(7,4) DEFAULT NULL"),
    ("odds_win_nil_2",   "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_win_nil_2   NUMERIC(7,4) DEFAULT NULL"),
    ("odds_no_goal",     "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_no_goal     NUMERIC(7,4) DEFAULT NULL"),
    ("odds_cs",          "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_cs          JSONB DEFAULT '{}'"),
    ("odds_fg_team1",    "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_fg_team1    NUMERIC(7,4) DEFAULT NULL"),
    ("odds_fg_team2",    "ALTER TABLE matches ADD COLUMN IF NOT EXISTS odds_fg_team2    NUMERIC(7,4) DEFAULT NULL"),
    ("elapsed_seconds",  "ALTER TABLE matches ADD COLUMN IF NOT EXISTS elapsed_seconds  INTEGER DEFAULT NULL"),
    ("slot_index",       "ALTER TABLE matches ADD COLUMN IF NOT EXISTS slot_index       SMALLINT NOT NULL DEFAULT 0"),
]


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """
    Thread-safe PostgreSQL wrapper backed by a psycopg2 SimpleConnectionPool.

    Pool size 1–4: enough for the collector's threading model (one writer
    at a time per match monitor) without exhausting Supabase's free-tier
    connection limit.
    """

    _instances: dict = {}
    _class_lock = threading.Lock()

    def __new__(cls, dsn: str = ""):
        key = dsn or "default"
        with cls._class_lock:
            if key not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[key] = instance
        return cls._instances[key]

    def __init__(self, dsn: str = ""):
        if self._initialized:
            return
        self._dsn  = dsn or config.DATABASE_URL
        self._lock = threading.Lock()
        self._connect()
        self._initialized = True

    # ── connection ────────────────────────────────────────────────────────────

    def _connect(self):
        if not self._dsn:
            logger.error(
                "DATABASE_URL is not set. "
                "Export it as an environment variable before starting."
            )
            sys.exit(1)

        for attempt in range(3):
            try:
                self._pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=2,
                    maxconn=20,
                    dsn=self._dsn,
                )
                # Verify connectivity
                conn = self._pool.getconn()
                conn.autocommit = True
                conn.cursor().execute("SELECT 1")
                self._pool.putconn(conn)

                self._setup_schema()
                logger.info("PostgreSQL database ready.")
                return

            except psycopg2.OperationalError as e:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}")
                time.sleep(3)

        logger.error("Could not connect to PostgreSQL after 3 attempts — exiting.")
        sys.exit(1)

    # ── schema ────────────────────────────────────────────────────────────────

    def _setup_schema(self):
        with self.cursor() as cur:
            cur.execute(CREATE_MATCHES)
            for idx in CREATE_INDEXES:
                cur.execute(idx)
            cur.execute(CREATE_FUNCTION)
            cur.execute(CREATE_TRIGGER)

            # Migrations: add any missing columns
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'matches'
            """)
            existing = {r["column_name"] for r in cur.fetchall()}
            for col_name, alter_sql in MIGRATIONS:
                if col_name not in existing:
                    cur.execute(alter_sql)
                    logger.info(f"Migration: added column '{col_name}'")

        logger.debug("Schema ready.")

    # ── cursor context manager ────────────────────────────────────────────────

    @contextmanager
    def cursor(self):
        """
        Yields a psycopg2 RealDictCursor inside a transaction.
        Retries up to 5 times with backoff if the pool is momentarily exhausted.
        Commits on success, rolls back on exception.
        Returns the connection to the pool afterwards.
        """
        conn = None
        for attempt in range(5):
            try:
                conn = self._pool.getconn()
                break
            except psycopg2.pool.PoolError:
                if attempt < 4:
                    time.sleep(0.2 * (attempt + 1))
                else:
                    raise
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            cur.close()
            self._pool.putconn(conn)

    # ── writes ────────────────────────────────────────────────────────────────

    def upsert_match(self, data: dict) -> bool:
        """
        Insert a new match row on first contact.
        ON CONFLICT DO NOTHING so a re-scan never overwrites.
        """
        sql = """
        INSERT INTO matches (
            id, tourney_id, iteration, slot_index, game_version,
            league, league_id, kickoff_time,
            team1_id, team2_id, team1_name, team2_name,
            team1_score, team2_score,
            odds_home, odds_draw, odds_away,
            odds_total_lines, odds_total_line, odds_over, odds_under,
            odds_hcap_line, odds_hcap_home, odds_hcap_away,
            odds_btts_yes, odds_btts_no,
            odds_ht_home, odds_ht_draw, odds_ht_away, odds_ht_nogoal,
            odds_htft_hh, odds_htft_dd, odds_htft_aa,
            odds_first_team1, odds_first_team2,
            odds_ht_draw_yes, odds_ht_draw_no,
            odds_win_nil_1, odds_win_nil_2, odds_no_goal,
            odds_cs, odds_fg_team1, odds_fg_team2,
            overround, implied_home, implied_draw, implied_away
        ) VALUES (
            %(id)s, %(tourney_id)s, %(iteration)s, %(slot_index)s, %(game_version)s,
            %(league)s, %(league_id)s, %(kickoff_time)s,
            %(team1_id)s, %(team2_id)s, %(team1_name)s, %(team2_name)s,
            %(team1_score)s, %(team2_score)s,
            %(odds_home)s, %(odds_draw)s, %(odds_away)s,
            %(odds_total_lines)s, %(odds_total_line)s, %(odds_over)s, %(odds_under)s,
            %(odds_hcap_line)s, %(odds_hcap_home)s, %(odds_hcap_away)s,
            %(odds_btts_yes)s, %(odds_btts_no)s,
            %(odds_ht_home)s, %(odds_ht_draw)s, %(odds_ht_away)s, %(odds_ht_nogoal)s,
            %(odds_htft_hh)s, %(odds_htft_dd)s, %(odds_htft_aa)s,
            %(odds_first_team1)s, %(odds_first_team2)s,
            %(odds_ht_draw_yes)s, %(odds_ht_draw_no)s,
            %(odds_win_nil_1)s, %(odds_win_nil_2)s, %(odds_no_goal)s,
            %(odds_cs)s, %(odds_fg_team1)s, %(odds_fg_team2)s,
            %(overround)s, %(implied_home)s, %(implied_draw)s, %(implied_away)s
        )
        ON CONFLICT (id) DO NOTHING
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, data)
            return True
        except Exception as e:
            logger.error(f"upsert_match({data.get('id')}): {e}")
            return False

    def backfill_odds(self, match_id: int, odds: dict) -> bool:
        """
        Fill in odds columns that are still NULL (missed on first contact
        because the match was already in-play when first seen).
        Only updates columns that are currently NULL — never overwrites
        a value already captured.
        """
        sql = """
        UPDATE matches SET
            odds_home        = COALESCE(odds_home,        %(odds_home)s),
            odds_draw        = COALESCE(odds_draw,        %(odds_draw)s),
            odds_away        = COALESCE(odds_away,        %(odds_away)s),
            odds_total_lines = CASE WHEN odds_total_lines = '[]'::jsonb
                                    THEN %(odds_total_lines)s::jsonb
                                    ELSE odds_total_lines END,
            odds_total_line  = COALESCE(odds_total_line,  %(odds_total_line)s),
            odds_over        = COALESCE(odds_over,        %(odds_over)s),
            odds_under       = COALESCE(odds_under,       %(odds_under)s),
            odds_hcap_line   = COALESCE(odds_hcap_line,   %(odds_hcap_line)s),
            odds_hcap_home   = COALESCE(odds_hcap_home,   %(odds_hcap_home)s),
            odds_hcap_away   = COALESCE(odds_hcap_away,   %(odds_hcap_away)s),
            odds_btts_yes    = COALESCE(odds_btts_yes,    %(odds_btts_yes)s),
            odds_btts_no     = COALESCE(odds_btts_no,     %(odds_btts_no)s),
            odds_ht_home     = COALESCE(odds_ht_home,     %(odds_ht_home)s),
            odds_ht_draw     = COALESCE(odds_ht_draw,     %(odds_ht_draw)s),
            odds_ht_away     = COALESCE(odds_ht_away,     %(odds_ht_away)s),
            odds_ht_nogoal   = COALESCE(odds_ht_nogoal,   %(odds_ht_nogoal)s),
            odds_htft_hh     = COALESCE(odds_htft_hh,     %(odds_htft_hh)s),
            odds_htft_dd     = COALESCE(odds_htft_dd,     %(odds_htft_dd)s),
            odds_htft_aa     = COALESCE(odds_htft_aa,     %(odds_htft_aa)s),
            odds_first_team1 = COALESCE(odds_first_team1, %(odds_first_team1)s),
            odds_first_team2 = COALESCE(odds_first_team2, %(odds_first_team2)s),
            odds_ht_draw_yes = COALESCE(odds_ht_draw_yes, %(odds_ht_draw_yes)s),
            odds_ht_draw_no  = COALESCE(odds_ht_draw_no,  %(odds_ht_draw_no)s),
            odds_win_nil_1   = COALESCE(odds_win_nil_1,   %(odds_win_nil_1)s),
            odds_win_nil_2   = COALESCE(odds_win_nil_2,   %(odds_win_nil_2)s),
            odds_no_goal     = COALESCE(odds_no_goal,     %(odds_no_goal)s),
            odds_cs          = CASE WHEN odds_cs = '{}'::jsonb
                                    THEN %(odds_cs)s::jsonb
                                    ELSE odds_cs END,
            odds_fg_team1    = COALESCE(odds_fg_team1,    %(odds_fg_team1)s),
            odds_fg_team2    = COALESCE(odds_fg_team2,    %(odds_fg_team2)s),
            overround        = COALESCE(overround,        %(overround)s),
            implied_home     = COALESCE(implied_home,     %(implied_home)s),
            implied_draw     = COALESCE(implied_draw,     %(implied_draw)s),
            implied_away     = COALESCE(implied_away,     %(implied_away)s)
        WHERE id = %(id)s
        """
        try:
            payload = dict(odds)
            payload["id"] = match_id
            with self.cursor() as cur:
                cur.execute(sql, payload)
            return True
        except Exception as e:
            logger.error(f"backfill_odds({match_id}): {e}")
            return False

    def record_goal(self, match_id: int, goal: dict) -> bool:
        """
        Atomically append a goal to goal_data (JSONB) and increment
        the scorer's score column — single UPDATE, no read-modify-write.
        """
        team_col = "team1_score" if goal["T"] == 1 else "team2_score"
        sql = f"""
        UPDATE matches
        SET goal_data  = goal_data || %s::jsonb,
            {team_col} = {team_col} + 1
        WHERE id = %s
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, (json.dumps([goal]), match_id))
            return True
        except Exception as e:
            logger.error(f"record_goal({match_id}): {e}")
            return False

    def finish_match(self, match_id: int, api_t1: int = None, api_t2: int = None, elapsed: int = None) -> bool:
        """
        Mark match finished and populate derived columns.

        api_t1 / api_t2 — the final scores direct from the API.
        These override whatever is in the DB, ensuring the result is correct
        even when goals were missed between polls.
        """
        row = self.get_match(match_id)
        if not row:
            return False

        goals  = row["goal_data"] if isinstance(row["goal_data"], list) \
                 else json.loads(row["goal_data"])

        # Use API scores if provided, fall back to what's in the DB
        t1 = api_t1 if api_t1 is not None else row["team1_score"]
        t2 = api_t2 if api_t2 is not None else row["team2_score"]

        result = "H" if t1 > t2 else ("A" if t2 > t1 else "D")
        fgt    = goals[0]["T"] if goals else None
        fgm    = goals[0]["M"] if goals else None

        sql = """
        UPDATE matches
        SET status          = 1,
            team1_score     = %s,
            team2_score     = %s,
            result          = %s,
            first_goal_team = %s,
            first_goal_min  = %s,
            elapsed_seconds = %s
        WHERE id = %s
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, (t1, t2, result, fgt, fgm, elapsed, match_id))
            logger.info(
                f"Finished: [{match_id}] "
                f"{row['team1_name']} {t1}–{t2} {row['team2_name']} "
                f"({result}) iter={row['iteration']} tourney={row['tourney_id']}"
            )
            return True
        except Exception as e:
            logger.error(f"finish_match({match_id}): {e}")
            return False

    def update_odd_locks(self, match_id: int, count: int) -> bool:
        """Keep the peak odd-lock count for this match."""
        sql = """
        UPDATE matches
        SET max_odd_locks = GREATEST(max_odd_locks, %s)
        WHERE id = %s
        """
        try:
            with self.cursor() as cur:
                cur.execute(sql, (count, match_id))
            return True
        except Exception as e:
            logger.error(f"update_odd_locks({match_id}): {e}")
            return False

    # ── reads ─────────────────────────────────────────────────────────────────

    def get_match(self, match_id: int) -> dict | None:
        """Return match row as dict or None."""
        try:
            with self.cursor() as cur:
                cur.execute("SELECT * FROM matches WHERE id = %s", (match_id,))
                row = cur.fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.error(f"get_match({match_id}): {e}")
            return None

    def get_active_ids(self) -> list:
        """Return IDs of all in-progress matches."""
        try:
            with self.cursor() as cur:
                cur.execute("SELECT id FROM matches WHERE status = 0")
                return [r["id"] for r in cur.fetchall()]
        except Exception as e:
            logger.error(f"get_active_ids: {e}")
            return []

    def is_finished(self, match_id: int) -> bool:
        try:
            with self.cursor() as cur:
                cur.execute(
                    "SELECT status FROM matches WHERE id = %s", (match_id,)
                )
                row = cur.fetchone()
                return bool(row and row["status"] == 1)
        except Exception:
            return False

    # ── teardown ──────────────────────────────────────────────────────────────

    def close(self):
        try:
            self._pool.closeall()
            logger.info("Database connection pool closed.")
        except Exception:
            pass
