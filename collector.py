# =============================================================================
# AURA — Dataset Ingestion Engine
# Polls the 1xBet FIFA esports live feed, extracts every useful field,
# and writes finished matches to the database.
# =============================================================================

import json
import logging
import math
import signal
import sys
import threading
import time
from functools import lru_cache

import requests

import config
from database import Database

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

session = requests.Session()
session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 Chrome/120.0"
    ),
    "Accept": "application/json, */*",
})


def api_get(url: str) -> dict | None:
    """GET with retry. Returns parsed JSON or None."""
    for attempt in range(config.MAX_RETRIES):
        try:
            resp = session.get(url, timeout=config.API_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout (attempt {attempt+1}): {url}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error (attempt {attempt+1}): {e}")
        except (ValueError, json.JSONDecodeError) as e:
            logger.error(f"Bad JSON: {e}")
            return None          # don't retry bad JSON
        if attempt < config.MAX_RETRIES - 1:
            time.sleep(config.RETRY_DELAY)
    logger.error(f"Gave up after {config.MAX_RETRIES} attempts: {url}")
    return None


# ---------------------------------------------------------------------------
# API URLs
# ---------------------------------------------------------------------------

def url_list() -> str:
    b = config.API_BASE_URL
    s, c, p, n = (
        config.SPORTS_ID, config.COUNTRY_ID,
        config.PARTNER_ID, config.GAMES_COUNT,
    )
    return (
        f"{b}/service-api/LiveFeed/Get1x2_VZip"
        f"?sports={s}&count={n}&lng=en&gr=666&mode=4"
        f"&country={c}&partner={p}"
        "&getEmpty=true&virtualSports=true&noFilterBlockEvent=true"
    )


def url_detail(match_id: int) -> str:
    b = config.API_BASE_URL
    p = config.PARTNER_ID
    return (
        f"{b}/service-api/LiveFeed/GetGameZip"
        f"?id={match_id}&lng=en&cfview=0&isSubGames=true"
        f"&GroupEvents=true&allEventsGroupSubGames=true"
        f"&countevents=250&partner={p}"
    )


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

@lru_cache(maxsize=512)
def _league_ok(league: str) -> bool:
    return not any(term in league for term in config.EXCLUDED_LEAGUE_TERMS)


def _game_starts_too_late(status: str, elapsed_seconds: int) -> bool:
    """True if the game is pre-match and starts too far in the future."""
    if status in ("Pre-match bets", "Pre-game betting"):
        return elapsed_seconds > config.MAX_PREMATCH_WAIT_MINUTES * 60
    return False


# ---------------------------------------------------------------------------
# Data extraction helpers
# ---------------------------------------------------------------------------

def _parse_scheduler(sc: dict) -> dict:
    """
    Extract iteration, tourney_id, game_version from SC.S array.
    These are the cycle-detection keys.
    """
    out = {"iteration": 0, "tourney_id": 0, "game_version": ""}
    for item in sc.get("S", []):
        k = item.get("Key", "")
        v = item.get("Value", "")
        if k == "iteration":
            try:
                out["iteration"] = int(v)
            except ValueError:
                pass
        elif k == "id_tourney":
            try:
                out["tourney_id"] = int(v)
            except ValueError:
                pass
        elif k == "game":
            out["game_version"] = v
    return out


def _parse_odds(ge: list) -> dict:
    """
    Extract the key pre-match markets from the GE grouped-events list.

    Markets decoded:
      G=1   → 1X2        (T=1 home, T=2 draw, T=3 away)
      G=17  → Totals     (T=9 over, T=10 under; CE=1 = main line)
      G=2   → Handicap   (T=7 home, T=8 away;  CE=1 = main line)
      G=14  → BTTS       (T=182 yes, T=183 no)

    Returns flat dict of all odds fields plus computed overround / implied probs.
    """
    result = {
        "odds_home": None, "odds_draw": None, "odds_away": None,
        "odds_total_line": None, "odds_over": None, "odds_under": None,
        "odds_hcap_line": None, "odds_hcap_home": None, "odds_hcap_away": None,
        "odds_btts_yes": None, "odds_btts_no": None,
        "overround": None,
        "implied_home": None, "implied_draw": None, "implied_away": None,
    }

    for group in ge:
        gid    = group.get("G")
        events = group.get("E", [])
        # Flatten: each row in E can itself be a list
        flat = [
            e for row in events
            for e in (row if isinstance(row, list) else [row])
        ]
        # Only use unlocked odds (B=False → market is open, game hasn't started)
        open_odds = [e for e in flat if not e.get("B", False)]
        if not open_odds:
            continue

        if gid == 1 and result["odds_home"] is None:
            by_t = {e["T"]: e["C"] for e in open_odds if "T" in e and "C" in e}
            result["odds_home"] = by_t.get(1)
            result["odds_draw"] = by_t.get(2)
            result["odds_away"] = by_t.get(3)

        elif gid == 17 and result["odds_total_line"] is None:
            # Prefer CE=1 (main line); fall back to first available
            main = [e for e in open_odds if e.get("CE") == 1] or open_odds
            by_t = {e["T"]: e for e in main if "T" in e}
            if 9 in by_t and 10 in by_t:
                result["odds_total_line"] = by_t[9].get("P")
                result["odds_over"]       = by_t[9].get("C")
                result["odds_under"]      = by_t[10].get("C")

        elif gid == 2 and result["odds_hcap_line"] is None:
            main = [e for e in open_odds if e.get("CE") == 1] or open_odds
            by_t = {e["T"]: e for e in main if "T" in e}
            if 7 in by_t and 8 in by_t:
                result["odds_hcap_line"] = by_t[7].get("P")
                result["odds_hcap_home"] = by_t[7].get("C")
                result["odds_hcap_away"] = by_t[8].get("C")

        elif gid == 14 and result["odds_btts_yes"] is None:
            by_t = {e["T"]: e["C"] for e in open_odds if "T" in e and "C" in e}
            result["odds_btts_yes"] = by_t.get(182)
            result["odds_btts_no"]  = by_t.get(183)

    # Derived: overround and margin-removed implied probabilities
    h = result["odds_home"]
    d = result["odds_draw"]
    a = result["odds_away"]
    if h and d and a:
        try:
            overround = (1/h) + (1/d) + (1/a)
            result["overround"]     = round(overround, 6)
            result["implied_home"]  = round((1/h) / overround, 6)
            result["implied_draw"]  = round((1/d) / overround, 6)
            result["implied_away"]  = round((1/a) / overround, 6)
        except ZeroDivisionError:
            pass

    return result


def _count_locks(ge: list) -> int:
    """Count outcomes with B=True (locked/suspended) across all GE groups."""
    count = 0
    for group in ge:
        for row in group.get("E", []):
            items = row if isinstance(row, list) else [row]
            count += sum(1 for e in items if e.get("B", False))
    return count


# ---------------------------------------------------------------------------
# Per-match monitor
# ---------------------------------------------------------------------------

class MatchMonitor:
    """
    Polls a single match on a timer until it finishes or the engine shuts down.
    All state lives in the database — this object holds no match state itself.
    """

    def __init__(self, match_id: int, db: Database, shutdown: threading.Event):
        self.match_id = match_id
        self.db       = db
        self.shutdown = shutdown
        self._timer: threading.Timer | None = None

    def start(self):
        self._schedule()

    def cancel(self):
        if self._timer:
            self._timer.cancel()

    def _schedule(self):
        if self.shutdown.is_set():
            return
        self._timer = threading.Timer(config.POLL_INTERVAL, self._poll)
        self._timer.name = f"match-{self.match_id}"
        self._timer.daemon = True
        self._timer.start()

    def _poll(self):
        if self.shutdown.is_set():
            return

        data = api_get(url_detail(self.match_id))
        if not data or "Value" not in data:
            logger.warning(f"[{self.match_id}] No data — will retry.")
            self._schedule()
            return

        game = data["Value"]
        sc   = game.get("SC", {})
        fs   = sc.get("FS", {})

        status    = sc.get("I", "")
        elapsed   = sc.get("TS", 0)
        t1_score  = fs.get("S1", 0)
        t2_score  = fs.get("S2", 0)
        half      = game.get("HMH", 1)
        minute    = math.floor(elapsed / 60) if elapsed else 0

        # Drop games that start too far in the future
        if _game_starts_too_late(status, elapsed):
            logger.info(
                f"[{self.match_id}] Starts in {minute}m — skipping."
            )
            return   # don't reschedule

        ge         = game.get("GE", [])
        lock_count = _count_locks(ge)

        row = self.db.get_match(self.match_id)

        if row is None:
            # ── First visit: create the row ──────────────────────────────────
            sched = _parse_scheduler(sc)
            odds  = _parse_odds(ge)

            record = {
                "id":           self.match_id,
                "tourney_id":   sched["tourney_id"],
                "iteration":    sched["iteration"],
                "game_version": sched["game_version"],
                "league":       game.get("L", ""),
                "league_id":    game.get("LI", 0),
                "kickoff_time": game.get("S", 0),
                "team1_id":     game.get("O1I", 0),
                "team2_id":     game.get("O2I", 0),
                "team1_name":   game.get("O1", ""),
                "team2_name":   game.get("O2", ""),
                "team1_score":  t1_score,
                "team2_score":  t2_score,
                **odds,
            }
            self.db.upsert_match(record)
            logger.info(
                f"[{self.match_id}] Created: "
                f"{record['team1_name']} vs {record['team2_name']} "
                f"| {record['league']} "
                f"| tourney={sched['tourney_id']} iter={sched['iteration']}"
            )

        else:
            # ── Subsequent visit: check for new goals ────────────────────────
            if t1_score > row["team1_score"]:
                self.db.record_goal(
                    self.match_id, {"T": 1, "M": minute, "H": half}
                )
                logger.info(
                    f"[{self.match_id}] ⚽ GOAL — "
                    f"{row['team1_name']} | min {minute} H{half}"
                )

            if t2_score > row["team2_score"]:
                self.db.record_goal(
                    self.match_id, {"T": 2, "M": minute, "H": half}
                )
                logger.info(
                    f"[{self.match_id}] ⚽ GOAL — "
                    f"{row['team2_name']} | min {minute} H{half}"
                )

        # ── Odd lock tracking ────────────────────────────────────────────────
        if lock_count:
            self.db.update_odd_locks(self.match_id, lock_count)
            if lock_count >= 5:
                logger.debug(
                    f"[{self.match_id}] ⚠ Odd locks: {lock_count}"
                )

        # ── Finished? ────────────────────────────────────────────────────────
        if status == "Match finished":
            self.db.finish_match(self.match_id)
            return   # do not reschedule

        # ── Log progress ─────────────────────────────────────────────────────
        row = self.db.get_match(self.match_id) or {}
        t1n = row.get("team1_name", "?")
        t2n = row.get("team2_name", "?")
        sec = elapsed % 60 if elapsed else 0

        if status in ("Pre-match bets", "Pre-game betting"):
            logger.info(f"[{self.match_id}] ⏳ {t1n} vs {t2n} — {minute:02d}:{sec:02d}")
        else:
            logger.info(
                f"[{self.match_id}] 🟢 {t1n} {t1_score}–{t2_score} {t2n} "
                f"| {minute:02d}:{sec:02d} H{half} 🔒×{lock_count}"
            )

        self._schedule()


# ---------------------------------------------------------------------------
# Engine — orchestrates all monitors
# ---------------------------------------------------------------------------

class Engine:
    """
    Main ingestion loop.
    Scans for new games every SCAN_INTERVAL seconds and starts a MatchMonitor
    for each one that passes the league filter.
    """

    def __init__(self, db: Database):
        self.db       = db
        self.shutdown = threading.Event()
        self._monitors: dict[int, MatchMonitor] = {}

        signal.signal(signal.SIGINT,  self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logger.info("Shutdown signal received.")
        self.shutdown.set()
        for m in self._monitors.values():
            m.cancel()
        self.db.close()
        sys.exit(0)

    def run(self):
        logger.info("=" * 60)
        logger.info("AURA — 1xBet FIFA Esports Dataset Ingestion Engine")
        logger.info(f"Excluded formats: {config.EXCLUDED_LEAGUE_TERMS}")
        logger.info("=" * 60)

        while not self.shutdown.is_set():
            try:
                self._scan()
                self._cleanup()
            except Exception as e:
                logger.error(f"Engine error: {e}", exc_info=True)
            self.shutdown.wait(timeout=config.SCAN_INTERVAL)

    def _scan(self):
        """Fetch the games list and start monitors for new qualifying matches."""
        data = api_get(url_list())
        if not data or "Value" not in data:
            logger.warning("Games list unavailable.")
            return

        new = 0
        for entry in data["Value"]:
            match_id = entry.get("I")
            league   = entry.get("L", "")

            if not match_id:
                continue
            if not _league_ok(league):
                continue
            if match_id in self._monitors:
                continue
            if self.db.is_finished(match_id):
                continue

            # Quick start-time check using the list entry data before
            # spinning up a full detail request
            sc      = entry.get("SC", {})
            status  = sc.get("I") or sc.get("CPS", "")
            elapsed = sc.get("TS", 0)
            if _game_starts_too_late(status, elapsed):
                continue

            monitor = MatchMonitor(match_id, self.db, self.shutdown)
            self._monitors[match_id] = monitor
            monitor.start()
            new += 1

        total = len(self._monitors)
        logger.info(f"Scan complete — monitoring {total} matches ({new} new)")

    def _cleanup(self):
        """Remove monitors whose timers have stopped (match finished)."""
        done = [
            mid for mid, m in self._monitors.items()
            if m._timer is None or not m._timer.is_alive()
        ]
        for mid in done:
            if self.db.is_finished(mid):
                del self._monitors[mid]
        if done:
            logger.debug(f"Cleaned up {len(done)} finished monitors.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
    )

    db = Database(config.DATABASE_FILE)
    engine = Engine(db)
    engine.run()


if __name__ == "__main__":
    main()
