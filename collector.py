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
    Extract all pre-match markets from the GE grouped-events list.

    Markets decoded:
      G=1    1X2            T=1 home, T=2 draw, T=3 away
      G=2    Asian handicap T=7 home, T=8 away (CE=1 = main line)
      G=5    HT result      T=184 home, T=185 draw, T=186 away, T=187 no goal
      G=8    Double chance  (not stored — derivable from 1X2)
      G=11   Correct score  T=15 1-0, T=16 2-0, T=17 3-0, T=18 0-1,
                            T=19 1-1, T=20 0-2, T=21 3-1+, T=22 2-1, T=23 0-0
      G=14   BTTS           T=182 yes, T=183 no
      G=15   Team totals    T=11 over, T=12 under (CE=1 = main line)
      G=17   Total goals    T=9 over, T=10 under — ALL lines stored + main flat
      G=18   HT/FT          T=188 HH, T=189 DD, T=190 AA
      G=19   First goal     T=180 team1, T=181 team2
      G=20   Win to nil     T=388 team1 P=1, T=389 team2 P=1, T=390 0-0 P=1
      G=43   Score first    T=504 team1, T=505 team2
      G=44   HT draw        T=506 yes, T=507 no
      G=62   Indiv totals   T=13 team1 over, T=14 team1 under (CE=1 main line)
    """
    import json as _json

    result = {
        # 1X2
        "odds_home": None, "odds_draw": None, "odds_away": None,
        # Totals
        "odds_total_lines": "[]",   # all lines as JSON
        "odds_total_line": None, "odds_over": None, "odds_under": None,
        # Handicap
        "odds_hcap_line": None, "odds_hcap_home": None, "odds_hcap_away": None,
        # BTTS
        "odds_btts_yes": None, "odds_btts_no": None,
        # HT result
        "odds_ht_home": None, "odds_ht_draw": None,
        "odds_ht_away": None, "odds_ht_nogoal": None,
        # HT/FT
        "odds_htft_hh": None, "odds_htft_dd": None, "odds_htft_aa": None,
        # Score first
        "odds_first_team1": None, "odds_first_team2": None,
        # HT draw
        "odds_ht_draw_yes": None, "odds_ht_draw_no": None,
        # Win to nil / 0-0
        "odds_win_nil_1": None, "odds_win_nil_2": None, "odds_no_goal": None,
        # Correct score
        "odds_cs": "{}",
        # First goal market
        "odds_fg_team1": None, "odds_fg_team2": None,
        # Derived
        "overround": None,
        "implied_home": None, "implied_draw": None, "implied_away": None,
    }

    for group in ge:
        gid    = group.get("G")
        events = group.get("E", [])
        flat   = [e for row in events for e in (row if isinstance(row, list) else [row])]
        # Only use unlocked odds (B=False → market is open)
        open_e = [e for e in flat if not e.get("B", False)]
        if not open_e:
            continue

        by_t = {e["T"]: e for e in open_e if "T" in e}

        if gid == 1 and result["odds_home"] is None:
            result["odds_home"] = by_t.get(1, {}).get("C")
            result["odds_draw"] = by_t.get(2, {}).get("C")
            result["odds_away"] = by_t.get(3, {}).get("C")

        elif gid == 17 and result["odds_total_line"] is None:
            # Collect ALL lines
            over_e  = [e for e in open_e if e.get("T") == 9 and e.get("P") is not None]
            under_e = [e for e in open_e if e.get("T") == 10 and e.get("P") is not None]
            over_by_p  = {e["P"]: e["C"] for e in over_e}
            under_by_p = {e["P"]: e["C"] for e in under_e}
            all_lines = []
            for p in sorted(over_by_p):
                all_lines.append({
                    "line":  p,
                    "over":  over_by_p[p],
                    "under": under_by_p.get(p),
                })
            result["odds_total_lines"] = _json.dumps(all_lines)
            # Main line (CE=1)
            main = [e for e in open_e if e.get("CE") == 1] or open_e
            main_by_t = {e["T"]: e for e in main if "T" in e}
            if 9 in main_by_t and 10 in main_by_t:
                result["odds_total_line"] = main_by_t[9].get("P")
                result["odds_over"]       = main_by_t[9].get("C")
                result["odds_under"]      = main_by_t[10].get("C")

        elif gid == 2 and result["odds_hcap_line"] is None:
            main = [e for e in open_e if e.get("CE") == 1] or open_e
            main_by_t = {e["T"]: e for e in main if "T" in e}
            if 7 in main_by_t and 8 in main_by_t:
                result["odds_hcap_line"] = main_by_t[7].get("P")
                result["odds_hcap_home"] = main_by_t[7].get("C")
                result["odds_hcap_away"] = main_by_t[8].get("C")

        elif gid == 14 and result["odds_btts_yes"] is None:
            result["odds_btts_yes"] = by_t.get(182, {}).get("C")
            result["odds_btts_no"]  = by_t.get(183, {}).get("C")

        elif gid == 5 and result["odds_ht_home"] is None:
            result["odds_ht_home"]   = by_t.get(184, {}).get("C")
            result["odds_ht_draw"]   = by_t.get(185, {}).get("C")
            result["odds_ht_away"]   = by_t.get(186, {}).get("C")
            result["odds_ht_nogoal"] = by_t.get(187, {}).get("C")

        elif gid == 18 and result["odds_htft_hh"] is None:
            result["odds_htft_hh"] = by_t.get(188, {}).get("C")
            result["odds_htft_dd"] = by_t.get(189, {}).get("C")
            result["odds_htft_aa"] = by_t.get(190, {}).get("C")

        elif gid == 43 and result["odds_first_team1"] is None:
            result["odds_first_team1"] = by_t.get(504, {}).get("C")
            result["odds_first_team2"] = by_t.get(505, {}).get("C")

        elif gid == 44 and result["odds_ht_draw_yes"] is None:
            result["odds_ht_draw_yes"] = by_t.get(506, {}).get("C")
            result["odds_ht_draw_no"]  = by_t.get(507, {}).get("C")

        elif gid == 20 and result["odds_win_nil_1"] is None:
            # T=388 P=1 = team1 win to nil, T=389 P=1 = team2, T=390 P=1 = 0-0
            for e in open_e:
                t = e.get("T")
                p = e.get("P")
                if t == 388 and p == 1:
                    result["odds_win_nil_1"] = e.get("C")
                elif t == 389 and p == 1:
                    result["odds_win_nil_2"] = e.get("C")
                elif t == 390 and p == 1:
                    result["odds_no_goal"] = e.get("C")

        elif gid == 11 and result["odds_cs"] == "{}":
            # Correct score: map T -> odds
            # T=15 1-0, T=16 2-0, T=17 3-0, T=18 0-1, T=19 1-1,
            # T=20 0-2, T=21 3-1+, T=22 2-1, T=23 0-0
            cs_labels = {
                15: "1-0", 16: "2-0", 17: "3-0",
                18: "0-1", 19: "1-1", 20: "0-2",
                21: "3-1+", 22: "2-1", 23: "0-0",
            }
            cs = {}
            for t, label in cs_labels.items():
                if t in by_t:
                    cs[label] = by_t[t].get("C")
            result["odds_cs"] = _json.dumps(cs)

        elif gid == 19 and result["odds_fg_team1"] is None:
            result["odds_fg_team1"] = by_t.get(180, {}).get("C")
            result["odds_fg_team2"] = by_t.get(181, {}).get("C")

    # Derived: overround and margin-removed implied probabilities
    h = result["odds_home"]
    d = result["odds_draw"]
    a = result["odds_away"]
    if h and d and a:
        try:
            overround = (1/h) + (1/d) + (1/a)
            result["overround"]    = round(overround, 6)
            result["implied_home"] = round((1/h) / overround, 6)
            result["implied_draw"] = round((1/d) / overround, 6)
            result["implied_away"] = round((1/a) / overround, 6)
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
                + (" [NO ODDS]" if odds["odds_home"] is None else "")
            )

        else:
            # ── Subsequent visit ─────────────────────────────────────────────

            # Back-fill odds if we missed them on first contact
            # (match was already in-play when first seen, odds were closed)
            if row.get("odds_home") is None:
                odds = _parse_odds(ge)
                if odds["odds_home"] is not None:
                    self.db.backfill_odds(self.match_id, odds)
                    logger.info(f"[{self.match_id}] ✅ Back-filled odds")

            # Score delta — record each goal we witnessed with timing
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
            # Pass the authoritative scores from the API so finish_match
            # can set result correctly even if goals were missed mid-game.
            # Also pass elapsed seconds (SC.TS) so we know actual game duration.
            self.db.finish_match(self.match_id, t1_score, t2_score, elapsed)
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

    db = Database(config.DATABASE_URL)
    engine = Engine(db)
    engine.run()


if __name__ == "__main__":
    main()
