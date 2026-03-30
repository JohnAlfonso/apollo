"""
orchestrator.py — Automated scraping scheduler.

Timeline per cycle (default 60 min):
  ├─ 0:00  Start  web_scrapper.py
  ├─ 0:50  Kill   web_scrapper.py   (50 min of scraping)
  ├─ 0:50  Run    auto_antibot.py   (solves Cloudflare checkbox)
  └─ 1:00  Repeat

Cloudflare resets roughly every 60 minutes. By stopping the scraper at 50 min,
running auto_antibot.py during the 10-min gap, and then restarting, the CF
challenge is solved automatically before every new scraping window.

Usage:
    python orchestrator.py [any web_scrapper.py args...]

Examples:
    python orchestrator.py
    python orchestrator.py --mode add_person --sources hunter.io-50-1000

Environment variables (optional overrides):
    RUN_MINUTES   = 50   # how long web_scrapper.py runs each cycle
    PAUSE_MINUTES = 10   # gap between cycles (auto_antibot.py runs here)
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# ── Timing ────────────────────────────────────────────────────────────────────
RUN_MINUTES   = int(os.environ.get("RUN_MINUTES",   "56"))
PAUSE_MINUTES = int(os.environ.get("PAUSE_MINUTES", "4"))

# ── Script locations ──────────────────────────────────────────────────────────
_HERE           = Path(__file__).parent
SCRAPER_SCRIPT  = _HERE / "web_scrapper.py"
ANTIBOT_SCRIPT  = _HERE / "auto_antibot.py"

# Any extra CLI arguments are forwarded directly to web_scrapper.py.
SCRAPER_EXTRA_ARGS = sys.argv[1:]


def _timestamp() -> str:
    return time.strftime("%H:%M:%S")


def run_antibot() -> None:
    """Run auto_antibot.py synchronously and wait for it to finish."""
    print(f"\n[{_timestamp()}] [Orchestrator] Running auto_antibot.py ...")
    try:
        result = subprocess.run(
            [sys.executable, str(ANTIBOT_SCRIPT)],
            timeout=120,   # 2-minute safety cap — CF solve should finish well within this
        )
        if result.returncode == 0:
            print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py finished (OK).")
        else:
            print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py exited with code {result.returncode}.")
    except subprocess.TimeoutExpired:
        print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py timed out (2 min). Continuing anyway.")
    except Exception as e:
        print(f"[{_timestamp()}] [Orchestrator] Failed to run auto_antibot.py: {e}")


def run_cycle(cycle_num: int) -> None:
    """Start web_scrapper.py, let it run for RUN_MINUTES, then kill it."""
    cmd = [sys.executable, str(SCRAPER_SCRIPT)] + SCRAPER_EXTRA_ARGS
    print(f"\n{'=' * 70}")
    print(f"[{_timestamp()}] [Orchestrator] Cycle {cycle_num} — "
          f"starting web_scrapper.py for {RUN_MINUTES} min")
    print(f"[{_timestamp()}] [Orchestrator] Command: {' '.join(cmd)}")
    print(f"{'=' * 70}\n")

    proc = subprocess.Popen(cmd)
    deadline = time.time() + RUN_MINUTES * 60

    try:
        while time.time() < deadline:
            if proc.poll() is not None:
                print(f"[{_timestamp()}] [Orchestrator] "
                      f"web_scrapper.py exited early (code={proc.returncode}).")
                return
            time.sleep(5)  # check every 5 s whether the scraper exited on its own
    except KeyboardInterrupt:
        print(f"\n[{_timestamp()}] [Orchestrator] KeyboardInterrupt — stopping scraper.")
        _kill(proc)
        raise

    print(f"\n[{_timestamp()}] [Orchestrator] "
          f"{RUN_MINUTES}-min window elapsed — killing web_scrapper.py ...")
    _kill(proc)
    print(f"[{_timestamp()}] [Orchestrator] web_scrapper.py stopped.")


def _kill(proc: subprocess.Popen) -> None:
    """Terminate a subprocess gracefully, then force-kill if it hangs."""
    try:
        proc.terminate()
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        print(f"[{_timestamp()}] [Orchestrator] Graceful shutdown timed out — force killing.")
        proc.kill()
        proc.wait()
    except Exception:
        pass


def main() -> None:
    print(f"[{_timestamp()}] [Orchestrator] ==========================================")
    print(f"[{_timestamp()}] [Orchestrator] Cycle = {RUN_MINUTES} min run  "
          f"+ {PAUSE_MINUTES} min pause  = {RUN_MINUTES + PAUSE_MINUTES} min total")
    print(f"[{_timestamp()}] [Orchestrator] Scraper args : {SCRAPER_EXTRA_ARGS or '(none)'}")
    print(f"[{_timestamp()}] [Orchestrator] Press Ctrl+C to stop.")
    print(f"[{_timestamp()}] [Orchestrator] ==========================================\n")

    cycle = 0
    try:
        while True:
            cycle += 1
            cycle_start = time.time()

            # ── 50 min: scrape ────────────────────────────────────────────────
            run_cycle(cycle)

            # ── Solve Cloudflare during the pause window ──────────────────────
            run_antibot()

            # ── Wait out the rest of the pause window before next cycle ───────
            elapsed_since_start = time.time() - cycle_start
            full_cycle_s = (RUN_MINUTES + PAUSE_MINUTES) * 60
            remaining = full_cycle_s - elapsed_since_start
            if remaining > 0:
                print(f"[{_timestamp()}] [Orchestrator] "
                      f"Waiting {remaining:.0f}s before starting cycle {cycle + 1} ...")
                time.sleep(remaining)

    except KeyboardInterrupt:
        print(f"\n[{_timestamp()}] [Orchestrator] Shutdown requested. Goodbye.")
        sys.exit(0)


if __name__ == "__main__":
    main()
