"""
orchestrator.py — Automated scraping scheduler.

Runs a target scraper script continuously.  No time-based cycling.
When the scraper exits with code 2 (overlay/Cloudflare detected):
  1. Run auto_antibot.py to solve the Cloudflare checkbox.
  2. Restart the script immediately.
Any other exit code restarts the scraper directly without antibot.

Usage:
    python orchestrator.py [--script SCRIPT] [any scraper args...]

Examples:
    python orchestrator.py --script web_scrapper.py --mode add_person --sources hunter.io-50-1000
    python orchestrator.py --script apollo_company.py --location US --worker_index 0
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

# ── Script locations ──────────────────────────────────────────────────────────
_HERE          = Path(__file__).parent
ANTIBOT_SCRIPT = _HERE / "auto_antibot.py"

# Parse --script from args; forward everything else to the target scraper.
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--script", default="web_scrapper.py",
                     help="Scraper script to run (default: web_scrapper.py)")
_orchestrator_args, SCRAPER_EXTRA_ARGS = _parser.parse_known_args(sys.argv[1:])

SCRAPER_SCRIPT = _HERE / _orchestrator_args.script


def _timestamp() -> str:
    return time.strftime("%H:%M:%S")


def run_antibot() -> None:
    """Run auto_antibot.py synchronously and wait for it to finish."""
    print(f"\n[{_timestamp()}] [Orchestrator] Running auto_antibot.py ...")
    try:
        result = subprocess.run(
            [sys.executable, str(ANTIBOT_SCRIPT)],
            timeout=120,   # 2-minute safety cap
        )
        if result.returncode == 0:
            print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py finished (OK).")
        else:
            print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py exited with code {result.returncode}.")
    except subprocess.TimeoutExpired:
        print(f"[{_timestamp()}] [Orchestrator] auto_antibot.py timed out (2 min). Continuing anyway.")
    except Exception as e:
        print(f"[{_timestamp()}] [Orchestrator] Failed to run auto_antibot.py: {e}")


def run_scraper() -> int:
    """Start the target script and wait until it exits on its own.
    Returns the exit code:
      0  = finished normally (no more work, or clean shutdown)
      2  = overlay/Cloudflare detected — auto_antibot.py must be run first
      other = crashed or killed externally
    """
    cmd = [sys.executable, str(SCRAPER_SCRIPT)] + SCRAPER_EXTRA_ARGS
    print(f"\n{'=' * 70}")
    print(f"[{_timestamp()}] [Orchestrator] Starting {SCRAPER_SCRIPT.name}")
    print(f"[{_timestamp()}] [Orchestrator] Command: {' '.join(cmd)}")
    print(f"{'=' * 70}\n")

    try:
        proc = subprocess.Popen(cmd)
        proc.wait()
        code = proc.returncode
        print(f"[{_timestamp()}] [Orchestrator] {SCRAPER_SCRIPT.name} exited (code={code}).")
        return code
    except KeyboardInterrupt:
        print(f"\n[{_timestamp()}] [Orchestrator] KeyboardInterrupt — stopping scraper.")
        try:
            proc.terminate()
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        raise


def main() -> None:
    print(f"[{_timestamp()}] [Orchestrator] ==========================================")
    print(f"[{_timestamp()}] [Orchestrator] Script      : {SCRAPER_SCRIPT.name}")
    print(f"[{_timestamp()}] [Orchestrator] Scraper args: {SCRAPER_EXTRA_ARGS or '(none)'}")
    print(f"[{_timestamp()}] [Orchestrator] Exit code 2 : overlay -> antibot -> restart")
    print(f"[{_timestamp()}] [Orchestrator] Press Ctrl+C to stop.")
    print(f"[{_timestamp()}] [Orchestrator] ==========================================\n")

    try:
        while True:
            exit_code = run_scraper()

            if exit_code == 2:
                # Overlay / Cloudflare detected inside the scraper.
                print(f"[{_timestamp()}] [Orchestrator] "
                      "Overlay detected (exit 2) — running auto_antibot.py then restarting.")
                run_antibot()
                # print(f"[{_timestamp()}] [Orchestrator] Waiting 10s before restarting ...")
                # time.sleep(10)
                continue

            if exit_code == 0:
                # Scraper finished cleanly (e.g. no more records in queue).
                print(f"[{_timestamp()}] [Orchestrator] Scraper finished cleanly. Restarting.")
                continue

            # Any other non-zero code: crash or external kill — restart after a short delay.
            print(f"[{_timestamp()}] [Orchestrator] "
                  f"{SCRAPER_SCRIPT.name} exited with code {exit_code}. Restarting in 10s ...")
            time.sleep(10)

    except KeyboardInterrupt:
        print(f"\n[{_timestamp()}] [Orchestrator] Shutdown requested. Goodbye.")
        sys.exit(0)


if __name__ == "__main__":
    main()
