#!/usr/bin/env python3
"""
queuectl - a small CLI-based background job queue with workers, retries,
exponential backoff and a simple web monitor.

Usage: python queuectl.py <command> ...

This file is a corrected and improved version of the original script.
"""

from __future__ import annotations

import json
import os
import signal
import sqlite3
import subprocess
import sys
import threading
import time
import datetime
from typing import Optional, Dict, Tuple, List

import typer
from http.server import SimpleHTTPRequestHandler, HTTPServer
from multiprocessing import Process

# --- Configuration / constants ---
DB_FILE = "queuectl.db"
PID_FILE = "queuectl.workers.pids"
MONITOR_PORT = 8000

app = typer.Typer(name="queuectl", help="A CLI-based background job queue system.")
worker_app = typer.Typer(name="worker", help="Manage worker processes.")
dlq_app = typer.Typer(name="dlq", help="Manage the Dead Letter Queue.")
config_app = typer.Typer(name="config", help="Manage system configuration.")

app.add_typer(worker_app)
app.add_typer(dlq_app)
app.add_typer(config_app)


# --- Database helpers ---
def get_db_connection(timeout: int = 5) -> sqlite3.Connection:
    """Return a sqlite3 connection with sensible defaults."""
    conn = sqlite3.connect(DB_FILE, timeout=timeout, detect_types=sqlite3.PARSE_DECLTYPES)
    # Use row factory for convenience
    conn.row_factory = sqlite3.Row
    return conn


def initialize_db() -> None:
    """Create tables and insert default config values if needed."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_retries INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                next_run_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        # defaults
        cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("backoff_base", "2"))
        cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("max_retries", "3"))
        conn.commit()
    finally:
        conn.close()


def get_config(key: str) -> Optional[str]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


# --- Backoff & util ---
def exponential_backoff(attempts: int, base: int) -> int:
    """Return backoff delay (in seconds) given attempt count and integer base."""
    # attempts should be >=1 for typical usage
    return int(base) ** int(attempts)


def utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


# --- PID file utilities ---
def append_pid(pid: int) -> None:
    """Append a pid to the pid file (one pid per line)."""
    try:
        # Ensure file exists and append newline-terminated pids
        with open(PID_FILE, "a") as f:
            f.write(f"{pid}\n")
    except Exception:
        # Avoid crashing worker if pid append fails
        pass


def read_pids_from_file() -> List[int]:
    if not os.path.exists(PID_FILE):
        return []
    try:
        with open(PID_FILE, "r") as f:
            lines = f.readlines()
        pids: List[int] = []
        for line in lines:
            s = line.strip()
            if s.isdigit():
                pids.append(int(s))
        return pids
    except Exception:
        return []


def prune_stale_pids() -> List[int]:
    """Return only currently running pids from file and rewrite file with them."""
    pids = read_pids_from_file()
    live: List[int] = []
    for pid in pids:
        try:
            os.kill(pid, 0)  # check process exists (may raise)
            live.append(pid)
        except ProcessLookupError:
            continue
        except PermissionError:
            # We cannot verify; assume alive
            live.append(pid)
        except Exception:
            continue
    # rewrite file
    try:
        with open(PID_FILE, "w") as f:
            f.write("\n".join(map(str, live)) + ("\n" if live else ""))
    except Exception:
        pass
    return live


def remove_pid(pid: int) -> None:
    """Remove a pid entry from the pid file."""
    pids = read_pids_from_file()
    pids = [p for p in pids if p != pid]
    try:
        with open(PID_FILE, "w") as f:
            f.write("\n".join(map(str, pids)) + ("\n" if pids else ""))
    except Exception:
        pass


# --- Job acquisition & state updates ---
def acquire_job() -> Optional[Dict]:
    """
    Atomically select one job that's ready and mark it 'processing'.
    This function returns a job dict or None if none available / contention occurred.
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # 1) select candidate job (ready to be run)
        # we include next_run_at check inside SQL
        cur.execute(
            """
            SELECT id, command, attempts, max_retries, created_at
            FROM jobs
            WHERE state IN ('pending', 'failed')
              AND (next_run_at IS NULL OR next_run_at <= datetime('now'))
            ORDER BY next_run_at ASC, created_at ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None

        job_id = row["id"]

        # 2) Try to atomically set state to 'processing' only if it is still pending/failed
        now = utc_now_iso()
        cur.execute(
            """
            UPDATE jobs
               SET state = 'processing', updated_at = ?
             WHERE id = ? AND state IN ('pending', 'failed')
            """,
            (now, job_id),
        )

        if cur.rowcount != 1:
            # race - someone else took it
            conn.rollback()
            return None

        conn.commit()

        # Return job details
        return {
            "id": job_id,
            "command": row["command"],
            "attempts": int(row["attempts"]),
            "max_retries": int(row["max_retries"]),
            "created_at": row["created_at"],
        }
    except sqlite3.OperationalError:
        if conn:
            conn.rollback()
        return None
    except Exception:
        if conn:
            conn.rollback()
        return None
    finally:
        conn.close()


def update_job_state(job: Dict, exit_code: int, output: Optional[str]) -> None:
    """
    Update database state after job execution.

    Logic:
     - exit_code == 0 -> completed
     - exit_code != 0 -> increment attempts; if attempts > max_retries -> dead; else -> failed and schedule next_run_at
    """
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        now_dt = datetime.datetime.now(datetime.timezone.utc)
        now_iso = now_dt.isoformat()

        if exit_code == 0:
            # Successful completion: increment attempts and mark completed
            new_attempts = job["attempts"] + 1
            cur.execute(
                """
                UPDATE jobs
                   SET state = 'completed', updated_at = ?, attempts = ?, next_run_at = NULL
                 WHERE id = ? AND state = 'processing'
                """,
                (now_iso, new_attempts, job["id"]),
            )
            if cur.rowcount == 0:
                typer.echo(f"WARNING: Completed-state update for job {job['id']} failed (race).", err=True)
        else:
            # Failure path
            attempts = job["attempts"] + 1
            max_retries = int(job["max_retries"])

            # If attempts exceeded max_retries -> move to DLQ (dead)
            if attempts >= max_retries:
                cur.execute(
                    """
                    UPDATE jobs
                       SET state = 'dead', updated_at = ?, attempts = ?, next_run_at = NULL
                     WHERE id = ? AND state = 'processing'
                    """,
                    (now_iso, attempts, job["id"]),
                )
                if cur.rowcount == 1:
                    typer.echo(f"Job {job['id']} moved to DLQ (dead). Attempts: {attempts}/{max_retries}.")
                else:
                    # If this update failed, warn. It's still in 'processing' or changed by a race.
                    typer.echo(f"WARNING: Failed to set job {job['id']} to 'dead' (race or DB issue).", err=True)
            else:
                # schedule retry with exponential backoff
                base = int(get_config("backoff_base") or "2")
                delay = exponential_backoff(attempts, base)
                next_run_at = (now_dt + datetime.timedelta(seconds=delay)).isoformat()
                cur.execute(
                    """
                    UPDATE jobs
                       SET state = 'failed', updated_at = ?, attempts = ?, next_run_at = ?
                     WHERE id = ? AND state = 'processing'
                    """,
                    (now_iso, attempts, next_run_at, job["id"]),
                )
                if cur.rowcount == 1:
                    typer.echo(f"Job {job['id']} failed (attempt {attempts}/{max_retries}). Retrying after {delay}s.")
                else:
                    typer.echo(f"WARNING: Failed to set job {job['id']} to 'failed' (race or DB issue).", err=True)

        conn.commit()
    except Exception as e:
        typer.echo(f"Error while updating job {job.get('id')}: {e}", err=True)
        if conn:
            conn.rollback()
    finally:
        conn.close()


# --- Worker main loop ---
def worker_process_main(worker_id: int) -> None:
    """Main function executed by worker processes."""
    pid = os.getpid()
    typer.echo(f"Worker-{worker_id} starting (pid={pid}).")
    append_pid(pid)

    running = True

    def _sigterm_handler(signum, frame):
        nonlocal running
        typer.echo(f"Worker-{worker_id} (pid={pid}) received SIGTERM; will stop after current job.", err=True)
        running = False

    signal.signal(signal.SIGTERM, _sigterm_handler)
    signal.signal(signal.SIGINT, _sigterm_handler)

    try:
        while running:
            job = acquire_job()
            if not job:
                # no job ready; sleep briefly to reduce busy loop
                time.sleep(1)
                continue

            typer.echo(f"[Worker-{worker_id}] Processing job {job['id']} (attempts={job['attempts']}) ...")
            exit_code = 1
            stdout_text = None
            try:
                # Execute job command with a timeout to avoid stuck jobs
                result = subprocess.run(job["command"], shell=True, capture_output=True, text=True, timeout=600)
                exit_code = result.returncode
                stdout_text = result.stdout
            except subprocess.TimeoutExpired:
                typer.echo(f"[Worker-{worker_id}] Job {job['id']} timed out.", err=True)
                exit_code = 1
            except Exception as e:
                typer.echo(f"[Worker-{worker_id}] Job {job['id']} execution error: {e}", err=True)
                exit_code = 1

            update_job_state(job, exit_code, stdout_text)
            # give tiny breathing time between jobs
            time.sleep(0.1)

    finally:
        # Remove this pid from pidfile on graceful exit
        try:
            remove_pid(pid)
        except Exception:
            pass
        typer.echo(f"Worker-{worker_id} shutting down (pid={pid}).")


# --- Monitor / HTTP server ---
def get_job_data_for_monitor() -> Tuple[List[sqlite3.Row], Dict[str, int]]:
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at
            FROM jobs
            ORDER BY created_at DESC
            """
        )
        jobs = cur.fetchall()
        cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
        rows = cur.fetchall()
        stats = {r["state"]: int(r["cnt"]) for r in rows}
        return jobs, stats
    finally:
        conn.close()


def generate_html(jobs, stats) -> str:
    """Create a simple dashboard HTML (all times in UTC)."""
    active_workers = len(prune_stale_pids())

    total_jobs = sum(stats.values()) if stats else 0
    now_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html_parts = [
        "<!doctype html>",
        "<html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>",
        "<title>QueueCTL Monitor</title>",
        "<meta http-equiv='refresh' content='5'>",
        "<style>",
        "body{font-family:Arial,Helvetica,sans-serif;background:#f4f4f9;margin:20px}",
        ".container{max-width:1200px;margin:auto;background:#fff;padding:18px;border-radius:8px;box-shadow:0 0 12px rgba(0,0,0,0.08)}",
        "table{width:100%;border-collapse:collapse;margin-top:16px}",
        "th,td{padding:10px;border:1px solid #e8e8e8;text-align:left;font-size:0.9rem}",
        "th{background:#222;color:#fff}",
        ".state-pending{color:#c67b00;font-weight:700}.state-processing{color:#1976d2;font-weight:700}",
        ".state-completed{color:#2e7d32;font-weight:700}.state-failed{color:#e65100;font-weight:700}",
        ".state-dead{color:#6a1b9a;font-weight:700}",
        ".stat{display:inline-block;padding:10px 14px;margin-right:8px;border-radius:6px;background:#eef6ff}",
        "</style></head><body>",
        "<div class='container'>",
        f"<h1>QueueCTL Monitor</h1><p>Last updated: {now_utc} (auto-refresh every 5s)</p>",
        "<div>",
        f"<span class='stat'>Total Jobs: <strong>{total_jobs}</strong></span>",
        f"<span class='stat'>Active Workers: <strong>{active_workers}</strong></span>",
        f"<span class='stat'>Pending: <strong>{stats.get('pending', 0)}</strong></span>",
        f"<span class='stat'>Processing: <strong>{stats.get('processing', 0)}</strong></span>",
        f"<span class='stat'>Failed: <strong>{stats.get('failed', 0)}</strong></span>",
        f"<span class='stat'>Dead (DLQ): <strong>{stats.get('dead', 0)}</strong></span>",
        "</div>"
    ]

    html_parts.append("<table><thead><tr><th>ID</th><th>State</th><th>Attempts</th><th>Max</th><th>Cmd</th><th>Created</th><th>Updated</th><th>Next Run</th></tr></thead><tbody>")

    for r in jobs:
        job_id = r["id"]
        state = r["state"]
        attempts = r["attempts"]
        max_retries = r["max_retries"]
        cmd = r["command"]
        created = (r["created_at"][:19].replace("T", " ")) if r["created_at"] else ""
        updated = (r["updated_at"][:19].replace("T", " ")) if r["updated_at"] else ""
        next_run = (r["next_run_at"][:19].replace("T", " ")) if r["next_run_at"] else "N/A"
        display_cmd = cmd if len(cmd) < 50 else cmd[:47] + "..."
        state_cls = f"state-{state}"
        html_parts.append(
            f"<tr><td>{job_id}</td><td class='{state_cls}'>{state.upper()}</td>"
            f"<td>{attempts}</td><td>{max_retries}</td><td><code>{display_cmd}</code></td>"
            f"<td>{created}</td><td>{updated}</td><td>{next_run}</td></tr>"
        )

    html_parts.append("</tbody></table><p style='margin-top:14px;font-size:0.85rem;color:#666'>All times UTC. Monitor refreshes every 5s.</p></div></body></html>")
    return "\n".join(html_parts)


class QueueMonitorHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/":
            jobs, stats = get_job_data_for_monitor()
            html = generate_html(jobs, stats)
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode("utf-8"))
        else:
            self.send_error(404)


# --- CLI commands ---
@app.callback(invoke_without_command=True)
def _main(ctx: typer.Context):
    initialize_db()
    if ctx.invoked_subcommand is None:
        typer.echo("QueueCTL - run 'queuectl --help' for commands.")


@app.command("enqueue")
def enqueue_job(job_data_json: str = typer.Argument("", help='JSON e.g. \'{"id":"job1","command":"sleep 2"}\'')):
    """
    Enqueue a job. Accepts JSON as argument or via stdin.
    """
    if not job_data_json:
        # Try reading stdin if piped
        if not sys.stdin.isatty():
            job_data_json = sys.stdin.read().strip()
        if not job_data_json:
            typer.echo("Error: Provide job JSON as argument or via stdin.", err=True)
            raise typer.Exit(code=1)

    try:
        data = json.loads(job_data_json)
    except json.JSONDecodeError:
        typer.echo("Error: Invalid JSON. Use double quotes for keys/strings.", err=True)
        raise typer.Exit(code=1)

    job_id = data.get("id")
    command = data.get("command")
    max_retries = data.get("max_retries", int(get_config("max_retries") or 3))

    if not job_id or not command:
        typer.echo("Error: 'id' and 'command' are required fields.", err=True)
        raise typer.Exit(code=1)

    conn = get_db_connection()
    try:
        now = utc_now_iso()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at)
            VALUES (?, ?, 'pending', 0, ?, ?, ?, NULL)
            """,
            (job_id, command, int(max_retries), now, now),
        )
        conn.commit()
        typer.echo(f"Enqueued job '{job_id}' -> {command} (max_retries={max_retries})")
    except sqlite3.IntegrityError:
        typer.echo(f"Error: Job ID '{job_id}' already exists.", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"Unexpected error while enqueueing: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        conn.close()


@app.command("status")
def show_status():
    """Show counts of job states and number of active workers."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state")
        rows = cur.fetchall()
        counts = {r["state"]: int(r["cnt"]) for r in rows}
    finally:
        conn.close()

    prune_stale_pids()
    active_workers = len(read_pids_from_file())

    typer.echo("\nQueueCTL Status")
    typer.echo("======================")
    for s in ["pending", "processing", "completed", "failed", "dead"]:
        typer.echo(f"{s.title():<12}: {counts.get(s, 0)}")
    typer.echo(f"{'Active Workers':<12}: {active_workers}")
    typer.echo("======================\n")


@app.command("list")
def list_jobs(state: str = typer.Option("pending", "--state", "-s", help="Filter by state")):
    """List jobs filtered by state (pending|processing|completed|failed|dead)."""
    state = state.lower()
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at
            FROM jobs
            WHERE state = ?
            ORDER BY created_at DESC
            """,
            (state,),
        )
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        typer.echo(f"No jobs in state '{state}'.")
        return

    typer.echo(f"\nJobs (state={state})")
    typer.echo("-" * 100)
    typer.echo(f"{'ID':<18} {'STATE':<12} {'ATT':<4}/{ 'MAX':<4} {'CMD':<40} {'CREATED':<19} {'UPDATED':<19} {'NEXT RUN':<19}")
    typer.echo("-" * 100)
    for r in rows:
        job_id = r["id"]
        cmd = r["command"]
        display_cmd = cmd if len(cmd) < 40 else cmd[:37] + "..."
        created = (r["created_at"][:19].replace("T", " ")) if r["created_at"] else ""
        updated = (r["updated_at"][:19].replace("T", " ")) if r["updated_at"] else ""
        next_run = (r["next_run_at"][:19].replace("T", " ")) if r["next_run_at"] else "N/A"
        typer.echo(f"{job_id:<18} {r['state']:<12} {r['attempts']:<4}/{r['max_retries']:<4} {display_cmd:<40} {created:<19} {updated:<19} {next_run:<19}")
    typer.echo("-" * 100)


@worker_app.command("start")
def start_workers(count: int = typer.Option(1, "--count", "-c", min=1, help="number of workers")):
    """Start N worker processes (backgrounded by multiprocessing)."""
    # clear pidfile so old stale entries aren't mixed with new
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass

    typer.echo(f"Starting {count} worker process(es)...")
    procs = []
    for i in range(count):
        p = Process(target=worker_process_main, args=(i + 1,))
        p.daemon = False
        p.start()
        procs.append(p)
        # Give a tiny gap so each worker writes its pid
        time.sleep(0.05)

    typer.echo(f"Started {len(procs)} workers. Use 'queuectl worker stop' to signal them to stop.")


@worker_app.command("stop")
def stop_workers():
    """Signal worker processes (from pidfile) with SIGTERM for graceful shutdown."""
    pids = read_pids_from_file()
    if not pids:
        typer.echo("No worker PIDs found.")
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
            except Exception:
                pass
        return

    typer.echo(f"Signaling {len(pids)} worker(s) to stop...")
    stopped = 0
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
            typer.echo(f"Sent SIGTERM to {pid}")
            stopped += 1
        except ProcessLookupError:
            typer.echo(f"PID {pid} not found (already stopped).", err=True)
        except Exception as e:
            typer.echo(f"Error signaling {pid}: {e}", err=True)
    # remove pid file
    try:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
    except Exception:
        pass
    typer.echo(f"Signaled {stopped} worker(s).")


@app.command("monitor")
def start_monitor_dashboard(port: int = typer.Option(MONITOR_PORT, "--port", "-p")):
    """Start the web dashboard (blocks)"""
    server_address = ("", port)
    try:
        httpd = HTTPServer(server_address, QueueMonitorHandler)
        typer.echo(f"Dashboard running at http://127.0.0.1:{port} (Ctrl+C to stop)")
        httpd.serve_forever()
    except KeyboardInterrupt:
        typer.echo("\nDashboard shutting down...")
        try:
            httpd.shutdown()
            httpd.server_close()
        except Exception:
            pass
        typer.echo("Stopped.")
    except Exception as e:
        typer.echo(f"Failed to start monitor: {e}", err=True)
        typer.echo("Try another port with -p <port>", err=True)


@app.command("delete")
def delete_job(job_id: str = typer.Argument(..., help="job id to delete")):
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        if cur.rowcount == 0:
            typer.echo(f"Job {job_id} not found.", err=True)
            raise typer.Exit(code=1)
        conn.commit()
        typer.echo(f"Deleted job {job_id}.")
    except Exception as e:
        typer.echo(f"Error deleting job: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        conn.close()


@dlq_app.command("list")
def dlq_list():
    """List jobs in the Dead Letter Queue."""
    list_jobs(state="dead")


@dlq_app.command("retry")
def dlq_retry(job_id: str = typer.Argument(..., help="id of a dead job to retry")):
    """Move a job from DLQ back to pending (reset attempts)."""
    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
        if not cur.fetchone():
            typer.echo(f"Job {job_id} not found in DLQ.", err=True)
            raise typer.Exit(code=1)
        now = utc_now_iso()
        cur.execute(
            """
            UPDATE jobs
               SET state = 'pending', attempts = 0, updated_at = ?, next_run_at = NULL
             WHERE id = ?
            """,
            (now, job_id),
        )
        conn.commit()
        typer.echo(f"Job {job_id} moved from DLQ to pending.")
    except Exception as e:
        typer.echo(f"Error retrying DLQ job {job_id}: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        conn.close()


@config_app.command("set")
def config_set(key: str, value: str):
    """Set a configuration key (max_retries or backoff_base)."""
    valid_keys = {"max_retries", "backoff_base"}
    if key not in valid_keys:
        typer.echo(f"Invalid key '{key}'. Valid keys: {', '.join(valid_keys)}", err=True)
        raise typer.Exit(code=1)
    try:
        intval = int(value)
    except ValueError:
        typer.echo("Value must be an integer.", err=True)
        raise typer.Exit(code=1)

    if key == "max_retries" and intval < 0:
        typer.echo("max_retries cannot be negative.", err=True)
        raise typer.Exit(code=1)
    if key == "backoff_base" and intval <= 0:
        typer.echo("backoff_base must be positive.", err=True)
        raise typer.Exit(code=1)

    conn = get_db_connection()
    try:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(intval)))
        conn.commit()
        typer.echo(f"Config updated: {key} = {intval}")
    finally:
        conn.close()


if __name__ == "__main__":
    app()
