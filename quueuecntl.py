import typer
import sqlite3
import json
import subprocess
import time
import os
import signal
import datetime
import sys
from multiprocessing import Process, current_process
from http.server import SimpleHTTPRequestHandler, HTTPServer
import threading # Used to run the web server non-blockingly

# --- Configuration Constants ---
DB_FILE = "queuectl.db"
PID_FILE = "queuectl.workers.pids"
MONITOR_PORT = 8000

APP = typer.Typer(name="queuectl", help="A CLI-based background job queue system.")
WORKER_APP = typer.Typer(name="worker", help="Manage worker processes.")
DLQ_APP = typer.Typer(name="dlq", help="Manage the Dead Letter Queue.")
CONFIG_APP = typer.Typer(name="config", help="Manage system configuration.")

# Register sub-applications
APP.add_typer(WORKER_APP)
APP.add_typer(DLQ_APP)
APP.add_typer(CONFIG_APP)

# --- Database & Persistence Layer ---

def get_db_connection(timeout=5):
    """Establishes a connection to the SQLite database."""
    # Setting timeout is crucial for concurrency/locking in SQLite
    return sqlite3.connect(DB_FILE, timeout=timeout)

def initialize_db():
    """Initializes the jobs and config tables if they don't exist."""
    conn = None 
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Jobs Table: Now explicitly includes all required fields
        cursor.execute("""
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
        """)

        # Config Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        # Insert default configuration if not present
        cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ('backoff_base', '2'))
        cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ('max_retries', '3'))

        conn.commit()
    except Exception as e:
        typer.echo(f"Error initializing database: {e}", err=True)
    finally:
        if conn:
            conn.close()

def get_config(key: str) -> str:
    """Retrieves a configuration value."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM config WHERE key = ?", (key,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

# --- Worker Logic ---

def exponential_backoff(attempts: int, base: int) -> int:
    """Calculates the delay in seconds using exponential backoff."""
    return base ** attempts

def worker_process_main(worker_id: int):
    """The main loop for an individual worker process."""
    typer.echo(f"Worker {worker_id} (PID: {os.getpid()}) started.")
    
    # Store PID for cleanup/stop command
    with open(PID_FILE, 'a') as f:
        f.write(f"{os.getpid()}\n")

    running = True
    
    def signal_handler(signum, frame):
        nonlocal running
        typer.echo(f"\nWorker {worker_id} (PID: {os.getpid()}) received stop signal. Finishing current job...")
        running = False
    
    signal.signal(signal.SIGTERM, signal_handler)

    while running:
        job = acquire_job()
        if job:
            typer.echo(f"[{worker_id}] Processing Job: {job['id']} (Attempt: {job['attempts'] + 1})")
            
            # --- 1. Execute Command ---
            try:
                result = subprocess.run(
                    job['command'],
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=600 
                )
                exit_code = result.returncode
            except Exception as e:
                typer.echo(f"[{worker_id}] Job {job['id']} failed during execution: {e}", err=True)
                exit_code = 1 

            # --- 2. Update Job State based on Exit Code ---
            update_job_state(job, exit_code, result.stdout if 'result' in locals() else None)

            time.sleep(0.5) 
        else:
            # DEBUGGING HINT: Job acquisition failed. It could be due to contention or backoff delay.
            if worker_id == 1 and time.time() % 10 < 1: 
                 typer.echo(f"[{worker_id}] DEBUG: Job acquisition failed. (Contention or Backoff Delay).")
            time.sleep(1)

    # Remove PID when gracefully shutting down
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                pids = [p.strip() for p in f.readlines()]
            
            pids = [p for p in pids if p != str(os.getpid())]
            
            with open(PID_FILE, 'w') as f:
                f.write('\n'.join(pids) + '\n')
        except Exception:
            pass 

    typer.echo(f"Worker {worker_id} (PID: {os.getpid()}) shutting down gracefully.")


def acquire_job() -> dict | None:
    """Atomically acquires a pending or failed/retryable job."""
    conn = None
    job_data = None
    
    try:
        conn = get_db_connection()
        conn.isolation_level = 'DEFERRED' 
        cursor = conn.cursor()
        
        # Check if the job is ready to be picked up (next_run_at <= now)
        cursor.execute("""
            SELECT id, command, attempts, max_retries, created_at
            FROM jobs
            WHERE state IN ('pending', 'failed') 
            AND (next_run_at IS NULL OR next_run_at <= datetime('now'))
            ORDER BY next_run_at ASC, created_at ASC
            LIMIT 1
        """)
        job_row = cursor.fetchone()

        if job_row:
            job_id, command, attempts, max_retries, created_at = job_row
            
            now = datetime.datetime.now(datetime.timezone.utc).isoformat()
            # Atomically mark job as processing
            cursor.execute("""
                UPDATE jobs SET state = 'processing', updated_at = ?
                WHERE id = ? AND state IN ('pending', 'failed')
            """, (now, job_id))

            if cursor.rowcount == 1:
                conn.commit()
                # Concurrency Lock Successful
                job_data = {
                    'id': job_id, 
                    'command': command, 
                    'attempts': attempts, 
                    'max_retries': max_retries, 
                    'created_at': created_at
                }
            else:
                # Failed to acquire due to race condition, rollback
                conn.rollback()
                job_data = None
        
    except sqlite3.OperationalError:
        if conn: conn.rollback()
        job_data = None
    except Exception as e:
        if conn: conn.rollback()
        typer.echo(f"Error acquiring job: {e}", err=True)
        job_data = None
    finally:
        if conn: conn.close()
    
    return job_data

def update_job_state(job: dict, exit_code: int, output: str):
    """Handles state transition, retry logic, and DLQ assignment."""
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc)
    
    try:
        if exit_code == 0:
            # --- Successful Completion ---
            new_state = 'completed'
            typer.echo(f" Job {job['id']} completed successfully.")
            
        else:
            # --- Failure Handling ---
            attempts = job['attempts'] + 1 # This is the total number of times it has executed
            max_retries = job['max_retries']
            
            # --- DLQ DEBUG LOG ---
            typer.echo(f" Job {job['id']} DEBUG: Checking DLQ. Current Attempts: {attempts}. Max Retries: {max_retries}.")
            
            # DLQ Check: If the current attempt count exceeds max_retries, move to dead.
            if attempts > max_retries: 
                # --- Move to Dead Letter Queue (DLQ) ---
                new_state = 'dead'
                next_run_at = None
                typer.echo(f" Job {job['id']} DLQ TRIGGERED: Failed on Attempt {attempts}/{max_retries}. Moving to DEAD.")
            else:
                # --- Retry with Backoff ---
                base = int(get_config('backoff_base'))
                delay = exponential_backoff(attempts, base)
                next_run_at = (now + datetime.timedelta(seconds=delay)).isoformat()
                new_state = 'failed'
                typer.echo(f" Job {job['id']} FAILED. Retrying in {delay} seconds (Attempt {attempts}/{max_retries}).")

            # Update job with new state, attempt count, and next run time
            cursor.execute("""
                UPDATE jobs 
                SET state = ?, updated_at = ?, attempts = ?, next_run_at = ?
                WHERE id = ? AND state = 'processing'
            """, (new_state, now.isoformat(), attempts, next_run_at, job['id']))
            
            # CRITICAL: If the DLQ update fails, we log it, but the job remains stuck in 'processing' 
            # or 'failed', allowing a future worker to re-attempt the acquisition.
            if cursor.rowcount == 0:
                typer.echo(f" WARNING: Job {job['id']} state update failed (likely race condition).", err=True)
                # If the commit fails, the job remains in 'processing', allowing another worker to try.


        # Final state update for successful jobs
        if new_state == 'completed':
             cursor.execute("""
                UPDATE jobs 
                SET state = ?, updated_at = ?, attempts = ?, next_run_at = NULL
                WHERE id = ? AND state = 'processing'
            """, (new_state, now.isoformat(), job['attempts'] + 1, job['id']))
             
             if cursor.rowcount == 0:
                 typer.echo(f" WARNING: Job {job['id']} state update failed (likely race condition).", err=True)


        conn.commit()

    except Exception as e:
        typer.echo(f"Error updating job state for {job['id']}: {e}", err=True)
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

# --- Web Dashboard Implementation ---

def get_job_data_for_monitor():
    """Fetches all job data for the monitor dashboard."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at 
        FROM jobs 
        ORDER BY created_at DESC
    """)
    jobs = cursor.fetchall()

    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    stats = dict(cursor.fetchall())
    conn.close()
    
    return jobs, stats

def generate_html(jobs, stats):
    """Generates the HTML content for the dashboard."""
    
    # Calculate active workers (borrowing logic from show_status)
    active_workers = 0
    if os.path.exists(PID_FILE):
        with open(PID_FILE, 'r') as f:
            pids = [int(p.strip()) for p in f.readlines() if p.strip().isdigit()]
        active_workers = len(pids)

    # Table Header
    html_content = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>QueueCTL Dashboard</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="5">
        <style>
            body {{ font-family: sans-serif; margin: 20px; background-color: #f4f4f9; }}
            .container {{ max-width: 1200px; margin: auto; background-color: white; padding: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; }}
            .stats {{ display: flex; flex-wrap: wrap; margin-bottom: 20px; }}
            .stat-box {{ background-color: #e0eaff; padding: 15px; margin: 10px 5px 0 0; border-radius: 4px; flex-grow: 1; min-width: 150px; text-align: center; }}
            .stat-box h3 {{ margin: 0; font-size: 1.5em; color: #1a5a9c; }}
            .stat-box p {{ margin: 0; font-size: 0.9em; color: #555; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
            th, td {{ padding: 12px 15px; border: 1px solid #ddd; text-align: left; font-size: 0.9em; word-wrap: break-word; }}
            th {{ background-color: #333; color: white; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .state-pending {{ color: #ff9800; font-weight: bold; }}
            .state-processing {{ color: #2196f3; font-weight: bold; }}
            .state-completed {{ color: #4caf50; font-weight: bold; }}
            .state-failed {{ color: #ff5722; font-weight: bold; }}
            .state-dead {{ color: #9c27b0; font-weight: bold; }}
            .worker-active {{ background-color: #c8e6c9; color: #2e7d32; }}
            .worker-inactive {{ background-color: #ffcdd2; color: #c62828; }}
        </style>
    </head>
    <body>
    <div class="container">
        <h1>QueueCTL Monitor Dashboard</h1>
        <p>Last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. Auto-refreshing every 5 seconds.</p>

        <h2>System Summary</h2>
        <div class="stats">
            <div class="stat-box">
                <p>Total Jobs</p>
                <h3>{sum(stats.values())}</h3>
            </div>
            <div class="stat-box">
                <p>Active Workers</p>
                <h3 class="{ 'worker-active' if active_workers > 0 else 'worker-inactive' }">{active_workers}</h3>
            </div>
            <div class="stat-box">
                <p>Pending</p>
                <h3>{stats.get('pending', 0)}</h3>
            </div>
            <div class="stat-box">
                <p>Processing</p>
                <h3>{stats.get('processing', 0)}</h3>
            </div>
            <div class="stat-box">
                <p>Failed (Retry)</p>
                <h3>{stats.get('failed', 0)}</h3>
            </div>
            <div class="stat-box">
                <p>Dead (DLQ)</p>
                <h3>{stats.get('dead', 0)}</h3>
            </div>
        </div>

        <h2>Job List (Most Recent First)</h2>
        <table>
            <thead>
                <tr>
                    <th>ID</th>
                    <th>State</th>
                    <th>Attempts</th>
                    <th>Max Retries</th>
                    <th>Command</th>
                    <th>Created At</th>
                    <th>Updated At</th>
                    <th>Next Run</th>
                </tr>
            </thead>
            <tbody>
    """
    
    # Table Rows
    for job in jobs:
        (job_id, command, state, attempts, max_retries, created_at, updated_at, next_run_at) = job
        state_class = f"state-{state}"
        
        # Display command concisely
        display_command = command if len(command) < 30 else command[:27] + '...'
        
        html_content += f"""
        <tr>
            <td>{job_id}</td>
            <td class="{state_class}">{state.upper()}</td>
            <td>{attempts} / {max_retries}</td>
            <td>{max_retries}</td>
            <td><code>{display_command}</code></td>
            <td>{created_at[:19].replace('T', ' ')}</td>
            <td>{updated_at[:19].replace('T', ' ')}</td>
            <td>{next_run_at[:16].replace('T', ' ') if next_run_at else 'N/A'}</td>
        </tr>
        """
    
    html_content += """
            </tbody>
        </table>
        <p style="margin-top: 20px;">*All times are UTC ISO 8601.</p>
    </div>
    </body>
    </html>
    """
    return html_content

class QueueMonitorHandler(SimpleHTTPRequestHandler):
    """Custom HTTP handler to serve job data."""
    
    def do_GET(self):
        if self.path == '/':
            jobs, stats = get_job_data_for_monitor()
            html = generate_html(jobs, stats)
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(html.encode('utf-8'))
        else:
            self.send_error(404)


@APP.command(name="monitor")
def start_monitor_dashboard(
    port: int = typer.Option(MONITOR_PORT, "--port", "-p", help="Port for the dashboard web server.")
):
    """Starts a minimal web dashboard for real-time job monitoring."""
    
    server_address = ('', port)
    
    # Use a separate thread for the server to allow the CLI command to exit if needed
    try:
        httpd = HTTPServer(server_address, QueueMonitorHandler)
        typer.echo(f"🚀 QueueCTL Dashboard running on http://127.0.0.1:{port}")
        typer.echo(f"Press Ctrl+C to stop the dashboard.")
        
        # Start the server (this is blocking, so we run it directly)
        httpd.serve_forever()
        
    except KeyboardInterrupt:
        typer.echo("\nDashboard shutting down...")
        if 'httpd' in locals():
            httpd.shutdown()
            httpd.server_close()
        typer.echo("Dashboard stopped.")
    except Exception as e:
        typer.echo(f"Error starting dashboard on port {port}: {e}", err=True)
        typer.echo("Try selecting a different port using '-p <PORT>'.", err=True)

# --- CLI Command Implementations ---

@APP.callback()
def main_callback(ctx: typer.Context):
    """Initializes the database before any command runs."""
    initialize_db()

@APP.command(name="enqueue")
def enqueue_job(
    job_data_json: str = typer.Argument("", help='Job data in JSON format, e.g., \'{"id":"job1","command":"sleep 2"}\'')
):
    """
    Adds a new job to the queue. 
    Accepts job data as a command-line argument or via piped standard input (stdin).
    """
    conn = None 
    job_id = None 
    
    # 1. Check for piped input (Stdin)
    if not job_data_json:
        try:
            job_data_json = sys.stdin.read().strip()
            if not job_data_json:
                typer.echo("Error: No job data provided as argument or via stdin.", err=True)
                raise typer.Exit(code=1)
        except Exception:
            typer.echo("Error reading job data from standard input.", err=True)
            raise typer.Exit(code=1)

    try:
        data = json.loads(job_data_json) 
        job_id = data.get("id")
        command = data.get("command")
        max_retries = data.get("max_retries", int(get_config('max_retries')))
        
        if not job_id or not command:
            typer.echo("Error: Job ID and command fields are required.", err=True)
            raise typer.Exit(code=1)

        conn = get_db_connection()
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at, next_run_at) 
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, command, 'pending', 0, max_retries, now, now, None)
        )
        conn.commit()
        typer.echo(f"Successfully enqueued job: {job_id} ('{command}') with max retries: {max_retries}")

    except json.JSONDecodeError:
        typer.echo("Error: Invalid JSON format provided. Remember keys and strings must use double quotes.", err=True)
        typer.echo(f"Received string was: {job_data_json}", err=True) 
        raise typer.Exit(code=1)
    except sqlite3.IntegrityError:
        typer.echo(f"Error: Job ID '{job_id}' already exists.", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"An unexpected error occurred: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        if conn: conn.close()


@APP.command(name="status")
def show_status():
    """Shows summary of all job states and active workers."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # 1. Job Counts
    cursor.execute("SELECT state, COUNT(*) FROM jobs GROUP BY state")
    job_counts = dict(cursor.fetchall())
    conn.close()

    typer.echo("\n📊 QueueCTL Status Summary")
    typer.echo("==============================")
    
    states = ['pending', 'processing', 'completed', 'failed', 'dead']
    for state in states:
        count = job_counts.get(state, 0)
        typer.echo(f" {state.title()} Jobs: \t{count}")
    typer.echo(f" Total Jobs: \t{sum(job_counts.values())}")
    typer.echo("==============================")

    # 2. Active Workers
    active_workers = 0
    if os.path.exists(PID_FILE):
        with open(PID_FILE, 'r') as f:
            pids = [int(p.strip()) for p in f.readlines() if p.strip().isdigit()]
        
        # Check if PIDs are still running
        running_pids = []
        for pid in pids:
            try:
                os.kill(pid, 0)
                running_pids.append(pid)
            except ProcessLookupError:
                continue
            except Exception:
                continue

        active_workers = len(running_pids)
        # Rewrite PID file with only active PIDs
        with open(PID_FILE, 'w') as f:
            f.write('\n'.join(map(str, running_pids)) + '\n')

    typer.echo(f" 👷 Active Workers: {active_workers}")
    typer.echo("==============================\n")


@APP.command(name="list")
def list_jobs(state: str = typer.Option("pending", "--state", "-s", help="Filter by job state (pending, processing, completed, failed, dead).")):
    """Lists jobs by state, showing full specification."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Query updated to fetch ALL 8 required fields
    cursor.execute("""
        SELECT id, command, state, attempts, max_retries, created_at, updated_at, next_run_at 
        FROM jobs 
        WHERE state = ? 
        ORDER BY created_at DESC
    """, (state.lower(),))
    jobs = cursor.fetchall()
    conn.close()

    if not jobs:
        typer.echo(f"No '{state}' jobs found.")
        return

    typer.echo(f"\n--- Listing {len(jobs)} Jobs in '{state.upper()}' State ---\n")
    typer.echo(f"{'ID':<10} | {'CMD':<25} | {'STATE':<12} | {'ATT':<4}/{'MAX':<4} | {'CREATED':<19} | {'UPDATED':<19} | {'NEXT RUN':<19}")
    typer.echo("-" * 115)
    
    for job in jobs:
        job_id, command, current_state, attempts, max_retries, created_at, updated_at, next_run_at = job
        
        # Format the output to ensure all 8 fields are displayed
        display_command = command if len(command) < 25 else command[:22] + '...'
        display_next_run = next_run_at[:19] if next_run_at else 'N/A'
        
        typer.echo(
            f"{job_id:<10} | {display_command:<25} | {current_state:<12} | {attempts:<4}/{max_retries:<4} | "
            f"{created_at[:19]:<19} | {updated_at[:19]:<19} | {display_next_run:<19}"
        )
    typer.echo("\n-------------------------------------------------------------------------------------------------------------------\n")


@WORKER_APP.command(name="start")
def start_workers(
    count: int = typer.Option(1, "--count", "-c", min=1, help="Number of workers to start.")
):
    """Starts one or more workers."""
    if os.path.exists(PID_FILE):
        try:
            os.remove(PID_FILE)
        except:
            pass
            
    typer.echo(f"Starting {count} worker processes...")
    processes = []
    
    for i in range(count):
        p = Process(target=worker_process_main, args=(i + 1,))
        p.start()
        processes.append(p)
    
    typer.echo(f"Workers started. To stop them, run 'queuectl worker stop'")


@WORKER_APP.command(name="stop")
def stop_workers():
    """Stops running workers gracefully."""
    if not os.path.exists(PID_FILE):
        typer.echo("No worker PID file found. Are any workers running?", err=True)
        return

    with open(PID_FILE, 'r') as f:
        pids = []
        for line in f.readlines():
            stripped_line = line.strip()
            if stripped_line.isdigit():
                pids.append(int(stripped_line))

    if not pids:
        typer.echo("No active worker PIDs found in file.")
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        return
    
    typer.echo(f"Attempting graceful shutdown for {len(pids)} workers...")
    
    successfully_stopped = 0
    
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM) 
            typer.echo(f"Sent SIGTERM to PID {pid}.")
            successfully_stopped += 1
        except ProcessLookupError:
            typer.echo(f"PID {pid} not found (already stopped).", err=True)
        except Exception as e:
            typer.echo(f"Error stopping PID {pid}: {e}", err=True)

    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
        
    typer.echo(f"Stop process completed. Successfully signaled {successfully_stopped} workers.")


@APP.command(name="delete")
def delete_job(job_id: str = typer.Argument(..., help="ID of the job to permanently delete.")):
    """Permanently deletes a job from the queue regardless of its state."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        
        if cursor.rowcount == 0:
            typer.echo(f"Error: Job ID '{job_id}' not found in the queue.", err=True)
            raise typer.Exit(code=1)
            
        conn.commit()
        typer.echo(f"Job ID '{job_id}' successfully deleted.")

    except Exception as e:
        typer.echo(f"An unexpected error occurred during job deletion: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        if conn: conn.close()


@DLQ_APP.command(name="list")
def dlq_list():
    """Lists all jobs in the Dead Letter Queue (DLQ)."""
    list_jobs(state="dead")


@DLQ_APP.command(name="retry")
def dlq_retry(job_id: str = typer.Argument(..., help="ID of the job to retry from the DLQ.")):
    """Retries a job from the DLQ by moving it to the pending state."""
    conn = get_db_connection()
    cursor = conn.cursor()
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
    try:
        cursor.execute("SELECT id FROM jobs WHERE id = ? AND state = 'dead'", (job_id,))
        if not cursor.fetchone():
            typer.echo(f"Error: Job ID '{job_id}' not found in the Dead Letter Queue (DLQ).", err=True)
            raise typer.Exit(code=1)

        cursor.execute("""
            UPDATE jobs 
            SET state = 'pending', attempts = 0, updated_at = ?, next_run_at = NULL
            WHERE id = ?
        """, (now, job_id))
        
        conn.commit()
        typer.echo(f"Job {job_id} successfully moved from DLQ to 'pending' state. It will be picked up by a worker shortly.")

    except Exception as e:
        typer.echo(f"An unexpected error occurred during DLQ retry: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        if conn: conn.close()


@CONFIG_APP.command(name="set")
def config_set(key: str, value: str):
    """Manages configuration (retry, backoff base, etc.)."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    valid_keys = ['max_retries', 'backoff_base']
    if key not in valid_keys:
        typer.echo(f"Error: Invalid configuration key '{key}'. Valid keys are: {', '.join(valid_keys)}", err=True)
        raise typer.Exit(code=1)

    try:
        int_value = int(value)
        
        # FIX: Allow max_retries >= 0 (0 means no retries, straight to DLQ)
        if key == 'max_retries' and int_value < 0:
            typer.echo(f"Error: Configuration value cannot be negative.", err=True)
            raise typer.Exit(code=1)
        
        # Ensure backoff_base is positive
        if key == 'backoff_base' and int_value <= 0:
            typer.echo(f"Error: Backoff base must be a positive integer.", err=True)
            raise typer.Exit(code=1)

        cursor.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(int_value)))
        conn.commit()
        typer.echo(f"Configuration updated: {key} set to {int_value}")

    except ValueError:
        typer.echo("Error: Value must be a valid integer.", err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.echo(f"An unexpected error occurred: {e}", err=True)
        raise typer.Exit(code=1)
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    APP()
