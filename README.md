# QueueCTL
# ⚙️ **QueueCTL — Background Job Queue System**

A **CLI-based background job management system** built with **Python, Typer, and SQLite** that handles job execution, retries, workers, and a Dead Letter Queue (DLQ).  
Developed as part of a backend engineering challenge to demonstrate background job orchestration and fault-tolerant design.

---

## 🧩 **Overview**

**QueueCTL** enables users to enqueue background jobs, execute them asynchronously through worker processes, automatically retry failed jobs using **exponential backoff**, and persist job data even after restarts.  

It provides a **CLI-driven workflow** and an optional **web-based dashboard** to monitor the system in real time.

---

## 🚀 **It Supports**

- Background job execution  
- Multiple concurrent worker processes  
- Automatic retries with exponential backoff  
- Dead Letter Queue (DLQ) for permanently failed jobs  
- Persistent storage using SQLite  
- CLI-based configuration and monitoring  
- Graceful worker shutdown and process management  
- **Real-Time Web Dashboard** for visual job tracking  

---

## 🧠 **Tech Stack**

| Layer | Technology |
|-------|-------------|
| **Language** | Python 3.10+ |
| **Framework** | Typer (CLI framework) |
| **Database** | SQLite |
| **Concurrency** | Python multiprocessing |
| **Web Interface** | Built-in `http.server` |
| **Process Management** | `subprocess`, `signal`, `threading` |

---

## 🧩 **Core Features**

- **Enqueue background jobs** directly from CLI  
- **Multiple worker processes** to handle jobs concurrently  
- **Automatic retries** with **exponential backoff** (configurable)  
- **Dead Letter Queue (DLQ)** for permanently failed jobs  
- **Persistent SQLite storage** — survives restarts  
- **Web Dashboard** at `http://127.0.0.1:8000` for monitoring  
- **CLI interface** for all operations (`enqueue`, `status`, `list`, `config`, `worker`, `dlq`)  
- **Configurable retry count and backoff base** via CLI  
- **Graceful worker shutdown** with safe cleanup  

---

## 🧪 **Demonstration & Screenshots**

Below are demonstration scenarios showing the project in action, with placeholders for screenshots.

---

### **1️⃣ Job Enqueued Successfully**

Jobs were successfully added to the queue and listed under the **Pending** state.

🖼️ *Job Enqueued Successfully Screenshot*

```bash
echo '{"id":"job1","command":"echo Hello from job1"}' | python queuectl.py enqueue
