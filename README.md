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

![Job Enqueue](https://github.com/VishalS-14/QueueCTL/blob/9c00b44fb4658afac3c3376a50448f6b298cc3c4/Enqueue.png)


### 👷 **Worker Execution & Job Processing**

This section explains how **QueueCTL’s Worker System** manages background jobs — including how it executes commands, handles failures, performs retries using exponential backoff, and updates job states in real-time.

---

## ⚙️ **Overview**

Workers are the backbone of **QueueCTL**.  
They are independent background processes that continuously fetch and execute jobs from the queue.  
Each worker runs in its own process and operates safely alongside others using the shared SQLite database.

---

## ▶️ **Starting Worker Processes**
![Workers assigned](https://github.com/VishalS-14/QueueCTL/blob/0054851d4a727240aa60ed6863d4907a610c4ad7/workers_assigned.png)

### ⚰️ Dead Letter Queue (DLQ) Listing

The **Dead Letter Queue** stores jobs that permanently failed after all retry attempts.  
You can list all DLQ jobs using:

![DLQ list](https://github.com/VishalS-14/QueueCTL/blob/0054851d4a727240aa60ed6863d4907a610c4ad7/dlq_list.png)

### 🔁 Retried Job from DLQ to Pending

Jobs in the **Dead Letter Queue (DLQ)** can be retried manually by moving them back to the **pending** state.  
Use the command below to requeue a DLQ job:

![dead state to pending](https://github.com/VishalS-14/QueueCTL/blob/0054851d4a727240aa60ed6863d4907a610c4ad7/dlq_retry.png)

### ⚙️ Multiple Jobs Running Concurrently

QueueCTL supports running **multiple worker processes** to handle several jobs at once.  
Each worker picks different jobs from the queue, allowing true parallel job execution.

![Multiple jobs](https://github.com/VishalS-14/QueueCTL/blob/0054851d4a727240aa60ed6863d4907a610c4ad7/multiple_jobs.png)

### ⚙️ Config Management — Max Retries Updated

QueueCTL lets you update configuration settings like **max retries** and **backoff base** directly from the CLI.  
These settings control how many times a job will retry and how long it waits between retries.

![configuration updated](https://github.com/VishalS-14/QueueCTL/blob/630ed3854e51f2b60855e918c7d91bc9a1e795b2/config.png)

### 📊 System Status Overview

The **System Status** command shows a real-time summary of all jobs and active workers.  
It helps you monitor how many jobs are **pending**, **processing**, **completed**, **failed**, or in the **DLQ**.

![Status](https://github.com/VishalS-14/QueueCTL/blob/0054851d4a727240aa60ed6863d4907a610c4ad7/final_status.png)

