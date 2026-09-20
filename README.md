# Apache Flink Jobs

A collection of **Apache Flink jobs** for processing and transforming data using distributed stream processing.

## 📌 Project Overview

This project focuses on building and running data processing jobs with **Apache Flink**.

The main goal is to practice:

* Flink Jobs
* Stream Processing
* Distributed Data Processing
* Data Transformation
* Job Submission and Monitoring
* Working with Flink's JobManager and TaskManagers

---

## 🏗️ Architecture

```text
                ┌───────────────┐
                │     Source    │
                │  Data / Event │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Apache Flink  │
                │     Job       │
                └───────┬───────┘
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
        Transformation        Processing
              │                   │
              └─────────┬─────────┘
                        ▼
                ┌───────────────┐
                │   Destination │
                │ DB / File /   │
                │    System     │
                └───────────────┘
```

---

## 🧰 Technologies

* **Apache Flink**
* **Java / Python** depending on the job implementation
* **Linux**
* **Git**
* **Distributed Processing**

---

## 📂 Project Structure

```text
flink_jobs/
│
├── README.md
│
├── jobs/
│   └── ...
│
├── data/
│   └── ...
│
├── config/
│   └── ...
│
└── scripts/
    └── ...
```

> The exact structure may vary depending on the implemented Flink jobs.

---

## ⚙️ Prerequisites

Before running the project, make sure you have:

1. Apache Flink installed and configured.
2. Java installed if the jobs are Java-based.
3. Python installed if the jobs are PyFlink-based.
4. Flink services running.
5. Git installed.

Check Java:

```bash
java -version
```

Check Flink:

```bash
flink --version
```

---

## 🚀 Running Flink

Start the Flink cluster according to your Flink installation.

Typical components are:

```text
JobManager
    │
    ├── TaskManager
    ├── TaskManager
    └── TaskManager
```

The **JobManager** coordinates the execution of Flink jobs, while **TaskManagers** execute the actual tasks.

---

## ▶️ Submit a Job

A typical Flink job can be submitted using:

```bash
flink run <path-to-job.jar>
```

For example:

```bash
flink run jobs/my-flink-job.jar
```

For a Python/PyFlink job:

```bash
flink run -py jobs/my_flink_job.py
```

---

## 📊 Monitoring Jobs

After submitting a job, you can monitor it through the **Flink Web UI**.

The Web UI provides information about:

* Running Jobs
* Job Status
* Task Managers
* Parallelism
* Operators
* Records processed
* Checkpoints
* Job execution details

Typical Flink Web UI:

```text
http://localhost:8081
```

---

## 🔄 Job Lifecycle

```text
Create Job
    │
    ▼
Submit Job
    │
    ▼
JobManager
    │
    ▼
TaskManagers
    │
    ▼
Execute Operators
    │
    ▼
Process Data
    │
    ▼
Output / Sink
```

---

## 🧠 Key Flink Concepts

### JobManager

The main coordinator of the Flink cluster.

Responsibilities include:

* Scheduling jobs
* Managing job execution
* Coordinating TaskManagers
* Handling failures and recovery

### TaskManager

Workers responsible for executing Flink tasks.

They execute the actual data processing operations assigned by the JobManager.

### Parallelism

Controls how many parallel instances of an operation can run.

Example:

```text
Parallelism = 3

             ┌── Task 1
Input ───────┼── Task 2
             └── Task 3
```

Higher parallelism can allow more data to be processed concurrently, depending on the workload and available resources.

---

## 🛠️ Git Workflow

Check the current status:

```bash
git status
```

Add changes:

```bash
git add .
```

Commit changes:

```bash
git commit -m "Add Flink jobs"
```

Push changes:

```bash
git push origin main
```

View commit history:

```bash
git log --oneline
```

---

## 🎯 Project Goals

This project is part of my **Data Engineering learning journey** and is focused on gaining practical experience with:

* Distributed Systems
* Stream Processing
* Apache Flink
* Job Management
* Parallel Processing
* Data Engineering pipelines

---

## 👨‍💻 Author

**Abdullah Emad**

Data Engineering Learning Project
