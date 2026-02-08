# 📘 Databricks Lakehouse Formats – Spark Architecture Labs

This repository contains **hands-on Apache Spark labs** designed to explain **real-world Spark architectures** commonly used in **Databricks-style lakehouse platforms**.

The goal of this repo is to move beyond *hello-world Spark* and help you understand:

* How Spark clusters actually work
* The difference between **clusters**, **workers**, **executors**, and **tasks**
* Why Databricks uses multiple clusters
* How Spark scales in production
* How long-running, shuffle-heavy jobs behave

All labs are **Docker-based**, reproducible, and runnable on a single Linux machine.

---

## 🎯 Who This Repo Is For

This repo is useful if you are:

* Learning Apache Spark beyond basics
* Preparing for Databricks / Spark interviews
* A cloud or data engineer wanting hands-on Spark internals
* Exploring lakehouse architectures (Delta / Iceberg / Parquet)
* Coming from a Databricks background and want to understand *what’s underneath*

---

## 📂 Repository Structure

```
spark/
├── multiple-clusters/
│   ├── docker-compose.yml
│   ├── README.md
│   └── apps/
│
└── single-cluster-multi-worker/
    ├── docker-compose.yml
    ├── README.md
    └── apps/
```

Each subdirectory is a **self-contained lab** with its own README and runnable examples.

---

## 🧪 Available Labs

### 1️⃣ Multiple Spark Clusters (Isolation Model)

**Path**

```
spark/multiple-clusters/
```

**What it demonstrates**

* Running multiple independent Spark clusters
* Each cluster has its own master, workers, and history server
* Jobs are isolated per cluster
* A single Spark job never spans multiple clusters

**Why this matters**
This mirrors:

* Databricks multi-workspace setups
* Team-level isolation
* Dev / Test / Prod separation

➡️ See `spark/multiple-clusters/README.md` for full details.

---

### 2️⃣ Single Spark Cluster with Multiple Workers (Production Model)

**Path**

```
spark/single-cluster-multi-worker/
```

**What it demonstrates**

* One Spark cluster
* Many worker nodes
* A single job using all workers in parallel
* Executor and task distribution

**Why this matters**
This is the **most common real-world Spark architecture**, used by:

* Databricks job clusters
* Spark on YARN
* Spark on Kubernetes (conceptually)

➡️ See `spark/single-cluster-multi-worker/README.md` for full details.

---

## 🧠 Core Concepts Covered

This repo helps you clearly understand:

| Concept                  | Covered |
| ------------------------ | ------- |
| Cluster isolation        | ✅       |
| Worker vs executor       | ✅       |
| Task scheduling          | ✅       |
| Shuffle-heavy workloads  | ✅       |
| Long-running Spark jobs  | ✅       |
| Spark UI analysis        | ✅       |
| Failure & retry behavior | ✅       |

---

## 🧪 Types of Jobs Included

* Simple jobs (WordCount)
* CPU + memory intensive jobs
* Shuffle-heavy aggregations
* Disk IO–intensive workloads
* Multi-minute runtime jobs (5–10+ minutes)

These are **intentionally realistic**, not toy examples.

---

## ⚠️ Notes on Runtime Artifacts

The following are **intentionally excluded from Git**:

* Spark event logs
* Generated data
* Temporary shuffle directories

These are created at runtime and should not be version-controlled.

---

## 🚀 Recommended Learning Path

1. Start with **single-cluster-multi-worker**
2. Observe task distribution in Spark UI
3. Tune partitions and executor settings
4. Move to **multiple-clusters**
5. Understand isolation vs scalability trade-offs

This progression mirrors how Spark is learned in real teams.

---

## 🔮 Possible Extensions

This repo is designed to grow. Natural next steps include:

* Delta Lake examples
* Apache Iceberg examples
* Performance tuning guides
* Spark-on-Kubernetes labs
* Architecture diagrams
* Databricks mapping explanations

---

## 🏁 Summary

This repository is a **practical Spark architecture lab**, not just a code dump.

If you understand everything in this repo, you understand:

* How Spark really works
* Why Databricks is designed the way it is
* How to reason about Spark performance and scaling

---
