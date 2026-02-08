# 📘 README — Multiple Spark Clusters (Isolation Model)

**Path**

```
spark/multiple-clusters/README.md
```

---

## 📘 Spark Multiple Clusters (Isolation Model)

This lab demonstrates how to run **multiple independent Apache Spark clusters** on a single machine using Docker Compose.

Each cluster has:

* Its own **Spark Master**
* Its own **Spark Workers**
* Its own **History Server**
* Complete **execution isolation**

This mirrors **real-world Databricks / enterprise Spark setups** where clusters are isolated by:

* team
* environment (dev / test / prod)
* workload type

---

## 🧠 Key Concept

> **A Spark job always runs on exactly ONE cluster.**
> Spark does **not** distribute a single job across multiple clusters.

Multiple clusters exist for **isolation**, not for parallelizing one job.

---

## 📂 Directory Structure

```
multiple-clusters/
├── docker-compose.yml
├── apps/
│   ├── wordcount.py
│   └── bigdata_job.py
├── data/        # runtime only (ignored by git)
└── eventlog/    # runtime only (ignored by git)
```

---

## ▶️ Start All Clusters

```bash
cd spark/multiple-clusters
docker compose up -d
```

Verify:

```bash
docker ps
```

---

## 🌐 Spark UIs

| Cluster   | Master UI                                      | History UI                                       |
| --------- | ---------------------------------------------- | ------------------------------------------------ |
| Cluster A | [http://localhost:8080](http://localhost:8080) | [http://localhost:18080](http://localhost:18080) |
| Cluster B | [http://localhost:8180](http://localhost:8180) | [http://localhost:18180](http://localhost:18180) |
| Cluster C | [http://localhost:8280](http://localhost:8280) | [http://localhost:18280](http://localhost:18280) |

---

## ▶️ Run a Job on Cluster A

```bash
docker compose exec spark-master-a \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master-a:7077 \
  /opt/spark-apps/wordcount.py
```

### Run the same job on Cluster B

```bash
docker compose exec spark-master-b \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master-b:7077 \
  /opt/spark-apps/wordcount.py
```

Each execution is **completely isolated**.

---

## 🔍 What to Observe in Spark UI

* Each cluster shows **only its own jobs**
* No workers are shared
* Executors exist only within that cluster
* Failures in one cluster do not affect others

---

## ✅ What This Lab Teaches

✔ Cluster isolation
✔ Job-to-cluster binding
✔ Why Spark does *not* span clusters
✔ How Databricks uses multiple clusters
✔ Multi-tenant Spark architecture

---

## 🧪 Recommended Experiments

* Run different jobs on each cluster
* Change worker count per cluster
* Kill one cluster and observe others remain healthy
* Compare job runtimes across clusters

---

## 🏁 Summary

This lab demonstrates **how Spark clusters are isolated execution environments**.
It is ideal for understanding **real-world Spark platform architecture**.

---
