# 📘 README — Single Spark Cluster with Multiple Workers (Production Model)

**Path**

```
spark/single-cluster-multi-worker/README.md
```

---

## 📘 Spark Single Cluster with Multiple Workers

This lab demonstrates the **most common production Spark architecture**:

> **One Spark cluster with many worker nodes processing a single job in parallel.**

This is how Spark is used in:

* Databricks job clusters
* On-prem Spark standalone
* Spark on YARN
* Spark on Kubernetes (conceptually)

---

## 🧠 Key Concept

> **Parallelism in Spark comes from WORKERS and EXECUTORS, not from multiple clusters.**

A single job:

* Runs on one cluster
* Uses all workers in that cluster
* Scales horizontally with more workers

---

## 📂 Directory Structure

```
single-cluster-multi-worker/
├── docker-compose.yml
├── apps/
│   ├── wordcount.py
│   └── bigdata_job.py
├── data/        # runtime only (ignored by git)
└── eventlog/    # runtime only (ignored by git)
```

---

## ▶️ Start Cluster with 10 Workers

```bash
cd spark/single-cluster-multi-worker
docker compose up -d --scale spark-worker=10
```

Verify:

```bash
docker ps
```

---

## 🌐 Spark UIs

* **Spark Master UI** → [http://localhost:8080](http://localhost:8080)
* **Spark History UI** → [http://localhost:18080](http://localhost:18080)

You should see:

```
Alive Workers: 10
```

---

## ▶️ Run a Simple Job

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/wordcount.py
```

---

## ▶️ Run a Big Data Job (5+ minutes)

```bash
docker compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.speculation=false \
  --conf spark.sql.shuffle.partitions=200 \
  /opt/spark-apps/bigdata_job.py
```

---

## 🔍 What to Observe in Spark UI

### Executors Tab

* Executors spread across multiple workers
* Tasks executed in parallel
* CPU & memory utilization across workers

### Stages Tab

* Shuffle-heavy stages
* Long-running tasks
* Wide transformations (groupBy, sort)

---

## 🧠 Important Insight

> **Workers are fixed. Executors are dynamic.**

* You always have the same number of workers
* Spark creates executors based on workload
* More partitions ⇒ more tasks ⇒ more parallelism

---

## ✅ What This Lab Teaches

✔ Real Spark scaling model
✔ Worker vs executor behavior
✔ Task scheduling
✔ Shuffle cost & performance
✔ Why single cluster scales better than multiple clusters

---

## 🧪 Recommended Experiments

* Compare runtime with 2 vs 10 workers
* Change `spark.sql.shuffle.partitions`
* Adjust executor cores & memory
* Kill a worker and observe task retries
* Monitor shuffle read/write sizes

---

## 🏁 Summary

This lab demonstrates **how Spark actually scales in production**:

* One cluster
* Many workers
* One job using all available resources

This is the **core Spark execution model**.

---


