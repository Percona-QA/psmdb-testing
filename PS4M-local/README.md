# PS4M-local

Local Docker Compose stack for **Percona Search for MongoDB (PS4M)**: a single-node PSMDB replica set with `mongot` for MongoDB Search.

| Service | Image | Host ports |
|---------|-------|------------|
| `mongod` | `perconalab/percona-server-mongodb:8.3.4-1` | `27017` |
| `mongot` | `perconalab/percona-server-mongodb-mongot:0.51.0-1` | `27028` (gRPC), `8080` (readiness) |

Replica set name: `rs`. Config lives under `config/` (`mongod.conf`, `mongot.yml`, shared `keyfile`).

On first start, mongod creates an admin user for client access:

| User | Password | Role |
|------|----------|------|
| `root` | `root` | `root` on `admin` |

## Prerequisites

- Docker Engine with Compose v2
- `mongosh` installed on the host

## Quick start

```bash
cd PS4M-local
docker compose up -d
docker compose ps
```

Wait until both services are healthy:

```bash
mongosh "mongodb://root:root@127.0.0.1:27017/?authSource=admin&directConnection=true" \
  --quiet --eval 'db.hello().isWritablePrimary'

curl -fsS http://127.0.0.1:8080/ready
```

Stop (keep data volumes) or remove volumes too:

```bash
docker compose down
docker compose down -v
```

If volumes already exist from an older run without the `root` user, recreate them with `docker compose down -v`.

## Connect

Open an interactive shell with local `mongosh`:

```bash
mongosh "mongodb://root:root@127.0.0.1:27017/?authSource=admin&directConnection=true"
```

All examples below are run inside that session. Snippets use `db.getSiblingDB(...)` instead of `use`, so each block can be pasted and run as a whole.

---

## Example: full-text search (`$search`)

Full-text search indexes document fields with Lucene. `mongot` builds the index and serves `$search` queries.

### 1. Create the search index

The collection must exist before you create a search index. Dynamic mappings index all supported field types automatically.

```javascript
db = db.getSiblingDB("search_demo")
db.movies.drop()
db.createCollection("movies")

db.movies.createSearchIndex(
  "default",
  { mappings: { dynamic: true } }
)

while (true) {
  const idxs = db.movies.getSearchIndexes()
  if (idxs.length && idxs.every(i => i.status === "READY")) break
  print("index status:", JSON.stringify(idxs))
  sleep(2000)
}
```

### 2. Upload data

```javascript
db = db.getSiblingDB("search_demo")

db.movies.insertMany([
  { title: "The Matrix", plot: "A computer hacker learns about reality" },
  { title: "Inception", plot: "Dreams within dreams" },
  { title: "The Martian", plot: "An astronaut is stranded on Mars" }
])
```

Give mongot a moment to sync new documents, then confirm:

```javascript
db = db.getSiblingDB("search_demo")
db.movies.getSearchIndexes()
```

### 3. Aggregate with `$search`

```javascript
db = db.getSiblingDB("search_demo")

db.movies.aggregate([
  {
    $search: {
      text: { query: "hacker", path: "plot" }
    }
  },
  {
    $project: {
      _id: 0,
      title: 1,
      plot: 1,
      score: { $meta: "searchScore" }
    }
  }
])
```

Expected result includes *The Matrix*.

### Optional: compound query

Combine must / mustNot clauses:

```javascript
db = db.getSiblingDB("search_demo")

db.movies.aggregate([
  {
    $search: {
      compound: {
        must: [{ text: { query: "mars", path: "plot" } }],
        mustNot: [{ text: { query: "dream", path: "plot" } }]
      }
    }
  },
  { $project: { _id: 0, title: 1, plot: 1 } }
])
```

### Drop the index

```javascript
db = db.getSiblingDB("search_demo")
db.movies.dropSearchIndex("default")
```

---

## Example: vector search (`$vectorSearch`) with manual embeddings

### What are vectors?

A **vector** (embedding) is a fixed-length list of floating-point numbers that represents the meaning of text, an image, or other data in a numeric space. Similar items land close together; unrelated items land far apart.

In production, you usually generate embeddings with a model (for example OpenAI, Hugging Face, or Voyage). Here we use **manual embeddings**: short arrays you write by hand so the flow is easy to follow. Real models typically produce 384–1536 dimensions; this demo uses 4.

`$vectorSearch` finds documents whose stored vectors are closest to a **query vector**, using a similarity metric such as `cosine`, `euclidean`, or `dotProduct`. The index `numDimensions` must match the length of every stored vector and the query vector.

### 1. Create the vector search index

The collection must exist before you create a search index.

```javascript
db = db.getSiblingDB("vector_demo")
db.items.drop()
db.createCollection("items")

db.items.createSearchIndex(
  "vector_index",
  "vectorSearch",
  {
    fields: [
      {
        type: "vector",
        path: "embedding",
        numDimensions: 4,
        similarity: "cosine"
      }
    ]
  }
)

while (true) {
  const idxs = db.items.getSearchIndexes("vector_index")
  if (idxs.length && idxs.every(i => i.status === "READY")) break
  print("index status:", JSON.stringify(idxs))
  sleep(2000)
}
```

### 2. Upload data

Each document stores its embedding under `embedding`. The first two items are “fruit-like” (high first component); the third is different.

```javascript
db = db.getSiblingDB("vector_demo")

db.items.insertMany([
  {
    name: "red apple",
    embedding: [0.95, 0.02, 0.01, 0.10]
  },
  {
    name: "green pear",
    embedding: [0.80, 0.15, 0.05, 0.12]
  },
  {
    name: "blue car",
    embedding: [0.05, 0.90, 0.20, 0.05]
  }
])
```

### 3. Aggregate with `$vectorSearch`

Pass a query vector close to “red apple”. `numCandidates` controls how many approx. neighbors to score; `limit` is how many to return.

```javascript
db = db.getSiblingDB("vector_demo")

db.items.aggregate([
  {
    $vectorSearch: {
      index: "vector_index",
      path: "embedding",
      queryVector: [0.92, 0.03, 0.02, 0.11],
      numCandidates: 10,
      limit: 3
    }
  },
  {
    $project: {
      _id: 0,
      name: 1,
      score: { $meta: "vectorSearchScore" }
    }
  }
])
```

Expected top hit is *red apple*, then *green pear*, then *blue car*.

For real workloads, replace these 4-d arrays with model output and set `numDimensions` to that model’s size (for example `768` or `1536`).

### Drop the index

```javascript
db = db.getSiblingDB("vector_demo")
db.items.dropSearchIndex("vector_index")
```

---

## Logs and troubleshooting

```bash
docker compose logs -f mongod
docker compose logs -f mongot
```

| Check | Command |
|-------|---------|
| Replica set | `mongosh "mongodb://root:root@127.0.0.1:27017/?authSource=admin&directConnection=true" --quiet --eval 'rs.status().ok'` |
| mongod → mongot | Confirm `mongotHost` / `searchIndexManagementHostAndPort` in `config/mongod.conf` |
| Keyfile perms | Host file `config/keyfile` should be mode `400`; compose copies it into each container |
| Reset stack | `docker compose down -v && docker compose up -d` |

`mongot` starts only after `mongod` reports healthy (writable primary). First startup initializes the replica set, creates the `root` user, and may take ~30s.

## Layout

```
PS4M-local/
├── docker-compose.yml
├── README.md
└── config/
    ├── keyfile
    ├── mongod.conf
    └── mongot.yml
```
