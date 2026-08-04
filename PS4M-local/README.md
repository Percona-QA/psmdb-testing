# PS4M-local

Local Docker Compose stack for **Percona Search for MongoDB (PS4M)**: a single-node PSMDB replica set with `mongot` for MongoDB Search.

| Service | Image (default) | Override | Host ports |
|---------|-----------------|----------|------------|
| `mongod` | `perconalab/percona-server-mongodb:8.3.4-1` | `MONGOD_IMAGE` | `27017` |
| `mongot` | `perconalab/percona-server-mongodb-mongot:0.51.0-1` | `MONGOT_IMAGE` | `27028` (gRPC), `8080` (readiness) |
| `tei` | `ghcr.io/huggingface/text-embeddings-inference:cpu-1.9` | — | `8085` → `80` (OpenAI-compatible embeddings) |
| `ollama` | `ollama/ollama:latest` | — | `11434` (OpenAI-compatible embeddings) |

`tei` (Hugging Face Text Embeddings Inference) and `ollama` are two
OpenAI-compatible embedding backends used for **auto-embedding** (see below).
Both run at once, so you can create different indexes against different models:

- **`tei`** (default): loads the model given by `--model-id`
  (default `BAAI/bge-small-en-v1.5`, 384-dim), cached in `tei_data`. On arm64
  hosts change the image tag to `cpu-arm64-1.9`.
- **`ollama`**: pulls `nomic-embed-text` (768-dim) on start, cached in
  `ollama_data`; `bge-m3` (1024-dim) is also in the catalog if you pull it.

Replica set name: `rs`. Config lives under `config/` (`mongod.conf`, `mongot.yml`, shared `keyfile`).

### Overriding images (e.g. a dev mongot build)

Both images can be overridden with env vars, which is useful for testing a
`mongot` image built from source. For example, to run against a mongot image
produced as a GitHub artifact by the `percona-mongot` `dev-docker-image`
workflow:

```bash
# download the artifact tarball, then:
docker load -i percona-search-mongodb-pr-123.tar.gz   # prints "Loaded image: <image>"
MONGOT_IMAGE=perconalab/percona-search-mongodb:pr-123 docker compose up -d
```

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

## Example: auto-embedding (`$vectorSearch` with text)

With **auto-embedding**, you store plain text and mongot generates the vectors
for you at index and query time by calling an embedding model — you never send
`queryVector` yourself. This stack wires mongot to two local, keyless
OpenAI-compatible engines (`tei` and `ollama`) via the on-disk model catalog
`config/embedding-service-configs.yml`, enabled by the `embedding:` section in
`config/mongot.yml`.

The catalog defines several models; pick one per index via the `model` field in
the index definition (you can even override it per query with a compatible,
same-dimension model). Available out of the box:

| `model` | Backend | Dimensions | Notes |
|---------|---------|-----------|-------|
| `bge-small` | `tei` | 384 | Default (`BAAI/bge-small-en-v1.5`), loaded by TEI at startup |
| `nomic-embed-text` | `ollama` | 768 | Pulled by Ollama at startup |
| `bge-m3` | `ollama` | 1024 | Needs `docker compose exec ollama ollama pull bge-m3` first |

> **Requires a mongot image with the OpenAI-compatible embedding provider**
> (PSMDB-2143). The default `perconalab/percona-server-mongodb-mongot:0.51.0-1`
> predates this feature and does not understand the `OPENAI_COMPATIBLE` provider.
> Set `MONGOT_IMAGE` to a build that includes it — e.g. a dev image produced by
> the `percona-mongot` `dev-docker-image` workflow (see "Overriding images"):
>
> ```bash
> MONGOT_IMAGE=perconalab/percona-search-mongodb:pr-17 docker compose up -d
> ```

The example below uses the default `bge-small` (TEI). To use an Ollama
model instead, just change the `model` in the index definition to
`nomic-embed-text` (or `bge-m3` after pulling it) — no other change needed.
Confirm the backends are up:

```bash
curl -fsS http://127.0.0.1:8085/health          # TEI
docker compose exec ollama ollama list           # Ollama models
```

### 1. Create an auto-embed vector index

Note the field `type: "autoEmbed"` with a `model` and `modality: "text"`. The
`path` points at the **text** field to embed; `numDimensions`/`similarity` are
resolved from the model catalog when omitted.

```javascript
db = db.getSiblingDB("autoembed_demo")
db.movies.drop()
db.createCollection("movies")

db.movies.createSearchIndex(
  "auto_index",
  "vectorSearch",
  {
    fields: [
      {
        type: "autoEmbed",
        path: "plot",
        model: "bge-small",
        modality: "text"
      }
    ]
  }
)

while (true) {
  const idxs = db.movies.getSearchIndexes("auto_index")
  if (idxs.length && idxs.every(i => i.status === "READY")) break
  print("index status:", JSON.stringify(idxs))
  sleep(2000)
}
```

### 2. Upload data (plain text, no vectors)

```javascript
db = db.getSiblingDB("autoembed_demo")

db.movies.insertMany([
  { title: "The Matrix", plot: "A computer hacker learns about the true nature of reality" },
  { title: "Inception", plot: "A thief enters people's dreams to steal secrets" },
  { title: "The Martian", plot: "An astronaut is stranded alone on Mars and must survive" }
])
```

### 3. Query with text

Pass `query` (a string) instead of `queryVector`; mongot embeds it with the same
model and runs the vector search.

```javascript
db = db.getSiblingDB("autoembed_demo")

db.movies.aggregate([
  {
    $vectorSearch: {
      index: "auto_index",
      path: "plot",
      query: "someone breaks into computer systems",
      numCandidates: 50,
      limit: 3
    }
  },
  {
    $project: {
      _id: 0,
      title: 1,
      plot: 1,
      score: { $meta: "vectorSearchScore" }
    }
  }
])
```

Expected top hit is *The Matrix*. To override the model per query (must be
compatible/same dimensions), add `model: "<name>"` alongside `query`.

### Drop the index

```javascript
db = db.getSiblingDB("autoembed_demo")
db.movies.dropSearchIndex("auto_index")
```

---

## Logs and troubleshooting

```bash
docker compose logs -f mongod
docker compose logs -f mongot
docker compose logs -f tei
docker compose logs -f ollama
```

| Check | Command |
|-------|---------|
| Replica set | `mongosh "mongodb://root:root@127.0.0.1:27017/?authSource=admin&directConnection=true" --quiet --eval 'rs.status().ok'` |
| mongod → mongot | Confirm `mongotHost` / `searchIndexManagementHostAndPort` in `config/mongod.conf` |
| Keyfile perms | Host file `config/keyfile` should be mode `400`; compose copies it into each container |
| Auto-embedding inactive | Check `mongot` logs for the loaded catalog; ensure `embedding.modelConfigFile` in `config/mongot.yml` resolves and `config/embedding-service-configs.yml` is valid (mongot fails closed, not silently) |
| Embedding backend down | TEI: `curl -fsS http://127.0.0.1:8085/health`; Ollama: `docker compose exec ollama ollama list`. First run downloads models. |
| Dimension mismatch | `outputDimensions` in the catalog must equal the model's native dimension (bge-small → 384, nomic-embed-text → 768, bge-m3 → 1024) |
| Reset stack | `docker compose down -v && docker compose up -d` |

`mongot` starts after `mongod` is healthy and `tei` has started. TEI downloads
its model on first run, so the first auto-embed index build or query may retry
briefly until the model is ready (mongot retries per the catalog config).

## Layout

```
PS4M-local/
├── docker-compose.yml
├── README.md
└── config/
    ├── keyfile
    ├── mongod.conf
    ├── mongot.yml
    └── embedding-service-configs.yml
```
