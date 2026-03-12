import os
import time
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest
from fastapi.responses import PlainTextResponse

app = FastAPI(title="DevOps Task API", version="1.0.0")

# --- Prometheus metrics ---
REQUEST_COUNT = Counter("http_requests_total", "Total HTTP requests", ["method", "endpoint", "status"])
REQUEST_DURATION = Histogram("http_request_duration_seconds", "Request duration", ["endpoint"])
TASKS_CREATED = Counter("tasks_created_total", "Total tasks created")
TASKS_COMPLETED = Counter("tasks_completed_total", "Total tasks completed")

# --- DB connection ---
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'devops')}:{os.getenv('DB_PASSWORD', 'devops2026')}"
    f"@{os.getenv('DB_HOST', 'postgres')}:5432/{os.getenv('DB_NAME', 'taskdb')}"
)
pool = None

class TaskIn(BaseModel):
    title: str
    description: str = ""

class TaskOut(BaseModel):
    id: int
    title: str
    description: str
    done: bool

@app.on_event("startup")
async def startup():
    global pool
    pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                done BOOLEAN DEFAULT FALSE
            )
        """)

@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()

@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return generate_latest()

@app.get("/health")
async def health():
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "healthy", "db": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/tasks", response_model=list[TaskOut])
async def list_tasks():
    start = time.time()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, title, description, done FROM tasks ORDER BY id")
    REQUEST_COUNT.labels("GET", "/tasks", 200).inc()
    REQUEST_DURATION.labels("/tasks").observe(time.time() - start)
    return [dict(r) for r in rows]

@app.post("/tasks", response_model=TaskOut, status_code=201)
async def create_task(task: TaskIn):
    start = time.time()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO tasks (title, description) VALUES ($1, $2) RETURNING id, title, description, done",
            task.title, task.description,
        )
    TASKS_CREATED.inc()
    REQUEST_COUNT.labels("POST", "/tasks", 201).inc()
    REQUEST_DURATION.labels("/tasks").observe(time.time() - start)
    return dict(row)

@app.patch("/tasks/{task_id}/complete", response_model=TaskOut)
async def complete_task(task_id: int):
    start = time.time()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "UPDATE tasks SET done = TRUE WHERE id = $1 RETURNING id, title, description, done",
            task_id,
        )
    if not row:
        raise HTTPException(status_code=404, detail="Task not found")
    TASKS_COMPLETED.inc()
    REQUEST_COUNT.labels("PATCH", "/tasks/complete", 200).inc()
    REQUEST_DURATION.labels("/tasks/complete").observe(time.time() - start)
    return dict(row)

@app.delete("/tasks/{task_id}", status_code=204)
async def delete_task(task_id: int):
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM tasks WHERE id = $1", task_id)
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Task not found")
    REQUEST_COUNT.labels("DELETE", "/tasks", 204).inc()
