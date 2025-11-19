# app.py
from flask import Flask, request, jsonify, abort
import pymysql
import os

app = Flask(__name__)

#  конфиг
MASTER_DB = {
    "host": os.getenv("MASTER_HOST", "project-rds-mysql-prod.c9gsa420upfe.eu-central-1.rds.amazonaws.com"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASS", "adminrds"),
    "database": os.getenv("DB_NAME", "project_db"),
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}
REPLICA_DB = {
    "host": os.getenv("REPLICA_HOST", "project-rds-mysql-read-replica.c9gsa420upfe.eu-central-1.rds.amazonaws.com"),
    "user": os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASS", "adminrds"),
    "database": os.getenv("DB_NAME", "project_db"),
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True,
}

def conn_write():
    return pymysql.connect(**MASTER_DB)

def conn_read():
    return pymysql.connect(**REPLICA_DB)

# --------- categories (минимум) ----------
@app.get("/categories")
def list_categories():
    with conn_read() as c, c.cursor() as cur:
        cur.execute("SELECT id, name FROM categories ORDER BY id")
        return jsonify(cur.fetchall())

@app.post("/categories")
def create_category():
    data = request.get_json(force=True)
    name = (data or {}).get("name")
    if not name:
        abort(400, "name is required")
    # не дропаем, вставка идемпотентна
    with conn_write() as c, c.cursor() as cur:
        cur.execute("INSERT IGNORE INTO categories(name) VALUES (%s)", (name,))
        # вернем текущую запись
        cur.execute("SELECT id, name FROM categories WHERE name=%s", (name,))
        return jsonify(cur.fetchone()), 201

# --------- todos ----------
@app.get("/todos")
def list_todos():
    """ЧТЕНИЕ только с реплики"""
    with conn_read() as c, c.cursor() as cur:
        cur.execute("""
          SELECT t.id, t.title, t.status, t.category_id, c.name AS category
          FROM todos t JOIN categories c ON c.id = t.category_id
          ORDER BY t.id
        """)
        return jsonify(cur.fetchall())

@app.post("/todos")
def create_todo():
    """ЗАПИСЬ на master"""
    data = request.get_json(force=True) or {}   
    title = data.get("title")
    status = data.get("status", "new")
    category_id = data.get("category_id")
    category = data.get("category")  # можно передавать name вместо id

    if not title:
        abort(400, "title is required")

    with conn_write() as c, c.cursor() as cur:
        if not category_id and category:
            # создадим категорию, если её нет
            cur.execute("INSERT IGNORE INTO categories(name) VALUES (%s)", (category,))
            cur.execute("SELECT id FROM categories WHERE name=%s", (category,))
            row = cur.fetchone()
            category_id = row["id"] if row else None

        if not category_id:
            abort(400, "category_id or category (name) is required")

        cur.execute(
            "INSERT INTO todos(title, status, category_id) VALUES (%s,%s,%s)",
            (title, status, category_id),
        )
        new_id = cur.lastrowid
        cur.execute("""
          SELECT t.id, t.title, t.status, t.category_id, c.name AS category
          FROM todos t JOIN categories c ON c.id=t.category_id
          WHERE t.id=%s
        """, (new_id,))
        return jsonify(cur.fetchone()), 201

@app.put("/todos/<int:todo_id>")
def update_todo(todo_id):
    """ЗАПИСЬ на master"""
    data = request.get_json(force=True) or {}
    fields = []
    params = []
    if "title" in data:
        fields.append("title=%s"); params.append(data["title"])
    if "status" in data:
        fields.append("status=%s"); params.append(data["status"])
    if "category_id" in data:
        fields.append("category_id=%s"); params.append(data["category_id"])
    if "category" in data and "category_id" not in data:
        with conn_write() as c, c.cursor() as cur:
            cur.execute("INSERT IGNORE INTO categories(name) VALUES (%s)", (data["category"],))
            cur.execute("SELECT id FROM categories WHERE name=%s", (data["category"],))
            params_id = cur.fetchone()["id"]
        fields.append("category_id=%s"); params.append(params_id)

    if not fields:
        abort(400, "nothing to update")

    with conn_write() as c, c.cursor() as cur:
        sql = f"UPDATE todos SET {', '.join(fields)} WHERE id=%s"
        params.append(todo_id)
        cur.execute(sql, tuple(params))
        cur.execute("""
          SELECT t.id, t.title, t.status, t.category_id, c.name AS category
          FROM todos t JOIN categories c ON c.id=t.category_id
          WHERE t.id=%s
        """, (todo_id,))
        row = cur.fetchone()
        if not row: abort(404, "todo not found")
        return jsonify(row)

@app.delete("/todos/<int:todo_id>")
def delete_todo(todo_id):
    """ЗАПИСЬ на master"""
    with conn_write() as c, c.cursor() as cur:
        cur.execute("DELETE FROM todos WHERE id=%s", (todo_id,))
        return jsonify({"deleted": cur.rowcount})

@app.get("/health")
def health():
    # проверка, что обе БД доступны
    try:
        with conn_write() as c: pass
        with conn_read() as c: pass
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}, 500

if __name__ == "__main__":
    # слушаем наружу (не забудь открыть 8080 в SG для своего IP)
    app.run(host="0.0.0.0", port=8080)
