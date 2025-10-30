from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
import pandas as pd
import sqlite3
import os

try:
    import psycopg
except ImportError:
    psycopg = None

app = Flask(__name__)
app.secret_key = "chave_super_secreta_123"

DATABASE_URL = os.environ.get("DATABASE_URL")
DB_FILE = "local.db"


def get_conn():
    if DATABASE_URL and psycopg:
        return psycopg.connect(DATABASE_URL)
    return sqlite3.connect(DB_FILE, check_same_thread=False)


def init_db():
    conn = get_conn()
    c = conn.cursor()

    if isinstance(conn, sqlite3.Connection):
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT UNIQUE NOT NULL,
                senha TEXT NOT NULL,
                role TEXT DEFAULT 'user'
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                nome TEXT UNIQUE NOT NULL,
                senha TEXT NOT NULL,
                role TEXT DEFAULT 'user'
            )
        """)

    if isinstance(conn, sqlite3.Connection):
        c.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                cpf TEXT UNIQUE
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id SERIAL PRIMARY KEY,
                nome TEXT,
                cpf TEXT UNIQUE
            )
        """)

    if isinstance(conn, sqlite3.Connection):
        c.execute("""
            CREATE TABLE IF NOT EXISTS telefones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente_id INTEGER,
                telefone TEXT,
                UNIQUE(cliente_id, telefone),
                FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS telefones (
                id SERIAL PRIMARY KEY,
                cliente_id INTEGER REFERENCES clientes(id) ON DELETE CASCADE,
                telefone TEXT,
                UNIQUE(cliente_id, telefone)
            )
        """)

    if isinstance(conn, sqlite3.Connection):
        c.execute("SELECT * FROM users WHERE role = ?", ("admin",))
    else:
        c.execute("SELECT * FROM users WHERE role = %s", ("admin",))

    if not c.fetchone():
        admin_name = "Leonardo"
        admin_pass = generate_password_hash("Tech@2025")

        if isinstance(conn, sqlite3.Connection):
            c.execute("INSERT INTO users (nome, senha, role) VALUES (?, ?, ?)",
                      (admin_name, admin_pass, "admin"))
        else:
            c.execute("INSERT INTO users (nome, senha, role) VALUES (%s, %s, %s)",
                      (admin_name, admin_pass, "admin"))

    conn.commit()
    conn.close()


def inserir_ou_atualizar_cliente(nome, cpf, telefones):
    conn = get_conn()
    c = conn.cursor()

    try:
        placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"

        c.execute(f"SELECT id FROM clientes WHERE cpf = {placeholder}", (cpf,))
        row = c.fetchone()

        if row:
            cliente_id = row[0]
            c.execute(f"UPDATE clientes SET nome = {placeholder} WHERE id = {placeholder}",
                      (nome, cliente_id))

        else:
            c.execute(f"INSERT INTO clientes (nome, cpf) VALUES ({placeholder}, {placeholder})",
                      (nome, cpf))
            cliente_id = c.lastrowid if isinstance(conn, sqlite3.Connection) else None
            if cliente_id is None:
                c.execute(f"SELECT id FROM clientes WHERE cpf = {placeholder}", (cpf,))
                cliente_id = c.fetchone()[0]

        c.execute(f"SELECT telefone FROM telefones WHERE cliente_id = {placeholder}", (cliente_id,))
        existentes = [t[0] for t in c.fetchall()]

        for tel in telefones:
            if tel and tel not in existentes:
                c.execute(f"INSERT OR IGNORE INTO telefones (cliente_id, telefone) VALUES ({placeholder}, {placeholder})"
                          if isinstance(conn, sqlite3.Connection)
                          else f"INSERT INTO telefones (cliente_id, telefone) VALUES ({placeholder}, {placeholder}) ON CONFLICT DO NOTHING",
                          (cliente_id, tel))

        conn.commit()

    except Exception as e:
        print("Erro ao inserir/atualizar cliente:", e)

    finally:
        conn.close()


@app.before_request
def ensure_db():
    if not hasattr(app, "_db_init_done"):
        init_db()
        app._db_init_done = True

@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        nome = request.form["nome"]
        senha = request.form["senha"]

        conn = get_conn()
        c = conn.cursor()
        if isinstance(conn, sqlite3.Connection):
            c.execute("SELECT id, nome, senha, role FROM users WHERE nome = ?", (nome,))
        else:
            c.execute("SELECT id, nome, senha, role FROM users WHERE nome = %s", (nome,))
        user = c.fetchone()
        conn.close()

        if user and check_password_hash(user[2], senha):
            session["user"] = user[1]
            session["role"] = user[3]
            return redirect(url_for("index"))
        return render_template("login.html", erro="Usuário ou senha inválidos")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    if request.method == "POST":
        nome = request.form["nome"]
        senha = generate_password_hash(request.form["senha"])
        role = request.form.get("role", "user")

        conn = get_conn()
        c = conn.cursor()
        try:
            if isinstance(conn, sqlite3.Connection):
                c.execute("INSERT INTO users (nome, senha, role) VALUES (?, ?, ?)",
                          (nome, senha, role))
            else:
                c.execute("INSERT INTO users (nome, senha, role) VALUES (%s, %s, %s)",
                          (nome, senha, role))
            conn.commit()
            msg = "Usuário criado com sucesso!"
        except Exception as e:
            msg = f"Erro ao criar usuário: {e}"
        finally:
            conn.close()
        return render_template("register.html", sucesso=msg)

    return render_template("register.html")

@app.route("/index")
def index():
    if "user" not in session:
        return redirect(url_for("login"))
    return render_template("index.html", usuario=session["user"], role=session["role"])


@app.route("/consultar", methods=["POST"])
def consultar():
    dado = request.form["dado"].strip()
    dado_limpo = "".join(filter(str.isdigit, dado))

    conn = get_conn()
    c = conn.cursor()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    query = f"""
        SELECT c.nome, c.cpf, t.telefone
        FROM clientes c
        LEFT JOIN telefones t ON c.id = t.cliente_id
        WHERE 
            REPLACE(REPLACE(REPLACE(c.cpf, '.', ''), '-', ''), ' ', '') = {placeholder}
            OR REPLACE(REPLACE(REPLACE(LTRIM(c.cpf, '0'), '.', ''), '-', ''), ' ', '') = LTRIM({placeholder}, '0')
            OR REPLACE(REPLACE(REPLACE(t.telefone, ' ', ''), '-', ''), '(', '') LIKE '%' || {placeholder} || '%'
    """

    c.execute(query, (dado_limpo, dado_limpo, dado_limpo))
    registros = c.fetchall()
    conn.close()

    if registros:
        nome = registros[0][0] or "-"
        cpf = registros[0][1] or "-"
        telefones = sorted({r[2] for r in registros if r[2]})

        return jsonify({
            "encontrado": True,
            "nome": nome,
            "cpf": cpf,
            "telefones": telefones
        })
    else:
        return jsonify({
            "encontrado": False,
            "mensagem": "Nenhum registro encontrado."
        })


def normalizar_cpf(cpf_raw):
    cpf = "".join(filter(str.isdigit, str(cpf_raw)))
    if len(cpf) < 11:
        cpf = cpf.zfill(11)
    return cpf

@app.route("/importar", methods=["GET", "POST"])
def importar():
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    if request.method == "POST":
        arquivo = request.files.get("arquivo")
        if not arquivo or not arquivo.filename.endswith(".csv"):
            return render_template("importar.html", erro="Envie um arquivo CSV válido.")

        try:
            df = pd.read_csv(arquivo, dtype=str, sep=";", keep_default_na=False, encoding="utf-8").fillna("")

            if DATABASE_URL and psycopg:
                bancos = [psycopg.connect(DATABASE_URL)]
            else:
                bancos = [sqlite3.connect("local.db", check_same_thread=False)]

            novos = 0
            atualizados = 0

            for conn in bancos:
                c = conn.cursor()
                placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"

                for _, row in df.iterrows():
                    nome = str(row.get("nome", "")).strip()

                    cpf_raw = row.get("cpf", "")
                    cpf = ""
                    try:
                        if isinstance(cpf_raw, float):
                            cpf = str(int(cpf_raw))
                        else:
                            cpf_str = str(cpf_raw).strip()
                            if "e" in cpf_str.lower():
                                cpf = str(int(float(cpf_str)))
                            else:
                                cpf = "".join(ch for ch in cpf_str if ch.isdigit())
                    except Exception:
                        cpf = ""

                    if not cpf:
                        continue

                    telefones = [
                        str(row.get(col, "")).strip()
                        for col in row.index
                        if "telefone" in col.lower() and str(row.get(col, "")).strip()
                    ]

                    c.execute(f"""
                        SELECT id FROM clientes
                        WHERE REPLACE(REPLACE(REPLACE(cpf, '.', ''), '-', ''), ' ', '') = {placeholder}
                           OR REPLACE(REPLACE(REPLACE(LTRIM(cpf, '0'), '.', ''), '-', ''), ' ', '') = REPLACE({placeholder}, '^0+', '')
                    """, (cpf, cpf))
                    cliente = c.fetchone()

                    if cliente:
                        cliente_id = cliente[0]
                        atualizados += 1
                    else:
                        if isinstance(conn, sqlite3.Connection):
                            c.execute(
                                f"INSERT INTO clientes (nome, cpf) VALUES ({placeholder}, {placeholder})",
                                (nome, cpf),
                            )
                            cliente_id = c.lastrowid
                        else:
                            c.execute(
                                f"INSERT INTO clientes (nome, cpf) VALUES ({placeholder}, {placeholder}) RETURNING id",
                                (nome, cpf),
                            )
                            cliente_id = c.fetchone()[0]
                        novos += 1

                    for tel in telefones:
                        if not tel:
                            continue
                        c.execute(
                            f"SELECT 1 FROM telefones WHERE cliente_id = {placeholder} AND telefone = {placeholder}",
                            (cliente_id, tel),
                        )
                        if not c.fetchone():
                            c.execute(
                                f"INSERT INTO telefones (cliente_id, telefone) VALUES ({placeholder}, {placeholder})",
                                (cliente_id, tel),
                            )

                conn.commit()
                conn.close()

            return render_template(
                "importar.html",
                sucesso=f"Importação concluída! {novos} novos e {atualizados} atualizados."
            )

        except Exception as e:
            return render_template("importar.html", erro=f"Erro ao importar: {e}")

    return render_template("importar.html")

@app.route("/usuarios")
def usuarios():
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT id, nome, role FROM users")
    lista = c.fetchall()
    conn.close()
    return render_template("usuarios.html", usuarios=lista)

@app.route("/editar_usuario/<int:user_id>", methods=["GET", "POST"])
def editar_usuario(user_id):
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    conn = get_conn()
    c = conn.cursor()
    placeholder = "?" if isinstance(conn, sqlite3.Connection) else "%s"

    if request.method == "POST":
        nome = request.form.get("nome", "").strip()
        senha = request.form.get("senha", "").strip()

        if not nome:
            conn.close()
            return render_template("editar.html", erro="O nome não pode ficar em branco.")
        
        c.execute(f"UPDATE users SET nome = {placeholder} WHERE id = {placeholder}", (nome, user_id))

        if senha:
            senha_hash = generate_password_hash(senha)
            c.execute(f"UPDATE users SET senha = {placeholder} WHERE id = {placeholder}", (senha_hash, user_id))

        conn.commit()
        conn.close()
        return redirect(url_for("usuarios"))

    c.execute(f"SELECT id, nome, senha, role FROM users WHERE id = {placeholder}", (user_id,))
    user = c.fetchone()
    conn.close()

    if not user:
        return "Usuário não encontrado.", 404

    return render_template("editar.html", user=user)

@app.route("/excluir/<int:user_id>", methods=["POST"])
def excluir(user_id):
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    conn = get_conn()
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id = %s", (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("usuarios"))

@app.route("/dashboard")
def dashboard():
    if "user" not in session or session.get("role") != "admin":
        return redirect(url_for("index"))

    total_pg = 0
    pg_size_mb = 0
    pg_size_pretty = "0 MB"
    render_total_mb = None

    total_local = 0
    local_used_mb = 0
    local_used_percent = 0
    sqlite_limit_mb = 50

    if DATABASE_URL and psycopg:
        try:
            conn_pg = psycopg.connect(DATABASE_URL)
            c_pg = conn_pg.cursor()
            c_pg.execute("SELECT COUNT(*) FROM clientes;")
            total_pg = c_pg.fetchone()[0]

            c_pg.execute("""
                SELECT pg_database_size(current_database()),
                       pg_size_pretty(pg_database_size(current_database()));
            """)
            size_bytes, size_pretty = c_pg.fetchone()
            pg_size_mb = round(size_bytes / (1024 * 1024), 2)
            pg_size_pretty = size_pretty

            conn_pg.close()
        except Exception as e:
            print("Erro PostgreSQL:", e)

    elif os.path.exists("local.db"):
        try:
            conn_local = sqlite3.connect("local.db")
            c_local = conn_local.cursor()
            c_local.execute("SELECT COUNT(*) FROM clientes;")
            total_local = c_local.fetchone()[0]
            conn_local.close()

            local_used_mb = round(os.path.getsize("local.db") / (1024 * 1024), 2)
            local_used_percent = min((local_used_mb / sqlite_limit_mb) * 100, 100)
        except Exception as e:
            print("Erro SQLite:", e)

    total_geral = total_pg + total_local

    return render_template(
        "dashboard.html",
        total_pg=total_pg,
        total_local=total_local,
        total_geral=total_geral,
        pg_size_mb=pg_size_mb,
        pg_size_pretty=pg_size_pretty,
        render_total_mb=render_total_mb,
        local_used_mb=local_used_mb,
        local_used_percent=local_used_percent,
        sqlite_limit_mb=sqlite_limit_mb
    )

if __name__ == "__main__":
    app.run(debug=True, port=5000)
