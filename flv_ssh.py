#!/usr/bin/env python3
"""
core-deploy  —  Síntesis S.A.
Gestión de servidores, logs y deploy COBOL.
Elaborado por Fernando López.
"""

import curses
import fcntl
import json
import os
import sys
import time
import threading
import subprocess
import psycopg2
import psycopg2.extras
from collections import deque
from datetime import datetime

APP_NAME    = "core-deploy"
APP_VERSION = "1.0.1"
APP_CREDIT  = "by Fernando · Síntesis"
CONFIG_FILE = os.path.expanduser("~/.config/core-deploy/config.json")

# ── Configuración de la BD (se rellena al cargar config) ──────────────────
DB_CONFIG = {
    "host":     "",
    "database": "",
    "user":     "",
    "password": "",
}

# ── Configuración de Hades — se sobreescribe al cargar config ──────────────
HADES = {
    "host": "hades",
    "port": 22122,
    "user": "",
    "key":  "",
}


# ── Config por usuario ─────────────────────────────────────────────────────

def load_user_config():
    """Carga config desde ~/.config/core-deploy/config.json."""
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def save_user_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_FILE), exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)
    os.chmod(CONFIG_FILE, 0o600)


def setup_wizard(stdscr):
    """
    Wizard de primer arranque.
    Pregunta usuario de hades, método de auth y credenciales de BD.
    Devuelve dict con la config o None si cancela.
    """
    init_colors()
    import getpass
    unix_user = getpass.getuser()

    # Paso 1: elegir método de autenticación
    auth_method = _wizard_pick_auth(stdscr, unix_user)
    if auth_method is None:
        return None

    # Paso 2: rellenar campos de hades según el método
    if auth_method == "key":
        fields = [
            ("hades_user", "Tu usuario en hades",         unix_user),
            ("hades_key",  "Ruta a tu clave SSH (~/.ssh2/usuario)", f"{os.path.expanduser('~')}/.ssh2/{unix_user}"),
        ]
    else:
        fields = [
            ("hades_user",     "Tu usuario en hades",     unix_user),
            ("hades_password", "Tu contraseña en hades",  ""),
        ]

    values = {"hades_auth": auth_method}
    for k, _, d in fields:
        values[k] = d

    current = 0

    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {APP_NAME.upper()} — Configuración de hades ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        method_txt = "Clave SSH" if auth_method == "key" else "Contraseña"
        stdscr.addstr(2, 4, f"Método seleccionado: {method_txt}", curses.color_pair(C_OK) | curses.A_BOLD)

        for i, (key, label, _) in enumerate(fields):
            y    = 4 + i * 3
            attr = curses.color_pair(C_SELECTED) | curses.A_BOLD if i == current \
                   else curses.color_pair(C_NORMAL)
            stdscr.addstr(y,     4, f"  {label}:", curses.A_BOLD)
            display = "***" if key == "hades_password" and values[key] \
                      else values[key]
            stdscr.addstr(y + 1, 4, f"  [{display:<60}]", attr)

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " ↑↓=Navegar  Enter=Editar  F10=Guardar y continuar  ESC=Volver ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key_pressed = stdscr.getch()

        if key_pressed == 27:
            return None
        elif key_pressed == curses.KEY_UP:
            current = (current - 1) % len(fields)
        elif key_pressed == curses.KEY_DOWN:
            current = (current + 1) % len(fields)
        elif key_pressed in (curses.KEY_ENTER, 10, 13):
            fkey, flabel, _ = fields[current]
            if fkey == "hades_password":
                new_val = _ask_password_wizard(stdscr, f"{flabel}: ")
            else:
                new_val = ask_input(stdscr, f"{flabel}: ")
            if new_val:
                values[fkey] = new_val
        elif key_pressed == curses.KEY_F10:
            if not values.get("hades_user", "").strip():
                continue
            if auth_method == "key" and not values.get("hades_key", "").strip():
                continue
            if auth_method == "password" and not values.get("hades_password", "").strip():
                continue
            # Paso 3: configuración de la base de datos
            db_cfg = _wizard_db_config(stdscr)
            if db_cfg is None:
                return None
            values.update(db_cfg)
            return values


def _wizard_db_config(stdscr):
    """Pantalla de configuración de la base de datos PostgreSQL."""
    fields = [
        ("db_host",     "Host / IP del servidor PostgreSQL", ""),
        ("db_name",     "Nombre de la base de datos",        ""),
        ("db_user",     "Usuario de la base de datos",       ""),
        ("db_password", "Contraseña de la base de datos",    ""),
    ]
    values  = {k: d for k, _, d in fields}
    current = 0

    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {APP_NAME.upper()} — Configuración de base de datos ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        stdscr.addstr(2, 4, "Credenciales de PostgreSQL (se guardan solo en tu equipo):",
                      curses.color_pair(C_WARN) | curses.A_BOLD)

        for i, (key, label, _) in enumerate(fields):
            y    = 4 + i * 3
            attr = curses.color_pair(C_SELECTED) | curses.A_BOLD if i == current \
                   else curses.color_pair(C_NORMAL)
            stdscr.addstr(y,     4, f"  {label}:", curses.A_BOLD)
            display = "***" if key == "db_password" and values[key] else values[key]
            stdscr.addstr(y + 1, 4, f"  [{display:<60}]", attr)

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " ↑↓=Navegar  Enter=Editar  F10=Guardar  ESC=Volver ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key_pressed = stdscr.getch()

        if key_pressed == 27:
            return None
        elif key_pressed == curses.KEY_UP:
            current = (current - 1) % len(fields)
        elif key_pressed == curses.KEY_DOWN:
            current = (current + 1) % len(fields)
        elif key_pressed in (curses.KEY_ENTER, 10, 13):
            fkey, flabel, _ = fields[current]
            if fkey == "db_password":
                new_val = _ask_password_wizard(stdscr, f"{flabel}: ")
            else:
                new_val = ask_input(stdscr, f"{flabel}: ")
            if new_val:
                values[fkey] = new_val
        elif key_pressed == curses.KEY_F10:
            if not values.get("db_host", "").strip():
                continue
            if not values.get("db_name", "").strip():
                continue
            if not values.get("db_user", "").strip():
                continue
            if not values.get("db_password", "").strip():
                continue
            return values


def _wizard_pick_auth(stdscr, unix_user):
    """Pantalla para elegir entre clave SSH o contraseña."""
    options  = [
        ("key",      "Clave SSH     (tengo ~/.ssh2/usuario o similar)"),
        ("password", "Contraseña    (no tengo clave SSH configurada)"),
    ]
    current = 0

    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {APP_NAME.upper()} — Configuración inicial ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        stdscr.addstr(2, 4, "Primera vez que ejecutas core-deploy en este equipo.", curses.A_BOLD)
        stdscr.addstr(3, 4, f"Usuario del sistema: {unix_user}")
        stdscr.addstr(5, 4, "¿Cómo te conectas a hades?", curses.A_BOLD)

        for i, (_, label) in enumerate(options):
            y    = 7 + i * 2
            attr = curses.color_pair(C_SELECTED) | curses.A_BOLD if i == current \
                   else curses.color_pair(C_NORMAL)
            mark = " ► " if i == current else "   "
            stdscr.addstr(y, 6, mark + label, attr)

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " ↑↓=Seleccionar  Enter=Confirmar  ESC=Salir ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key = stdscr.getch()
        if key == 27:
            return None
        elif key == curses.KEY_UP:
            current = (current - 1) % len(options)
        elif key == curses.KEY_DOWN:
            current = (current + 1) % len(options)
        elif key in (curses.KEY_ENTER, 10, 13):
            return options[current][0]


def _ask_password_wizard(stdscr, prompt):
    """Pide contraseña sin mostrarla en pantalla."""
    h, w  = stdscr.getmaxyx()
    win_w = min(60, w - 4)
    win   = curses.newwin(5, win_w, h // 2 - 2, (w - win_w) // 2)
    win.bkgd(' ', curses.color_pair(C_HEADER))
    win.border()
    win.addstr(1, 2, prompt[:win_w - 4], curses.A_BOLD)
    win.addstr(2, 2, " " * (win_w - 4))
    win.refresh()
    curses.noecho()
    curses.curs_set(1)
    password = ""
    while True:
        ch = win.getch(2, 2 + len(password))
        if ch in (curses.KEY_ENTER, 10, 13):
            break
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            if password:
                password = password[:-1]
                win.addstr(2, 2, " " * (win_w - 4))
                win.addstr(2, 2, "*" * len(password))
                win.refresh()
        elif 32 <= ch <= 126:
            password += chr(ch)
            win.addstr(2, 2 + len(password) - 1, "*")
            win.refresh()
    curses.curs_set(0)
    del win
    stdscr.touchwin(); stdscr.refresh()
    return password

# ── Colores ────────────────────────────────────────────────────────────────
C_HEADER   = 1
C_SELECTED = 2
C_NORMAL   = 3
C_SEARCH   = 4
C_STATUS   = 5
C_TITLE    = 6
C_DIM      = 7
C_ERROR    = 8
C_GREP     = 9
C_OK       = 10
C_WARN     = 11


def init_colors():
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(C_HEADER,   curses.COLOR_BLACK,  curses.COLOR_CYAN)
    curses.init_pair(C_SELECTED, curses.COLOR_BLACK,  curses.COLOR_GREEN)
    curses.init_pair(C_NORMAL,   curses.COLOR_WHITE,  -1)
    curses.init_pair(C_SEARCH,   curses.COLOR_YELLOW, -1)
    curses.init_pair(C_STATUS,   curses.COLOR_BLACK,  curses.COLOR_YELLOW)
    curses.init_pair(C_TITLE,    curses.COLOR_CYAN,   -1)
    curses.init_pair(C_DIM,      8,                   -1)
    curses.init_pair(C_ERROR,    curses.COLOR_RED,    -1)
    curses.init_pair(C_GREP,     curses.COLOR_GREEN,  -1)
    curses.init_pair(C_OK,       curses.COLOR_GREEN,  -1)
    curses.init_pair(C_WARN,     curses.COLOR_YELLOW, -1)


# ── Base de datos ──────────────────────────────────────────────────────────

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def fetch_clientes(search=""):
    query = """
        SELECT
            c.nro_cliente,
            c.desc_cliente,
            c.servidor,
            host(c.ip_servidor)              AS ip,
            c.iniciales,
            c.desc_cobol,
            c.path,
            c.path_hades,
            COALESCE(m.ssh_user,     'tuxedo') AS ssh_user,
            COALESCE(m.ssh_password, '')        AS ssh_password,
            COALESCE(m.ssh_port,     22)        AS ssh_port
        FROM clientes c
        LEFT JOIN maquinas m ON LOWER(m.nombre) = LOWER(c.servidor)
        WHERE
            c.desc_cliente ILIKE %s OR
            c.servidor     ILIKE %s OR
            host(c.ip_servidor) ILIKE %s OR
            c.desc_cobol   ILIKE %s
        ORDER BY c.desc_cliente
    """
    pat = f"%{search}%"
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (pat, pat, pat, pat))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── CRUD Clientes ──────────────────────────────────────────────────────────

CLIENTE_FIELDS = [
    ("nro_cliente",  "Nro Cliente",       "int",  False),  # (campo, label, tipo, editable_en_edicion)
    ("desc_cliente", "Nombre",            "str",  True),
    ("servidor",     "Servidor",          "str",  True),
    ("ip_servidor",  "IP",                "str",  True),
    ("iniciales",    "Iniciales",         "str",  True),
    ("desc_cobol",   "Desc COBOL",        "str",  True),
    ("path",         "Path Producción",   "str",  True),
    ("path_hades",   "Path Hades",        "str",  True),
]


def db_insert_cliente(data):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clientes
                    (nro_cliente, desc_cliente, servidor, ip_servidor,
                     iniciales, desc_cobol, path, path_hades)
                VALUES (%s,%s,%s,%s::inet,%s,%s,%s,%s)
            """, (
                data["nro_cliente"], data["desc_cliente"], data["servidor"],
                data["ip_servidor"], data["iniciales"], data["desc_cobol"],
                data["path"], data["path_hades"],
            ))
        conn.commit()
    finally:
        conn.close()


def db_update_cliente(nro, data):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE clientes SET
                    desc_cliente=%s, servidor=%s, ip_servidor=%s::inet,
                    iniciales=%s, desc_cobol=%s, path=%s, path_hades=%s
                WHERE nro_cliente=%s
            """, (
                data["desc_cliente"], data["servidor"], data["ip_servidor"],
                data["iniciales"], data["desc_cobol"], data["path"],
                data["path_hades"], nro,
            ))
        conn.commit()
    finally:
        conn.close()


def db_delete_cliente(nro):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM clientes WHERE nro_cliente=%s", (nro,))
        conn.commit()
    finally:
        conn.close()


def cliente_form(stdscr, row=None):
    """
    Formulario para crear o editar un cliente.
    row=None → nuevo cliente.
    Devuelve dict con los datos o None si cancela.
    """
    is_new = row is None
    values = {
        "nro_cliente":  str(row["nro_cliente"]) if row else "",
        "desc_cliente": row.get("desc_cliente") or "" if row else "",
        "servidor":     row.get("servidor")     or "" if row else "",
        "ip_servidor":  row.get("ip")           or "" if row else "",
        "iniciales":    row.get("iniciales")    or "" if row else "",
        "desc_cobol":   row.get("desc_cobol")   or "" if row else "",
        "path":         row.get("path")         or "" if row else "",
        "path_hades":   row.get("path_hades")   or "" if row else "",
    }
    current  = 0
    error    = ""
    # En edición el nro_cliente no se puede cambiar
    editable_fields = [f for f in CLIENTE_FIELDS if f[3] or is_new]

    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        titulo = "NUEVO CLIENTE" if is_new else f"EDITAR CLIENTE — {values['desc_cliente']}"
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {titulo} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        for i, (key, label, typ, editable) in enumerate(CLIENTE_FIELDS):
            y    = 2 + i * 2
            val  = values[key]
            # Marcar campo seleccionado en los editables
            field_idx = editable_fields.index((key, label, typ, editable)) \
                        if (key, label, typ, editable) in editable_fields else -1
            is_cur = (field_idx == current)

            lbl_attr  = curses.A_BOLD if is_cur else 0
            val_attr  = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if is_cur \
                        else (curses.color_pair(C_DIM) if not editable else curses.color_pair(C_NORMAL))

            stdscr.addstr(y, 2, f"{label:<20}", lbl_attr)
            val_display = f" {val:<55} " if is_cur else f" {val:<55}"
            try:
                stdscr.addstr(y, 24, val_display[:w - 26], val_attr)
            except curses.error:
                pass

        if error:
            stdscr.attron(curses.color_pair(C_ERROR) | curses.A_BOLD)
            stdscr.addstr(h - 3, 2, f" ✗ {error} ")
            stdscr.attroff(curses.color_pair(C_ERROR) | curses.A_BOLD)

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " ↑↓=Navegar  Enter=Editar campo  F10=Guardar  ESC=Cancelar ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key_pressed = stdscr.getch()
        error = ""

        if key_pressed == 27:
            return None
        elif key_pressed == curses.KEY_UP:
            current = (current - 1) % len(editable_fields)
        elif key_pressed == curses.KEY_DOWN:
            current = (current + 1) % len(editable_fields)
        elif key_pressed in (curses.KEY_ENTER, 10, 13):
            fkey, flabel, _, _ = editable_fields[current]
            nueva = ask_input(stdscr, f"{flabel}: ")
            if nueva or fkey not in ("nro_cliente", "desc_cliente"):
                values[fkey] = nueva
        elif key_pressed == curses.KEY_F10:
            # Validar
            if not values["nro_cliente"].strip():
                error = "Nro Cliente es obligatorio"; continue
            if not values["desc_cliente"].strip():
                error = "Nombre es obligatorio"; continue
            if not values["ip_servidor"].strip():
                error = "IP es obligatoria"; continue
            try:
                values["nro_cliente"] = int(values["nro_cliente"])
            except ValueError:
                error = "Nro Cliente debe ser un número"; continue
            return values


def show_detail(stdscr, row):
    """Muestra todos los campos de un cliente en modo lectura."""
    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        nombre = row.get("desc_cliente", "")
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" DETALLE CLIENTE — {nombre} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        detalles = [
            ("Nro Cliente",      str(row.get("nro_cliente", ""))),
            ("Nombre",           row.get("desc_cliente", "")),
            ("Servidor",         row.get("servidor", "")),
            ("IP",               row.get("ip", "")),
            ("Iniciales",        row.get("iniciales", "") or "—"),
            ("Desc COBOL",       row.get("desc_cobol", "") or "—"),
            ("Path Producción",  row.get("path", "") or "—"),
            ("Path Hades",       row.get("path_hades", "") or "—"),
            ("SSH Usuario",      row.get("ssh_user", "")),
            ("SSH Puerto",       str(row.get("ssh_port", ""))),
        ]
        for i, (label, val) in enumerate(detalles):
            y = 2 + i
            stdscr.addstr(y, 4, f"{label:<20}", curses.A_BOLD)
            stdscr.addstr(y, 25, val[:w - 27])

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0, " ESC/q/Enter=Volver ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        k = stdscr.getch()
        if k in (27, ord('q'), ord('Q'), curses.KEY_ENTER, 10, 13):
            break


def confirm_dialog(stdscr, msg):
    """Muestra diálogo de confirmación. Devuelve True si el usuario presiona S/s/y."""
    h, w   = stdscr.getmaxyx()
    win_w  = min(len(msg) + 16, w - 4)
    win    = curses.newwin(5, win_w, h // 2 - 2, (w - win_w) // 2)
    win.bkgd(' ', curses.color_pair(C_ERROR))
    win.border()
    win.addstr(1, 2, msg[:win_w - 4], curses.A_BOLD)
    win.addstr(3, 2, " [S]í  /  [N]o ", curses.A_BOLD)
    win.refresh()
    while True:
        k = win.getch()
        if k in (ord('s'), ord('S'), ord('y'), ord('Y')):
            del win; return True
        if k in (ord('n'), ord('N'), 27):
            del win; return False


def fetch_maquinas(search=""):
    query = """
        SELECT
            nombre,
            ip::text        AS ip,
            ssh_user,
            COALESCE(ssh_password, '') AS ssh_password,
            ssh_port,
            COALESCE(descripcion, '') AS descripcion
        FROM maquinas
        WHERE
            nombre      ILIKE %s OR
            ip::text    ILIKE %s OR
            descripcion ILIKE %s
        ORDER BY nombre
    """
    pat = f"%{search}%"
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (pat, pat, pat))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Deploys programados — BD ──────────────────────────────────────────────

def ensure_deploys_table():
    """Crea la tabla deploys_programados si no existe."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS deploys_programados (
                    id          SERIAL PRIMARY KEY,
                    usuario     TEXT        NOT NULL,
                    nro_cliente INTEGER     NOT NULL,
                    desc_cliente TEXT       NOT NULL,
                    servicios   TEXT        NOT NULL,
                    fecha_hora  TIMESTAMP   NOT NULL,
                    estado      TEXT        NOT NULL DEFAULT 'pendiente',
                    detalle     TEXT        NOT NULL DEFAULT '',
                    creado_at   TIMESTAMP   NOT NULL DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()


def db_insert_deploy_programado(usuario, nro_cliente, desc_cliente, servicios, fecha_hora):
    """Inserta un deploy programado. servicios es lista de .cbl."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO deploys_programados
                    (usuario, nro_cliente, desc_cliente, servicios, fecha_hora)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (usuario, nro_cliente, desc_cliente,
                  json.dumps(servicios), fecha_hora))
            new_id = cur.fetchone()[0]
        conn.commit()
        return new_id
    finally:
        conn.close()


def db_fetch_deploys_usuario(usuario):
    """Devuelve los deploys del usuario ordenados por fecha."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT id, usuario, nro_cliente, desc_cliente,
                       servicios, fecha_hora, estado, detalle, creado_at
                FROM deploys_programados
                WHERE usuario = %s
                ORDER BY fecha_hora
            """, (usuario,))
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                d["servicios"] = json.loads(d["servicios"])
                rows.append(d)
            return rows
    finally:
        conn.close()


def db_fetch_deploys_pendientes():
    """Devuelve todos los deploys pendientes cuya hora ya llegó."""
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT d.*, c.ip_servidor::text AS ip, c.path, c.path_hades,
                       COALESCE(m.ssh_user, 'tuxedo') AS ssh_user,
                       COALESCE(m.ssh_password, '')   AS ssh_password,
                       COALESCE(m.ssh_port, 22)       AS ssh_port
                FROM deploys_programados d
                JOIN clientes c ON c.nro_cliente = d.nro_cliente
                LEFT JOIN maquinas m ON LOWER(m.nombre) = LOWER(c.servidor)
                WHERE d.estado = 'pendiente'
                  AND d.fecha_hora <= NOW()
                ORDER BY d.fecha_hora
            """)
            rows = []
            for r in cur.fetchall():
                d = dict(r)
                d["servicios"] = json.loads(d["servicios"])
                rows.append(d)
            return rows
    finally:
        conn.close()


def db_proximo_deploy_pendiente():
    """Devuelve cuántos segundos faltan para el próximo deploy pendiente, o None."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXTRACT(EPOCH FROM (fecha_hora - NOW()))
                FROM deploys_programados
                WHERE estado = 'pendiente' AND fecha_hora > NOW()
                ORDER BY fecha_hora
                LIMIT 1
            """)
            row = cur.fetchone()
            return float(row[0]) if row else None
    finally:
        conn.close()


def db_update_deploy_estado(deploy_id, estado, detalle=""):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE deploys_programados SET estado=%s, detalle=%s
                WHERE id=%s
            """, (estado, detalle, deploy_id))
        conn.commit()
    finally:
        conn.close()


def db_delete_deploy(deploy_id):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM deploys_programados WHERE id=%s", (deploy_id,))
        conn.commit()
    finally:
        conn.close()


# ── Scheduler en segundo plano ────────────────────────────────────────────

_scheduler_event   = threading.Event()
_scheduler_running = False


def _run_scheduled_deploy(deploy):
    """Ejecuta un deploy programado en segundo plano (sin UI curses)."""
    servicios = deploy["servicios"]
    row = {
        "ip":           deploy["ip"],
        "ssh_user":     deploy["ssh_user"],
        "ssh_password": deploy["ssh_password"],
        "ssh_port":     deploy["ssh_port"],
        "path":         deploy["path"],
        "path_hades":   deploy["path_hades"],
        "desc_cliente": deploy["desc_cliente"],
    }

    resultados = []
    # Leer build.server una sola vez
    r_bs = subprocess.run(
        ssh_cmd_base(row["ip"], row["ssh_user"], row["ssh_port"]) +
        [f'cat "{row["path"]}/build.server" 2>/dev/null'],
        capture_output=True, text=True, timeout=15,
        env=_sshenv(row["ssh_password"]),
    )
    build_content = r_bs.stdout

    for cbl in servicios:
        ok, detalle = _deploy_one_silent(row, cbl, build_content, lambda m: None)
        resultados.append(f"{'✓' if ok else '✗'} {cbl}: {detalle}")

    errores = sum(1 for r in resultados if r.startswith("✗"))
    estado  = "error" if errores else "ok"
    db_update_deploy_estado(deploy["id"], estado, "\n".join(resultados))


def _scheduler_loop():
    """Hilo del scheduler. Duerme exactamente hasta el próximo deploy."""
    global _scheduler_running
    while _scheduler_running:
        try:
            pendientes = db_fetch_deploys_pendientes()
            for dep in pendientes:
                db_update_deploy_estado(dep["id"], "ejecutando")
                threading.Thread(
                    target=_run_scheduled_deploy,
                    args=(dep,),
                    daemon=True,
                ).start()

            # Calcular tiempo hasta el próximo deploy
            secs = db_proximo_deploy_pendiente()
            # Máximo 5 min de espera para no consumir recursos innecesariamente
            wait = min(secs, 300) if secs is not None else 300
        except Exception:
            wait = 300

        # Event.wait libera el GIL y no consume CPU mientras duerme.
        # Se puede despertar antes si llega un nuevo deploy (notify_scheduler).
        _scheduler_event.wait(timeout=wait)
        _scheduler_event.clear()


def notify_scheduler():
    """Despierta el scheduler inmediatamente (llamar al programar un nuevo deploy)."""
    _scheduler_event.set()


# ── SSH helpers ────────────────────────────────────────────────────────────

def _sshenv(password=""):
    """Env dict con SSHPASS seteado, para usar con sshpass -e.
    Evita que la contraseña quede visible en la lista de procesos."""
    e = os.environ.copy()
    if password:
        e["SSHPASS"] = password
    return e


def ssh_cmd_base(ip, user, port):
    """Comando SSH base. Usar junto con env=_sshenv(password) en subprocess."""
    return [
        "sshpass", "-e",
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=QUIET",
        "-p", str(port),
        f"{user}@{ip}",
    ]


def hades_scp_cmd(remote_path, local_path):
    """Construye el comando scp desde hades según el método de auth."""
    common = [
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=QUIET",
        "-P", str(HADES["port"]),
    ]
    src = f"{HADES['user']}@{HADES['host']}:{remote_path}"
    if HADES.get("auth") == "password":
        return ["sshpass", "-e", "scp"] + common + [src, local_path]
    else:
        return ["scp", "-i", HADES["key"]] + common + [src, local_path]


def hades_cmd_base():
    """Construye el comando SSH base para hades según el método de auth configurado."""
    common = [
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=QUIET",
        "-p", str(HADES["port"]),
    ]
    target = f"{HADES['user']}@{HADES['host']}"

    if HADES.get("auth") == "password":
        return ["sshpass", "-e", "ssh"] + common + [target]
    else:
        return ["ssh", "-i", HADES["key"]] + common + [target]


def ssh_connect(ip, user, password, port=22, remote_path=None):
    """SSH interactivo, opcionalmente hace cd al path."""
    curses.endwin()
    print(f"\n  Conectando a {user}@{ip}:{port} ...")
    if remote_path:
        print(f"  Directorio: {remote_path}")
    print()

    remote_cmd = f'cd "{remote_path}" && exec bash -l' if remote_path else None

    cmd = ssh_cmd_base(ip, user, port)
    cmd.insert(3, "-t")          # TTY para shell interactivo (pos 3 = después de "ssh")
    if remote_cmd:
        cmd.append(remote_cmd)

    try:
        subprocess.run(cmd, env=_sshenv(password))
    except FileNotFoundError:
        print("\n  ERROR: 'sshpass' no está instalado.")
        print("  sudo apt install sshpass\n")
        input("  Presiona Enter para continuar...")
    except KeyboardInterrupt:
        pass


# ── Viewer de logs / grep en tiempo real ──────────────────────────────────

def stream_viewer(stdscr, row, grep_pattern=None):
    """Muestra en tiempo real: cd path && log  (o log|grep patron)."""
    ip       = row["ip"]
    user     = row["ssh_user"]
    password = row["ssh_password"]
    port     = row["ssh_port"]
    path     = row.get("path") or ""
    nombre   = row.get("desc_cliente") or row.get("nombre", ip)

    if grep_pattern:
        remote_cmd = f'cd "{path}" && log|grep {grep_pattern}'
    else:
        remote_cmd = f'cd "{path}" && log'

    cmd = ssh_cmd_base(ip, user, port) + [remote_cmd]

    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=0,
        start_new_session=True,
        env=_sshenv(password),
    )

    fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
    fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

    h, w      = stdscr.getmaxyx()
    MAX_LINES = 2000
    lines     = deque(maxlen=MAX_LINES)
    paused    = False
    scroll_off = 0

    stdscr.nodelay(True)

    try:
        while True:
            try:
                raw = proc.stdout.read(4096)
                if raw:
                    for line in raw.decode("utf-8", errors="replace").splitlines():
                        lines.append(line)
                    if not paused:
                        scroll_off = 0
            except (BlockingIOError, TypeError):
                pass

            h, w = stdscr.getmaxyx()
            visible_h = h - 3

            stdscr.erase()

            if grep_pattern:
                title = f" GREP — {nombre}  log|grep {grep_pattern} "
                hdr_color = curses.color_pair(C_GREP) | curses.A_BOLD
            else:
                title = f" LOG — {nombre}  cd {path} && log "
                hdr_color = curses.color_pair(C_HEADER) | curses.A_BOLD
            stdscr.attron(hdr_color)
            stdscr.addstr(0, 0, title[:w - 1].ljust(w - 1))
            stdscr.attroff(hdr_color)

            stdscr.attron(curses.color_pair(C_TITLE))
            stdscr.addstr(1, 0, "─" * (w - 1))
            stdscr.attroff(curses.color_pair(C_TITLE))

            all_lines = list(lines)
            total     = len(all_lines)
            scroll_off = max(0, min(scroll_off, max(0, total - visible_h)))
            end_idx    = total - scroll_off
            start_idx  = max(0, end_idx - visible_h)
            visible    = all_lines[start_idx:end_idx]

            for i, line in enumerate(visible):
                try:
                    if grep_pattern:
                        stdscr.attron(curses.color_pair(C_GREP))
                        stdscr.addstr(2 + i, 0, line[:w - 1])
                        stdscr.attroff(curses.color_pair(C_GREP))
                    else:
                        stdscr.addstr(2 + i, 0, line[:w - 1])
                except curses.error:
                    pass

            mode_txt = "PAUSADO" if paused else "EN VIVO"
            footer = (
                f" {mode_txt}  q/ESC=Volver  ↑↓/PgUp/PgDn=Scroll  "
                f"Inicio=Top  Fin=Ultimo  p=Pausar  [{total} líneas] "
            )
            stdscr.attron(curses.color_pair(C_STATUS))
            stdscr.addstr(h - 1, 0, footer[:w - 1].ljust(w - 1))
            stdscr.attroff(curses.color_pair(C_STATUS))
            stdscr.refresh()

            key = stdscr.getch()
            if key in (ord('q'), ord('Q'), 27):
                break
            elif key == ord('p'):
                paused = not paused
            elif key == curses.KEY_UP:
                scroll_off += 1
                paused = True
            elif key == curses.KEY_DOWN:
                scroll_off = max(0, scroll_off - 1)
            elif key == curses.KEY_PPAGE:
                scroll_off += visible_h
                paused = True
            elif key == curses.KEY_NPAGE:
                scroll_off = max(0, scroll_off - visible_h)
            elif key == curses.KEY_HOME:
                scroll_off = max(0, total - visible_h)
                paused = True
            elif key == curses.KEY_END:
                scroll_off = 0
                paused = False

            if proc.poll() is not None and not lines:
                break

            time.sleep(0.05)

    finally:
        try:
            os.killpg(os.getpgid(proc.pid), 9)
        except Exception:
            pass
        try:
            proc.wait(timeout=2)
        except Exception:
            pass
        stdscr.nodelay(False)


# ── Deploy COBOL ───────────────────────────────────────────────────────────

def hades_run(remote_cmd, timeout=60):
    """Ejecuta un comando en hades y devuelve (stdout, stderr, returncode)."""
    cmd = hades_cmd_base() + [remote_cmd]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout,
                            env=_sshenv(HADES.get("password", "")))
    return result.stdout, result.stderr, result.returncode


def list_cbl_files(path_hades):
    """Lista archivos .cbl en path_hades (hades). Devuelve lista ordenada."""
    stdout, _, _ = hades_run(
        f'ls "{path_hades}"/*.cbl "{path_hades}"/*.CBL 2>/dev/null | xargs -I{{}} basename {{}}'
    )
    files = sorted(set(f.strip() for f in stdout.splitlines() if f.strip()))
    return files


def cbl_picker(stdscr, row):
    """
    Muestra lista de archivos .cbl del path_hades del cliente.
    Devuelve el nombre del archivo seleccionado, o None si cancela.
    """
    path_hades = row.get("path_hades") or ""
    nombre     = row.get("desc_cliente", "")

    h, w = stdscr.getmaxyx()

    # Pantalla de carga
    stdscr.erase()
    stdscr.attron(curses.color_pair(C_HEADER))
    stdscr.addstr(0, 0, f" DEPLOY — {nombre} — cargando .cbl de hades...".ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_HEADER))
    stdscr.refresh()

    try:
        files = list_cbl_files(path_hades)
    except Exception as e:
        files = []
        _show_message(stdscr, f"Error conectando a hades: {e}", error=True)
        return None

    if not files:
        _show_message(stdscr, f"No se encontraron .cbl en {path_hades}", error=True)
        return None

    selected = 0
    offset   = 0
    search   = ""

    while True:
        filtered = [f for f in files if search.lower() in f.lower()]

        h, w    = stdscr.getmaxyx()
        list_h  = h - 6

        selected = min(selected, max(0, len(filtered) - 1))
        if selected < offset:
            offset = selected
        elif selected >= offset + list_h:
            offset = selected - list_h + 1

        stdscr.erase()

        # Header
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" DEPLOY — {nombre} — Selecciona .cbl a compilar ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        # Info
        stdscr.attron(curses.color_pair(C_WARN))
        stdscr.addstr(1, 2, f"Hades path: {path_hades}")
        stdscr.attroff(curses.color_pair(C_WARN))

        # Búsqueda
        stdscr.attron(curses.color_pair(C_SEARCH))
        stdscr.addstr(2, 0, " " * (w - 1))
        stdscr.addstr(2, 2, f" Buscar: {search}_")
        stdscr.attroff(curses.color_pair(C_SEARCH))

        # Columna
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        stdscr.addstr(3, 0, f"  {'ARCHIVO .cbl':<50}  {'ACCIÓN'}".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

        # Lista
        for i, fname in enumerate(filtered[offset: offset + list_h]):
            y     = 4 + i
            abs_i = offset + i
            int_name = fname.rsplit(".", 1)[0] + ".int"
            line = f"  {fname:<50}  → compilar → {int_name}"
            if abs_i == selected:
                stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                stdscr.addstr(y, 0, line[:w - 1].ljust(w - 1))
                stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            else:
                stdscr.addstr(y, 0, line[:w - 1])

        for i in range(len(filtered[offset: offset + list_h]), list_h):
            try:
                stdscr.addstr(4 + i, 0, " " * (w - 1))
            except curses.error:
                pass

        # Footer
        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            f" Enter=Compilar y deployar  ESC/q=Cancelar  [{len(filtered)} archivos] ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))

        stdscr.refresh()

        key = stdscr.getch()
        if key == -1:
            time.sleep(0.05)
            continue

        if key in (ord('q'), ord('Q'), 27):
            return None
        elif key == curses.KEY_UP:
            selected = max(0, selected - 1)
        elif key == curses.KEY_DOWN:
            selected = min(len(filtered) - 1, selected + 1)
        elif key == curses.KEY_PPAGE:
            selected = max(0, selected - list_h)
            offset   = max(0, offset  - list_h)
        elif key == curses.KEY_NPAGE:
            selected = min(len(filtered) - 1, selected + list_h)
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            search = search[:-1]
            selected = 0; offset = 0
        elif key in (curses.KEY_ENTER, 10, 13):
            if filtered:
                return filtered[selected]
        elif 32 <= key <= 126:
            search += chr(key)
            selected = 0; offset = 0


def find_build_target(build_server_content, int_file):
    """
    Parsea build.server (Makefile) y devuelve el todoXX/TODOXX exacto
    que contiene el .int — respeta mayúsculas/minúsculas tal como
    está escrito en el archivo.
    """
    import re
    lines = build_server_content.splitlines()

    # 1. Encontrar en qué bloque compilaXX/COMPILAXX está el .int
    current_compila = None
    target_num = None
    for line in lines:
        m = re.match(r'^(compila\d+)\s*:', line, re.IGNORECASE)
        if m:
            current_compila = m.group(1)
        if current_compila and int_file in line:
            num = re.search(r'(\d+)$', current_compila)
            if num:
                target_num = num.group(1)
                break

    if not target_num:
        return None

    # 2. Buscar el target todoXX/TODOXX exacto con ese número en el archivo
    for line in lines:
        m = re.match(r'^(todo' + target_num + r')\s*:', line, re.IGNORECASE)
        if m:
            return m.group(1)   # devuelve el nombre exacto: todo15 o TODO15

    return None


def run_deploy(stdscr, row, cbl_file):
    """
    Pipeline completo de deploy COBOL:
      1. Compilar en hades:  cd path_hades && cob Servicio.cbl
      2. Descargar .int de hades → /tmp/
      3. Backup en producción: Servicio.int → Servicio.int.YYYYMMDD
      4. Subir .int a producción
      5. Build en producción: . ./env.pro && make -f build.server todoXX
    """
    ip         = row["ip"]
    user       = row["ssh_user"]
    password   = row["ssh_password"]
    port       = row["ssh_port"]
    path_prod  = row["path"]
    path_hades = row["path_hades"]
    nombre     = row["desc_cliente"]

    service    = cbl_file.rsplit(".", 1)[0]          # sin extensión
    int_file   = service + ".int"
    date_str   = time.strftime("%Y%m%d")
    backup_name = f"{int_file}.{date_str}"
    local_tmp  = f"/tmp/flv_deploy_{int_file}"

    # Log de pasos: lista de (estado, mensaje)
    # estado: "wait" | "run" | "ok" | "err"
    steps = [
        ["wait", f"1/5  Compilar en hades:  cd {path_hades} && cob {cbl_file}"],
        ["wait", f"2/5  Descargar de hades:  {path_hades}/{int_file}  →  {local_tmp}"],
        ["wait", f"3/5  Backup en producción:  {int_file}  →  {backup_name}"],
        ["wait", f"4/5  Subir a producción:  {local_tmp}  →  {path_prod}/{int_file}"],
        ["wait", f"5/5  Build en producción:  . ./env.pro && make -f build.server todoXX"],
    ]
    output_lines = deque(maxlen=200)

    def redraw(current_step=-1, done=False, error=False):
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        # Header
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" DEPLOY — {nombre} — {cbl_file} → {int_file} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        # Pasos
        for i, (estado, msg) in enumerate(steps):
            y = 2 + i
            if estado == "ok":
                mark = " ✓ "
                attr = curses.color_pair(C_OK) | curses.A_BOLD
            elif estado == "err":
                mark = " ✗ "
                attr = curses.color_pair(C_ERROR) | curses.A_BOLD
            elif estado == "run":
                mark = " » "
                attr = curses.color_pair(C_WARN) | curses.A_BOLD
            else:
                mark = "   "
                attr = curses.color_pair(C_DIM)
            try:
                stdscr.addstr(y, 0, mark, attr)
                stdscr.addstr(y, 3, msg[:w - 4], attr)
            except curses.error:
                pass

        # Separador  (5 pasos en filas 2..6, separador en 8)
        stdscr.attron(curses.color_pair(C_TITLE))
        try:
            stdscr.addstr(8, 0, "─" * (w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_TITLE))

        # Salida del comando
        out_h = h - 11
        lines = list(output_lines)[-(out_h):]
        for i, line in enumerate(lines):
            try:
                stdscr.addstr(9 + i, 0, line[:w - 1])
            except curses.error:
                pass

        # Footer
        if done and not error:
            footer = " ✓ Deploy completado!  Presiona Enter para volver "
            stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
        elif error:
            footer = " ✗ Error en deploy  Presiona Enter para volver "
            stdscr.attron(curses.color_pair(C_ERROR) | curses.A_BOLD)
        else:
            footer = " Ejecutando... "
            stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h - 1, 0, footer.ljust(w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
        stdscr.attroff(curses.color_pair(C_ERROR) | curses.A_BOLD)
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    ssh_env  = _sshenv(password)
    hades_env = _sshenv(HADES.get("password", ""))

    def run_step(idx, cmd, timeout=120, env=None):
        steps[idx][0] = "run"
        redraw(idx)
        output_lines.append(f"$ {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        out = (result.stdout + result.stderr).strip()
        for line in out.splitlines():
            output_lines.append(line)
        if result.returncode == 0:
            steps[idx][0] = "ok"
        else:
            steps[idx][0] = "err"
        redraw(idx)
        return result.returncode == 0

    redraw()
    error = False

    # ── Paso 1: Compilar en hades ──────────────────────────────────────────
    cmd1 = hades_cmd_base() + [f'cd "{path_hades}" && cob {cbl_file}']
    if not run_step(0, cmd1, timeout=120, env=hades_env):
        error = True

    # ── Paso 2: Descargar .int de hades ───────────────────────────────────
    if not error:
        cmd2 = hades_scp_cmd(f"{path_hades}/{int_file}", local_tmp)
        if not run_step(1, cmd2, timeout=60, env=hades_env):
            error = True

    # ── Paso 3: Backup en producción ──────────────────────────────────────
    if not error:
        backup_cmd = f'cp "{path_prod}/{int_file}" "{path_prod}/{backup_name}" 2>/dev/null; echo "backup ok"'
        cmd3 = ssh_cmd_base(ip, user, port) + [backup_cmd]
        # El backup puede fallar si el .int no existe aún (primera vez) — lo permitimos
        steps[2][0] = "run"
        redraw(2)
        output_lines.append(f"$ backup {int_file} → {backup_name}")
        result3 = subprocess.run(cmd3, capture_output=True, text=True, timeout=30, env=ssh_env)
        out3 = (result3.stdout + result3.stderr).strip()
        for line in out3.splitlines():
            output_lines.append(line)
        steps[2][0] = "ok"   # backup es best-effort
        redraw(2)

    # ── Paso 4: Subir .int a producción ───────────────────────────────────
    if not error:
        cmd4 = [
            "sshpass", "-e",
            "scp",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "LogLevel=QUIET",
            "-P", str(port),
            local_tmp,
            f"{user}@{ip}:{path_prod}/{int_file}",
        ]
        if not run_step(3, cmd4, timeout=60, env=ssh_env):
            error = True

    # ── Paso 5: Build en producción ───────────────────────────────────────
    if not error:
        steps[4][0] = "run"
        # Leer build.server para encontrar el todoXX
        read_cmd = ssh_cmd_base(ip, user, port) + [f'cat "{path_prod}/build.server" 2>/dev/null']
        r_bs = subprocess.run(read_cmd, capture_output=True, text=True, timeout=15, env=ssh_env)
        build_content = r_bs.stdout

        target = find_build_target(build_content, int_file) if build_content else None

        if not target:
            output_lines.append(f"! No se encontró target para {int_file} en build.server")
            steps[4][0] = "err"
            error = True
        else:
            steps[4][1] = f"5/5  Build en producción:  . ./env.pro && make -f build.server {target}"
            redraw(4)
            output_lines.append(f"$ target encontrado: {target}")

            build_cmd_remote = f'cd "{path_prod}" && . ./env.pro && make -f build.server {target}'
            cmd5 = ssh_cmd_base(ip, user, port) + [build_cmd_remote]

            output_lines.append(f"$ {build_cmd_remote}")
            proc5 = subprocess.Popen(cmd5, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                     bufsize=1, text=True, env=ssh_env)
            for line in proc5.stdout:
                output_lines.append(line.rstrip())
                redraw(4)
            proc5.wait(timeout=180)

            if proc5.returncode == 0:
                steps[4][0] = "ok"
            else:
                steps[4][0] = "err"
                error = True
            redraw(4)

    # Limpiar temporal
    try:
        os.remove(local_tmp)
    except Exception:
        pass

    redraw(done=True, error=error)

    # Esperar Enter para volver
    stdscr.nodelay(False)
    while True:
        key = stdscr.getch()
        if key in (curses.KEY_ENTER, 10, 13, ord('q'), ord('Q'), 27):
            break


# ── Multi-Deploy ──────────────────────────────────────────────────────────

def multiline_input(stdscr, title, hint=""):
    """
    Caja de texto multi-línea para pegar lista de .cbl (uno por línea).
    Devuelve lista de strings o [] si cancela.
    Confirma con F10 o Ctrl+D. Cancela con ESC.
    """
    h, w = stdscr.getmaxyx()
    lines   = [""]
    cur_row = 0
    box_y   = 3
    box_h   = h - 7
    box_w   = w - 4
    offset  = 0

    curses.curs_set(1)
    stdscr.nodelay(False)

    while True:
        stdscr.erase()

        # Header
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {title} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        # Hint
        stdscr.attron(curses.color_pair(C_WARN))
        stdscr.addstr(1, 2, hint[:w - 3])
        stdscr.attroff(curses.color_pair(C_WARN))

        # Borde de la caja
        stdscr.attron(curses.color_pair(C_TITLE))
        stdscr.addstr(box_y - 1, 0, "┌" + "─" * (box_w - 1) + "┐")
        for r in range(box_h):
            stdscr.addstr(box_y + r, 0, "│")
            stdscr.addstr(box_y + r, box_w, "│")
        stdscr.addstr(box_y + box_h, 0, "└" + "─" * (box_w - 1) + "┘")
        stdscr.attroff(curses.color_pair(C_TITLE))

        # Ajustar offset vertical
        if cur_row < offset:
            offset = cur_row
        elif cur_row >= offset + box_h:
            offset = cur_row - box_h + 1

        # Dibujar líneas
        for r in range(box_h):
            idx = offset + r
            text = lines[idx][:box_w - 2] if idx < len(lines) else ""
            try:
                stdscr.addstr(box_y + r, 1, text.ljust(box_w - 1))
            except curses.error:
                pass

        # Footer
        count = sum(1 for l in lines if l.strip())
        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            f" F10/Ctrl+D=Confirmar ({count} servicio(s))  ESC=Cancelar  Pega tu lista aquí ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))

        # Cursor
        try:
            stdscr.move(box_y + (cur_row - offset), 1 + len(lines[cur_row]))
        except curses.error:
            pass
        stdscr.refresh()

        key = stdscr.getch()

        # Confirmar
        if key in (curses.KEY_F10, 4):   # F10 o Ctrl+D
            curses.curs_set(0)
            result = [l.strip() for l in lines if l.strip()]
            return result

        # Cancelar
        elif key == 27:
            curses.curs_set(0)
            return []

        # Backspace
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if lines[cur_row]:
                lines[cur_row] = lines[cur_row][:-1]
            elif cur_row > 0:
                prev = lines[cur_row - 1]
                lines.pop(cur_row)
                cur_row -= 1
                lines[cur_row] = prev  # fusionar (la línea anterior ya era la misma)

        # Nueva línea (Enter o pegar con \n)
        elif key in (10, 13, curses.KEY_ENTER):
            lines.insert(cur_row + 1, "")
            cur_row += 1

        # Flechas
        elif key == curses.KEY_UP:
            cur_row = max(0, cur_row - 1)
        elif key == curses.KEY_DOWN:
            cur_row = min(len(lines) - 1, cur_row + 1)

        # Carácter normal
        elif 32 <= key <= 126:
            lines[cur_row] += chr(key)


def _deploy_one_silent(row, cbl_file, build_content, log_cb):
    """
    Ejecuta el pipeline de deploy para un solo .cbl sin UI curses.
    Llama log_cb(msg) para reportar progreso.
    Retorna (ok: bool, detalle: str)
    """
    ip         = row["ip"]
    user       = row["ssh_user"]
    password   = row["ssh_password"]
    port       = row["ssh_port"]
    path_prod  = row["path"]
    path_hades = row["path_hades"]

    service     = cbl_file.rsplit(".", 1)[0]
    int_file    = service + ".int"
    date_str    = time.strftime("%Y%m%d")
    backup_name = f"{int_file}.{date_str}"
    local_tmp   = f"/tmp/flv_mdeploy_{int_file}"

    ssh_env   = _sshenv(password)
    hades_env = _sshenv(HADES.get("password", ""))

    def run(cmd, timeout=120, env=None):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        return r.returncode == 0, (r.stdout + r.stderr).strip()

    # 1. Compilar en hades
    log_cb(f"  [1/5] cob {cbl_file}")
    ok, out = run(hades_cmd_base() + [f'cd "{path_hades}" && cob {cbl_file}'], env=hades_env)
    if not ok:
        return False, f"Error compilando: {out[-200:]}"

    # 2. Descargar .int
    log_cb(f"  [2/5] scp {int_file} ← hades")
    ok, out = run(hades_scp_cmd(f"{path_hades}/{int_file}", local_tmp), timeout=60, env=hades_env)
    if not ok:
        return False, f"Error descargando .int: {out[-200:]}"

    # 3. Backup (best-effort)
    log_cb(f"  [3/5] backup {int_file} → {backup_name}")
    run(ssh_cmd_base(ip, user, port) +
        [f'cp "{path_prod}/{int_file}" "{path_prod}/{backup_name}" 2>/dev/null; true'],
        timeout=15, env=ssh_env)

    # 4. Subir .int
    log_cb(f"  [4/5] scp {int_file} → producción")
    ok, out = run([
        "sshpass", "-e", "scp",
        "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=QUIET", "-P", str(port),
        local_tmp, f"{user}@{ip}:{path_prod}/{int_file}",
    ], timeout=60, env=ssh_env)
    if not ok:
        return False, f"Error subiendo .int: {out[-200:]}"

    # 5. Build
    target = find_build_target(build_content, int_file)
    if not target:
        return False, f"No se encontró target en build.server para {int_file}"
    log_cb(f"  [5/5] make {target}")
    build_cmd = f'cd "{path_prod}" && . ./env.pro && make -f build.server {target}'
    ok, out = run(ssh_cmd_base(ip, user, port) + [build_cmd], timeout=180, env=ssh_env)

    try:
        os.remove(local_tmp)
    except Exception:
        pass

    if not ok:
        return False, f"Error en make {target}: {out[-200:]}"
    return True, f"OK → {target}"


def run_multi_deploy(stdscr, row, cbl_files):
    """
    Despliega múltiples .cbl de forma secuencial con vista de progreso.
    """
    ip       = row["ip"]
    user     = row["ssh_user"]
    password = row["ssh_password"]
    port     = row["ssh_port"]
    path_prod = row["path"]
    nombre   = row["desc_cliente"]
    total    = len(cbl_files)

    # Estado por servicio: "wait" | "run" | "ok" | "err"
    states  = ["wait"] * total
    details = [""] * total
    log_lines = deque(maxlen=300)

    # Leer build.server una sola vez
    log_lines.append("Leyendo build.server...")
    r_bs = subprocess.run(
        ssh_cmd_base(ip, user, port) + [f'cat "{path_prod}/build.server" 2>/dev/null'],
        capture_output=True, text=True, timeout=15, env=_sshenv(password),
    )
    build_content = r_bs.stdout

    def redraw(current=-1, done=False):
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        # Header
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        ok_n  = states.count("ok")
        err_n = states.count("err")
        stdscr.addstr(0, 0,
            f" MULTI-DEPLOY — {nombre} — {ok_n}/{total} OK  {err_n} errores ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        # Lista de servicios (mitad izquierda, max 20 filas)
        list_h  = min(total, (h - 4) // 2)
        list_w  = w // 2 - 2
        svc_off = max(0, current - list_h + 1) if current >= 0 else 0

        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        stdscr.addstr(1, 0, f" {'SERVICIO':<35}  ST  DETALLE"[:list_w + 6])
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

        for i in range(list_h):
            idx = svc_off + i
            if idx >= total:
                break
            st  = states[idx]
            cbl = cbl_files[idx]
            det = details[idx]
            if st == "ok":
                mark = "✓"; attr = curses.color_pair(C_OK) | curses.A_BOLD
            elif st == "err":
                mark = "✗"; attr = curses.color_pair(C_ERROR) | curses.A_BOLD
            elif st == "run":
                mark = "»"; attr = curses.color_pair(C_WARN) | curses.A_BOLD
            else:
                mark = " "; attr = curses.color_pair(C_DIM)
            line = f" {mark} {cbl:<35}  {det}"
            try:
                if idx == current:
                    stdscr.attron(attr | curses.A_BOLD)
                    stdscr.addstr(2 + i, 0, line[:w - 1].ljust(w - 1))
                    stdscr.attroff(attr | curses.A_BOLD)
                else:
                    stdscr.attron(attr)
                    stdscr.addstr(2 + i, 0, line[:w - 1])
                    stdscr.attroff(attr)
            except curses.error:
                pass

        # Separador
        sep_y = 2 + list_h
        stdscr.attron(curses.color_pair(C_TITLE))
        try:
            stdscr.addstr(sep_y, 0, "─" * (w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_TITLE))

        # Log de salida
        log_h = h - sep_y - 3
        visible_log = list(log_lines)[-(log_h):]
        for i, line in enumerate(visible_log):
            try:
                stdscr.addstr(sep_y + 1 + i, 0, line[:w - 1])
            except curses.error:
                pass

        # Footer
        if done:
            ok_n  = states.count("ok")
            err_n = states.count("err")
            if err_n == 0:
                footer = f" ✓ Completado: {ok_n}/{total} OK  —  Enter para volver "
                stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
            else:
                footer = f" ✗ Completado con errores: {ok_n} OK  {err_n} fallidos  —  Enter para volver "
                stdscr.attron(curses.color_pair(C_ERROR) | curses.A_BOLD)
        else:
            footer = f" Ejecutando {current + 1}/{total}... "
            stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h - 1, 0, footer.ljust(w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
        stdscr.attroff(curses.color_pair(C_ERROR) | curses.A_BOLD)
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    redraw()

    for i, cbl_file in enumerate(cbl_files):
        states[i] = "run"
        details[i] = "..."
        log_lines.append(f"▶ [{i+1}/{total}] {cbl_file}")
        redraw(i)

        def log_cb(msg):
            log_lines.append(msg)
            redraw(i)

        ok, detail = _deploy_one_silent(row, cbl_file, build_content, log_cb)
        states[i]  = "ok" if ok else "err"
        details[i] = detail
        log_lines.append(f"{'✓' if ok else '✗'} {cbl_file}: {detail}")
        redraw(i)

    redraw(current=total - 1, done=True)

    stdscr.nodelay(False)
    while True:
        key = stdscr.getch()
        if key in (curses.KEY_ENTER, 10, 13, ord('q'), ord('Q'), 27):
            break


# ── Widgets compartidos ────────────────────────────────────────────────────

def _show_message(stdscr, msg, error=False):
    h, w  = stdscr.getmaxyx()
    win_w = min(len(msg) + 8, w - 4)
    win   = curses.newwin(3, win_w, h // 2 - 1, (w - win_w) // 2)
    win.bkgd(' ', curses.color_pair(C_ERROR if error else C_OK))
    win.border()
    win.addstr(1, 2, msg[:win_w - 4])
    win.refresh()
    time.sleep(2)
    del win
    stdscr.touchwin()
    stdscr.refresh()


def ask_input(stdscr, prompt):
    h, w  = stdscr.getmaxyx()
    win_w = min(70, w - 4)
    win   = curses.newwin(5, win_w, h // 2 - 2, (w - win_w) // 2)
    win.bkgd(' ', curses.color_pair(C_HEADER))
    win.border()
    win.addstr(1, 2, prompt[:win_w - 4], curses.A_BOLD)
    win.refresh()
    curses.echo()
    curses.curs_set(1)
    try:
        value = win.getstr(2, 2, win_w - 6).decode("utf-8").strip()
    except Exception:
        value = ""
    finally:
        curses.noecho()
        curses.curs_set(0)
    del win
    stdscr.touchwin()
    stdscr.refresh()
    return value


# ── Widgets de deploy programado ─────────────────────────────────────────

def ask_datetime(stdscr, prompt="Seleccioná fecha y hora:"):
    """Popup calendario (fase 1) + popup hora (fase 2). Devuelve datetime o None."""
    import calendar as _cal

    now   = datetime.now()
    year  = now.year
    month = now.month
    day   = now.day

    DIAS  = ["Lu", "Ma", "Mi", "Ju", "Vi", "Sá", "Do"]
    MESES = ["Enero","Febrero","Marzo","Abril","Mayo","Junio",
             "Julio","Agosto","Septiembre","Octubre","Noviembre","Diciembre"]

    # ── Fase 1: popup calendario ─────────────────────────────────────────────
    def draw_cal():
        sh, sw = stdscr.getmaxyx()
        pop_w, pop_h = 34, 13
        py = max(1, (sh - pop_h) // 2)
        px = max(0, (sw - pop_w) // 2)

        # bordes
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try:
            stdscr.addstr(py, px, "┌" + "─" * (pop_w - 2) + "┐")
            for r in range(1, pop_h - 1):
                stdscr.addstr(py + r, px, "│" + " " * (pop_w - 2) + "│")
            stdscr.addstr(py + pop_h - 1, px, "└" + "─" * (pop_w - 2) + "┘")
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

        # título
        tit = f"  {MESES[month - 1]} {year}  "
        try:
            stdscr.addstr(py, px + (pop_w - len(tit)) // 2, tit,
                          curses.color_pair(C_HEADER) | curses.A_BOLD)
        except curses.error:
            pass

        # prompt
        try:
            stdscr.addstr(py + 1, px + 2, prompt[:pop_w - 4])
        except curses.error:
            pass

        # cabecera días
        for i, d in enumerate(DIAS):
            try:
                stdscr.addstr(py + 2, px + 2 + i * 4, f"{d:>3}",
                              curses.color_pair(C_TITLE) | curses.A_BOLD)
            except curses.error:
                pass

        # días del mes
        first_wd, days_in_month = _cal.monthrange(year, month)
        row_y = py + 3
        col   = first_wd
        for d in range(1, days_in_month + 1):
            x = px + 2 + col * 4
            try:
                if d == day:
                    stdscr.addstr(row_y, x, f"{d:>3}",
                                  curses.color_pair(C_SELECTED) | curses.A_BOLD)
                else:
                    stdscr.addstr(row_y, x, f"{d:>3}")
            except curses.error:
                pass
            col += 1
            if col > 6:
                col = 0
                row_y += 1

        hint = " ←→↑↓  PgUp/PgDn=Mes  Enter=OK  ESC=Cancelar "
        try:
            stdscr.addstr(py + pop_h - 1,
                          px + max(0, (pop_w - len(hint)) // 2),
                          hint, curses.color_pair(C_STATUS))
        except curses.error:
            pass
        stdscr.refresh()

    while True:
        draw_cal()
        key = stdscr.getch()

        if key == 27:
            return None
        elif key in (curses.KEY_ENTER, 10, 13):
            break

        _, days_in_month = _cal.monthrange(year, month)
        if key == curses.KEY_RIGHT:
            day = min(day + 1, days_in_month)
        elif key == curses.KEY_LEFT:
            day = max(day - 1, 1)
        elif key == curses.KEY_DOWN:
            day = min(day + 7, days_in_month)
        elif key == curses.KEY_UP:
            day = max(day - 7, 1)
        elif key == curses.KEY_PPAGE:
            month -= 1
            if month < 1:
                month = 12; year -= 1
            _, dim = _cal.monthrange(year, month)
            day = min(day, dim)
        elif key == curses.KEY_NPAGE:
            month += 1
            if month > 12:
                month = 1; year += 1
            _, dim = _cal.monthrange(year, month)
            day = min(day, dim)

    # ── Fase 2: popup hora ───────────────────────────────────────────────────
    time_buf = ""   # hasta 4 dígitos: HHMM
    err_msg  = ""

    def draw_time():
        sh, sw = stdscr.getmaxyx()
        pop_w, pop_h = 28, 8
        py = max(1, (sh - pop_h) // 2)
        px = max(0, (sw - pop_w) // 2)

        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try:
            stdscr.addstr(py, px, "┌" + "─" * (pop_w - 2) + "┐")
            for r in range(1, pop_h - 1):
                stdscr.addstr(py + r, px, "│" + " " * (pop_w - 2) + "│")
            stdscr.addstr(py + pop_h - 1, px, "└" + "─" * (pop_w - 2) + "┘")
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

        tit = " Hora del deploy "
        try:
            stdscr.addstr(py, px + (pop_w - len(tit)) // 2, tit,
                          curses.color_pair(C_HEADER) | curses.A_BOLD)
        except curses.error:
            pass

        fecha_str = f"Fecha: {day:02d}/{month:02d}/{year}"
        try:
            stdscr.addstr(py + 1, px + 2, fecha_str)
        except curses.error:
            pass

        try:
            stdscr.addstr(py + 2, px + 2, "Hora:  (ingresá HH:MM)")
        except curses.error:
            pass

        # campo HH:MM — mostrar dígitos tipados + _ para pendientes
        d = time_buf.ljust(4, "_")
        campo = f"  {d[0]}{d[1]}:{d[2]}{d[3]}  "
        try:
            stdscr.addstr(py + 4, px + (pop_w - len(campo)) // 2,
                          campo, curses.color_pair(C_SELECTED) | curses.A_BOLD)
        except curses.error:
            pass

        if err_msg:
            try:
                stdscr.addstr(py + 5, px + 2,
                              err_msg[:pop_w - 4], curses.color_pair(C_ERROR))
            except curses.error:
                pass

        hint = " 0-9=Escribir  Bksp=Borrar  Enter=OK "
        try:
            stdscr.addstr(py + pop_h - 1,
                          px + max(0, (pop_w - len(hint)) // 2),
                          hint, curses.color_pair(C_STATUS))
        except curses.error:
            pass
        stdscr.refresh()

    while True:
        draw_time()
        err_msg = ""
        key = stdscr.getch()

        if key == 27:
            return None
        elif key in (curses.KEY_ENTER, 10, 13):
            if len(time_buf) < 4:
                err_msg = "Completá HH:MM"
                continue
            hh, mm = int(time_buf[:2]), int(time_buf[2:])
            if not (0 <= hh <= 23 and 0 <= mm <= 59):
                err_msg = "Hora inválida (00:00 – 23:59)"
                time_buf = ""
                continue
            try:
                return datetime(year, month, day, hh, mm)
            except ValueError:
                err_msg = "Fecha inválida"
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            time_buf = time_buf[:-1]
        elif 48 <= key <= 57 and len(time_buf) < 4:   # dígitos 0-9
            time_buf += chr(key)


def deploy_when_picker(stdscr, titulo=""):
    """Pregunta si ejecutar ahora o programar. Devuelve 'now', 'schedule' o None."""
    options = [
        ("now",      "Ejecutar ahora"),
        ("schedule", "Programar para después"),
    ]
    current = 0
    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {titulo} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        stdscr.addstr(2, 4, "¿Cuándo ejecutar?", curses.A_BOLD)
        for i, (_, label) in enumerate(options):
            y    = 4 + i * 2
            attr = curses.color_pair(C_SELECTED) | curses.A_BOLD if i == current \
                   else curses.color_pair(C_NORMAL)
            mark = " ► " if i == current else "   "
            stdscr.addstr(y, 6, mark + label, attr)

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0, " ↑↓=Seleccionar  Enter=Confirmar  ESC=Cancelar ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key = stdscr.getch()
        if key == 27:
            return None
        elif key == curses.KEY_UP:
            current = (current - 1) % len(options)
        elif key == curses.KEY_DOWN:
            current = (current + 1) % len(options)
        elif key in (curses.KEY_ENTER, 10, 13):
            return options[current][0]


def draw_programados(stdscr, rows, selected, offset):
    """Dibuja la lista de deploys programados del usuario."""
    h, w   = stdscr.getmaxyx()
    list_h = h - 5

    header = f"  {'CLIENTE':<22}  {'SERVICIOS':<25}  {'FECHA/HORA':<17}  {'ESTADO'}"
    stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
    stdscr.addstr(3, 0, header[:w - 1].ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

    ESTADO_COLOR = {
        "pendiente":  C_WARN,
        "ejecutando": C_SEARCH,
        "ok":         C_OK,
        "error":      C_ERROR,
    }

    visible = rows[offset: offset + list_h]
    for i, dep in enumerate(visible):
        y     = 4 + i
        abs_i = offset + i
        svcs  = ", ".join(dep["servicios"])
        fh    = dep["fecha_hora"].strftime("%d/%m/%Y %H:%M") \
                if hasattr(dep["fecha_hora"], "strftime") else str(dep["fecha_hora"])[:16]
        est   = dep["estado"]
        color = ESTADO_COLOR.get(est, C_NORMAL)
        line  = f"  {dep['desc_cliente']:<22}  {svcs:<25}  {fh:<17}  {est.upper()}"
        if abs_i == selected:
            stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            stdscr.addstr(y, 0, line[:w - 1].ljust(w - 1))
            stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.attron(curses.color_pair(color))
            stdscr.addstr(y, 0, line[:w - 1])
            stdscr.attroff(curses.color_pair(color))

    for i in range(len(visible), list_h):
        try:
            stdscr.addstr(4 + i, 0, " " * (w - 1))
        except curses.error:
            pass


# ── Tabs y dibujo ──────────────────────────────────────────────────────────

TABS = [
    ("1", "Clientes",      "clientes"),
    ("2", "Servidores",    "maquinas"),
    ("3", "Logs",          "logs"),
    ("4", "Grep vivo",     "grep"),
    ("5", "Deploy",        "deploy"),
    ("6", "Multi-Deploy",  "multideploy"),
    ("7", "Programados",   "programados"),
]


def draw_header(stdscr, mode, search):
    h, w = stdscr.getmaxyx()
    title  = f" {APP_NAME.upper()} v{APP_VERSION} · Síntesis "
    credit = f" {APP_CREDIT} "
    stdscr.attron(curses.color_pair(C_HEADER))
    stdscr.addstr(0, 0, " " * (w - 1))
    stdscr.addstr(0, max(0, (w - len(title)) // 2), title, curses.A_BOLD)
    # Crédito pequeño en esquina derecha
    try:
        stdscr.addstr(0, w - len(credit) - 1, credit, curses.color_pair(C_DIM))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_HEADER))

    stdscr.addstr(1, 0, " " * (w - 1))
    x = 2
    for key, label, m in TABS:
        tab_txt = f" [{key}] {label} "
        if m == mode:
            stdscr.addstr(1, x, tab_txt, curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.addstr(1, x, tab_txt, curses.color_pair(C_DIM))
        x += len(tab_txt) + 1

    stdscr.attron(curses.color_pair(C_SEARCH))
    stdscr.addstr(2, 0, " " * (w - 1))
    stdscr.addstr(2, 2, f" Buscar: {search}_")
    stdscr.attroff(curses.color_pair(C_SEARCH))


def draw_footer(stdscr, msg="", mode=""):
    h, w = stdscr.getmaxyx()

    if mode == "clientes":
        shortcuts = " Enter=SSH  F3=Detalle  F4=Editar  F2=Nuevo  Supr=Eliminar  q=Salir"
        if msg and not msg.endswith("resultado(s)"):
            left = f" {msg}"
            right = ""
        else:
            left  = shortcuts
            right = f"  {msg} " if msg else ""
        line = left + right.rjust(w - 1 - len(left))
    elif mode == "programados":
        shortcuts = " Supr=Cancelar pendiente  F5/ESC=Actualizar  1-7=Tab  q=Salir "
        line = f" {msg} " if msg else shortcuts
    else:
        footer = " Enter=Acción  1-7=Tab  ESC=Borrar búsqueda  q=Salir "
        line   = f" {msg} " if msg else footer

    stdscr.attron(curses.color_pair(C_STATUS))
    stdscr.addstr(h - 1, 0, line[:w - 1].ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_STATUS))


def draw_list(stdscr, rows, selected, offset, mode):
    h, w    = stdscr.getmaxyx()
    list_h  = h - 5
    start_y = 4

    if mode == "maquinas":
        col_header = f"{'SERVIDOR':<20}  {'IP':<16}  {'USUARIO':<12}  {'PUERTO'}  {'DESCRIPCIÓN'}"
    elif mode == "deploy":
        col_header = f"{'#':>5}  {'CLIENTE':<25}  {'SERVIDOR':<15}  {'IP':<16}  {'PATH HADES'}"
    else:
        col_header = f"{'#':>5}  {'CLIENTE':<25}  {'SERVIDOR':<15}  {'IP':<16}  {'PATH'}"

    stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
    stdscr.addstr(3, 0, col_header[:w - 1].ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

    visible = rows[offset: offset + list_h]
    for i, row in enumerate(visible):
        y     = start_y + i
        abs_i = offset + i

        if mode == "maquinas":
            line = (
                f"{row['nombre']:<20}  "
                f"{row['ip']:<16}  "
                f"{row['ssh_user']:<12}  "
                f"{str(row['ssh_port']):<6}  "
                f"{row['descripcion']}"
            )
        elif mode == "deploy":
            ph = row.get("path_hades") or ""
            icon = " ✓" if ph else " —"
            line = (
                f"{row['nro_cliente']:>5}  "
                f"{row['desc_cliente']:<25}  "
                f"{row['servidor']:<15}  "
                f"{row['ip']:<16}  "
                f"{ph}{icon}"
            )
        else:
            log_icon = " [LOG]" if row.get("path") else ""
            line = (
                f"{row['nro_cliente']:>5}  "
                f"{row['desc_cliente']:<25}  "
                f"{row['servidor']:<15}  "
                f"{row['ip']:<16}  "
                f"{row.get('path') or ''}{log_icon}"
            )

        if abs_i == selected:
            stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            stdscr.addstr(y, 0, line[:w - 1].ljust(w - 1))
            stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.attron(curses.color_pair(C_NORMAL))
            stdscr.addstr(y, 0, line[:w - 1])
            stdscr.attroff(curses.color_pair(C_NORMAL))

    for i in range(len(visible), list_h):
        try:
            stdscr.addstr(start_y + i, 0, " " * (w - 1))
        except curses.error:
            pass


# ── Loop principal ─────────────────────────────────────────────────────────

def main(stdscr):
    init_colors()
    curses.curs_set(0)
    stdscr.keypad(True)
    stdscr.timeout(100)

    # ── Cargar config de usuario ───────────────────────────────────────────
    cfg = load_user_config()
    # Correr wizard si no hay config o si le faltan los campos de BD
    if not cfg or not cfg.get("db_host"):
        cfg = setup_wizard(stdscr)
        if not cfg:
            return
        save_user_config(cfg)

    HADES["user"]     = cfg.get("hades_user", "")
    HADES["auth"]     = cfg.get("hades_auth", "key")
    HADES["key"]      = cfg.get("hades_key", "")
    HADES["password"] = cfg.get("hades_password", "")

    DB_CONFIG["host"]     = cfg.get("db_host", "")
    DB_CONFIG["database"] = cfg.get("db_name", "")
    DB_CONFIG["user"]     = cfg.get("db_user", "")
    DB_CONFIG["password"] = cfg.get("db_password", "")

    # Verificar conexión a la BD con la config ya cargada
    try:
        conn = get_connection()
        conn.close()
    except Exception as e:
        curses.endwin()
        print(f"\nError conectando a la base de datos: {e}")
        print(f"Config: {DB_CONFIG['user']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}")
        print(f"\nRevisá la configuración:")
        print(f"  rm ~/.config/core-deploy/config.json && core-deploy\n")
        sys.exit(1)

    ensure_deploys_table()

    usuario = HADES["user"]

    mode         = "clientes"
    search       = ""
    selected     = 0
    offset       = 0
    status       = ""
    rows         = []
    needs_reload = True

    while True:
        if needs_reload:
            try:
                if mode == "maquinas":
                    rows = fetch_maquinas(search)
                elif mode == "programados":
                    rows = db_fetch_deploys_usuario(usuario)
                else:
                    rows = fetch_clientes(search)
                selected = min(selected, max(0, len(rows) - 1))
            except Exception as e:
                status = f"ERROR BD: {e}"
                rows = []
            needs_reload = False

        h, w   = stdscr.getmaxyx()
        list_h = h - 5

        if selected < offset:
            offset = selected
        elif selected >= offset + list_h:
            offset = selected - list_h + 1

        stdscr.erase()
        draw_header(stdscr, mode, search)
        if mode == "programados":
            draw_programados(stdscr, rows, selected, offset)
        else:
            draw_list(stdscr, rows, selected, offset, mode)

        # en tab programados: mostrar detalle de error en footer
        footer_txt = status if status else f"{len(rows)} resultado(s)"
        if not status and mode == "programados" and rows:
            dep = rows[selected]
            if dep.get("estado") == "error" and dep.get("detalle"):
                footer_txt = f"ERROR: {dep['detalle']}"
        draw_footer(stdscr, footer_txt, mode=mode)
        stdscr.refresh()

        try:
            key = stdscr.getch()
        except curses.error:
            continue

        if key == -1:
            continue

        status = ""

        # ── Salir ──────────────────────────────────────────────────────────
        if key in (ord('q'), ord('Q')):
            break

        # ── Cambiar tab ────────────────────────────────────────────────────
        elif key in (ord('1'), ord('2'), ord('3'), ord('4'), ord('5'), ord('6'), ord('7')):
            idx  = int(chr(key)) - 1
            mode = TABS[idx][2]
            search = ""; selected = 0; offset = 0
            needs_reload = True

        elif key == 9:   # TAB
            modes = [t[2] for t in TABS]
            mode  = modes[(modes.index(mode) + 1) % len(modes)]
            search = ""; selected = 0; offset = 0
            needs_reload = True

        # ── Navegación ─────────────────────────────────────────────────────
        elif key == curses.KEY_UP:
            selected = max(0, selected - 1)
        elif key == curses.KEY_DOWN:
            selected = min(len(rows) - 1, selected + 1)
        elif key == curses.KEY_PPAGE:
            selected = max(0, selected - list_h)
            offset   = max(0, offset  - list_h)
        elif key == curses.KEY_NPAGE:
            selected = min(len(rows) - 1, selected + list_h)

        # ── Limpiar búsqueda ───────────────────────────────────────────────
        elif key in (27, curses.KEY_F5):
            search = ""; selected = 0; offset = 0
            needs_reload = True
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if search:
                search = search[:-1]
                selected = 0; offset = 0
                needs_reload = True

        # ── Acción con Enter ───────────────────────────────────────────────
        elif key in (curses.KEY_ENTER, 10, 13):
            if not rows:
                status = "No hay resultados"
                continue

            if mode == "programados":
                continue

            row      = rows[selected]
            ip       = row["ip"]
            user     = row["ssh_user"]
            password = row["ssh_password"]
            port     = row["ssh_port"]
            path     = row.get("path")

            if not password:
                password = ask_input(stdscr, f"Contraseña para {user}@{ip}: ")
            if not password and mode != "deploy":
                status = "Conexión cancelada"
                continue

            if mode == "clientes":
                ssh_connect(ip, user, password, port, remote_path=path)
                stdscr = curses.initscr()
                init_colors(); curses.curs_set(0)
                stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "maquinas":
                ssh_connect(ip, user, password, port)
                stdscr = curses.initscr()
                init_colors(); curses.curs_set(0)
                stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "logs":
                if not path:
                    status = f"Sin path para {row['desc_cliente']}"
                    continue
                stream_viewer(stdscr, row)
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "grep":
                if not path:
                    status = f"Sin path para {row['desc_cliente']}"
                    continue
                patron = ask_input(stdscr, f"Patrón grep para {row['desc_cliente']}: ")
                if patron:
                    stream_viewer(stdscr, row, grep_pattern=patron)
                    init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                    needs_reload = True
                else:
                    status = "Búsqueda cancelada"

            elif mode == "deploy":
                if not row.get("path_hades"):
                    status = f"Sin path_hades para {row['desc_cliente']}"
                    continue
                if not row.get("path"):
                    status = f"Sin path producción para {row['desc_cliente']}"
                    continue
                cbl = cbl_picker(stdscr, row)
                if cbl:
                    when = deploy_when_picker(stdscr, f"DEPLOY — {row['desc_cliente']} — {cbl}")
                    if when == "now":
                        run_deploy(stdscr, row, cbl)
                    elif when == "schedule":
                        fh = ask_datetime(stdscr)
                        if fh:
                            db_insert_deploy_programado(
                                usuario, row["nro_cliente"], row["desc_cliente"],
                                [cbl], fh,
                            )
                            notify_scheduler()
                            status = f"✓ Deploy de {cbl} programado para {fh.strftime('%d/%m/%Y %H:%M')}"
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "multideploy":
                if not row.get("path_hades"):
                    status = f"Sin path_hades para {row['desc_cliente']}"
                    continue
                if not row.get("path"):
                    status = f"Sin path producción para {row['desc_cliente']}"
                    continue
                cbl_list = multiline_input(
                    stdscr,
                    title=f"MULTI-DEPLOY — {row['desc_cliente']} — Pega la lista de .cbl",
                    hint="Un archivo .cbl por línea  |  F10 o Ctrl+D = Iniciar  |  ESC = Cancelar",
                )
                if cbl_list:
                    when = deploy_when_picker(stdscr, f"MULTI-DEPLOY — {row['desc_cliente']}")
                    if when == "now":
                        run_multi_deploy(stdscr, row, cbl_list)
                    elif when == "schedule":
                        fh = ask_datetime(stdscr)
                        if fh:
                            db_insert_deploy_programado(
                                usuario, row["nro_cliente"], row["desc_cliente"],
                                cbl_list, fh,
                            )
                            notify_scheduler()
                            status = f"✓ {len(cbl_list)} servicio(s) programados para {fh.strftime('%d/%m/%Y %H:%M')}"
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "programados":
                pass  # Enter no hace nada en programados

        # ── Acciones CRUD (solo en tab Clientes) ──────────────────────────
        elif mode == "clientes" and key == curses.KEY_F3:
            # Ver detalle
            if rows:
                show_detail(stdscr, rows[selected])
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "clientes" and key == curses.KEY_F4:
            # Editar cliente
            if rows:
                data = cliente_form(stdscr, rows[selected])
                if data:
                    try:
                        db_update_cliente(rows[selected]["nro_cliente"], data)
                        status = f"✓ {data['desc_cliente']} actualizado"
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "clientes" and key == curses.KEY_F2:
            # Nuevo cliente
            data = cliente_form(stdscr, None)
            if data:
                try:
                    db_insert_cliente(data)
                    status = f"✓ {data['desc_cliente']} creado"
                except Exception as ex:
                    status = f"✗ Error: {ex}"
                needs_reload = True
            init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "clientes" and key == curses.KEY_DC:
            # Eliminar cliente (tecla Supr)
            if rows:
                row = rows[selected]
                if confirm_dialog(stdscr, f"¿Eliminar '{row['desc_cliente']}'?"):
                    try:
                        db_delete_cliente(row["nro_cliente"])
                        status = f"✓ {row['desc_cliente']} eliminado"
                        selected = max(0, selected - 1)
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                stdscr.touchwin(); stdscr.refresh()
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "programados" and key == curses.KEY_DC:
            if rows:
                dep = rows[selected]
                if dep["estado"] in ("pendiente", "error"):
                    lbl = "cancelar" if dep["estado"] == "pendiente" else "eliminar"
                    if confirm_dialog(stdscr, f"¿{lbl.capitalize()} deploy de '{dep['desc_cliente']}'?"):
                        try:
                            db_delete_deploy(dep["id"])
                            status = f"✓ Deploy {lbl}do"
                            selected = max(0, selected - 1)
                        except Exception as ex:
                            status = f"✗ Error: {ex}"
                        needs_reload = True
                else:
                    status = "Solo se pueden eliminar deploys pendientes o con error"
                stdscr.touchwin(); stdscr.refresh()
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        # ── Tipeo en búsqueda ──────────────────────────────────────────────
        elif 32 <= key <= 126 and mode != "programados":
            search  += chr(key)
            selected = 0; offset = 0
            needs_reload = True


def run():
    global _scheduler_running

    _scheduler_running = True
    t = threading.Thread(target=_scheduler_loop, daemon=True)
    t.start()

    try:
        curses.wrapper(main)
    finally:
        _scheduler_running = False
        _scheduler_event.set()  # desbloquear el hilo para que termine


if __name__ == "__main__":
    run()
