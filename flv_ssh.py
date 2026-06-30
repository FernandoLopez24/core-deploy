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
import re
import sys
import time
import threading
import subprocess
import smtplib
import psycopg2
import psycopg2.extras
from collections import deque
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

APP_NAME    = "core-deploy"
APP_VERSION = "1.0.1"
APP_CREDIT  = "by Fernando · Síntesis"
CONFIG_FILE = os.path.expanduser("~/.config/core-deploy/config.json")

# ── SMTP corporativo (fijo para todos) ────────────────────────────────────
SMTP_HOST = "199.14.10.83"
SMTP_PORT = 25
SMTP_FROM = "coredeploy@sintesis.com.bo"

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


# ── Notificaciones por email ───────────────────────────────────────────────

def send_notification(cfg, subject, body):
    """Envía email usando el servidor SMTP corporativo. Solo requiere email_to en cfg."""
    to = cfg.get("email_to", "").strip()
    if not to:
        return  # usuario sin email configurado → no hacer nada

    msg = MIMEMultipart()
    msg["From"]    = SMTP_FROM
    msg["To"]      = to
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as srv:
            srv.sendmail(SMTP_FROM, to.split(","), msg.as_string())
    except Exception:
        pass  # notificaciones opcionales → error silencioso


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


def _wizard_email_config(stdscr, existing=None):
    """Pide solo el email destino. El servidor SMTP es corporativo y está hardcodeado."""
    existing   = existing or {}
    current_to = existing.get("email_to", "")

    while True:
        stdscr.erase()
        h, w = stdscr.getmaxyx()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {APP_NAME.upper()} — Notificaciones por email ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        stdscr.addstr(2, 4, "Servidor SMTP corporativo (preconfigurado):", curses.A_BOLD)
        stdscr.addstr(3, 6, f"Host : {SMTP_HOST}:{SMTP_PORT}", curses.color_pair(C_DIM))
        stdscr.addstr(4, 6, f"From : {SMTP_FROM}",            curses.color_pair(C_DIM))

        stdscr.addstr(6, 4, "Tu email de destino para notificaciones:", curses.A_BOLD)
        attr = curses.color_pair(C_SELECTED) | curses.A_BOLD
        try:
            stdscr.addstr(7, 6, f"  [{current_to:<60}]", attr)
        except curses.error:
            pass

        stdscr.addstr(9, 4, "Podés poner varios separados por coma.", curses.color_pair(C_DIM))
        stdscr.addstr(10, 4, "Dejá vacío para no recibir notificaciones.", curses.color_pair(C_DIM))

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " Enter=Editar email  F10=Guardar  ESC=Cancelar ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key_pressed = stdscr.getch()

        if key_pressed == 27:
            return {}
        elif key_pressed in (curses.KEY_ENTER, 10, 13):
            new_val = ask_input(stdscr, "Tu email de destino: ")
            if new_val is not None:
                current_to = new_val
        elif key_pressed == curses.KEY_F10:
            return {"email_to": current_to}


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
            # Paso 4: email (opcional, se puede saltar con ESC)
            email_cfg = _wizard_email_config(stdscr)
            values.update(email_cfg)
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
            COALESCE(c.libpath, '')          AS libpath,
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


def fetch_cliente_by_path_prefix(prefix):
    """Busca un cliente cuyo path_hades termina en /prefix (insensible a mayúsculas)."""
    query = """
        SELECT
            c.nro_cliente, c.desc_cliente, c.servidor,
            host(c.ip_servidor) AS ip,
            c.iniciales, c.desc_cobol, c.path, c.path_hades,
            COALESCE(c.libpath, '') AS libpath,
            COALESCE(m.ssh_user,     'tuxedo') AS ssh_user,
            COALESCE(m.ssh_password, '')        AS ssh_password,
            COALESCE(m.ssh_port,     22)        AS ssh_port
        FROM clientes c
        LEFT JOIN maquinas m ON LOWER(m.nombre) = LOWER(c.servidor)
        WHERE LOWER(regexp_replace(c.path_hades, '^.+/', '')) = LOWER(%s)
    """
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (prefix,))
            row = cur.fetchone()
            return dict(row) if row else None
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
    ("libpath",      "Lib Path",          "str",  True),
]


def db_insert_cliente(data):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO clientes
                    (nro_cliente, desc_cliente, servidor, ip_servidor,
                     iniciales, desc_cobol, path, path_hades, libpath)
                VALUES (%s,%s,%s,%s::inet,%s,%s,%s,%s,%s)
            """, (
                data["nro_cliente"], data["desc_cliente"], data["servidor"],
                data["ip_servidor"], data["iniciales"], data["desc_cobol"],
                data["path"], data["path_hades"], data.get("libpath", ""),
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
                    iniciales=%s, desc_cobol=%s, path=%s, path_hades=%s, libpath=%s
                WHERE nro_cliente=%s
            """, (
                data["desc_cliente"], data["servidor"], data["ip_servidor"],
                data["iniciales"], data["desc_cobol"], data["path"],
                data["path_hades"], data.get("libpath", ""), nro,
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


# ── CRUD Maquinas ──────────────────────────────────────────────────────────

MAQUINA_FIELDS = [
    ("nombre",       "Nombre",      "str", False),
    ("ip",           "IP",          "str", True),
    ("ssh_user",     "Usuario SSH", "str", True),
    ("ssh_password", "Contraseña",  "str", True),
    ("ssh_port",     "Puerto",      "int", True),
    ("descripcion",  "Descripción", "str", True),
]


def db_insert_maquina(data, sistema="cobol"):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO maquinas (nombre, ip, ssh_user, ssh_password, ssh_port, descripcion, sistema)
                VALUES (%s, %s::inet, %s, %s, %s, %s, %s)
            """, (
                data["nombre"], data["ip"], data["ssh_user"],
                data["ssh_password"], int(data["ssh_port"] or 22), data["descripcion"], sistema,
            ))
        conn.commit()
    finally:
        conn.close()


def db_update_maquina(nombre, data):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE maquinas SET
                    ip=%s::inet, ssh_user=%s, ssh_password=%s, ssh_port=%s, descripcion=%s
                WHERE nombre=%s
            """, (
                data["ip"], data["ssh_user"], data["ssh_password"],
                int(data["ssh_port"] or 22), data["descripcion"], nombre,
            ))
        conn.commit()
    finally:
        conn.close()


def db_delete_maquina(nombre):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM maquinas WHERE nombre=%s", (nombre,))
        conn.commit()
    finally:
        conn.close()


def maquina_form(stdscr, row=None):
    is_new = row is None
    values = {
        "nombre":       row["nombre"]       if row else "",
        "ip":           row["ip"]           if row else "",
        "ssh_user":     row["ssh_user"]     if row else "tuxedo",
        "ssh_password": row["ssh_password"] if row else "",
        "ssh_port":     str(row["ssh_port"] if row else 22),
        "descripcion":  row["descripcion"]  if row else "",
    }
    editable_fields = [f for f in MAQUINA_FIELDS if f[3] or is_new]
    current = 0
    error   = ""

    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        titulo = "NUEVO SERVIDOR" if is_new else f"EDITAR SERVIDOR — {values['nombre']}"
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, f" {titulo} ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        for i, (key, label, typ, editable) in enumerate(MAQUINA_FIELDS):
            y = 2 + i * 2
            val = values[key]
            field_idx = editable_fields.index((key, label, typ, editable)) \
                        if (key, label, typ, editable) in editable_fields else -1
            is_cur = (field_idx == current)
            lbl_attr = curses.A_BOLD if is_cur else 0
            val_attr = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if is_cur \
                       else (curses.color_pair(C_DIM) if not editable else curses.color_pair(C_NORMAL))
            stdscr.addstr(y, 2, f"{label:<20}", lbl_attr)
            display = ("*" * len(val)) if key == "ssh_password" and not is_cur else val
            val_display = f" {display:<55} " if is_cur else f" {display:<55}"
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

        k = stdscr.getch()

        if k == curses.KEY_F10:
            if not values["nombre"].strip():
                error = "El nombre es obligatorio"
                continue
            if not values["ip"].strip():
                error = "La IP es obligatoria"
                continue
            try:
                values["ssh_port"] = str(int(values["ssh_port"]))
            except ValueError:
                error = "El puerto debe ser un número"
                continue
            return values
        elif k == 27:
            return None
        elif k == curses.KEY_UP:
            current = max(0, current - 1)
            error = ""
        elif k == curses.KEY_DOWN:
            current = min(len(editable_fields) - 1, current + 1)
            error = ""
        elif k in (curses.KEY_ENTER, 10, 13):
            fkey = editable_fields[current][0]
            new_val = ask_input(stdscr, f"{editable_fields[current][1]}: ")
            if new_val is not None:
                values[fkey] = new_val
            error = ""


def _servidor_picker(stdscr):
    """
    Muestra un picker de servidores de la tabla maquinas.
    Devuelve dict {"nombre": ..., "ip": ...} o None si cancela.
    """
    try:
        maquinas = fetch_maquinas(sistema="cobol")
    except Exception as e:
        _show_message(stdscr, f"Error cargando servidores: {e}", error=True)
        return None

    if not maquinas:
        _show_message(stdscr, "No hay servidores registrados en la BD.", error=True)
        return None

    search    = ""
    selected  = 0

    while True:
        filtradas = [m for m in maquinas
                     if search.lower() in m["nombre"].lower()
                     or search.lower() in (m["ip"] or "")]

        h, w = stdscr.getmaxyx()
        stdscr.erase()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, " SELECCIONAR SERVIDOR ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        # Barra de búsqueda
        stdscr.attron(curses.A_BOLD)
        stdscr.addstr(1, 2, "Buscar: ")
        stdscr.attroff(curses.A_BOLD)
        try:
            stdscr.addstr(1, 10, search[:w - 12])
        except curses.error:
            pass

        list_h = h - 4
        selected = max(0, min(selected, len(filtradas) - 1))
        offset   = max(0, selected - list_h + 1)

        if not filtradas:
            try:
                stdscr.addstr(3, 4, "Sin resultados.", curses.color_pair(C_DIM))
            except curses.error:
                pass
        else:
            for i, m in enumerate(filtradas[offset:offset + list_h]):
                idx  = i + offset
                y    = 3 + i
                line = f"  {m['nombre']:<25}  {m['ip'] or '':<18}"
                if idx == selected:
                    stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                    try:
                        stdscr.addstr(y, 0, line[:w - 1].ljust(w - 1))
                    except curses.error:
                        pass
                    stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                else:
                    try:
                        stdscr.addstr(y, 0, line[:w - 1])
                    except curses.error:
                        pass

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0,
            " ↑↓=Navegar  Escribir=Filtrar  Enter=Seleccionar  ESC=Cancelar ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        k = stdscr.getch()

        if k == 27:
            return None
        elif k == curses.KEY_UP:
            selected = max(0, selected - 1)
        elif k == curses.KEY_DOWN:
            selected = min(len(filtradas) - 1, selected + 1)
        elif k in (curses.KEY_ENTER, 10, 13):
            if filtradas:
                m = filtradas[selected]
                return {"nombre": m["nombre"], "ip": m["ip"] or ""}
        elif k in (curses.KEY_BACKSPACE, 127, 8):
            search = search[:-1]
            selected = 0
        elif 32 <= k <= 126:
            search += chr(k)
            selected = 0


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
        "libpath":      row.get("libpath")      or "" if row else "",
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

        fkey_cur = editable_fields[current][0] if editable_fields else ""
        if fkey_cur in ("servidor", "ip_servidor"):
            footer_hint = " ↑↓=Navegar  Enter=Elegir servidor  F10=Guardar  ESC=Cancelar "
        else:
            footer_hint = " ↑↓=Navegar  Enter=Editar campo  F10=Guardar  ESC=Cancelar "
        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0, footer_hint.ljust(w - 1))
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
            if fkey in ("servidor", "ip_servidor"):
                sel = _servidor_picker(stdscr)
                if sel:
                    values["servidor"]    = sel["nombre"]
                    values["ip_servidor"] = sel["ip"]
            else:
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
            ("Lib Path",         row.get("libpath", "") or "—"),
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


def fetch_maquinas(search="", sistema="cobol"):
    query = """
        SELECT
            nombre,
            split_part(ip::text, '/', 1) AS ip,
            ssh_user,
            COALESCE(ssh_password, '') AS ssh_password,
            ssh_port,
            COALESCE(descripcion, '') AS descripcion
        FROM maquinas
        WHERE
            COALESCE(sistema, 'cobol') = %s AND (
                nombre      ILIKE %s OR
                ip::text    ILIKE %s OR
                descripcion ILIKE %s
            )
        ORDER BY nombre
    """
    pat = f"%{search}%"
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(query, (sistema, pat, pat, pat))
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


# ── Deploys programados — BD ──────────────────────────────────────────────

def ensure_schema():
    """Aplica migraciones de esquema. Limpia columnas obsoletas si existen."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE clientes DROP COLUMN IF EXISTS hades_user")
            cur.execute("""
                ALTER TABLE maquinas
                    ADD COLUMN IF NOT EXISTS sistema VARCHAR(50) DEFAULT 'cobol'
            """)
            cur.execute("""
                ALTER TABLE clientes
                    ADD COLUMN IF NOT EXISTS libpath VARCHAR(500) DEFAULT ''
            """)
        conn.commit()
    finally:
        conn.close()


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
                SELECT d.*,
                       COALESCE(c.ip_servidor::text, '') AS ip,
                       COALESCE(c.path, '')              AS path,
                       COALESCE(c.path_hades, '')        AS path_hades,
                       COALESCE(m.ssh_user, 'tuxedo')   AS ssh_user,
                       COALESCE(m.ssh_password, '')      AS ssh_password,
                       COALESCE(m.ssh_port, 22)          AS ssh_port
                FROM deploys_programados d
                LEFT JOIN clientes c ON c.nro_cliente = d.nro_cliente
                                     AND d.nro_cliente > 0
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


def _run_scheduled_reinicio(deploy):
    """Ejecuta un reinicio de dominio Tuxedo programado (sin UI curses)."""
    ip       = str(deploy["ip"]).split("/")[0].strip()
    user     = deploy["ssh_user"]
    password = deploy["ssh_password"]
    port     = deploy["ssh_port"]
    path     = deploy["path"] or ""
    STALL_SHUTDOWN = 45    # tmshutdown puede tardar si hay servicios lentos
    STALL_BOOT     = 120  # tmboot con muchos servicios puede tardar más de 30s

    def run_cmd(cmd_str, stall=45):
        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')
        proc = subprocess.Popen(
            ssh, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            bufsize=0, env=_sshenv(password),
        )
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)
        out        = []
        last_out   = time.time()
        force_used = False
        while proc.poll() is None:
            try:
                raw = proc.stdout.read(4096)
                if raw:
                    out.append(raw.decode("utf-8", errors="replace"))
                    last_out = time.time()
            except (BlockingIOError, TypeError):
                pass
            if time.time() - last_out > stall:
                force_used = True
                try:
                    proc.stdin.write(b'\x03'); proc.stdin.flush()
                    time.sleep(0.4)
                    proc.stdin.write(b'y\n');  proc.stdin.flush()
                except Exception:
                    pass
                last_out = time.time()
            time.sleep(0.1)
        try:
            rest = proc.stdout.read()
            if rest:
                out.append(rest.decode("utf-8", errors="replace"))
        except Exception:
            pass
        try:
            proc.kill()
        except Exception:
            pass
        return force_used, "".join(out)

    svcs     = deploy.get("servicios", [])
    ubb_name = next((s.split(":", 1)[1] for s in svcs if s.startswith("__ubb__:")), None)
    dm_name  = next((s.split(":", 1)[1] for s in svcs if s.startswith("__dm__:")),  None)

    logs = []
    force, out = run_cmd("tmshutdown -y", stall=STALL_SHUTDOWN)
    logs.append(f"▶ tmshutdown\n{out.strip()}")
    shutdown_failed = (
        "Shutdown failed" in out
        or "Cannot shutdown BBL" in out
    )
    if force or shutdown_failed:
        _, out2 = run_cmd("tmipcrm -y", stall=STALL_SHUTDOWN)
        logs.append(f"▶ tmipcrm\n{out2.strip()}")
    if ubb_name:
        _, out_ubb = run_cmd(f"tmloadcf -y {ubb_name}", stall=STALL_SHUTDOWN)
        logs.append(f"▶ tmloadcf ({ubb_name})\n{out_ubb.strip()}")
    if dm_name:
        _, out_dm = run_cmd(f"dmloadcf -y {dm_name}", stall=STALL_SHUTDOWN)
        logs.append(f"▶ dmloadcf ({dm_name})\n{out_dm.strip()}")
    _, out3 = run_cmd("tmboot -y", stall=STALL_BOOT)
    logs.append(f"▶ tmboot\n{out3.strip()}")

    needs_ipc = force or shutdown_failed
    estado    = "ok"
    sep       = "\n" + "─" * 50 + "\n"
    detalle   = sep.join(logs) + f"\n{'═'*50}\n✓ Reinicio completado\n"
    db_update_deploy_estado(deploy["id"], estado, detalle)

    cfg = load_user_config() or {}
    fh  = deploy.get("fecha_hora")
    fh_str = fh.strftime("%d/%m/%Y %H:%M") if hasattr(fh, "strftime") else str(fh)[:16]
    subject = f"[core-deploy] ✓ Reinicio — {deploy['desc_cliente']} ({fh_str})"
    body = (
        f"Reinicio de dominio completado{'(con cierre forzado/limpieza IPC)' if needs_ipc else ''}.\n"
        f"Cliente : {deploy['desc_cliente']}\n"
        f"Servidor: {deploy.get('ip', '')}\n"
        f"Path    : {deploy.get('path', '')}\n"
        f"Hora    : {fh_str}\n\n"
        f"{'='*60}\n\n"
        f"{detalle}"
    )
    threading.Thread(target=send_notification, args=(cfg, subject, body), daemon=True).start()


def _run_scheduled_genesis_reinicio_headless(servidor_nombre):
    """Ejecuta reinicio Genesis-CPP en background, sin UI curses."""
    try:
        servers = fetch_maquinas(sistema="genesis")
        srv = next((s for s in servers if s["nombre"] == servidor_nombre), None)
        if not srv:
            return
    except Exception:
        return

    ip       = srv["ip"]
    user     = srv["ssh_user"]
    password = srv["ssh_password"]
    port     = srv["ssh_port"]
    path     = srv["descripcion"].strip() if srv["descripcion"].strip().startswith("/") \
               else "/home/sistemas/GENESIS_C/RUN"
    STALL    = 30

    def run_cmd(cmd_str, timeout=180):
        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')
        proc = subprocess.Popen(ssh, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, bufsize=0, env=_sshenv(password))
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)
        last_out = time.time()
        deadline = time.time() + timeout
        while proc.poll() is None:
            try:
                raw = proc.stdout.read(4096)
                if raw:
                    last_out = time.time()
            except (BlockingIOError, TypeError):
                pass
            if time.time() - last_out > STALL:
                try:
                    proc.stdin.write(b'y\n'); proc.stdin.flush()
                except Exception:
                    pass
                last_out = time.time()
            if time.time() > deadline:
                proc.kill()
                break
            time.sleep(0.1)
        try: proc.kill()
        except Exception: pass
        try: proc.wait(timeout=3)
        except Exception: pass

    try:
        r = subprocess.run(
            ssh_cmd_base(ip, user, port) + ["ps -fea | grep 'Genesis-CPP' | grep -v grep"],
            capture_output=True, text=True, timeout=10, env=_sshenv(password),
        )
        pids = [l.split()[1] for l in r.stdout.splitlines()
                if l.strip() and len(l.split()) >= 2]
        if pids:
            subprocess.run(
                ssh_cmd_base(ip, user, port) + [f"kill -9 {' '.join(pids)}"],
                capture_output=True, text=True, timeout=10, env=_sshenv(password),
            )
    except Exception:
        pass

    run_cmd("tmshutdown -y",      timeout=120)
    run_cmd("dmloadcf -y DMGENESISC", timeout=60)
    run_cmd("tmboot -y",          timeout=180)

    try:
        subprocess.run(
            ssh_cmd_base(ip, user, port) + [
                f'cd "{path}" && . ./env.pro && nohup ./Genesis-CPP > /dev/null 2>&1 & disown'
            ],
            capture_output=True, text=True, timeout=15, env=_sshenv(password),
        )
    except Exception:
        pass


def _run_scheduled_deploy(deploy):
    """Ejecuta un deploy o reinicio programado en segundo plano (sin UI curses)."""
    if deploy["servicios"] == ["__genesis_reinicio__"]:
        _run_scheduled_genesis_reinicio_headless(deploy["desc_cliente"])
        return
    if deploy["servicios"] and deploy["servicios"][0] == "__reinicio__":
        _run_scheduled_reinicio(deploy)
        return

    dm_name   = next((s.split(":", 1)[1] for s in deploy["servicios"] if s.startswith("__dm__:")), None)
    use_hilos = "__hilos__" in deploy["servicios"]
    servicios = [s for s in deploy["servicios"] if not s.startswith("__")]
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
        ok, detalle = _deploy_one_silent(row, cbl, build_content, lambda m: None,
                                         use_hilos=use_hilos)
        resultados.append(f"{'✓' if ok else '✗'} {cbl}: {detalle}")

    # ── DM headless (tmshutdown → dmloadcf → tmboot) ────────────────────────
    if dm_name and not any(r.startswith("✗") for r in resultados):
        try:
            ip       = str(row["ip"]).split("/")[0].strip()
            ssh_env  = _sshenv(row["ssh_password"])
            path     = row["path"]
            STALL    = 120  # tmboot puede tener muchos servicios

            def _run_cmd_silent(cmd_str):
                ssh = ssh_cmd_base(ip, row["ssh_user"], row["ssh_port"])
                ssh.insert(3, "-tt")
                ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')
                proc = subprocess.Popen(ssh, stdin=subprocess.PIPE,
                                        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                        bufsize=0, env=ssh_env)
                fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
                fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)
                out = []; last_out = time.time(); force = False
                while proc.poll() is None:
                    try:
                        raw = proc.stdout.read(4096)
                        if raw:
                            out.append(raw.decode("utf-8", errors="replace"))
                            last_out = time.time()
                    except (BlockingIOError, TypeError):
                        pass
                    if time.time() - last_out > STALL:
                        force = True
                        try:
                            proc.stdin.write(b'\x03'); proc.stdin.flush()
                            time.sleep(0.4)
                            proc.stdin.write(b'y\n');  proc.stdin.flush()
                        except Exception:
                            pass
                        last_out = time.time()
                    time.sleep(0.1)
                try:
                    rest = proc.stdout.read()
                    if rest: out.append(rest.decode("utf-8", errors="replace"))
                except Exception:
                    pass
                try: proc.kill()
                except Exception: pass
                return force, "".join(out)

            force_sd, out_sd = _run_cmd_silent("tmshutdown -y")
            sd_failed = "Shutdown failed" in out_sd or "Cannot shutdown BBL" in out_sd
            if force_sd or sd_failed:
                _run_cmd_silent("tmipcrm -y")
            _run_cmd_silent(f"dmloadcf -y {dm_name}")
            _run_cmd_silent("tmboot -y")
            resultados.append(f"✓ DM {dm_name}: cargado y módulo reiniciado")
        except Exception as e:
            resultados.append(f"✗ DM {dm_name}: {e}")

    errores  = sum(1 for r in resultados if r.startswith("✗"))
    estado   = "error" if errores else "ok"
    icono_f  = "✓" if estado == "ok" else "✗"
    detalle  = "\n".join(resultados) + f"\n{'═'*50}\n{icono_f} Deploy {'completado' if estado == 'ok' else 'con errores'}\n"
    db_update_deploy_estado(deploy["id"], estado, detalle)

    cfg = load_user_config() or {}
    fh  = deploy.get("fecha_hora")
    fh_str  = fh.strftime("%d/%m/%Y %H:%M") if hasattr(fh, "strftime") else str(fh)[:16]
    icono   = "✓" if estado == "ok" else "✗"
    svcs_str = ", ".join(servicios) + (f" + DM:{dm_name}" if dm_name else "") + (" +hilos" if use_hilos else "")
    subject = f"[core-deploy] {icono} Deploy — {deploy['desc_cliente']} — {svcs_str} ({fh_str})"
    body = (
        f"Deploy {'completado' if estado == 'ok' else 'con errores'}.\n"
        f"Cliente  : {deploy['desc_cliente']}\n"
        f"Servidor : {row['ip']}\n"
        f"Servicios: {svcs_str}\n"
        f"Hora     : {fh_str}\n\n"
        f"{'='*60}\n\n"
        f"{detalle}"
    )
    threading.Thread(target=send_notification, args=(cfg, subject, body), daemon=True).start()


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


def _clean_ip(ip):
    """Elimina notación CIDR si la hubiera (ej: 192.168.1.1/32 → 192.168.1.1)."""
    return str(ip).split("/")[0].strip()


def ssh_cmd_base(ip, user, port):
    """Comando SSH base. Usar junto con env=_sshenv(password) en subprocess."""
    return [
        "sshpass", "-e",
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=QUIET",
        "-p", str(port),
        f"{user}@{_clean_ip(ip)}",
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


_HADES_BASE = {
    "luisl":    "intraplatinum",
    "israelcr": "intraplatinum",
    "juanc":    "intraplatinum",
    "marioy":   "intraplatinum",
    "paolac":   "intraplatinum",
    "antonioa": "platinum",
    "carlosv":  "platinum",
    "danielao": "platinum",
    "victorv":  "platinum",
}

def _resolve_hades_path(path):
    """Sustituye /home/<usuario>/<base>/ según el usuario hades actual."""
    if not path:
        return path
    user = HADES["user"]
    base = _HADES_BASE.get(user, "intraplatinum")
    return re.sub(r'^/home/[^/]+/[^/]+/', f'/home/{user}/{base}/', path)


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
    resolved = _resolve_hades_path(path_hades)
    stdout, _, _ = hades_run(
        f'ls "{resolved}"/*.cbl "{resolved}"/*.CBL 2>/dev/null | xargs -I{{}} basename {{}}'
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


def multicbl_picker(stdscr, row):
    """
    Lista todos los .cbl del cliente con multi-selección por Espacio.
    Devuelve lista de nombres seleccionados, o [] si cancela.
    """
    path_hades = row.get("path_hades") or ""
    nombre     = row.get("desc_cliente", "")

    stdscr.erase()
    stdscr.attron(curses.color_pair(C_HEADER))
    try:
        stdscr.addstr(0, 0, f" MULTI-DEPLOY — {nombre} — cargando .cbl...".ljust(stdscr.getmaxyx()[1] - 1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_HEADER))
    stdscr.refresh()

    try:
        files = list_cbl_files(path_hades)
    except Exception as e:
        _show_message(stdscr, f"Error conectando a hades: {e}", error=True)
        return []

    if not files:
        _show_message(stdscr, f"No se encontraron .cbl en {path_hades}", error=True)
        return []

    cursor       = 0
    offset       = 0
    search       = ""
    selected_set = set()

    while True:
        filtered = [f for f in files if search.lower() in f.lower()]
        h, w     = stdscr.getmaxyx()
        list_h   = h - 6

        cursor = min(cursor, max(0, len(filtered) - 1))
        if cursor < offset:
            offset = cursor
        elif cursor >= offset + list_h:
            offset = cursor - list_h + 1

        stdscr.erase()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try:
            stdscr.addstr(0, 0,
                f" MULTI-DEPLOY — {nombre} — {len(selected_set)} seleccionado(s) "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        stdscr.attron(curses.color_pair(C_WARN))
        try: stdscr.addstr(1, 2, f"Hades: {path_hades}")
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_WARN))

        stdscr.attron(curses.color_pair(C_SEARCH))
        try: stdscr.addstr(2, 0, f" Buscar: {search}_".ljust(w - 1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_SEARCH))

        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(3, 0, f"  {'ARCHIVO':<40}".ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

        for i, fname in enumerate(filtered[offset: offset + list_h]):
            idx  = offset + i
            y    = 4 + i
            mark = "[x]" if fname in selected_set else "[ ]"
            line = f"  {mark} {fname}"
            if idx == cursor:
                stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                try: stdscr.addstr(y, 0, line[:w-1].ljust(w-1))
                except curses.error: pass
                stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            else:
                try: stdscr.addstr(y, 0, line[:w-1])
                except curses.error: pass

        n_sel = len(selected_set)
        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h-1, 0,
                f" Espacio=marcar  Enter=Deploy({n_sel})  ESC/q=Cancelar  [{len(filtered)} archivos] "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        key = stdscr.getch()
        if key in (ord('q'), ord('Q'), 27):
            return []
        elif key == curses.KEY_UP:
            cursor = max(0, cursor - 1)
        elif key == curses.KEY_DOWN:
            cursor = min(len(filtered) - 1, cursor + 1)
        elif key == curses.KEY_PPAGE:
            cursor = max(0, cursor - list_h)
            offset = max(0, offset - list_h)
        elif key == curses.KEY_NPAGE:
            cursor = min(len(filtered) - 1, cursor + list_h)
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            search = search[:-1]; cursor = 0; offset = 0
        elif key == ord(' '):
            if filtered:
                f = filtered[cursor]
                if f in selected_set:
                    selected_set.discard(f)
                else:
                    selected_set.add(f)
        elif key in (curses.KEY_ENTER, 10, 13):
            return [f for f in files if f in selected_set]   # orden original
        elif 32 <= key <= 126:
            search += chr(key); cursor = 0; offset = 0


def _parse_makefile(content):
    """
    Parsea un Makefile uniendo continuaciones (\) y retorna:
    {target: {'deps': [...], 'cmds': [...]}}
    Preserva el case original de targets y comandos.
    Acepta indentación con tab o con espacios.
    """
    result  = {}
    cur     = None
    pending = None
    for line in content.splitlines():
        m = re.match(r'^(\w[\w.-]*)\s*:(.*)', line)
        if m and not line[0:1].isspace():
            if pending is not None and cur:
                result[cur]['cmds'].append(re.sub(r'\s+', ' ', pending).strip())
                pending = None
            cur = m.group(1)
            result[cur] = {'deps': m.group(2).split(), 'cmds': []}
        elif cur and line[0:1] in ('\t', ' ') and line.strip():
            body = line.strip()
            if pending is not None:
                if body.endswith('\\'):
                    pending += ' ' + body[:-1].strip()
                else:
                    pending += ' ' + body
                    result[cur]['cmds'].append(re.sub(r'\s+', ' ', pending).strip())
                    pending = None
            else:
                if body.endswith('\\'):
                    pending = body[:-1].strip()
                else:
                    result[cur]['cmds'].append(body)
        else:
            if pending is not None and cur:
                result[cur]['cmds'].append(re.sub(r'\s+', ' ', pending).strip())
                pending = None
    if pending is not None and cur:
        result[cur]['cmds'].append(re.sub(r'\s+', ' ', pending).strip())
    return result


def find_build_target(build_server_content, int_file):
    """
    Devuelve el todoXX exacto (preservando case del archivo) que contiene el .int.
    Soporta buildserver con múltiples -f y targets TODO/todo/TODO en cualquier case.
    """
    service = int_file.rsplit(".", 1)[0].lower()
    parsed  = _parse_makefile(build_server_content)

    # Sub-targets cuyos comandos o deps mencionan el .int o el service
    matching_subs = set()
    for tgt, info in parsed.items():
        for cmd in info['cmds']:
            if int_file.lower() in cmd.lower() or \
               re.search(rf'\b{re.escape(service)}\b', cmd, re.IGNORECASE):
                matching_subs.add(tgt)
                break
        for dep in info['deps']:
            if re.search(rf'\b{re.escape(service)}\b', dep, re.IGNORECASE):
                matching_subs.add(tgt)
                break

    if not matching_subs:
        return None

    # TODOXX que depende de esos sub-targets (case-insensitive en el nombre del target)
    for tgt, info in parsed.items():
        if re.match(r'^(?:todo|comp(?:ila)?)\d*$', tgt, re.IGNORECASE):
            if any(dep in matching_subs for dep in info['deps']):
                return tgt
            if any(re.search(rf'\b{re.escape(service)}\b', dep, re.IGNORECASE)
                   for dep in info['deps']):
                return tgt

    return None


def find_buildserver_cmd(build_server_content, int_file):
    """
    Extrae el comando buildserver completo (uniendo continuaciones \)
    que incluye el .int. Un buildserver puede tener múltiples -f.
    """
    parsed = _parse_makefile(build_server_content)
    for info in parsed.values():
        for cmd in info['cmds']:
            if cmd.lower().startswith('buildserver') and int_file.lower() in cmd.lower():
                return cmd
    return None


# ── Cargar DM Tuxedo ───────────────────────────────────────────────────────

def _run_dm_load(stdscr, ip, user, password, port, path_prod, dm_name, ssh_env):
    """tmshutdown (con stall→tmipcrm) → dmloadcf -y DM → tmboot, con streaming."""
    lines     = deque(maxlen=300)
    phase_txt = ["—"]
    STALL     = 45   # segundos sin output antes de considerar tmshutdown colgado

    def draw(footer=""):
        h2, w2 = stdscr.getmaxyx()
        stdscr.erase()
        title = f" CARGAR DM — {dm_name}  [{user}@{ip}]  {path_prod} "
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(0, 0, title[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(1, 0, f" Fase: {phase_txt[0]} "[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(2, 0, "─" * (w2-1), curses.color_pair(C_DIM))
        except curses.error: pass
        log_h   = h2 - 5
        visible = list(lines)[-(log_h):]
        for i, line in enumerate(visible):
            try: stdscr.addstr(3 + i, 0, line[:w2-1])
            except curses.error: pass
        stdscr.attron(curses.color_pair(C_STATUS))
        try: stdscr.addstr(h2-1, 0, footer[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    def run_phase(cmd_str, phase_name, timeout=180, stall_cmd=None):
        phase_txt[0] = phase_name
        lines.append("")
        lines.append(f"▶ {phase_name}")
        lines.append(f"  $ {cmd_str}")
        draw(f" {phase_name}...")

        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path_prod}" && . ./env.pro && {cmd_str}')

        proc = subprocess.Popen(ssh, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                bufsize=0, env=ssh_env)
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

        last_out  = time.time()
        deadline  = time.time() + timeout
        stalled   = False

        stdscr.nodelay(True)
        try:
            while proc.poll() is None:
                try:
                    raw = proc.stdout.read(4096)
                    if raw:
                        for ln in raw.decode("utf-8", errors="replace").splitlines():
                            if ln.strip():
                                lines.append(f"  {ln}")
                        last_out = time.time()
                except (BlockingIOError, TypeError):
                    pass
                draw(f" {phase_name}...")
                # Stall detection: tmshutdown colgado → tmipcrm
                if stall_cmd and not stalled and time.time() - last_out > STALL:
                    stalled = True
                    lines.append(f"  ! Sin respuesta {STALL}s — ejecutando: {stall_cmd}")
                    draw(f" {stall_cmd}...")
                    try: proc.kill()
                    except Exception: pass
                    r_ipc = subprocess.run(
                        ssh_cmd_base(ip, user, port) + [
                            f'cd "{path_prod}" && . ./env.pro && {stall_cmd}'
                        ],
                        capture_output=True, text=True, timeout=30, env=ssh_env,
                    )
                    for ln in (r_ipc.stdout + r_ipc.stderr).splitlines():
                        if ln.strip():
                            lines.append(f"  {ln}")
                    break
                if time.time() > deadline:
                    lines.append(f"  [!] Timeout ({timeout}s) — continuando...")
                    try: proc.kill()
                    except Exception: pass
                    break
                time.sleep(0.1)
            try:
                rest = proc.stdout.read()
                if rest:
                    for ln in rest.decode("utf-8", errors="replace").splitlines():
                        if ln.strip():
                            lines.append(f"  {ln}")
            except Exception:
                pass
        finally:
            try: proc.kill()
            except Exception: pass
            try: proc.wait(timeout=3)
            except Exception: pass
            stdscr.nodelay(False)

    run_phase("tmshutdown -y",         "TMSHUTDOWN", timeout=180, stall_cmd="tmipcrm -y")
    run_phase(f"dmloadcf -y {dm_name}", "DMLOADCF",  timeout=60)
    run_phase("tmboot -y",             "TMBOOT",     timeout=180)

    phase_txt[0] = "COMPLETADO"
    lines.append("")
    lines.append(f"  ✓ DM {dm_name} cargado.")
    h2, w2 = stdscr.getmaxyx()
    draw("")
    stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
    try:
        stdscr.addstr(h2-1, 0,
                      f" ✓ DM {dm_name} cargado — Enter para volver"[:w2-1].ljust(w2-1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
    stdscr.refresh()
    while True:
        k = stdscr.getch()
        if k in (curses.KEY_ENTER, 10, 13, ord('q'), ord('Q'), 27):
            break


# ── Opciones previas al deploy ─────────────────────────────────────────────

def _deploy_options_dialog(stdscr, views, dm_default, cbl_file):
    """
    Pantalla única de opciones antes del deploy.
    Retorna (selected_views: list, dm_name: str|None, use_hilos: bool).
    """
    view_checked = [True] * len(views)
    opt_dm    = False;  dm_name   = dm_default
    opt_hilos = False
    cursor    = 0

    while True:
        h, w   = stdscr.getmaxyx()
        n_opts = len(views) + 2   # vistas + DM + Hilos
        stdscr.erase()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try:
            stdscr.addstr(0, 0, f" OPCIONES DE DEPLOY — {cbl_file} "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        row_y = 2

        for i, (base, _, src) in enumerate(views):
            mark  = "[x]" if view_checked[i] else "[ ]"
            label = f"{base}.V" + (f"  ({src})" if src else "")
            attr  = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if cursor == i \
                    else curses.color_pair(C_NORMAL)
            try:
                stdscr.addstr(row_y, 2, f"  {mark} {label}"[:w-3], attr)
            except curses.error:
                pass
            row_y += 1

        if views:
            row_y += 1

        dm_idx    = len(views)
        hilos_idx = len(views) + 1

        # Opción DM
        mark = "[x]" if opt_dm else "[ ]"
        attr = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if cursor == dm_idx \
               else curses.color_pair(C_NORMAL)
        try:
            stdscr.addstr(row_y, 2, f"  {mark} Cargar DM y reiniciar módulo"[:w-3], attr)
        except curses.error:
            pass
        row_y += 1
        if opt_dm:
            try:
                stdscr.addstr(row_y, 6, f"DM: {dm_name}", curses.color_pair(C_DIM))
            except curses.error:
                pass
            row_y += 1

        # Opción Hilos
        mark = "[x]" if opt_hilos else "[ ]"
        attr = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if cursor == hilos_idx \
               else curses.color_pair(C_NORMAL)
        try:
            stdscr.addstr(row_y, 2,
                f"  {mark} Deploy por Hilos  (mantiene disponibilidad)"[:w-3], attr)
        except curses.error:
            pass
        row_y += 1

        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h-1, 0,
                " ↑↓=Navegar  Espacio=marcar/desmarcar  Enter=Continuar  ESC=Cancelar "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        k = stdscr.getch()
        if k == 27:
            return [], None, False
        elif k == curses.KEY_UP:
            cursor = (cursor - 1) % n_opts
        elif k == curses.KEY_DOWN:
            cursor = (cursor + 1) % n_opts
        elif k == ord(' '):
            if cursor < len(views):
                view_checked[cursor] = not view_checked[cursor]
            elif cursor == dm_idx:
                opt_dm = not opt_dm
                if opt_dm:
                    inp = (ask_input(stdscr, f"Nombre del DM [{dm_default}]: ") or "").strip()
                    dm_name = inp if inp else dm_default
            else:
                opt_hilos = not opt_hilos
        elif k in (curses.KEY_ENTER, 10, 13):
            sel = [v for v, chk in zip(views, view_checked) if chk]
            return sel, (dm_name if opt_dm else None), opt_hilos


def _get_hilos_ids(ip, user, password, port, path_prod, service_name, ssh_env):
    """
    Dado un nombre de servicio (sin .cbl), devuelve (servidor, [ids]) encontrados en psc+ps.
    """
    def run(cmd):
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20, env=ssh_env)
        return r.stdout + r.stderr

    # 1. psc → encontrar el Prog Name que corre este servicio
    psc_out = run(ssh_cmd_base(ip, user, port) + [
        f'cd "{path_prod}" && . ./env.pro && echo "psc" | tmadmin 2>/dev/null'
    ])
    servidor = None
    for line in psc_out.splitlines():
        parts = line.split()
        if parts and parts[0].lower() == service_name.lower():
            # columna Prog Name es la 3ra (índice 2)
            if len(parts) >= 3:
                servidor = parts[2]
                break
    if not servidor:
        return None, []

    # 2. ps → extraer IDs de instancias (replicando la lógica de hilachas)
    ps_out = run(ssh_cmd_base(ip, user, port) + [
        f'ps -fea | grep "{servidor}" | grep -v grep'
    ])
    ids = []
    for line in ps_out.splitlines():
        m = re.search(r'\s-i\s+(\d+)', line)
        if m:
            ids.append(int(m.group(1)))
    ids = sorted(set(ids))
    return servidor, ids


def _deploy_hilos_streaming(stdscr, ip, user, password, port, path_prod,
                             service_name, ssh_env, lines, draw_cb,
                             buildserver_cmd=None):
    """
    buildserver (una vez) + reinicio instancia por instancia.
    lines: deque compartido con run_deploy para streaming.
    draw_cb: función de redibujado.
    """
    lines.append(f"")
    lines.append(f"▶ Deploy por Hilos — {service_name}")
    draw_cb()

    servidor, ids = _get_hilos_ids(ip, user, password, port, path_prod,
                                    service_name, ssh_env)
    if not servidor:
        lines.append(f"  ✗ No se encontró servidor en psc para '{service_name}'")
        draw_cb()
        return False

    lines.append(f"  Servidor: {servidor}  —  {len(ids)} instancia(s): {ids}")
    draw_cb()

    def run_cmd(cmd_str, timeout=60):
        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path_prod}" && . ./env.pro && {cmd_str}')
        proc = subprocess.Popen(ssh, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                bufsize=0, env=ssh_env)
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)
        out = []
        deadline = time.time() + timeout
        while proc.poll() is None and time.time() < deadline:
            try:
                raw = proc.stdout.read(4096)
                if raw:
                    for ln in raw.decode("utf-8", errors="replace").splitlines():
                        if ln.strip():
                            lines.append(f"    {ln}")
                            out.append(ln)
                    draw_cb()
            except (BlockingIOError, TypeError):
                pass
            time.sleep(0.05)
        try:
            rest = proc.stdout.read()
            if rest:
                for ln in rest.decode("utf-8", errors="replace").splitlines():
                    if ln.strip():
                        lines.append(f"    {ln}")
        except Exception:
            pass
        try: proc.kill()
        except Exception: pass
        return proc.returncode, "\n".join(out)

    # buildserver (una vez, antes de reiniciar instancias)
    if buildserver_cmd:
        lines.append(f"  ── buildserver ──")
        lines.append(f"  $ {buildserver_cmd}")
        draw_cb()
        rc, _ = run_cmd(buildserver_cmd, timeout=120)
        if rc != 0:
            lines.append(f"  ✗ buildserver falló (rc={rc})")
            draw_cb()
            return False
        lines.append(f"  ✓ buildserver OK")
        draw_cb()
    else:
        lines.append(f"  ! No se encontró buildserver en build.server")
        draw_cb()

    for i in ids:
        lines.append(f"  ── Instancia {i} ──")
        draw_cb()
        run_cmd(f"tmshutdown -i {i} -w 5")
        time.sleep(2)
        run_cmd(f"tmboot -i {i}")
        lines.append(f"  ✓ Instancia {i} reiniciada")
        draw_cb()

    lines.append(f"  ✓ Deploy por Hilos completado — {len(ids)} instancia(s)")
    draw_cb()
    return True


def _deploy_hilos_silent(ip, user, password, port, path_prod,
                          service_name, ssh_env, log_cb, buildserver_cmd=None):
    """Versión headless de deploy por hilos para deploys programados."""
    servidor, ids = _get_hilos_ids(ip, user, password, port, path_prod,
                                    service_name, ssh_env)
    if not servidor:
        log_cb(f"  ✗ Hilos: no se encontró servidor para '{service_name}'")
        return False

    log_cb(f"  Hilos: {servidor}  {ids}")

    def run_cmd(cmd_str, timeout=60):
        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path_prod}" && . ./env.pro && {cmd_str}')
        try:
            r = subprocess.run(ssh, capture_output=True, text=True,
                               timeout=timeout, env=ssh_env)
            return r.returncode, r.stdout + r.stderr
        except Exception as e:
            return -1, str(e)

    if buildserver_cmd:
        log_cb(f"  $ {buildserver_cmd}")
        rc, out = run_cmd(buildserver_cmd, timeout=120)
        if rc != 0:
            log_cb(f"  ✗ buildserver falló: {out.strip()[:120]}")
            return False
        log_cb(f"  ✓ buildserver OK")
    else:
        log_cb(f"  ! No se encontró buildserver en build.server")

    for i in ids:
        run_cmd(f"tmshutdown -i {i} -w 5")
        time.sleep(2)
        run_cmd(f"tmboot -i {i}")
        log_cb(f"  ✓ Instancia {i} reiniciada")

    return True


# ── Vistas COBOL (LIBAXS) ──────────────────────────────────────────────────

def detect_copy_views(grep_output, path_hades):
    """
    Parsea salida de grep -H sobre .cbl buscando COPY *.var.
    Devuelve lista de (basename, hades_path, source_cbl) sin duplicados de vista.
    """
    import posixpath
    seen = {}   # basename → (hades_path, source_cbl)
    for line in grep_output.splitlines():
        # grep -H produce "archivo.cbl:      copy ..." — extraer fuente
        source_cbl = ""
        m_src = re.match(r'^([^:]+\.cbl):', line, re.IGNORECASE)
        if m_src:
            source_cbl = os.path.basename(m_src.group(1))
            line = line[m_src.end():]

        m = re.search(r'COPY\s+"([^"]*\.var)"', line, re.IGNORECASE)
        if not m:
            continue
        ref  = m.group(1)
        base = posixpath.splitext(posixpath.basename(ref))[0]
        if base.upper() == "PRO":
            continue
        if base in seen:
            continue   # ya registrada — primera aparición gana
        rel_dir    = posixpath.dirname(ref)
        hades_path = posixpath.normpath(
            posixpath.join(path_hades, rel_dir, base + ".V")
        )
        seen[base] = (hades_path, source_cbl)
    return [(b, hp, src) for b, (hp, src) in seen.items()]
    # [(basename, hades_path, source_cbl), ...]


def multiselect_maquinas_dialog(stdscr, maquinas, title="¿A qué servidores llevar la vista?"):
    """Multi-select de maquinas. Devuelve lista de maquinas seleccionadas, o [] si cancela."""
    if not maquinas:
        return []
    selected_set = set(range(len(maquinas)))   # todos preseleccionados
    cursor = 0
    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(0, 0, f" {title} "[:w-1].ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(2, 2, "Espacio=marcar/desmarcar  Enter=confirmar  ESC=cancelar",
                           curses.color_pair(C_DIM))
        except curses.error: pass
        for i, maq in enumerate(maquinas):
            y    = 4 + i
            mark = "[x]" if i in selected_set else "[ ]"
            line = f"  {mark} {maq['nombre']:<20}  {maq['ip']}"
            attr = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if i == cursor \
                   else curses.color_pair(C_NORMAL)
            try: stdscr.addstr(y, 0, line[:w-1].ljust(w-1) if i == cursor else line[:w-1], attr)
            except curses.error: pass
        stdscr.refresh()
        k = stdscr.getch()
        if k == 27:
            return []
        elif k == curses.KEY_UP:
            cursor = (cursor - 1) % len(maquinas)
        elif k == curses.KEY_DOWN:
            cursor = (cursor + 1) % len(maquinas)
        elif k == ord(' '):
            if cursor in selected_set:
                selected_set.discard(cursor)
            else:
                selected_set.add(cursor)
        elif k in (curses.KEY_ENTER, 10, 13):
            return [maquinas[i] for i in sorted(selected_set)]


def deploy_view_files(stdscr, views, path_hades_resolved, libpath, maquinas, hades_env):
    """
    Copia archivos .V desde hades/LIBAXS al LIBPATH de cada maquina seleccionada.
    views = [(basename, hades_path), ...]
    """
    lines = deque(maxlen=300)
    h, w  = stdscr.getmaxyx()

    def draw(msg=""):
        stdscr.erase()
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(0, 0, " DEPLOY VISTAS — LIBAXS ".ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(2, 0, "─" * (w-1), curses.color_pair(C_DIM))
        except curses.error: pass
        log_h   = h - 5
        visible = list(lines)[-(log_h):]
        for i, line in enumerate(visible):
            try: stdscr.addstr(3 + i, 0, line[:w-1])
            except curses.error: pass
        stdscr.attron(curses.color_pair(C_STATUS))
        try: stdscr.addstr(h-1, 0, msg[:w-1].ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    for view_entry in views:
        import posixpath as _pp
        basename, hades_path = view_entry[0], view_entry[1]
        local_tmp = f"/tmp/flv_view_{basename}.V"
        lines.append(f"▶ Vista: {basename}.V")
        draw(f" Descargando {basename}.V desde hades...")

        # 1. Intentar descargar .V de hades
        dl_cmd = hades_scp_cmd(hades_path, local_tmp)
        r = subprocess.run(dl_cmd, capture_output=True, text=True, timeout=30, env=hades_env)

        if r.returncode != 0:
            err_out = (r.stdout + r.stderr).strip()
            if "no such file" in err_out.lower():
                # .V no existe — compilarlo con viewc32
                lib_dir  = _pp.dirname(hades_path)          # .../LIBCONSIN
                base_dir = _pp.dirname(lib_dir)             # .../intraplatinum
                lib_name = _pp.basename(lib_dir)            # LIBCONSIN
                src_v    = f"{basename}.v"                  # coslbanc.v
                lines.append(f"  ! .V no encontrado — compilando: viewc32 -C {src_v}")
                draw(f" Compilando {src_v} en hades...")
                vc_cmd = (
                    f'cd "{base_dir}" && . ./env.desa '
                    f'&& cd "{lib_name}" && viewc32 -C {src_v}'
                )
                vc_r = subprocess.run(
                    hades_cmd_base() + [vc_cmd],
                    capture_output=True, text=True, timeout=30, env=hades_env,
                )
                for ln in (vc_r.stdout + vc_r.stderr).splitlines():
                    if ln.strip():
                        lines.append(f"  {ln}")
                draw(f" Reintentando descarga de {basename}.V...")
                r = subprocess.run(dl_cmd, capture_output=True, text=True,
                                   timeout=30, env=hades_env)
                if r.returncode != 0:
                    lines.append(
                        f"  ✗ {basename}.V: {(r.stdout+r.stderr).strip()[:100]}")
                    continue
            else:
                lines.append(f"  ✗ Error: {err_out[:120]}")
                continue

        lines.append(f"  ✓ {basename}.V descargado")

        # 2. Subir a cada maquina
        for maq in maquinas:
            maq_ip   = maq["ip"]
            maq_user = maq["ssh_user"]
            maq_port = maq["ssh_port"]
            maq_pass = maq["ssh_password"]
            dst      = f"{libpath}/{basename}.V"
            draw(f" Copiando {basename}.V → {maq['nombre']} ({maq_ip}:{libpath})")

            # Crear directorio si no existe
            subprocess.run(
                ssh_cmd_base(maq_ip, maq_user, maq_port) + [f'mkdir -p "{libpath}"'],
                capture_output=True, text=True, timeout=10, env=_sshenv(maq_pass),
            )

            ul_cmd, ul_env = _scp_upload_cmd(local_tmp, maq_ip, maq_user, maq_port,
                                              dst, maq_pass)
            r2 = subprocess.run(ul_cmd, capture_output=True, text=True, timeout=60, env=ul_env)
            if r2.returncode != 0:
                lines.append(
                    f"  ✗ {maq['nombre']}: {(r2.stdout+r2.stderr).strip()[:100]}")
            else:
                lines.append(f"  ✓ {maq['nombre']}: {dst}")
        lines.append("")
    lines.append("  ✓ Todas las vistas copiadas.")

    h3, w3 = stdscr.getmaxyx()
    stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
    try:
        stdscr.addstr(h3-1, 0,
                      " ✓ Vistas copiadas — Enter para continuar con el deploy COBOL "[:w3-1].ljust(w3-1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
    stdscr.refresh()
    while True:
        k = stdscr.getch()
        if k in (curses.KEY_ENTER, 10, 13, ord('q'), ord('Q'), 27):
            break


def run_deploy(stdscr, row, cbl_file, pre_options=None):
    """
    Pipeline completo de deploy COBOL.
    pre_options: (carry_views, dm_name, use_hilos) ya calculados; si None se muestra el dialog.
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
    date_str   = time.strftime("%Y%m%d_%H%M%S")
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

    ssh_env     = _sshenv(password)
    hades_env   = _sshenv(HADES.get("password", ""))
    path_hades  = _resolve_hades_path(path_hades)

    libpath = row.get("libpath", "").strip()

    # ── Opciones de deploy (dialog o pre_options ya calculados) ───────────
    iniciales  = row.get("iniciales", "").strip()
    dm_default = f"DM{iniciales}" if iniciales else "DM"
    if pre_options is not None:
        carry_views, dm_name, use_hilos = pre_options
    else:
        # Detectar vistas y mostrar dialog
        h2, w2 = stdscr.getmaxyx()
        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h2 - 1, 0,
                          f" Verificando vistas en {cbl_file}... "[:w2-1].ljust(w2-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()
        try:
            clean_path = path_hades.rstrip("/")
            grep_r = subprocess.run(
                hades_cmd_base() + [
                    f"grep -Hi 'copy.*\\.var' \"{clean_path}/{cbl_file}\" 2>/dev/null || true"
                ],
                capture_output=True, text=True, timeout=15, env=hades_env,
            )
            views = detect_copy_views(grep_r.stdout, clean_path)
        except Exception:
            views = []
        carry_views, dm_name, use_hilos = _deploy_options_dialog(
            stdscr, views, dm_default, cbl_file
        )
        init_colors(); stdscr.keypad(True)

    if carry_views:
        if not libpath:
            _show_message(stdscr,
                "Sin libpath configurado — no se pueden copiar vistas", error=True)
        else:
            all_maquinas = fetch_maquinas(sistema="cobol")
            selected_maqs = multiselect_maquinas_dialog(
                stdscr, all_maquinas, title="Servidores destino para vistas")
            init_colors(); stdscr.keypad(True)
            if selected_maqs:
                deploy_view_files(stdscr, carry_views, path_hades, libpath,
                                  selected_maqs, hades_env)
        init_colors(); stdscr.keypad(True)

    def run_step(idx, cmd, timeout=120, env=None):
        steps[idx][0] = "run"
        redraw(idx)
        output_lines.append(f"$ {' '.join(cmd)}")
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
            out = (result.stdout + result.stderr).strip()
            ok  = result.returncode == 0
        except subprocess.TimeoutExpired:
            out = f"Timeout ({timeout}s)"
            ok  = False
        for line in out.splitlines():
            output_lines.append(line)
        if ok:
            steps[idx][0] = "ok"
        else:
            steps[idx][0] = "err"
        redraw(idx)
        return ok

    redraw()
    error = False

    # ── Paso 1: Compilar en hades (streaming para ver output de cob) ───────
    steps[0][0] = "run"
    redraw(0)
    cmd1 = hades_cmd_base() + [f'cd "{path_hades}" && cob {cbl_file}']
    output_lines.append(f"$ cob {cbl_file}")
    proc1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             bufsize=1, text=True, env=hades_env)
    for line in proc1.stdout:
        output_lines.append(line.rstrip())
        redraw(0)
    proc1.wait(timeout=120)
    if proc1.returncode == 0:
        steps[0][0] = "ok"
        output_lines.append(f"✓ {cbl_file} → {int_file}")
    else:
        steps[0][0] = "err"
        error = True
    redraw(0)

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
            f"{user}@{_clean_ip(ip)}:{path_prod}/{int_file}",
        ]
        if not run_step(3, cmd4, timeout=60, env=ssh_env):
            error = True

    # ── Paso 5: Build / Hilos ─────────────────────────────────────────────
    if not error:
        steps[4][0] = "run"
        read_cmd = ssh_cmd_base(ip, user, port) + [f'cat "{path_prod}/build.server" 2>/dev/null']
        r_bs = subprocess.run(read_cmd, capture_output=True, text=True, timeout=15, env=ssh_env)
        build_content = r_bs.stdout

        if use_hilos:
            bs_cmd = find_buildserver_cmd(build_content, int_file) if build_content else None
            steps[4][1] = f"5/5  Deploy por Hilos:  {service} (mantiene disponibilidad)"
            redraw(4)
            ok_h = _deploy_hilos_streaming(
                stdscr, ip, user, password, port, path_prod,
                service, ssh_env, output_lines,
                lambda: redraw(4),
                buildserver_cmd=bs_cmd,
            )
            steps[4][0] = "ok" if ok_h else "err"
            if not ok_h:
                error = True
            redraw(4)
        else:
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
                steps[4][0] = "ok" if proc5.returncode == 0 else "err"
                if proc5.returncode != 0:
                    error = True
                redraw(4)

    # Limpiar temporal
    try:
        os.remove(local_tmp)
    except Exception:
        pass

    redraw(done=True, error=error)

    # Si se eligió cargar DM, correrlo automáticamente tras el deploy
    if not error and dm_name:
        redraw(done=True, error=False)
        stdscr.nodelay(False)
        stdscr.getch()   # esperar Enter para ver el resultado del deploy
        _run_dm_load(stdscr, ip, user, password, port, path_prod, dm_name, ssh_env)
        init_colors(); stdscr.keypad(True)
        return

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


def _deploy_one_silent(row, cbl_file, build_content, log_cb, use_hilos=False):
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
    date_str    = time.strftime("%Y%m%d_%H%M%S")
    backup_name = f"{int_file}.{date_str}"
    local_tmp   = f"/tmp/flv_mdeploy_{int_file}"

    ssh_env    = _sshenv(password)
    hades_env  = _sshenv(HADES.get("password", ""))
    path_hades = _resolve_hades_path(path_hades)

    def run(cmd, timeout=120, env=None):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
            return r.returncode == 0, (r.stdout + r.stderr).strip()
        except subprocess.TimeoutExpired:
            return False, f"Timeout ({timeout}s)"

    def fail(prefix, out):
        for line in out.splitlines():
            if line.strip():
                log_cb(f"    {line.rstrip()}")
        last = next((l.strip() for l in reversed(out.splitlines()) if l.strip()), out[:80])
        return False, f"{prefix}: {last}"

    # 1. Compilar en hades
    log_cb(f"  [1/5] cob {cbl_file}")
    ok, out = run(hades_cmd_base() + [f'cd "{path_hades}" && cob {cbl_file}'], env=hades_env)
    if not ok:
        return fail("Error compilando", out)

    # 2. Descargar .int
    log_cb(f"  [2/5] scp {int_file} ← hades")
    ok, out = run(hades_scp_cmd(f"{path_hades}/{int_file}", local_tmp), timeout=60, env=hades_env)
    if not ok:
        return fail("Error descargando .int", out)

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
        local_tmp, f"{user}@{_clean_ip(ip)}:{path_prod}/{int_file}",
    ], timeout=60, env=ssh_env)
    if not ok:
        return fail("Error subiendo .int", out)

    # 5. Build / Hilos
    if use_hilos:
        bs_cmd = find_buildserver_cmd(build_content, int_file)
        log_cb(f"  [5/5] hilos {service}")
        ok = _deploy_hilos_silent(ip, user, password, port, path_prod,
                                   service, ssh_env, log_cb, buildserver_cmd=bs_cmd)
        out = "" if ok else f"Hilos: error"
    else:
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
        label = "hilos" if use_hilos else f"make {target}"
        return fail(f"Error en {label}", out)
    label = "hilos" if use_hilos else f"make {target}"
    return True, f"OK → {label}"


def run_multi_deploy(stdscr, row, cbl_files, use_hilos=False):
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

        ok, detail = _deploy_one_silent(row, cbl_file, build_content, log_cb, use_hilos=use_hilos)
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

def _run_batch_deploy(stdscr, texto, usuario):
    """Parsea PREFIX/archivo.cbl, agrupa por cliente y ejecuta multi-deploy por cada uno."""
    grupos = {}
    for linea in texto:
        linea = linea.strip()
        if not linea or '/' not in linea:
            continue
        prefix, archivo = linea.split('/', 1)
        prefix  = prefix.strip()
        archivo = archivo.strip()
        if prefix and archivo:
            grupos.setdefault(prefix, []).append(archivo)

    if not grupos:
        _show_message(stdscr, "No se encontraron líneas PREFIX/archivo.cbl válidas", error=True)
        return

    errores = []
    for prefix, archivos in grupos.items():
        row = fetch_cliente_by_path_prefix(prefix)
        if not row:
            errores.append(f"'{prefix}': sin cliente con esas iniciales")
            continue
        if not row.get("path_hades"):
            errores.append(f"'{prefix}' ({row['desc_cliente']}): sin path_hades")
            continue
        if not row.get("path"):
            errores.append(f"'{prefix}' ({row['desc_cliente']}): sin path producción")
            continue
        run_multi_deploy(stdscr, row, archivos)
        init_colors(); stdscr.keypad(True); stdscr.timeout(100)

    if errores:
        _show_message(stdscr, "Sin cliente: " + ", ".join(errores), error=True)


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


def _show_programado_detalle(stdscr, dep):
    """Muestra el detalle/log de un deploy programado a pantalla completa."""
    svcs = dep.get("servicios", [])
    if svcs and svcs[0] == "__reinicio__":
        extras = [s.split(":", 1)[1] for s in svcs if s.startswith(("__ubb__:", "__dm__:"))]
        tipo = "⟳ REINICIO" + (f"  +{', '.join(extras)}" if extras else "")
    else:
        clean = [s for s in svcs if not s.startswith("__")]
        meta  = [s.split(":", 1)[1] for s in svcs if s.startswith("__dm__:")]
        tipo  = ", ".join(clean) + (f"  +DM:{meta[0]}" if meta else "")

    fh     = dep["fecha_hora"].strftime("%d/%m/%Y %H:%M") \
             if hasattr(dep["fecha_hora"], "strftime") else str(dep["fecha_hora"])[:16]
    est    = dep.get("estado", "").upper()
    detalle = dep.get("detalle") or "(sin detalle)"
    lines  = detalle.splitlines()
    offset = 0

    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        # Header
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try:
            stdscr.addstr(0, 0,
                f" {dep['desc_cliente']}  —  {tipo}  —  {fh}  —  {est} "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        try:
            stdscr.addstr(1, 0, "─" * (w - 1), curses.color_pair(C_DIM))
        except curses.error:
            pass

        log_h   = h - 4
        visible = lines[offset: offset + log_h]
        for i, ln in enumerate(visible):
            attr = curses.color_pair(C_NORMAL)
            if ln.startswith("═") or (ln.startswith("✓") and "completado" in ln.lower()):
                attr = curses.color_pair(C_OK) | curses.A_BOLD
            elif ln.startswith("✗") or "error" in ln.lower():
                attr = curses.color_pair(C_ERROR) | curses.A_BOLD
            elif ln.startswith("✓"):
                attr = curses.color_pair(C_OK)
            elif ln.startswith("  [!]") or "forzad" in ln.lower():
                attr = curses.color_pair(C_WARN)
            elif ln.startswith("▶") or ln.startswith("  $"):
                attr = curses.color_pair(C_TITLE)
            elif ln.startswith("─"):
                attr = curses.color_pair(C_DIM)
            try:
                stdscr.addstr(2 + i, 0, ln[:w-1], attr)
            except curses.error:
                pass

        pct = f"{offset+1}-{min(offset+log_h, len(lines))}/{len(lines)}" if lines else "0/0"
        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h-1, 0,
                f" ↑↓/PgUp/PgDn=Scroll  q/ESC/Enter=Volver  [{pct} líneas] "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        k = stdscr.getch()
        if k in (ord('q'), ord('Q'), 27, curses.KEY_ENTER, 10, 13):
            break
        elif k == curses.KEY_UP:
            offset = max(0, offset - 1)
        elif k == curses.KEY_DOWN:
            offset = min(max(0, len(lines) - log_h), offset + 1)
        elif k == curses.KEY_PPAGE:
            offset = max(0, offset - log_h)
        elif k == curses.KEY_NPAGE:
            offset = min(max(0, len(lines) - log_h), offset + log_h)
        elif k == curses.KEY_HOME:
            offset = 0
        elif k == curses.KEY_END:
            offset = max(0, len(lines) - log_h)


def draw_programados(stdscr, rows, selected, offset):
    """Dibuja la lista de deploys/reinicios programados del usuario."""
    h, w   = stdscr.getmaxyx()
    list_h = h - 6

    # Anchos de columna adaptados al ancho de terminal
    C_CLI  = max(20, min(28, w // 4))
    C_TIPO = max(30, min(45, w // 3))
    C_FH   = 16
    # ESTADO ocupa el resto

    def _trunc(s, n):
        return s if len(s) <= n else s[:n - 1] + "…"

    header = (f"  {'CLIENTE':<{C_CLI}}  {'TIPO / SERVICIOS':<{C_TIPO}}"
              f"  {'FECHA/HORA':<{C_FH}}  ESTADO")
    stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
    try:
        stdscr.addstr(4, 0, header[:w - 1].ljust(w - 1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

    ESTADO_COLOR = {
        "pendiente":  C_WARN,
        "ejecutando": C_SEARCH,
        "ok":         C_OK,
        "error":      C_ERROR,
    }

    visible = rows[offset: offset + list_h]
    for i, dep in enumerate(visible):
        y     = 5 + i
        abs_i = offset + i
        svcs  = dep["servicios"]
        if svcs and svcs[0] == "__reinicio__":
            extras = [s.split(":", 1)[1] for s in svcs if s.startswith(("__ubb__:", "__dm__:"))]
            tipo = "⟳ REINICIO" + (f"  +{', '.join(extras)}" if extras else "")
        else:
            clean = [s for s in svcs if not s.startswith("__")]
            meta  = [s.split(":", 1)[1] for s in svcs if s.startswith("__dm__:")]
            tipo  = ", ".join(clean) + (f"  +DM:{meta[0]}" if meta else "")
        fh    = dep["fecha_hora"].strftime("%d/%m/%Y %H:%M") \
                if hasattr(dep["fecha_hora"], "strftime") else str(dep["fecha_hora"])[:16]
        est   = dep["estado"]
        color = ESTADO_COLOR.get(est, C_NORMAL)
        cli   = _trunc(dep["desc_cliente"], C_CLI)
        tip   = _trunc(tipo, C_TIPO)
        line  = f"  {cli:<{C_CLI}}  {tip:<{C_TIPO}}  {fh:<{C_FH}}  {est.upper()}"
        if abs_i == selected:
            stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            try:
                stdscr.addstr(y, 0, line[:w - 1].ljust(w - 1))
            except curses.error:
                pass
            stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.attron(curses.color_pair(color))
            try:
                stdscr.addstr(y, 0, line[:w - 1])
            except curses.error:
                pass
            stdscr.attroff(curses.color_pair(color))

    for i in range(len(visible), list_h):
        try:
            stdscr.addstr(5 + i, 0, " " * (w - 1))
        except curses.error:
            pass


# ── Tabs y dibujo ──────────────────────────────────────────────────────────

def draw_batch_tab(stdscr):
    h, w = stdscr.getmaxyx()
    lines = [
        "BATCH DEPLOY",
        "",
        "Pega una lista en formato:",
        "  PREFIX/archivo.cbl",
        "  (una línea por archivo)",
        "",
        "Presiona  Enter  para comenzar.",
    ]
    start = max(1, h // 2 - len(lines) // 2)
    for i, line in enumerate(lines):
        x = max(0, (w - len(line)) // 2)
        attr = curses.A_BOLD if i == 0 else curses.A_NORMAL
        try:
            stdscr.addstr(start + i, x, line, attr)
        except curses.error:
            pass


TABS = [
    ("1", "Clientes",      "clientes"),
    ("2", "Servidores",    "maquinas"),
    ("3", "Deploy",        "deploy"),
    ("4", "Deploy-MultiArchivo", "multideploy"),
    ("5", "Reinicio",       "reinicio"),
    ("6", "Multi-Cliente",  "batch"),
    ("7", "Programados",   "programados"),
]


def draw_header(stdscr, mode, search, searching=False):
    h, w = stdscr.getmaxyx()
    title  = f" {APP_NAME.upper()} v{APP_VERSION} · Síntesis "
    credit = f" {APP_CREDIT} "
    stdscr.attron(curses.color_pair(C_HEADER))
    stdscr.addstr(0, 0, " " * (w - 1))
    stdscr.addstr(0, max(0, (w - len(title)) // 2), title, curses.A_BOLD)
    try:
        stdscr.addstr(0, w - len(credit) - 1, credit, curses.color_pair(C_DIM))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_HEADER))

    # Tabs en 2 filas: [1-4] Acceso/Monitoreo  |  [5-8] Operaciones
    for row_idx, group in enumerate((TABS[:4], TABS[4:])):
        stdscr.addstr(1 + row_idx, 0, " " * (w - 1))
        x = 2
        for key, label, m in group:
            tab_txt = f" [{key}] {label} "
            if m == mode:
                stdscr.addstr(1 + row_idx, x, tab_txt, curses.color_pair(C_SELECTED) | curses.A_BOLD)
            else:
                stdscr.addstr(1 + row_idx, x, tab_txt, curses.color_pair(C_DIM))
            x += len(tab_txt) + 1

    stdscr.attron(curses.color_pair(C_SEARCH))
    stdscr.addstr(3, 0, " " * (w - 1))
    if searching:
        stdscr.addstr(3, 2, f" BUSCAR: {search}_  [ESC=cancelar]")
    else:
        stdscr.addstr(3, 2, f" Buscar: {search}_")
    stdscr.attroff(curses.color_pair(C_SEARCH))


def draw_footer(stdscr, msg="", mode=""):
    h, w = stdscr.getmaxyx()

    if mode == "reinicio":
        shortcuts = " Enter=Reiniciar dominio  1-8=Tab  q=Salir"
        left  = shortcuts
        right = f"  {msg} " if msg else ""
        line  = left + right.rjust(w - 1 - len(left))
    elif mode == "maquinas":
        shortcuts = " Enter=SSH  F4=Editar  F2=Nuevo  Supr=Eliminar  q=Menú"
        left  = shortcuts
        right = f"  {msg} " if msg else ""
        line  = left + right.rjust(w - 1 - len(left))
    elif mode == "clientes":
        shortcuts = " Enter=SSH  F3=Detalle  F4=Editar  F2=Nuevo  Supr=Eliminar  q=Menú"
        if msg and not msg.endswith("resultado(s)"):
            left = f" {msg}"
            right = ""
        else:
            left  = shortcuts
            right = f"  {msg} " if msg else ""
        line = left + right.rjust(w - 1 - len(left))
    elif mode == "programados":
        shortcuts = " F4=Reprogramar  Supr=Cancelar/Eliminar  F5=Actualizar  1-8=Tab  q=Salir"
        if msg and not msg.startswith("ERROR:") and not msg.endswith("resultado(s)"):
            line = f" {msg} "
        elif msg.startswith("ERROR:"):
            left  = f" {msg} "
            right = shortcuts
            line  = left[:w - 1 - len(shortcuts)] + right if len(left) + len(shortcuts) > w - 1 else left + right.rjust(w - 1 - len(left))
        else:
            left  = shortcuts
            right = f"  {msg} " if msg else ""
            line  = left + right.rjust(w - 1 - len(left))
    else:
        footer = " Enter=Acción  1-9=Tab  ESC=Borrar búsqueda  Ctrl+E=Email  q=Menú "
        line   = f" {msg} " if msg else footer

    stdscr.attron(curses.color_pair(C_STATUS))
    stdscr.addstr(h - 1, 0, line[:w - 1].ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_STATUS))


def draw_list(stdscr, rows, selected, offset, mode):
    h, w    = stdscr.getmaxyx()
    list_h  = h - 6
    start_y = 5

    if mode == "maquinas":
        col_header = f"{'SERVIDOR':<20}  {'IP':<16}  {'USUARIO':<12}  {'PUERTO'}  {'DESCRIPCIÓN'}"
    elif mode == "deploy":
        col_header = f"{'#':>5}  {'CLIENTE':<25}  {'SERVIDOR':<15}  {'IP':<16}  {'PATH HADES'}"
    else:
        col_header = f"{'#':>5}  {'CLIENTE':<25}  {'SERVIDOR':<15}  {'IP':<16}  {'PATH'}"

    stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
    stdscr.addstr(4, 0, col_header[:w - 1].ljust(w - 1))
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


# ── Lectura de teclas con soporte manual de secuencias de escape ───────────

# Mapeo de secuencias de escape a KEY_Fx para terminales que no usan terminfo
_ESC_SEQUENCES = {
    # Linux console: \x1b[[A-E → F1-F5
    (ord('['), ord('['), ord('A')): curses.KEY_F1,
    (ord('['), ord('['), ord('B')): curses.KEY_F2,
    (ord('['), ord('['), ord('C')): curses.KEY_F3,
    (ord('['), ord('['), ord('D')): curses.KEY_F4,
    (ord('['), ord('['), ord('E')): curses.KEY_F5,
    # xterm/VT100 SS3: \x1bOP-OS → F1-F4
    (ord('O'), ord('P')): curses.KEY_F1,
    (ord('O'), ord('Q')): curses.KEY_F2,
    (ord('O'), ord('R')): curses.KEY_F3,
    (ord('O'), ord('S')): curses.KEY_F4,
    # rxvt/xterm CSI: \x1b[11~-\x1b[15~ → F1-F5
    (ord('['), ord('1'), ord('1'), ord('~')): curses.KEY_F1,
    (ord('['), ord('1'), ord('2'), ord('~')): curses.KEY_F2,
    (ord('['), ord('1'), ord('3'), ord('~')): curses.KEY_F3,
    (ord('['), ord('1'), ord('4'), ord('~')): curses.KEY_F4,
    (ord('['), ord('1'), ord('5'), ord('~')): curses.KEY_F5,
}

def read_key(stdscr):
    key = stdscr.getch()
    if key != 27:
        return key
    # ESC recibido: leer hasta 5 chars con timeout corto para armar la secuencia
    stdscr.timeout(50)
    seq = []
    for _ in range(5):
        ch = stdscr.getch()
        if ch == -1:
            break
        seq.append(ch)
    stdscr.timeout(100)
    if not seq:
        return 27  # ESC solo
    # buscar coincidencia desde la secuencia más larga posible
    for length in range(len(seq), 0, -1):
        mapped = _ESC_SEQUENCES.get(tuple(seq[:length]))
        if mapped is not None:
            return mapped
    return 27  # secuencia desconocida → tratar como ESC


# ── Reinicio de dominio Tuxedo ─────────────────────────────────────────────

def _reinicio_options_dialog(stdscr, row):
    """
    Pantalla de opciones para el reinicio: cargar DM y/o UBB.
    Retorna (ubb_name: str|None, dm_name: str|None).
    """
    iniciales = row.get("iniciales", "").strip()
    dm_def  = f"DM{iniciales}"  if iniciales else "DM"
    ubb_def = f"UBB{iniciales}" if iniciales else "UBB"

    opt_ubb = False;  ubb_name = ubb_def
    opt_dm  = False;  dm_name  = dm_def
    cursor  = 0        # 0=UBB, 1=DM
    nombre  = row.get("desc_cliente", "")

    while True:
        h, w = stdscr.getmaxyx()
        stdscr.erase()

        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try:
            stdscr.addstr(0, 0, f" OPCIONES DE REINICIO — {nombre} "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

        row_y = 2
        opts = [("ubb", opt_ubb, ubb_name), ("dm", opt_dm, dm_name)]
        labels = {
            "ubb": ("Cargar UBB  (tmloadcf -y ", ubb_name, ")"),
            "dm":  ("Cargar DM   (dmloadcf -y ", dm_name,  ")"),
        }
        for i, (key, checked, name) in enumerate(opts):
            mark = "[x]" if checked else "[ ]"
            lbl_pre, lbl_name, lbl_suf = labels[key]
            label = f"{lbl_pre}{lbl_name}{lbl_suf}"
            attr  = (curses.color_pair(C_SELECTED) | curses.A_BOLD) if cursor == i \
                    else curses.color_pair(C_NORMAL)
            try:
                stdscr.addstr(row_y, 2, f"  {mark} {label}"[:w-3], attr)
            except curses.error:
                pass
            row_y += 1

        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h-1, 0,
                " ↑↓=Navegar  Espacio=marcar  Enter=Continuar  ESC=Cancelar "[:w-1].ljust(w-1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        k = stdscr.getch()
        if k == 27:
            return True, None, None   # (cancelled, ubb, dm)
        elif k == curses.KEY_UP:
            cursor = (cursor - 1) % 2
        elif k == curses.KEY_DOWN:
            cursor = (cursor + 1) % 2
        elif k == ord(' '):
            if cursor == 0:
                opt_ubb = not opt_ubb
                if opt_ubb:
                    inp = (ask_input(stdscr, f"Nombre UBB [{ubb_def}]: ") or "").strip()
                    ubb_name = inp if inp else ubb_def
                    labels["ubb"] = ("Cargar UBB  (tmloadcf -y ", ubb_name, ")")
            else:
                opt_dm = not opt_dm
                if opt_dm:
                    inp = (ask_input(stdscr, f"Nombre DM [{dm_def}]: ") or "").strip()
                    dm_name = inp if inp else dm_def
                    labels["dm"] = ("Cargar DM   (dmloadcf -y ", dm_name, ")")
        elif k in (curses.KEY_ENTER, 10, 13):
            return False, (ubb_name if opt_ubb else None), (dm_name if opt_dm else None)


def reinicio_tuxedo(stdscr, row, usuario="", ubb_name=None, dm_name=None):
    """Reinicia el dominio Tuxedo: tmshutdown → [tmloadcf] → [dmloadcf] → tmboot."""
    ip       = row["ip"]
    user     = row["ssh_user"]
    password = row["ssh_password"]
    port     = row["ssh_port"]
    path     = row.get("path") or ""
    nombre   = row.get("desc_cliente", ip)

    if not path:
        _show_message(stdscr, "Este cliente no tiene PATH configurado", error=True)
        return

    when = deploy_when_picker(stdscr, f"REINICIO — {nombre}")
    if when is None:
        return

    if when == "schedule":
        fh = ask_datetime(stdscr)
        if fh:
            svcs = ["__reinicio__"]
            if ubb_name:
                svcs.append(f"__ubb__:{ubb_name}")
            if dm_name:
                svcs.append(f"__dm__:{dm_name}")
            db_insert_deploy_programado(
                usuario, row["nro_cliente"], nombre, svcs, fh,
            )
            notify_scheduler()
            extras = []
            if ubb_name: extras.append(f"UBB:{ubb_name}")
            if dm_name:  extras.append(f"DM:{dm_name}")
            sufijo = f" + {', '.join(extras)}" if extras else ""
            _show_message(stdscr, f"✓ Reinicio{sufijo} programado para {fh.strftime('%d/%m/%Y %H:%M')}")
        return

    STALL_SECS = 30

    lines     = deque(maxlen=500)
    phase_txt = ["—"]
    cancelled = [False]

    def draw(footer=""):
        h, w = stdscr.getmaxyx()
        stdscr.erase()
        title = f" REINICIO — {nombre}  [{user}@{_clean_ip(ip)}]  {path} "
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try:
            stdscr.addstr(0, 0, title[:w - 1].ljust(w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try:
            stdscr.addstr(1, 0, f" Fase: {phase_txt[0]} "[:w - 1].ljust(w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try:
            stdscr.addstr(2, 0, "─" * (w - 1), curses.color_pair(C_DIM))
        except curses.error:
            pass
        log_h   = h - 5
        visible = list(lines)[-(log_h):]
        for i, line in enumerate(visible):
            try:
                stdscr.addstr(3 + i, 0, line[:w - 1])
            except curses.error:
                pass
        stdscr.attron(curses.color_pair(C_STATUS))
        try:
            stdscr.addstr(h - 1, 0, footer[:w - 1].ljust(w - 1))
        except curses.error:
            pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    def run_phase(cmd_str, phase_name, stall_timeout=None):
        phase_txt[0] = phase_name
        lines.append("")
        lines.append(f"▶ {phase_name}")
        lines.append(f"  $ {cmd_str}")
        lines.append("")
        draw(f" {phase_name}... q=Cancelar")

        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')

        proc = subprocess.Popen(
            ssh,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            env=_sshenv(password),
        )
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

        stdscr.nodelay(True)
        start       = time.time()
        last_output = time.time()
        force_used  = False

        try:
            while proc.poll() is None:
                try:
                    raw = proc.stdout.read(4096)
                    if raw:
                        for ln in raw.decode("utf-8", errors="replace").splitlines():
                            if ln.strip():
                                lines.append(f"  {ln}")
                        last_output = time.time()
                except (BlockingIOError, TypeError):
                    pass

                elapsed = int(time.time() - start)
                stalled = stall_timeout and (time.time() - last_output > stall_timeout)

                if stalled:
                    force_used  = True
                    lines.append(f"  [!] Sin respuesta por {stall_timeout}s → Ctrl+C + y")
                    draw(f" {phase_name}  {elapsed}s  ⚠ COLGADO — enviando Ctrl+C...")
                    try:
                        proc.stdin.write(b'\x03')
                        proc.stdin.flush()
                    except Exception:
                        pass
                    time.sleep(0.4)
                    try:
                        proc.stdin.write(b'y\n')
                        proc.stdin.flush()
                    except Exception:
                        pass
                    last_output = time.time()
                else:
                    draw(f" {phase_name}  {elapsed}s  q=Cancelar")

                k = stdscr.getch()
                if k in (ord('q'), ord('Q'), 27):
                    cancelled[0] = True
                    try:
                        proc.stdin.write(b'\x03')
                        proc.stdin.flush()
                    except Exception:
                        pass
                    break

                time.sleep(0.1)

            try:
                rest = proc.stdout.read()
                if rest:
                    for ln in rest.decode("utf-8", errors="replace").splitlines():
                        if ln.strip():
                            lines.append(f"  {ln}")
            except Exception:
                pass

        finally:
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=3)
            except Exception:
                pass
            stdscr.nodelay(False)

        return force_used

    # ── Fase 1: Bajada ────────────────────────────────────────────────────────
    snap_before = len(lines)
    force = run_phase("tmshutdown -y", "BAJADA", stall_timeout=STALL_SECS)

    shutdown_output = " ".join(list(lines)[snap_before:])
    shutdown_failed = (
        "Shutdown failed" in shutdown_output
        or "Cannot shutdown BBL" in shutdown_output
    )

    # ── Fase 2: Limpieza IPC si hubo cierre forzado o bajada incompleta ───────
    if not cancelled[0] and (force or shutdown_failed):
        lines.append("")
        if shutdown_failed:
            lines.append("  [!] BBL no bajó (Shutdown failed) → ejecutando tmipcrm para limpiar IPC...")
        else:
            lines.append("  [!] Bajada forzada → ejecutando tmipcrm para limpiar IPC...")
        run_phase("tmipcrm -y", "LIMPIEZA IPC")

    # ── Fase 3: Carga UBB (opcional) ─────────────────────────────────────────
    if not cancelled[0] and ubb_name:
        run_phase(f"tmloadcf -y {ubb_name}", f"CARGA UBB ({ubb_name})")

    # ── Fase 4: Carga DM (opcional) ──────────────────────────────────────────
    if not cancelled[0] and dm_name:
        run_phase(f"dmloadcf -y {dm_name}", f"CARGA DM ({dm_name})")

    # ── Fase 5: Subida ────────────────────────────────────────────────────────
    if not cancelled[0]:
        run_phase("tmboot -y", "SUBIDA")

    # ── Footer final ──────────────────────────────────────────────────────────
    lines.append("")
    if cancelled[0]:
        phase_txt[0] = "CANCELADO"
        lines.append("  ✗ Reinicio cancelado.")
        footer = " ✗ Cancelado — Enter/q para volver"
        color  = C_ERROR
    else:
        phase_txt[0] = "COMPLETADO"
        lines.append("  ✓ Reinicio completado.")
        footer = " ✓ Reinicio completado — Enter/q para volver"
        color  = C_OK

    draw("")
    h, w = stdscr.getmaxyx()
    stdscr.attron(curses.color_pair(color) | curses.A_BOLD)
    try:
        stdscr.addstr(h - 1, 0, footer[:w - 1].ljust(w - 1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(color) | curses.A_BOLD)
    stdscr.refresh()

    while True:
        k = stdscr.getch()
        if k in (ord('q'), ord('Q'), 27, curses.KEY_ENTER, 10, 13):
            break


# ── Instalación Genesis-CPP ────────────────────────────────────────────────

def _scp_cmd(src_ip, src_user, src_port, src_path, dst_path, password):
    """SCP desde servidor remoto a ruta local."""
    return (
        ["sshpass", "-e", "scp",
         "-o", "StrictHostKeyChecking=no",
         "-o", "UserKnownHostsFile=/dev/null",
         "-o", "LogLevel=QUIET",
         "-P", str(src_port),
         f"{src_user}@{_clean_ip(src_ip)}:{src_path}",
         dst_path],
        _sshenv(password),
    )


def _scp_upload_cmd(local_path, dst_ip, dst_user, dst_port, dst_path, password):
    """SCP desde ruta local a servidor remoto."""
    return (
        ["sshpass", "-e", "scp",
         "-o", "StrictHostKeyChecking=no",
         "-o", "UserKnownHostsFile=/dev/null",
         "-o", "LogLevel=QUIET",
         "-P", str(dst_port),
         local_path,
         f"{dst_user}@{_clean_ip(dst_ip)}:{dst_path}"],
        _sshenv(password),
    )


def genesis_instalacion_run(stdscr, srv, ares_srv):
    """Instala nueva versión de Genesis-CPP desde ares al servidor prod indicado."""
    ip           = srv["ip"]
    user         = srv["ssh_user"]
    password     = srv["ssh_password"]
    port         = srv["ssh_port"]
    path         = srv["descripcion"].strip() if srv["descripcion"].strip().startswith("/") \
                   else "/home/sistemas/GENESIS_C/RUN"
    nombre       = srv["nombre"]

    ares_ip      = ares_srv["ip"]
    ares_user    = ares_srv["ssh_user"]
    ares_password= ares_srv["ssh_password"]
    ares_port    = ares_srv["ssh_port"]
    ares_path    = ares_srv["descripcion"].strip() if ares_srv["descripcion"].strip().startswith("/") \
                   else "/home/sistemas/GENESIS_C/RUN"

    lines     = deque(maxlen=500)
    phase_txt = ["—"]
    local_tmp = "/tmp/genesis_install"

    def draw(footer=""):
        h2, w2 = stdscr.getmaxyx()
        stdscr.erase()
        title = f" INSTALACIÓN GENESIS — {nombre}  [{user}@{ip}]  {path} "
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(0, 0, title[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(1, 0, f" Fase: {phase_txt[0]} "[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(2, 0, "─" * (w2-1), curses.color_pair(C_DIM))
        except curses.error: pass
        log_h   = h2 - 5
        visible = list(lines)[-(log_h):]
        for i, line in enumerate(visible):
            try: stdscr.addstr(3 + i, 0, line[:w2-1])
            except curses.error: pass
        stdscr.attron(curses.color_pair(C_STATUS))
        try: stdscr.addstr(h2-1, 0, footer[:w2-1].ljust(w2-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    def run_phase(cmd_str, phase_name, timeout=180):
        phase_txt[0] = phase_name
        lines.append("")
        lines.append(f"▶ {phase_name}")
        lines.append(f"  $ {cmd_str}")
        lines.append("")
        draw(f" {phase_name}...")

        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')

        proc = subprocess.Popen(ssh, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, bufsize=0, env=_sshenv(password))
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

        stdscr.nodelay(True)
        deadline = time.time() + timeout
        try:
            while proc.poll() is None:
                try:
                    raw = proc.stdout.read(4096)
                    if raw:
                        for ln in raw.decode("utf-8", errors="replace").splitlines():
                            if ln.strip():
                                lines.append(f"  {ln}")
                except (BlockingIOError, TypeError):
                    pass
                draw(f" {phase_name}...")
                if time.time() > deadline:
                    lines.append(f"  [!] Timeout ({timeout}s) — continuando...")
                    proc.kill()
                    break
                time.sleep(0.1)
            try:
                rest = proc.stdout.read()
                if rest:
                    for ln in rest.decode("utf-8", errors="replace").splitlines():
                        if ln.strip():
                            lines.append(f"  {ln}")
            except Exception:
                pass
        finally:
            try: proc.kill()
            except Exception: pass
            try: proc.wait(timeout=3)
            except Exception: pass
            stdscr.nodelay(False)

    def scp_phase(phase_name, cmd_list, env_dict, timeout=180):
        phase_txt[0] = phase_name
        lines.append("")
        lines.append(f"▶ {phase_name}")
        draw(f" {phase_name}...")
        try:
            r = subprocess.run(cmd_list, capture_output=True, text=True,
                               timeout=timeout, env=env_dict)
            out = (r.stdout + r.stderr).strip()
            for ln in out.splitlines():
                if ln.strip():
                    lines.append(f"  {ln}")
            if r.returncode == 0:
                lines.append(f"  ✓ {phase_name} OK")
            else:
                lines.append(f"  ✗ {phase_name} falló (rc={r.returncode})")
        except subprocess.TimeoutExpired:
            lines.append(f"  [!] Timeout ({timeout}s)")
        except Exception as ex:
            lines.append(f"  [!] Error: {ex}")
        draw("")

    # ── Paso 1: Kill Genesis-CPP ──────────────────────────────────────────
    phase_txt[0] = "KILL GENESIS-CPP"
    lines.append("▶ KILL GENESIS-CPP")
    draw(" Buscando procesos...")
    try:
        r = subprocess.run(
            ssh_cmd_base(ip, user, port) + ["ps -fea | grep 'Genesis-CPP' | grep -v grep"],
            capture_output=True, text=True, timeout=10, env=_sshenv(password),
        )
        proc_lines = [l for l in r.stdout.splitlines() if l.strip()]
        pids = []
        for l in proc_lines:
            parts = l.split()
            if len(parts) >= 2:
                try: pids.append(parts[1])
                except Exception: pass
            lines.append(f"  {l}")
        if pids:
            kill_cmd = f"kill -9 {' '.join(pids)}"
            lines.append(f"  $ {kill_cmd}")
            subprocess.run(ssh_cmd_base(ip, user, port) + [kill_cmd],
                           capture_output=True, text=True, timeout=10, env=_sshenv(password))
            lines.append(f"  ✓ PIDs eliminados: {' '.join(pids)}")
        else:
            lines.append("  (no había procesos corriendo)")
    except Exception as ex:
        lines.append(f"  [!] Error: {ex}")

    # ── Paso 2: tmshutdown ────────────────────────────────────────────────
    run_phase("tmshutdown -y", "TMSHUTDOWN", timeout=120)

    # ── Paso 3: Backup archivos actuales ──────────────────────────────────
    phase_txt[0] = "BACKUP"
    lines.append("")
    lines.append("▶ BACKUP de archivos actuales → INSTALACIONES/")
    draw(" Creando backup...")
    backup_cmd = (
        f'cd "{path}" && TODAY=$(date +%Y%m%d) '
        f'&& cp Genesis-CPP INSTALACIONES/Genesis-CPP.$TODAY '
        f'&& cp libIntWS.so INSTALACIONES/libIntWS.so.$TODAY '
        f'&& echo "✓ Backup: Genesis-CPP.$TODAY  libIntWS.so.$TODAY"'
    )
    try:
        r = subprocess.run(
            ssh_cmd_base(ip, user, port) + [backup_cmd],
            capture_output=True, text=True, timeout=20, env=_sshenv(password),
        )
        for ln in (r.stdout + r.stderr).splitlines():
            if ln.strip(): lines.append(f"  {ln}")
    except Exception as ex:
        lines.append(f"  [!] Error en backup: {ex}")

    # ── Paso 4: dmloadcf ─────────────────────────────────────────────────
    run_phase("dmloadcf -y DMGENESISC", "DMLOADCF", timeout=60)

    # ── Paso 5: tmboot ───────────────────────────────────────────────────
    run_phase("tmboot -y", "TMBOOT", timeout=180)

    # ── Paso 6+7: Copiar Genesis-CPP desde ares ──────────────────────────
    os.makedirs(local_tmp, exist_ok=True)

    cmd_dl, env_dl = _scp_cmd(ares_ip, ares_user, ares_port,
                               f"{ares_path}/Genesis-CPP",
                               f"{local_tmp}/Genesis-CPP", ares_password)
    scp_phase("DESCARGA Genesis-CPP (ares→local)", cmd_dl, env_dl, timeout=300)

    cmd_ul, env_ul = _scp_upload_cmd(f"{local_tmp}/Genesis-CPP",
                                      ip, user, port,
                                      f"{path}/Genesis-CPP", password)
    scp_phase(f"SUBIDA Genesis-CPP (local→{nombre})", cmd_ul, env_ul, timeout=300)

    # ── Paso 8+9: Copiar libIntWS.so desde ares ──────────────────────────
    cmd_dl2, env_dl2 = _scp_cmd(ares_ip, ares_user, ares_port,
                                  f"{ares_path}/libIntWS.so",
                                  f"{local_tmp}/libIntWS.so", ares_password)
    scp_phase("DESCARGA libIntWS.so (ares→local)", cmd_dl2, env_dl2, timeout=120)

    cmd_ul2, env_ul2 = _scp_upload_cmd(f"{local_tmp}/libIntWS.so",
                                         ip, user, port,
                                         f"{path}/libIntWS.so", password)
    scp_phase(f"SUBIDA libIntWS.so (local→{nombre})", cmd_ul2, env_ul2, timeout=120)

    # ── Paso 10: Permisos ─────────────────────────────────────────────────
    run_phase("chmod +x Genesis-CPP libIntWS.so", "PERMISOS", timeout=15)

    # ── Paso 11: Iniciar Genesis-CPP ─────────────────────────────────────
    phase_txt[0] = "INICIO GENESIS-CPP"
    lines.append("")
    lines.append("▶ INICIO GENESIS-CPP")
    draw(" Iniciando Genesis-CPP...")
    try:
        r = subprocess.run(
            ssh_cmd_base(ip, user, port) + [
                f'cd "{path}" && . ./env.pro && nohup ./Genesis-CPP > /dev/null 2>&1 & disown; sleep 1; echo "PID: $!"'
            ],
            capture_output=True, text=True, timeout=15, env=_sshenv(password),
        )
        for ln in (r.stdout + r.stderr).splitlines():
            if ln.strip(): lines.append(f"  {ln}")
        lines.append("  ✓ Genesis-CPP iniciado en background")
    except Exception as ex:
        lines.append(f"  [!] Error iniciando: {ex}")

    # ── Footer final ──────────────────────────────────────────────────────
    phase_txt[0] = "COMPLETADO"
    lines.append("")
    lines.append("  ✓ Instalación completada.")
    h2, w2 = stdscr.getmaxyx()
    draw("")
    stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
    try:
        stdscr.addstr(h2-1, 0,
                      " ✓ Instalación completada — Enter/q para volver"[:w2-1].ljust(w2-1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
    stdscr.refresh()

    while True:
        k = stdscr.getch()
        if k in (ord('q'), ord('Q'), 27, curses.KEY_ENTER, 10, 13):
            break


# ── Reinicio Genesis-CPP ───────────────────────────────────────────────────

def genesis_reinicio_run(stdscr, srv):
    """Ejecuta el ciclo completo de reinicio de Genesis-CPP en el servidor indicado."""
    ip       = srv["ip"]
    user     = srv["ssh_user"]
    password = srv["ssh_password"]
    port     = srv["ssh_port"]
    path     = srv["descripcion"].strip() if srv["descripcion"].strip().startswith("/") \
               else "/home/sistemas/GENESIS_C/RUN"
    nombre   = srv["nombre"]

    lines     = deque(maxlen=500)
    phase_txt = ["—"]

    def draw(footer=""):
        h, w = stdscr.getmaxyx()
        stdscr.erase()
        title = f" REINICIO GENESIS — {nombre}  [{user}@{ip}]  {path} "
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        try: stdscr.addstr(0, 0, title[:w-1].ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(1, 0, f" Fase: {phase_txt[0]} "[:w-1].ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)
        try: stdscr.addstr(2, 0, "─" * (w-1), curses.color_pair(C_DIM))
        except curses.error: pass
        log_h   = h - 5
        visible = list(lines)[-(log_h):]
        for i, line in enumerate(visible):
            try: stdscr.addstr(3 + i, 0, line[:w-1])
            except curses.error: pass
        stdscr.attron(curses.color_pair(C_STATUS))
        try: stdscr.addstr(h-1, 0, footer[:w-1].ljust(w-1))
        except curses.error: pass
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

    def run_phase(cmd_str, phase_name, timeout=120):
        phase_txt[0] = phase_name
        lines.append("")
        lines.append(f"▶ {phase_name}")
        lines.append(f"  $ {cmd_str}")
        lines.append("")
        draw(f" {phase_name}...")

        ssh = ssh_cmd_base(ip, user, port)
        ssh.insert(3, "-tt")
        ssh.append(f'cd "{path}" && . ./env.pro && {cmd_str}')

        proc = subprocess.Popen(
            ssh,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            env=_sshenv(password),
        )
        fl = fcntl.fcntl(proc.stdout.fileno(), fcntl.F_GETFL)
        fcntl.fcntl(proc.stdout.fileno(), fcntl.F_SETFL, fl | os.O_NONBLOCK)

        stdscr.nodelay(True)
        deadline = time.time() + timeout
        try:
            while proc.poll() is None:
                try:
                    raw = proc.stdout.read(4096)
                    if raw:
                        for ln in raw.decode("utf-8", errors="replace").splitlines():
                            if ln.strip():
                                lines.append(f"  {ln}")
                except (BlockingIOError, TypeError):
                    pass
                draw(f" {phase_name}...")
                if time.time() > deadline:
                    lines.append(f"  [!] Timeout ({timeout}s) — continuando...")
                    proc.kill()
                    break
                time.sleep(0.1)
            try:
                rest = proc.stdout.read()
                if rest:
                    for ln in rest.decode("utf-8", errors="replace").splitlines():
                        if ln.strip():
                            lines.append(f"  {ln}")
            except Exception:
                pass
        finally:
            try: proc.kill()
            except Exception: pass
            try: proc.wait(timeout=3)
            except Exception: pass
            stdscr.nodelay(False)

    # ── Paso 1: Kill Genesis-CPP ───────────────────────────────────────────
    phase_txt[0] = "KILL GENESIS-CPP"
    lines.append("▶ KILL GENESIS-CPP")
    lines.append("  $ ps -fea | grep Genesis-CPP | grep -v grep")
    draw(" Buscando procesos Genesis-CPP...")

    ssh_cmd = ssh_cmd_base(ip, user, port) + \
              ["ps -fea | grep 'Genesis-CPP' | grep -v grep"]
    try:
        r = subprocess.run(ssh_cmd, capture_output=True, text=True,
                           timeout=10, env=_sshenv(password))
        proc_lines = [l for l in r.stdout.splitlines() if l.strip()]
        pids = []
        for l in proc_lines:
            parts = l.split()
            if len(parts) >= 2:
                try: pids.append(parts[1])
                except Exception: pass
            lines.append(f"  {l}")

        if pids:
            kill_cmd = f"kill -9 {' '.join(pids)}"
            lines.append(f"")
            lines.append(f"  $ {kill_cmd}")
            draw(" Matando procesos...")
            r2 = subprocess.run(
                ssh_cmd_base(ip, user, port) + [kill_cmd],
                capture_output=True, text=True, timeout=10, env=_sshenv(password),
            )
            lines.append(f"  PIDs eliminados: {' '.join(pids)}")
        else:
            lines.append("  (no había procesos Genesis-CPP corriendo)")
    except Exception as ex:
        lines.append(f"  [!] Error: {ex}")

    # ── Paso 2: tmshutdown ────────────────────────────────────────────────
    run_phase("tmshutdown -y", "TMSHUTDOWN", timeout=120)

    # ── Paso 3: dmloadcf ──────────────────────────────────────────────────
    run_phase("dmloadcf -y DMGENESISC", "DMLOADCF", timeout=60)

    # ── Paso 4: tmboot ────────────────────────────────────────────────────
    run_phase("tmboot -y", "TMBOOT", timeout=180)

    # ── Paso 5: Iniciar Genesis-CPP en background ─────────────────────────
    phase_txt[0] = "INICIO GENESIS-CPP"
    lines.append("")
    lines.append("▶ INICIO GENESIS-CPP")
    lines.append("  $ nohup ./Genesis-CPP > /dev/null 2>&1 &")
    draw(" Iniciando Genesis-CPP...")
    try:
        r = subprocess.run(
            ssh_cmd_base(ip, user, port) + \
            [f'cd "{path}" && . ./env.pro && nohup ./Genesis-CPP > /dev/null 2>&1 & disown; sleep 1; echo "PID: $!"'],
            capture_output=True, text=True, timeout=15, env=_sshenv(password),
        )
        out = (r.stdout + r.stderr).strip()
        for ln in out.splitlines():
            if ln.strip():
                lines.append(f"  {ln}")
        lines.append("  ✓ Genesis-CPP iniciado en background")
    except Exception as ex:
        lines.append(f"  [!] Error iniciando: {ex}")

    # ── Footer final ──────────────────────────────────────────────────────
    phase_txt[0] = "COMPLETADO"
    lines.append("")
    lines.append("  ✓ Reinicio completado.")
    draw("")
    h, w = stdscr.getmaxyx()
    stdscr.attron(curses.color_pair(C_OK) | curses.A_BOLD)
    try:
        stdscr.addstr(h-1, 0, " ✓ Reinicio completado — Enter/q para volver"[:w-1].ljust(w-1))
    except curses.error:
        pass
    stdscr.attroff(curses.color_pair(C_OK) | curses.A_BOLD)
    stdscr.refresh()

    while True:
        k = stdscr.getch()
        if k in (ord('q'), ord('Q'), 27, curses.KEY_ENTER, 10, 13):
            break


# ── Loop principal ─────────────────────────────────────────────────────────

SISTEMAS = [
    ("1", "COBOL",       "cobol"),
    ("2", "GENESIS",     "genesis"),
    ("3", "INTERMEDIOS", "intermedios"),
]


def _draw_main_menu(stdscr, selected):
    h, w = stdscr.getmaxyx()
    stdscr.erase()

    title = f" {APP_NAME.upper()} v{APP_VERSION} · {APP_CREDIT} "
    stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
    stdscr.addstr(0, 0, " " * (w - 1))
    stdscr.addstr(0, max(0, (w - len(title)) // 2), title)
    stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

    label = "Selecciona el sistema:"
    stdscr.addstr(h // 2 - len(SISTEMAS) - 1, max(0, (w - len(label)) // 2),
                  label, curses.A_BOLD)

    for i, (num, nombre, _) in enumerate(SISTEMAS):
        y    = h // 2 - len(SISTEMAS) // 2 + i
        text = f"  [{num}]  {nombre:<15}"
        x    = max(0, (w - len(text)) // 2)
        if i == selected:
            stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
            stdscr.addstr(y, x, text)
            stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.addstr(y, x, text)

    footer = " ↑↓=Navegar  Enter=Seleccionar  1-3=Acceso directo  q=Salir "
    stdscr.attron(curses.color_pair(C_STATUS))
    stdscr.addstr(h - 1, 0, footer.ljust(w - 1))
    stdscr.attroff(curses.color_pair(C_STATUS))
    stdscr.refresh()


GENESIS_TABS = [
    ("1", "Estado",      "estado"),
    ("2", "Reinicio",    "reinicio"),
    ("3", "Instalacion", "instalacion"),
    ("4", "Servidores",  "servidores"),
]


def _genesis_check_status(srv):
    """Consulta si Genesis-CPP está corriendo en el servidor. Devuelve (estado, detalle)."""
    try:
        cmd = ssh_cmd_base(srv["ip"], srv["ssh_user"], srv["ssh_port"]) + \
              ["ps -fea | grep 'Genesis-CPP' | grep -v grep"]
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=10, env=_sshenv(srv["ssh_password"]))
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        if not lines:
            return "down", "INACTIVO"
        # Tomar la línea del proceso hijo (mayor PID o la que tiene CPU time > 0)
        procs = []
        for l in lines:
            parts = l.split()
            if len(parts) >= 8:
                procs.append(parts)
        if procs:
            p = max(procs, key=lambda x: x[6])  # mayor tiempo de CPU
            pid     = p[1]
            cputime = p[6]
            return "up", f"ACTIVO  PID: {pid}  CPU: {cputime}"
        return "up", "ACTIVO"
    except subprocess.TimeoutExpired:
        return "err", "Timeout"
    except Exception as ex:
        return "err", str(ex)[:40]


def _genesis_draw_header(stdscr, mode):
    h, w = stdscr.getmaxyx()
    title = f" {APP_NAME.upper()} · GENESIS "
    stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
    stdscr.addstr(0, 0, " " * (w - 1))
    stdscr.addstr(0, max(0, (w - len(title)) // 2), title)
    stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)

    # Dibujar fondo de la fila de tabs
    stdscr.attron(curses.color_pair(C_TITLE))
    stdscr.addstr(2, 0, " " * (w - 1))
    stdscr.attroff(curses.color_pair(C_TITLE))

    # Dibujar cada tab individualmente con su color
    x = 1
    for n, label, m in GENESIS_TABS:
        seg = f"[{n}] {label}"
        if m == mode:
            stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.attron(curses.color_pair(C_TITLE))
        try:
            stdscr.addstr(2, x, seg)
        except curses.error:
            pass
        if m == mode:
            stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
        else:
            stdscr.attroff(curses.color_pair(C_TITLE))
        x += len(seg) + 2


def genesis_main(stdscr):
    mode           = "estado"
    search         = ""
    searching      = False
    selected       = 0
    offset         = 0
    status         = ""
    servers        = []          # lista de maquinas genesis
    statuses       = {}          # {nombre: (estado, detalle)}
    needs_reload   = True
    last_check     = 0.0         # timestamp del último chequeo de estado
    check_interval = 5           # segundos entre chequeos automáticos
    checking       = False       # True mientras hay un thread corriendo

    while True:
        if needs_reload:
            try:
                servers = fetch_maquinas("", sistema="genesis")
                selected = min(selected, max(0, len(servers) - 1))
                if mode == "estado":
                    statuses = {s["nombre"]: ("?", "Sin consultar — F5 para verificar")
                                for s in servers}
            except Exception as e:
                status = f"ERROR BD: {e}"
                servers = []
            needs_reload = False

        # ── Auto-chequeo de estado (solo en tab Estado) ────────────────────
        if mode == "estado" and servers and not checking:
            if time.time() - last_check >= check_interval:
                checking = True
                for s in servers:
                    if s["nombre"] not in statuses:
                        statuses[s["nombre"]] = ("?", "Verificando...")

                def _run_checks(srv_list):
                    nonlocal checking, statuses, last_check
                    results = {}
                    threads = []
                    def _check(s):
                        results[s["nombre"]] = _genesis_check_status(s)
                    for s in srv_list:
                        t = threading.Thread(target=_check, args=(s,), daemon=True)
                        threads.append(t)
                        t.start()
                    for t in threads:
                        t.join(timeout=12)
                    statuses = {s["nombre"]: results.get(s["nombre"], ("err", "Sin respuesta"))
                                for s in srv_list}
                    last_check = time.time()
                    checking   = False

                threading.Thread(target=_run_checks, args=(list(servers),), daemon=True).start()

        h, w   = stdscr.getmaxyx()
        list_h = h - 7

        stdscr.erase()
        _genesis_draw_header(stdscr, mode)

        # ── Render por tab ─────────────────────────────────────────────────
        if mode == "estado":
            stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
            stdscr.addstr(4, 0,
                f"{'SERVIDOR':<20}  {'IP':<16}  {'ESTADO':<8}  DETALLE"[:w-1].ljust(w-1))
            stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

            rows = servers
            if selected < offset:           offset = selected
            elif selected >= offset+list_h: offset = selected - list_h + 1

            for i, srv in enumerate(rows[offset: offset + list_h]):
                idx   = offset + i
                y     = 5 + i
                st, det = statuses.get(srv["nombre"], ("?", ""))
                if st == "up":
                    mark = "✓"; attr = curses.color_pair(C_OK) | curses.A_BOLD
                elif st == "down":
                    mark = "✗"; attr = curses.color_pair(C_ERROR) | curses.A_BOLD
                elif st == "err":
                    mark = "!"; attr = curses.color_pair(C_WARN) | curses.A_BOLD
                else:
                    mark = "?"; attr = curses.color_pair(C_DIM)
                line = f"{srv['nombre']:<20}  {srv['ip']:<16}  {mark}       {det}"
                sel_attr = (curses.color_pair(C_SELECTED)|curses.A_BOLD) if idx==selected else attr
                try:
                    stdscr.attron(sel_attr)
                    stdscr.addstr(y, 0, line[:w-1].ljust(w-1) if idx==selected else line[:w-1])
                    stdscr.attroff(sel_attr)
                except curses.error:
                    pass

            chk_txt = " actualizando..." if checking else \
                      (f" actualizado {time.strftime('%H:%M:%S', time.localtime(last_check))}" if last_check else "")
            footer = f" Enter=SSH  ↑↓=Navegar  1-4=Tab  q=Menú  |{chk_txt}  {status} "

        elif mode == "reinicio":
            stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
            stdscr.addstr(4, 0,
                f"{'SERVIDOR':<20}  {'IP':<16}  {'DESCRIPCIÓN'}"[:w-1].ljust(w-1))
            stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

            rows = servers
            if selected < offset:           offset = selected
            elif selected >= offset+list_h: offset = selected - list_h + 1

            for i, srv in enumerate(rows[offset: offset + list_h]):
                idx  = offset + i
                y    = 5 + i
                line = f"{srv['nombre']:<20}  {srv['ip']:<16}  {srv['descripcion']}"
                if idx == selected:
                    stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                    stdscr.addstr(y, 0, line[:w-1].ljust(w-1))
                    stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                else:
                    try: stdscr.addstr(y, 0, line[:w-1])
                    except curses.error: pass

            footer = f" Enter=Reiniciar Genesis-CPP  ↑↓=Navegar  1-4=Tab  q=Menú   {status} "

        elif mode == "instalacion":
            # Separar ares (fuente) de servidores prod
            ares_srv   = next((s for s in servers if s["nombre"].lower() == "ares"), None)
            prod_srvs  = [s for s in servers if s["nombre"].lower() != "ares"]

            # Cabecera info
            if ares_srv:
                src_txt = f"  Fuente: {ares_srv['nombre']} ({ares_srv['ip']})  →  " \
                          f"{len(prod_srvs)} servidor(es) prod"
            else:
                src_txt = "  [!] Servidor 'ares' no encontrado — agrégalo en [4] Servidores"
            stdscr.attron(curses.color_pair(C_DIM))
            try: stdscr.addstr(3, 0, src_txt[:w-1])
            except curses.error: pass
            stdscr.attroff(curses.color_pair(C_DIM))

            stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
            try:
                stdscr.addstr(4, 0,
                    f"{'SERVIDOR PROD':<20}  {'IP':<16}  {'RUN PATH'}"[:w-1].ljust(w-1))
            except curses.error: pass
            stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

            inst_selected = min(selected, max(0, len(prod_srvs) - 1))
            if inst_selected < offset:            offset = inst_selected
            elif inst_selected >= offset+list_h:  offset = inst_selected - list_h + 1

            for i, s in enumerate(prod_srvs[offset: offset + list_h]):
                idx  = offset + i
                y    = 5 + i
                line = f"{s['nombre']:<20}  {s['ip']:<16}  {s['descripcion']}"
                if idx == inst_selected:
                    stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                    try: stdscr.addstr(y, 0, line[:w-1].ljust(w-1))
                    except curses.error: pass
                    stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                else:
                    try: stdscr.addstr(y, 0, line[:w-1])
                    except curses.error: pass

            footer = f" Enter=Instalar en servidor  ↑↓=Navegar  1-4=Tab  q=Menú   {status} "

        else:  # servidores
            search_line = f" Buscar: {search}_" if searching else f" Buscar: {search}"
            stdscr.attron(curses.color_pair(C_SEARCH) if searching else curses.color_pair(C_DIM))
            stdscr.addstr(1, 0, search_line[:w-1].ljust(w-1))
            stdscr.attroff(curses.color_pair(C_SEARCH) if searching else curses.color_pair(C_DIM))

            try:
                rows = fetch_maquinas(search, sistema="genesis")
            except Exception:
                rows = servers

            stdscr.attron(curses.color_pair(C_TITLE) | curses.A_BOLD)
            stdscr.addstr(4, 0,
                f"{'SERVIDOR':<20}  {'IP':<16}  {'USUARIO':<12}  {'PUERTO'}  {'DESCRIPCIÓN'}"[:w-1].ljust(w-1))
            stdscr.attroff(curses.color_pair(C_TITLE) | curses.A_BOLD)

            if selected < offset:           offset = selected
            elif selected >= offset+list_h: offset = selected - list_h + 1

            for i, row in enumerate(rows[offset: offset + list_h]):
                idx  = offset + i
                y    = 5 + i
                line = (f"{row['nombre']:<20}  {row['ip']:<16}  "
                        f"{row['ssh_user']:<12}  {str(row['ssh_port']):<6}  {row['descripcion']}")
                if idx == selected:
                    stdscr.attron(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                    stdscr.addstr(y, 0, line[:w-1].ljust(w-1))
                    stdscr.attroff(curses.color_pair(C_SELECTED) | curses.A_BOLD)
                else:
                    try: stdscr.addstr(y, 0, line[:w-1])
                    except curses.error: pass

            footer = f" Enter=SSH  F4=Editar  F2=Nuevo  Supr=Eliminar  1-4=Tab  q=Menú   {status} "

        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h-1, 0, footer[:w-1].ljust(w-1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()

        try:
            key = read_key(stdscr)
        except curses.error:
            continue
        if key == -1:
            continue

        status = ""

        # ── Navegación global ──────────────────────────────────────────────
        if key in (ord('q'), ord('Q')) and not searching:
            break
        elif key in (ord('1'),ord('2'),ord('3'),ord('4')) and not searching:
            nuevo = GENESIS_TABS[int(chr(key))-1][2]
            if nuevo != mode:
                mode = nuevo; selected = 0; offset = 0; search = ""; searching = False
                needs_reload = True
                if mode == "estado":
                    last_check = 0.0  # chequear inmediatamente al entrar
            continue
        elif key == 9 and not searching:  # TAB
            modes = [t[2] for t in GENESIS_TABS]
            mode  = modes[(modes.index(mode)+1) % len(modes)]
            selected = 0; offset = 0; needs_reload = True
            if mode == "estado":
                last_check = 0.0
            continue
        elif key == curses.KEY_UP:
            selected = max(0, selected-1)
        elif key == curses.KEY_DOWN:
            rows_len = len(servers) if mode != "servidores" else len(rows) if 'rows' in dir() else 0
            selected = min(max(0, rows_len-1), selected+1)

        # ── Acciones por tab ───────────────────────────────────────────────
        elif mode == "estado":
            if key in (curses.KEY_ENTER, 10, 13) and servers:
                srv = servers[selected]
                if not srv["ssh_password"]:
                    srv["ssh_password"] = ask_input(stdscr, f"Contraseña para {srv['ssh_user']}@{srv['ip']}: ")
                if srv["ssh_password"]:
                    ssh_connect(srv["ip"], srv["ssh_user"], srv["ssh_password"], srv["ssh_port"])
                    stdscr = curses.initscr()
                    init_colors(); curses.curs_set(0)
                    stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "reinicio":
            if key in (curses.KEY_ENTER, 10, 13) and servers:
                srv = servers[selected]
                when = deploy_when_picker(stdscr, f"GENESIS REINICIO — {srv['nombre']}")
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                if when == "now":
                    if confirm_dialog(stdscr, f"¿Reiniciar Genesis-CPP en '{srv['nombre']}'?"):
                        genesis_reinicio_run(stdscr, srv)
                        status = f"✓ Reinicio ejecutado en {srv['nombre']}"
                    init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                elif when == "schedule":
                    fh = ask_datetime(stdscr)
                    init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                    if fh:
                        db_insert_deploy_programado(
                            usuario, 0, srv["nombre"],
                            ["__genesis_reinicio__"], fh,
                        )
                        notify_scheduler()
                        status = f"✓ Reinicio programado: {fh.strftime('%d/%m/%Y %H:%M')}"

        elif mode == "instalacion":
            prod_srvs_key = [s for s in servers if s["nombre"].lower() != "ares"]
            ares_srv_key  = next((s for s in servers if s["nombre"].lower() == "ares"), None)
            if key in (curses.KEY_ENTER, 10, 13) and prod_srvs_key:
                inst_sel = min(selected, max(0, len(prod_srvs_key) - 1))
                srv = prod_srvs_key[inst_sel]
                if not ares_srv_key:
                    _show_message(stdscr,
                                  "Servidor 'ares' no encontrado. Agrégalo en [4] Servidores.",
                                  error=True)
                elif confirm_dialog(stdscr,
                                    f"¿Instalar Genesis-CPP en '{srv['nombre']}' desde ares?"):
                    genesis_instalacion_run(stdscr, srv, ares_srv_key)
                    status = f"✓ Instalación completada en {srv['nombre']}"
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "servidores":
            if key == ord('/') and not searching:
                searching = True
            elif key in (27, curses.KEY_F5):
                search = ""; searching = False; selected = 0; offset = 0; needs_reload = True
            elif key in (curses.KEY_BACKSPACE, 127, 8):
                if search:
                    search = search[:-1]
                    if not search: searching = False
                    selected = 0; offset = 0; needs_reload = True
            elif key in (curses.KEY_ENTER, 10, 13) and rows:
                row = rows[selected]
                if not row["ssh_password"]:
                    row["ssh_password"] = ask_input(stdscr, f"Contraseña para {row['ssh_user']}@{row['ip']}: ")
                if row["ssh_password"]:
                    ssh_connect(row["ip"], row["ssh_user"], row["ssh_password"], row["ssh_port"])
                    stdscr = curses.initscr()
                    init_colors(); curses.curs_set(0)
                    stdscr.keypad(True); stdscr.timeout(100)
                    needs_reload = True
            elif key == curses.KEY_F4 and rows:
                data = maquina_form(stdscr, rows[selected])
                if data:
                    try:
                        db_update_maquina(rows[selected]["nombre"], data)
                        status = f"✓ {rows[selected]['nombre']} actualizado"
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
            elif key == curses.KEY_F2:
                data = maquina_form(stdscr, None)
                if data:
                    try:
                        db_insert_maquina(data, sistema="genesis")
                        status = f"✓ {data['nombre']} creado"
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
            elif key == curses.KEY_DC and rows:
                row = rows[selected]
                if confirm_dialog(stdscr, f"¿Eliminar servidor '{row['nombre']}'?"):
                    try:
                        db_delete_maquina(row["nombre"])
                        status = f"✓ {row['nombre']} eliminado"
                        selected = max(0, selected-1)
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                stdscr.touchwin(); stdscr.refresh()
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
            elif 32 <= key <= 126:
                search += chr(key)
                searching = True; selected = 0; offset = 0; needs_reload = True


def intermedios_main(stdscr):
    h, w = stdscr.getmaxyx()
    while True:
        stdscr.erase()
        stdscr.attron(curses.color_pair(C_HEADER) | curses.A_BOLD)
        stdscr.addstr(0, 0, " INTERMEDIOS ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_HEADER) | curses.A_BOLD)
        msg = "Próximamente..."
        stdscr.addstr(h // 2, max(0, (w - len(msg)) // 2), msg, curses.A_BOLD)
        stdscr.attron(curses.color_pair(C_STATUS))
        stdscr.addstr(h - 1, 0, " q=Volver al menú ".ljust(w - 1))
        stdscr.attroff(curses.color_pair(C_STATUS))
        stdscr.refresh()
        k = stdscr.getch()
        if k in (ord('q'), ord('Q'), 27):
            return


def cobol_main(stdscr, usuario):
    mode         = "clientes"
    search       = ""
    searching    = False
    selected     = 0
    offset       = 0
    status       = ""
    rows         = []
    needs_reload = True

    while True:
        if needs_reload:
            try:
                if mode == "maquinas":
                    rows = fetch_maquinas(search, sistema="cobol")
                elif mode == "programados":
                    rows = db_fetch_deploys_usuario(usuario)
                elif mode == "batch":
                    rows = []
                else:
                    rows = fetch_clientes(search)
                selected = min(selected, max(0, len(rows) - 1))
            except Exception as e:
                status = f"ERROR BD: {e}"
                rows = []
            needs_reload = False

        h, w   = stdscr.getmaxyx()
        list_h = h - 6

        if selected < offset:
            offset = selected
        elif selected >= offset + list_h:
            offset = selected - list_h + 1

        stdscr.erase()
        draw_header(stdscr, mode, search, searching)
        if mode == "programados":
            draw_programados(stdscr, rows, selected, offset)
        elif mode == "batch":
            draw_batch_tab(stdscr)
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
            key = read_key(stdscr)
        except curses.error:
            continue

        if key == -1:
            continue

        status = ""

        # ── Salir ──────────────────────────────────────────────────────────
        if key in (ord('q'), ord('Q')) and not searching:
            break

        # ── Cambiar tab ────────────────────────────────────────────────────
        elif key in (ord('1'), ord('2'), ord('3'), ord('4'), ord('5'), ord('6'), ord('7')) and not searching:
            idx  = int(chr(key)) - 1
            mode = TABS[idx][2]
            search = ""; searching = False; selected = 0; offset = 0
            needs_reload = True

        elif key == 9 and not searching:   # TAB
            modes = [t[2] for t in TABS]
            mode  = modes[(modes.index(mode) + 1) % len(modes)]
            search = ""; searching = False; selected = 0; offset = 0
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

        # ── Activar modo búsqueda ──────────────────────────────────────────
        elif key == ord('/') and mode not in ("programados", "batch"):
            searching = True

        # ── Limpiar búsqueda ───────────────────────────────────────────────
        elif key in (27, curses.KEY_F5):
            search = ""; searching = False; selected = 0; offset = 0
            needs_reload = True
        elif key in (curses.KEY_BACKSPACE, 127, 8):
            if search:
                search = search[:-1]
                if not search:
                    searching = False
                selected = 0; offset = 0
                needs_reload = True

        # ── Acción con Enter ───────────────────────────────────────────────
        elif key in (curses.KEY_ENTER, 10, 13):
            if mode == "batch":
                texto = multiline_input(
                    stdscr,
                    title="BATCH DEPLOY — Pega la lista  PREFIX/archivo.cbl",
                    hint="Un archivo por línea  |  F10 o Ctrl+D = Iniciar  |  ESC = Cancelar",
                )
                if texto:
                    _run_batch_deploy(stdscr, texto, usuario)
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True
                continue

            if not rows:
                status = "No hay resultados"
                continue

            if mode == "programados":
                if rows:
                    _show_programado_detalle(stdscr, rows[selected])
                    init_colors(); stdscr.keypad(True); stdscr.timeout(100)
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

            elif mode == "deploy":
                if not row.get("path_hades"):
                    status = f"Sin path_hades para {row['desc_cliente']}"
                    continue
                if not row.get("path"):
                    status = f"Sin path producción para {row['desc_cliente']}"
                    continue
                cbl = cbl_picker(stdscr, row)
                if cbl:
                    # ── Detectar vistas y pedir opciones antes de elegir cuándo ──
                    _libpath_d  = row.get("libpath", "").strip()
                    _ph_d       = _resolve_hades_path(row.get("path_hades", ""))
                    _henv_d     = _sshenv(HADES.get("password", ""))
                    _iniciales_d = row.get("iniciales", "").strip()
                    _dm_def_d   = f"DM{_iniciales_d}" if _iniciales_d else "DM"
                    _views_d    = []
                    if _ph_d:
                        try:
                            _gr_d = subprocess.run(
                                hades_cmd_base() + [
                                    f"grep -Hi 'copy.*\\.var' \"{_ph_d}/{cbl}\" 2>/dev/null || true"
                                ],
                                capture_output=True, text=True, timeout=15, env=_henv_d,
                            )
                            _views_d = detect_copy_views(_gr_d.stdout, _ph_d)
                        except Exception:
                            _views_d = []
                    _carry_d, _dm_d, _hilos_d = _deploy_options_dialog(stdscr, _views_d, _dm_def_d, cbl)
                    init_colors(); stdscr.keypad(True)

                    when = deploy_when_picker(stdscr, f"DEPLOY — {row['desc_cliente']} — {cbl}")
                    if when == "now":
                        if _carry_d:
                            if not _libpath_d:
                                _show_message(stdscr,
                                    "Sin libpath configurado — no se pueden copiar vistas", error=True)
                            else:
                                _all_maqs_d = fetch_maquinas(sistema="cobol")
                                _sel_maqs_d = multiselect_maquinas_dialog(
                                    stdscr, _all_maqs_d, title="Servidores destino para vistas")
                                init_colors(); stdscr.keypad(True)
                                if _sel_maqs_d:
                                    deploy_view_files(stdscr, _carry_d, _ph_d, _libpath_d,
                                                      _sel_maqs_d, _henv_d)
                            init_colors(); stdscr.keypad(True)
                        run_deploy(stdscr, row, cbl, pre_options=([], _dm_d, _hilos_d))
                    elif when == "schedule":
                        fh = ask_datetime(stdscr)
                        if fh:
                            svcs_d = [cbl]
                            if _dm_d:
                                svcs_d.append(f"__dm__:{_dm_d}")
                            if _hilos_d:
                                svcs_d.append("__hilos__")
                            db_insert_deploy_programado(
                                usuario, row["nro_cliente"], row["desc_cliente"],
                                svcs_d, fh,
                            )
                            notify_scheduler()
                            sufijo_d = f" + DM:{_dm_d}" if _dm_d else ""
                            if _hilos_d:
                                sufijo_d += " +hilos"
                            status = f"✓ Deploy de {cbl}{sufijo_d} programado para {fh.strftime('%d/%m/%Y %H:%M')}"
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "multideploy":
                if not row.get("path_hades"):
                    status = f"Sin path_hades para {row['desc_cliente']}"
                    continue
                if not row.get("path"):
                    status = f"Sin path producción para {row['desc_cliente']}"
                    continue
                cbl_list = multicbl_picker(stdscr, row)
                if cbl_list:
                    # ── Detectar vistas y pedir opciones antes de elegir cuándo ──
                    _libpath   = row.get("libpath", "").strip()
                    _ph        = _resolve_hades_path(row.get("path_hades", ""))
                    _henv      = _sshenv(HADES.get("password", ""))
                    _iniciales = row.get("iniciales", "").strip()
                    _dm_def    = f"DM{_iniciales}" if _iniciales else "DM"
                    _views     = []
                    if _ph:
                        try:
                            _files_arg = " ".join(f'"{_ph}/{f}"' for f in cbl_list)
                            _gr = subprocess.run(
                                hades_cmd_base() + [
                                    f"grep -Hi 'copy.*\\.var' {_files_arg} 2>/dev/null || true"
                                ],
                                capture_output=True, text=True, timeout=15, env=_henv,
                            )
                            _views = detect_copy_views(_gr.stdout, _ph)
                        except Exception:
                            _views = []
                    _carry_views, _dm_name, _hilos_m = _deploy_options_dialog(
                        stdscr, _views, _dm_def, f"{len(cbl_list)} archivos",
                    )
                    init_colors(); stdscr.keypad(True)

                    when = deploy_when_picker(stdscr, f"MULTI-DEPLOY — {row['desc_cliente']}")
                    if when == "now":
                        if _carry_views:
                            if not _libpath:
                                _show_message(stdscr,
                                    "Sin libpath configurado — no se pueden copiar vistas", error=True)
                            else:
                                _all_maqs = fetch_maquinas(sistema="cobol")
                                _sel_maqs = multiselect_maquinas_dialog(
                                    stdscr, _all_maqs, title="Servidores destino para vistas")
                                init_colors(); stdscr.keypad(True)
                                if _sel_maqs:
                                    deploy_view_files(stdscr, _carry_views, _ph, _libpath,
                                                      _sel_maqs, _henv)
                            init_colors(); stdscr.keypad(True)
                        run_multi_deploy(stdscr, row, cbl_list, use_hilos=_hilos_m)
                        if _dm_name and row.get("path"):
                            _ssh_env = _sshenv(row.get("ssh_password", ""))
                            _run_dm_load(stdscr, row["ip"], row["ssh_user"],
                                         row["ssh_password"], row["ssh_port"],
                                         row["path"], _dm_name, _ssh_env)
                        init_colors(); stdscr.keypad(True)
                    elif when == "schedule":
                        fh = ask_datetime(stdscr)
                        if fh:
                            svcs_m = cbl_list + ([f"__dm__:{_dm_name}"] if _dm_name else [])
                            if _hilos_m:
                                svcs_m.append("__hilos__")
                            db_insert_deploy_programado(
                                usuario, row["nro_cliente"], row["desc_cliente"],
                                svcs_m, fh,
                            )
                            notify_scheduler()
                            sufijo_m = f" + DM:{_dm_name}" if _dm_name else ""
                            if _hilos_m:
                                sufijo_m += " +hilos"
                            status = f"✓ {len(cbl_list)} servicio(s){sufijo_m} programados para {fh.strftime('%d/%m/%Y %H:%M')}"
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

            elif mode == "reinicio":
                if not row.get("path"):
                    status = f"Sin path para {row['desc_cliente']}"
                    continue
                _cancelled, _ubb, _dm = _reinicio_options_dialog(stdscr, row)
                init_colors(); stdscr.keypad(True)
                if not _cancelled:
                    reinicio_tuxedo(stdscr, row, usuario,
                                    ubb_name=_ubb, dm_name=_dm)
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)
                needs_reload = True

        # ── Acciones CRUD Servidores ───────────────────────────────────────
        elif mode == "maquinas" and key == curses.KEY_F4:
            if rows:
                data = maquina_form(stdscr, rows[selected])
                if data:
                    try:
                        db_update_maquina(rows[selected]["nombre"], data)
                        status = f"✓ {rows[selected]['nombre']} actualizado"
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "maquinas" and key == curses.KEY_F2:
            data = maquina_form(stdscr, None)
            if data:
                try:
                    db_insert_maquina(data, sistema="cobol")
                    status = f"✓ {data['nombre']} creado"
                except Exception as ex:
                    status = f"✗ Error: {ex}"
                needs_reload = True
            init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        elif mode == "maquinas" and key == curses.KEY_DC:
            if rows:
                row = rows[selected]
                if confirm_dialog(stdscr, f"¿Eliminar servidor '{row['nombre']}'?"):
                    try:
                        db_delete_maquina(row["nombre"])
                        status = f"✓ {row['nombre']} eliminado"
                        selected = max(0, selected - 1)
                    except Exception as ex:
                        status = f"✗ Error: {ex}"
                    needs_reload = True
                stdscr.touchwin(); stdscr.refresh()
                init_colors(); stdscr.keypad(True); stdscr.timeout(100)

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

        elif mode == "programados" and key == curses.KEY_F4:
            if rows:
                dep = rows[selected]
                if dep["estado"] == "pendiente":
                    nueva_fh = ask_datetime(stdscr, f"Reprogramar: {dep['desc_cliente']}")
                    if nueva_fh:
                        try:
                            conn = get_connection()
                            with conn.cursor() as cur:
                                cur.execute(
                                    "UPDATE deploys_programados SET fecha_hora=%s WHERE id=%s",
                                    (nueva_fh, dep["id"]),
                                )
                            conn.commit()
                            notify_scheduler()
                            status = f"✓ Reprogramado para {nueva_fh.strftime('%d/%m/%Y %H:%M')}"
                        except Exception as ex:
                            status = f"✗ Error: {ex}"
                        needs_reload = True
                else:
                    status = "Solo se pueden reprogramar deploys pendientes"
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

        # ── Configurar email (Ctrl+E) ──────────────────────────────────────
        elif key == 5 and not searching:
            email_cfg = _wizard_email_config(stdscr, existing=cfg)
            if email_cfg:
                cfg.update(email_cfg)
                save_user_config(cfg)
                status = "✓ Config de email guardada"
            init_colors(); stdscr.keypad(True); stdscr.timeout(100)

        # ── Tipeo en búsqueda ──────────────────────────────────────────────
        elif 32 <= key <= 126 and mode not in ("programados", "batch"):
            search  += chr(key)
            searching = True
            selected = 0; offset = 0
            needs_reload = True


def main(stdscr):
    init_colors()
    curses.curs_set(0)
    stdscr.keypad(True)
    stdscr.timeout(100)

    # ── Cargar config ──────────────────────────────────────────────────────
    cfg = load_user_config()
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

    ensure_schema()
    ensure_deploys_table()

    usuario  = HADES["user"]
    selected = 0

    # ── Menú principal ─────────────────────────────────────────────────────
    while True:
        _draw_main_menu(stdscr, selected)
        key = stdscr.getch()

        if key in (ord('q'), ord('Q')):
            break
        elif key == curses.KEY_UP:
            selected = max(0, selected - 1)
        elif key == curses.KEY_DOWN:
            selected = min(len(SISTEMAS) - 1, selected + 1)
        elif key in [ord(s[0]) for s in SISTEMAS]:
            selected = next(i for i, s in enumerate(SISTEMAS) if ord(s[0]) == key)
            key = 10  # forzar Enter
        if key in (curses.KEY_ENTER, 10, 13):
            sistema = SISTEMAS[selected][2]
            if sistema == "cobol":
                cobol_main(stdscr, usuario)
            elif sistema == "genesis":
                genesis_main(stdscr)
            elif sistema == "intermedios":
                intermedios_main(stdscr)
            init_colors()
            curses.curs_set(0)
            stdscr.keypad(True)
            stdscr.timeout(100)


def run():
    global _scheduler_running

    os.environ.setdefault('ESCDELAY', '25')

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
