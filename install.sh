#!/usr/bin/env bash
set -e

REPO_URL="https://raw.githubusercontent.com/FernandoLopez24/core-deploy/main/flv_ssh.py"
BIN_DIR="$HOME/.local/bin"
BIN_PATH="$BIN_DIR/core-deploy"

echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║        core-deploy — instalador      ║"
echo "  ╚══════════════════════════════════════╝"
echo ""

# ── 1. sshpass ────────────────────────────────────────────────────────────
echo "  [1/4] Instalando sshpass..."

if command -v sshpass &>/dev/null; then
    echo "        ya instalado."
elif command -v apt-get &>/dev/null; then
    sudo apt-get install -y sshpass 2>/dev/null || {
        # apt falló: descargar .deb directo
        ARCH=$(dpkg --print-architecture 2>/dev/null || echo "amd64")
        TMP=$(mktemp /tmp/sshpass.XXXXXX.deb)
        curl -fsSL "http://old-releases.ubuntu.com/ubuntu/pool/universe/s/sshpass/sshpass_1.06-1_${ARCH}.deb" \
            -o "$TMP" && sudo dpkg -i "$TMP"; rm -f "$TMP"
    }
elif command -v dnf &>/dev/null; then
    sudo dnf install -y sshpass
elif command -v zypper &>/dev/null; then
    sudo zypper install -y sshpass
elif command -v pacman &>/dev/null; then
    sudo pacman -Sy --noconfirm sshpass
else
    echo "        ADVERTENCIA: instalá sshpass manualmente con tu gestor de paquetes."
fi

# ── 2. psycopg2 ───────────────────────────────────────────────────────────
echo "  [2/4] Instalando psycopg2..."

python3 -c "import psycopg2" 2>/dev/null && echo "        ya instalado." || {
    pip3 install --user psycopg2-binary --quiet || \
    pip3 install psycopg2-binary --quiet || \
    sudo apt-get install -y python3-psycopg2 2>/dev/null || \
    sudo dnf install -y python3-psycopg2 2>/dev/null || \
    sudo pacman -Sy --noconfirm python-psycopg2 2>/dev/null || \
    echo "        ADVERTENCIA: no se pudo instalar psycopg2 automáticamente."
}

# ── 3. Descargar core-deploy ──────────────────────────────────────────────
echo "  [3/4] Descargando core-deploy..."
mkdir -p "$BIN_DIR"

if curl -fSL "$REPO_URL" -o "$BIN_PATH" 2>&1; then
    chmod +x "$BIN_PATH"
    echo "        descargado en $BIN_PATH"
else
    echo ""
    echo "  ✗ Error descargando el script. Verificá tu conexión a internet."
    exit 1
fi

if [ ! -s "$BIN_PATH" ]; then
    echo "  ✗ El archivo descargado está vacío."
    exit 1
fi

# ── 4. PATH ───────────────────────────────────────────────────────────────
echo "  [4/4] Verificando PATH..."

add_to_path() {
    local rc="$1"
    if [ -f "$rc" ] && ! grep -q 'local/bin' "$rc" 2>/dev/null; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$rc"
        echo "        Agregado a $rc"
    fi
}

if ! echo "$PATH" | grep -q "$BIN_DIR"; then
    add_to_path "$HOME/.bashrc"
    add_to_path "$HOME/.zshrc"
fi

echo ""
echo "  ✓ Instalación completa."
echo ""

echo "  Ejecutá:"
echo ""
echo "    core-deploy"
echo ""
