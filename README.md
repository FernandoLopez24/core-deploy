# core-deploy

Herramienta de terminal (TUI) para gestión de servidores, visualización de logs y deploy de servicios COBOL. Desarrollada para uso interno en Síntesis S.A.

## Características

- **[1] Clientes** — lista de clientes con acceso SSH directo al path de producción
- **[2] Servidores** — gestión de máquinas de la red interna
- **[3] Logs en tiempo real** — visor de logs con scroll y pausa
- **[4] Grep vivo** — filtrado de logs en tiempo real con patrón grep
- **[5] Deploy** — pipeline completo: compilar en hades → descargar .int → backup → subir a producción → make
- **[6] Multi-Deploy** — deploy secuencial de varios servicios .cbl a la vez
- **[7] Programados** — programa deploys para una fecha y hora específica; cada usuario ve y gestiona solo los suyos

## Requisitos

- Linux (Ubuntu, Debian, Fedora, CentOS, Arch, openSUSE y derivados)
- Python 3.6 o superior
- Acceso a la red interna de Síntesis S.A.
- Usuario y contraseña (o clave SSH) para hades
- Credenciales de la base de datos PostgreSQL (pedirlas a Fernando)

## Instalación

### Opción A — Script automático (recomendado)

Detecta tu sistema operativo e instala todo solo:

```bash
curl -fsSL https://raw.githubusercontent.com/FernandoLopez24/core-deploy/main/install.sh -o /tmp/cd-install.sh && bash /tmp/cd-install.sh
```

### Opción B — Manual

**Ubuntu / Debian / Mint / Pop!_OS**
```bash
sudo apt update && sudo apt install -y sshpass python3-psycopg2
```

> **Ubuntu 20.04 o anterior** — si da error de conexión al instalar:
> ```bash
> sudo sed -i 's|archive.ubuntu.com|old-releases.ubuntu.com|g' /etc/apt/sources.list
> sudo sed -i 's|security.ubuntu.com|old-releases.ubuntu.com|g' /etc/apt/sources.list
> sudo apt update && sudo apt install -y sshpass python3-psycopg2
> ```

**Fedora**
```bash
sudo dnf install -y sshpass python3-psycopg2
```

**RHEL / CentOS / Rocky / AlmaLinux**
```bash
sudo dnf install -y epel-release
sudo dnf install -y sshpass python3-psycopg2
```

**openSUSE**
```bash
sudo zypper install -y sshpass python3-psycopg2
```

**Arch / Manjaro**
```bash
sudo pacman -Sy sshpass python-psycopg2
```

Luego, en cualquier distro:
```bash
mkdir -p ~/.local/bin
curl -fsSL https://raw.githubusercontent.com/FernandoLopez24/core-deploy/main/flv_ssh.py \
    -o ~/.local/bin/core-deploy
chmod +x ~/.local/bin/core-deploy
echo $PATH | grep -q ".local/bin" || echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
```

## Configuración (primera vez)

Al ejecutar `core-deploy` por primera vez aparece un wizard en dos pasos:

**Paso 1 — Conexión a hades**
1. Método de autenticación: **Clave SSH** o **Contraseña**
2. Tu usuario en hades
3. Ruta a tu clave SSH (`~/.ssh2/usuario`) — o tu contraseña de hades

**Paso 2 — Base de datos**
1. Host / IP del servidor PostgreSQL
2. Nombre de la base de datos
3. Usuario de la base de datos
4. Contraseña de la base de datos

Presioná `F10` para guardar cada paso. La configuración queda en `~/.config/core-deploy/config.json` y no vuelve a pedirse.

Para reconfigurar:
```bash
rm ~/.config/core-deploy/config.json && core-deploy
```

## Uso

```bash
core-deploy
```

### Navegación general

| Tecla | Acción |
|---|---|
| `↑` `↓` | Moverse por la lista |
| Escribir | Buscar / filtrar en tiempo real |
| `ESC` | Limpiar búsqueda |
| `1`–`7` | Cambiar de pestaña |
| `q` | Salir |

### Atajos en pestaña Clientes `[1]`

| Tecla | Acción |
|---|---|
| `Enter` | Conectar por SSH al path del cliente |
| `F2` | Nuevo cliente |
| `F3` | Ver detalle |
| `F4` | Editar cliente |
| `Supr` | Eliminar cliente |

### Logs / Grep vivo `[3]` `[4]`

| Tecla | Acción |
|---|---|
| `p` | Pausar / reanudar |
| `↑` `↓` / `PgUp` `PgDn` | Scroll |
| `Fin` | Volver al final (en vivo) |
| `q` / `ESC` | Volver |

### Deploy y Multi-Deploy `[5]` `[6]`

Al seleccionar un cliente y los servicios, la herramienta pregunta:

```
¿Cuándo ejecutar?
  ► Ejecutar ahora
    Programar para después
```

Si elegís **Programar**, pedirá fecha y hora en formato `DD/MM/AAAA HH:MM`.

### Programados `[7]`

| Tecla | Acción |
|---|---|
| `Supr` | Cancelar un deploy pendiente |
| `F5` / `ESC` | Actualizar la lista |

Cada usuario ve únicamente sus propios deploys programados. Los estados posibles son:

| Estado | Significado |
|---|---|
| `PENDIENTE` | Esperando su hora de ejecución |
| `EJECUTANDO` | En proceso en este momento |
| `OK` | Completado con éxito |
| `ERROR` | Falló — ver detalle en la BD |

El scheduler corre en segundo plano con consumo mínimo de CPU: duerme exactamente hasta el próximo deploy programado y se despierta al instante cuando agregás uno nuevo.

## Actualizar

```bash
curl -fsSL https://raw.githubusercontent.com/FernandoLopez24/core-deploy/main/flv_ssh.py \
    -o ~/.local/bin/core-deploy
```

## Problemas frecuentes

**`command not found: core-deploy`**
```bash
source ~/.bashrc
```

**`Error conectando a la base de datos`**
- Verificá que estás conectado a la VPN o a la red interna de Síntesis
- Revisá las credenciales: `rm ~/.config/core-deploy/config.json && core-deploy`

**`Warning: Identity file not accessible`**
- La ruta a la clave SSH está mal configurada
```bash
rm ~/.config/core-deploy/config.json && core-deploy
```

**`No se encontraron .cbl`**
- El cliente no tiene `path_hades` configurado en la BD. Consultá con Fernando.

---

Soporte: Fernando López — Síntesis S.A.
