# CDV — Development Setup

This guide walks through setting up the development environment for CDV: Rust, Tauri, and the frontend toolchain.

## Overview

CDV is a Tauri v2 app with a Rust backend and a React + Vite frontend. You need:

- **Rust** (toolchain + target for your platform)
- **Tauri prerequisites** (platform-specific system libraries)
- **Node.js or Bun** (for the frontend and Tauri CLI)

---

## 1. Rust

### Install Rust

Use [rustup](https://rustup.rs/) (recommended):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Restart your terminal, then confirm:

```bash
rustc --version
cargo --version
```

### Toolchain

The project uses **Rust 2021 edition**. Your default stable toolchain from rustup is sufficient. To ensure you’re on stable:

```bash
rustup default stable
```

### Optional: Add targets for cross-compilation

Only needed if you build for another OS/arch (e.g. Linux from macOS):

- **macOS (Apple Silicon):** `rustup target add aarch64-apple-darwin`
- **macOS (Intel):** `rustup target add x86_64-apple-darwin`
- **Linux (x64):** `rustup target add x86_64-unknown-linux-gnu`
- **Linux (ARM):** `rustup target add aarch64-unknown-linux-gnu`

---

## 2. Tauri prerequisites

Tauri needs platform-specific build tools and libraries.

### macOS

- **Xcode Command Line Tools** (required for linking):

  ```bash
  xcode-select --install
  ```

- No extra packages are usually required for Tauri 2 on macOS.

### Linux (Debian / Ubuntu)

```bash
sudo apt update
sudo apt install libwebkit2gtk-4.1-dev \
  build-essential \
  curl \
  wget \
  file \
  libxdo-dev \
  libssl-dev \
  libayatana-appindicator3-dev \
  librsvg2-dev
```

### Linux (Fedora)

```bash
sudo dnf install webkit2gtk4.1-devel \
  openssl-devel \
  curl \
  wget \
  file \
  libXdo-devel \
  libappindicator-gtk3-devel \
  librsvg2-devel
```

### Linux (Arch)

```bash
sudo pacman -S webkit2gtk-4.1 \
  base-devel \
  curl \
  wget \
  file \
  openssl \
  libxdo \
  libappindicator-gtk3 \
  librsvg
```

For other distros and for **Windows**, see the official [Tauri v2 prerequisites](https://v2.tauri.app/start/prerequisites/).

---

## 3. Node.js or Bun

The frontend and Tauri CLI run via Node or Bun.

- **Node.js:** LTS (v18+) from [nodejs.org](https://nodejs.org/) or via [nvm](https://github.com/nvm-sh/nvm).
- **Bun:** [bun.sh](https://bun.sh/) — the project’s `tauri.conf.json` uses `bun run dev`, so Bun is the default.

Check:

```bash
node --version   # or
bun --version
```

---

## 4. Install project dependencies

From the repo root:

```bash
# With Bun (default in this project)
bun install

# Or with npm
npm install
```

---

## 5. Verify setup

Run the app in development mode:

```bash
bun tauri dev
# or
npm run tauri dev
```

The first run will compile the Rust backend (may take a few minutes). A window should open with the app and the Vite dev server will serve the frontend.

---

## 6. Build for production

```bash
bun tauri build
# or
npm run tauri build
```

Platform-specific builds (see `package.json`):

- **macOS (Apple Silicon):** `bun run build:mac-arm`
- **macOS (Intel):** `bun run build:mac-intel`
- **macOS (Universal):** `bun run build:mac-universal`
- **Linux (x64):** `bun run build:linux-x64`
- **Linux (ARM):** `bun run build:linux-arm`

Output is under `src-tauri/target/release/` (or the appropriate target directory); installers and bundles are in `src-tauri/target/release/bundle/`.

---

## 7. IDE / editor

Recommended for Rust and Tauri:

- **VS Code** (or Cursor) with:
  - [Tauri](https://marketplace.visualstudio.com/items?itemName=tauri-apps.tauri-vscode)
  - [rust-analyzer](https://marketplace.visualstudio.com/items?itemName=rust-lang.rust-analyzer)

---

## Troubleshooting

| Issue | What to try |
|--------|--------------|
| `rustc` / `cargo` not found | Restart the terminal after installing rustup; ensure `~/.cargo/bin` is in your `PATH`. |
| Linker or “framework not found” on macOS | Install/update Xcode Command Line Tools: `xcode-select --install`. |
| WebKit or GTK errors on Linux | Install the Tauri prerequisites for your distro (see section 2). |
| Port 1420 in use | Stop any other process using port 1420, or change `devUrl` in `src-tauri/tauri.conf.json`. |
| First `tauri dev` very slow | Normal; the first full Rust build can take several minutes. Later runs are incremental. |
| Bun not found | Use `npm install` and `npm run tauri dev` instead, or install Bun from [bun.sh](https://bun.sh/). |

For more detail, see the [Tauri v2 documentation](https://v2.tauri.app/) and [Rust installation guide](https://www.rust-lang.org/tools/install).
