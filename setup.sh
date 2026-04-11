#!/bin/sh
# setup.sh — One-time setup for Ubuntu. Installs Docker and prepares .env.
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"

die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n'        "$*" >&2; }

# --- Install Docker if missing ----------------------------------------------
if command -v docker >/dev/null 2>&1; then
    info "Docker already installed."
else
    info "Installing Docker..."
    curl -fsSL https://get.docker.com | sudo sh
    sudo usermod -aG docker "$USER"
    info "Docker installed. You may need to log out and back in for group changes to take effect."
fi

# --- Start Docker -----------------------------------------------------------
if ! docker info >/dev/null 2>&1; then
    sudo systemctl enable --now docker
fi

# --- Start SearXNG ----------------------------------------------------------
if docker ps -q -f "name=searxng" | grep -q .; then
    info "SearXNG already running."
else
    info "Starting SearXNG..."
    docker compose -f "$DIR/searxng/docker-compose.yml" up -d
    info "SearXNG started at http://localhost:8888"
fi

# --- Create .env from template ----------------------------------------------
if [ -f "$DIR/.env" ]; then
    info ".env already exists — skipping."
else
    cp "$DIR/env.example" "$DIR/.env"
    info "Created .env — edit it now with your credentials:"
    info "  nano $DIR/.env"
fi

info ""
info "Setup complete. Next steps:"
info "  1. Edit .env with your APIFY_TOKEN"
info "  2. Run:  ./scrape.sh leads.csv"
