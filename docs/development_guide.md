# RepoSquirrel Development Guide

This guide covers local development setup, manual configuration, and the Docker/Makefile workflow. For a product overview and high-level feature list, see the main [README](../README.md).

## Requirements

### System Requirements
- **Python 3.7+**
- **Git** installed and available on the command line
- **tokei** for language statistics (https://github.com/XAMPPRocky/tokei)
- **Unix-like environment** (Linux or macOS recommended; Windows via WSL works)

### Installing tokei
The easiest way is via Cargo (requires the Rust toolchain) so that JSON output support is enabled:
```bash
cargo install tokei --features all
```
Alternatively, install via your package manager or download a release from https://github.com/XAMPPRocky/tokei/releases and add the binary to your `PATH`. Set `TOKEI_BIN` if it lives outside of your PATH.

### Python Dependencies
```
flask
```
Install with:
```bash
pip3 install flask
```

## Quick Start (Source Checkout)

1. **Install dependencies**
   ```bash
   # Install system dependencies
   sudo apt-get install git  # Ubuntu/Debian
   # or
   brew install git          # macOS

   # Install tokei (choose one)
   cargo install tokei --features all
   # or download/package-manager install

   # Install Python dependencies
   pip3 install flask
   ```
2. **Clone and start**
   ```bash
   git clone <repository-url>
   cd reposquirrel
   python3 dashboard_server.py
   ```
3. **Open the dashboard** at `http://localhost:5000` to configure repositories, teams, subsystems, aliases, and to run analyses.

## Usage Notes

### Web Dashboard
Start the server with:
```bash
python3 dashboard_server.py
```
The dashboard lets you:
- Browse developer statistics and contributions
- View subsystem ownership and trends
- Explore team performance
- Configure repositories/teams/subsystems through the UI
- Monitor real-time progress for data generation

## Configuration Files
Configuration lives in the `configuration/` directory and can be edited manually or via the UI.

### services.json
Defines how repositories are organized into services/subsystems:
```json
{
  "repo-name": {
    "service1": ["service1/"],
    "service2": ["service2/"],
    "main": [""]
  }
}
```

### teams.json
Organizes developers into teams:
```json
{
  "team-id": {
    "name": "Team Display Name",
    "description": "Team description",
    "members": ["user1", "user2", "user3"]
  }
}
```

### alias.json
Maps alternative usernames to canonical names:
```json
{
  "canonical-username": ["alias1", "alias2"],
  "other-user": ["alternative-name"]
}
```

### team_subsystem_responsibilities.json
Links teams to subsystems they own:
```json
{
  "team-id": ["subsystem1", "subsystem2"],
  "other-team": ["subsystem3"]
}
```

### ignore_user.txt
List of usernames to exclude (one per line):
```
bot-account
automated-user
ci-bot
```

## Project Structure
```
GIT_REPO_SQUIRREL_NEW/
├── master.py                 # Main orchestration script
├── dashboard_server.py        # Web dashboard server
├── summery.py                 # User statistics generator
├── service.py                 # Subsystem statistics generator
├── blame.py                   # Ownership analysis via git blame
├── repo.py                    # Repository utilities
├── configuration/             # Configuration files
│   ├── services.json
│   ├── teams.json
│   ├── alias.json
│   ├── team_subsystem_responsibilities.json
│   └── ignore_user.txt
├── templates/                 # HTML templates for dashboard
├── static/                    # CSS, JavaScript for dashboard
├── repos/                     # Your cloned repositories go here
└── stats/                     # Generated analytics (created automatically)
```

## How It Works
1. **Repository Scanning** – Scans Git repositories in the specified directory
2. **Commit Analysis** – Monthly/yearly stats through `git log`
3. **Attribution** – Commits attributed to developers with alias resolution
4. **Subsystem Mapping** – Files mapped to services/subsystems
5. **Blame Analysis** – `git blame` determines current ownership
6. **Aggregation** – Data aggregated by user, team, subsystem, and time period
7. **Dashboard** – Web interface for exploration

## Docker & Makefile Workflow

You can containerize RepoSquirrel with the included `Dockerfile` and `Makefile`.

### Build the image
```bash
make build IMAGE=repo-squirrel TAG=latest
```

### Run the dashboard
```bash
make run IMAGE=repo-squirrel TAG=latest PORT=5001 \
  REPO_DIR=$PWD/repos STATS_DIR=$PWD/stats CONFIG_DIR=$PWD/configuration
```
This maps host directories so repository clones, generated stats, and configuration live on your filesystem. Toggle read-only mode with `READ_ONLY=true`:
```bash
make run READ_ONLY=true
```

### Exporting the image
```bash
make save IMAGE=repo-squirrel TAG=latest SAVE_FILE=reposquirrel.tar.gz
scp reposquirrel.tar.gz other-host:/path/
```
Load it on another machine:
```bash
gunzip -c reposquirrel.tar.gz | docker load
```
Or run the container manually:
```bash
docker run --rm -it \
  -p 5001:5001 \
  -e PORT=5001 -e READ_ONLY=false \
  -v $PWD/repos:/app/repos \
  -v $PWD/stats:/app/stats \
  -v $PWD/configuration:/app/configuration \
  repo-squirrel:latest
```
