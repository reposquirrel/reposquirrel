---
title: RepoSquirrel – Git Repository Analytics
---

# RepoSquirrel 🐿️  
*A comprehensive Git repository analytics tool for large, multi-repo codebases.*

RepoSquirrel helps you understand **who owns what** in your codebase by analyzing Git history and presenting it in an interactive web dashboard. :contentReference[oaicite:1]{index=1}

---

## Features

### 👤 Developer Analytics
- Track lines **added, removed, and modified** per developer
- View **historical stats** by month and year
- See **current ownership** via `git blame`
- Group contributors into **teams** for aggregated insights :contentReference[oaicite:2]{index=2}

### 🧩 Subsystem / Service Analysis
- Analyze **multiple repositories** at once
- Define **services** by directory paths
- See contributions per **service/component**
- Aggregate stats across related subsystems :contentReference[oaicite:3]{index=3}

### 🧠 Advanced Capabilities
- User **aliases** (merge multiple Git identities)
- **Ignore lists** for bots/automation
- **Language detection** (with `cloc`) :contentReference[oaicite:4]{index=4}
- Interactive **web dashboard** with real-time updates

---

## Quick Start

### 1. Requirements

- **Python 3.7+**
- **Git** (on your PATH)
- Optional but recommended: **`cloc`** for language statistics :contentReference[oaicite:5]{index=5}  

Install `cloc`, for example on Ubuntu/Debian:

```bash
sudo apt-get install cloc

