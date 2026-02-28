# 🐉 HaoXai Intelligence

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/Flask-2.0+-green.svg)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)
[![Interface](https://img.shields.io/badge/UI-Premium_Notebook-orange.svg)](#features)

**HaoXai Intelligence** is a cutting-edge research platform designed for advanced data management, virology research, and AI-driven insights. It transforms raw scientific data into actionable intelligence through a high-performance notebook interface and integrated machine learning models.

---

## 🌟 Key Features

### 🔬 Research Notebook
- **Multi-Language Support**: Seamlessly execute **SQL**, **Python**, and **R** in a unified, premium notebook interface.
- **Smart Fix AI**: Automatically detects execution errors and suggests actionable solutions in real-time.
- **Data Visualization**: Integrated support for `matplotlib`, `seaborn`, and `ggplot2` with high-fidelity rendering.

### 🔍 Intelligence Hub
- **Universal Search**: "Google Chrome-like" search across files, database schemas, and notebook cells.
- **Web Insights**: Direct integration with Google and Stack Overflow for instant research.
- **Taxonomy Management**: specialized modules for bat species identification and sample tracking.

### 🛡️ Core Infrastructure
- **Hybrid Database**: Support for both **SQLite** (local) and **MySQL/MariaDB** (enterprise).
- **Security**: Robust authentication and session management.
- **Flexibility**: Fully containerized with Docker for rapid deployment.

---

## 🚀 Quick Start

### 1. Installation
```powershell
# Clone the repository
git clone https://github.com/HaoXai-Team/HaoXai.git
cd HaoXai

# Install dependencies
pip install -r requirements.txt
```

### 2. Configuration
Copy the template configuration files and set your environment variables:
```powershell
cp config.example.py config.py
cp db_settings.example.json db_settings.json
```

### 3. Launch
```powershell
python app.py
```
Visit `http://localhost:5000` to access the console.

---

## 📂 Project Structure

| Directory | Description |
| :--- | :--- |
| `app.py` | Main application entry point |
| `routes/` | API and UI route handlers |
| `templates/` | High-fidelity HTML5 templates |
| `database/` | Schema management and connection logic |
| `utils/` | AI/ML trainers and helper utilities |
| `static/` | CSS/JS assets and design system |

---

## 🛠️ Tech Stack

- **Backend**: Python / Flask / SocketIO
- **Database**: SQLite / MariaDB / MySQL
- **ML/AI**: Scikit-Learn / OpenAI API / Custom Heuristics
- **Frontend**: Vanilla CSS3 (Custom Design System) / JavaScript / CodeMirror

---

## 📊 Documentation

- [Design Document](HAOXAI_DESIGN_DOCUMENT.md)
- [Database Schema](HAOXAI_DATABASE_DIAGRAM.md)

---
*Created with ❤️ for Advanced Scientific Research.*