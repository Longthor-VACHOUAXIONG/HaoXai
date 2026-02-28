# <img src="static/logo-icon.svg" width="40" height="40" style="vertical-align: middle; margin-right: 10px;"> HaoXai Intelligence

A specialized research and data management system designed for scientific workflows, laboratory tracking, and AI-accelerated data analysis.

---

## 🛠 Features

### 1. Intelligence Notebook & Smart Fix
- **Multi-Runtime Execution**: Run **SQL**, **Python**, and **R** code blocks in a unified notebook interface.
- **AI-Powered Troubleshooting**: Automatically detects execution errors (e.g., `NameError`, `SyntaxError`) and provides specific code fix suggestions in the sidebar.
- **Universal Search**: Integrated search bar that indexes local files, notebook cells, and database schemas, with direct research links to Google and Stack Overflow.

### 2. Scientific ML & Identification
- **Bat Taxonomy Identification**: Specialized machine learning models (scikit-learn) trained to identify bat genus and species (e.g., *Hipposideros*, *Rhinolophus*) based on measurement data.
- **Taxonomic Classification**: Automated lookup of full biological classification for detected species.

### 3. Laboratory Extraction Tools
- **Plate Generator**: Automated generation of PCR and cDNA plate layouts (8x12 format) from sample lists.
- **H2O Randomization**: Intelligent placement of H2O controls within plate layouts to prevent experimental bias.
- **Extraction Tracking**: Streamlined tracking of samples from environmental collection to laboratory processing.

### 4. Data Linking & Database Hub
- **Database Support**: Native connection management for **SQLite**, **MySQL**, and **MariaDB**.
- **Auto-Linking Engine**: Bulk-link related tables using ID-date matching and metadata patterns to fix fragmented records.
- **Excel Power-Tools**: specialized import/export logic for large scientific spreadsheets, including data merging and validation.

### 5. AI Data Assistant
- **Smart Local Chat**: A chat interface that combines rule-based logic and ML models to query local databases using natural language.
- **Recursive Profiling**: Automatically traverses foreign key relationships to pull a complete history for any specific sample ID or record.

### 6. Real-time Infrastructure
- **Live Terminal**: Real-time execution feedback and status updates powered by **SocketIO**.
- **ML Operations**: A dedicated dashboard for monitoring model training status and accuracy metrics.

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- Pip package manager
- (Optional) R environment for R-cell execution

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/HaoXai-Team/HaoXai.git
   cd HaoXai
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Initialize configuration:
   ```bash
   cp config.example.py config.py
   ```
4. Run the application:
   ```bash
   python app.py
   ```

---

## 📂 Project Structure
- `routes/`: Functional logic for ML identification, extraction, and AI chat.
- `templates/`: Premium HTML5 workstation interfaces.
- `database/`: Cross-database connection and schema management.
- `utils/`: Core AI trainers and laboratory helper scripts.

---
*Built for the future of scientific research and data intelligence.*