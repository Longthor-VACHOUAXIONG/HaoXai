# ViroDB System Architecture Diagrams

## 1. High-Level System Architecture

```mermaid
graph TB
    subgraph "User Layer"
        U[Web Browser]
        M[Mobile Device]
    end
    
    subgraph "Presentation Layer"
        UI[HTML/CSS/JS Interface]
        API[REST API Endpoints]
        WS[WebSocket Connections]
    end
    
    subgraph "Application Layer"
        FLASK[Flask Application]
        AUTH[Authentication Module]
        ROUTES[Route Handlers]
        VALID[Data Validation]
    end
    
    subgraph "Business Logic Layer"
        IMPORT[Excel Import Engine]
        QUERY[Query Processor]
        EXTRACTION[Data Extraction]
        AI[AI/ML Module]
        REALTIME[Real-time Updates]
    end
    
    subgraph "Data Layer"
        DB[(SQLite/MySQL Database)]
        FILES[File Storage]
        CACHE[Cache Layer]
    end
    
    U --> UI
    M --> UI
    UI --> API
    UI --> WS
    API --> FLASK
    WS --> FLASK
    FLASK --> AUTH
    FLASK --> ROUTES
    ROUTES --> VALID
    ROUTES --> IMPORT
    ROUTES --> QUERY
    ROUTES --> EXTRACTION
    ROUTES --> AI
    ROUTES --> REALTIME
    IMPORT --> DB
    QUERY --> DB
    EXTRACTION --> DB
    AI --> DB
    REALTIME --> WS
    IMPORT --> FILES
    DB --> CACHE
```

## 2. Database Schema Architecture

```mermaid
erDiagram
    HOSTS {
        int id PK
        string source_id UK
        string host_type
        string scientific_name
        string province
        string district
        string village
        date collection_date
        string sex
        float weight_g
    }
    
    SAMPLES {
        int id PK
        string sample_code UK
        string sample_type
        string source_id FK
        date collection_date
        string storage_temperature
        boolean rna_extracted
    }
    
    SCREENING {
        int id PK
        string sample_code FK
        string test_type
        date test_date
        string test_result
        float ct_value
        string tested_by
    }
    
    STORAGE {
        int id PK
        string sample_code FK
        string freezer_name
        int cabinet_no
        int box_no
        int spot_position
        date storage_date
    }
    
    LOCATIONS {
        int id PK
        string province
        string district
        string village
        float latitude
        float longitude
        float altitude
    }
    
    TAXONOMY {
        int id PK
        string kingdom
        string phylum
        string class
        string order
        string family
        string genus
        string species
    }
    
    HOSTS ||--o{ SAMPLES : "has"
    SAMPLES ||--o{ SCREENING : "tested"
    SAMPLES ||--|| STORAGE : "stored"
    HOSTS }o--|| LOCATIONS : "located"
    HOSTS }o--|| TAXONOMY : "classified"
```

## 3. Application Module Architecture

```mermaid
graph LR
    subgraph "Core Application"
        APP[app.py]
        CONFIG[config.py]
    end
    
    subgraph "Route Modules"
        AUTH[auth.py]
        MAIN[main.py]
        DB[database.py]
        QUERY[query.py]
        EXTRACTION[extraction.py]
        SEQUENCE[sequence.py]
        CHAT[chat.py]
        ML[ml.py]
    end
    
    subgraph "Database Layer"
        DB_MGR[db_manager.py]
        DB_FLASK[db_manager_flask.py]
        SAMPLE_MGR[sample_manager.py]
    end
    
    subgraph "Templates"
        BASE[base.html]
        DASH[dashboard.html]
        IMPORT[import.html]
        QUERY_T[query.html]
    end
    
    subgraph "Static Assets"
        CSS[styles.css]
        JS[scripts.js]
        IMAGES[images/]
    end
    
    APP --> AUTH
    APP --> MAIN
    APP --> DB
    APP --> QUERY
    APP --> EXTRACTION
    APP --> SEQUENCE
    APP --> CHAT
    APP --> ML
    
    AUTH --> DB_FLASK
    MAIN --> DB_FLASK
    DB --> DB_MGR
    QUERY --> DB_FLASK
    EXTRACTION --> DB_FLASK
    
    MAIN --> DASH
    DB --> IMPORT
    QUERY --> QUERY_T
    
    BASE --> CSS
    BASE --> JS
    BASE --> IMAGES
```

## 4. Data Flow Architecture

```mermaid
sequenceDiagram
    participant User
    participant UI
    participant API
    participant Validator
    participant Processor
    participant Database
    participant WebSocket
    
    User->>UI: Upload Excel File
    UI->>API: POST /database/import
    API->>Validator: Validate File Format
    Validator->>API: Validation Result
    API->>Processor: Process Data
    Processor->>WebSocket: Progress Update
    WebSocket->>UI: Real-time Progress
    Processor->>Database: Insert Records
    Database->>Processor: Insert Result
    Processor->>API: Processing Complete
    API->>UI: Success Response
    UI->>User: Import Complete
```

## 5. Real-time Update Architecture

```mermaid
graph TD
    subgraph "Client Side"
        CLIENT[Web Client]
        SOCKET_CLIENT[Socket.IO Client]
    end
    
    subgraph "Server Side"
        FLASK[Flask App]
        SOCKET_SERVER[Socket.IO Server]
        EVENT_HANDLER[Event Handlers]
        DB_LISTENER[Database Listener]
    end
    
    subgraph "Data Sources"
        DATABASE[(Database)]
        IMPORT[Import Process]
        QUERY[Query Results]
    end
    
    CLIENT --> SOCKET_CLIENT
    SOCKET_CLIENT --> SOCKET_SERVER
    SOCKET_SERVER --> EVENT_HANDLER
    EVENT_HANDLER --> FLASK
    
    DATABASE --> DB_LISTENER
    IMPORT --> DB_LISTENER
    QUERY --> DB_LISTENER
    
    DB_LISTENER --> EVENT_HANDLER
    EVENT_HANDLER --> SOCKET_SERVER
    SOCKET_SERVER --> SOCKET_CLIENT
    SOCKET_CLIENT --> CLIENT
```

## 6. Security Architecture

```mermaid
graph TB
    subgraph "External Access"
        USER[User]
        BROWSER[Web Browser]
    end
    
    subgraph "Security Layer"
        AUTH[Authentication]
        SESSION[Session Management]
        CSRF[CSRF Protection]
        VALID[Input Validation]
    end
    
    subgraph "Application Layer"
        FLASK[Flask App]
        ROUTES[Route Handlers]
        DB_CONN[Database Connections]
    end
    
    subgraph "Data Protection"
        ENCRYPT[Data Encryption]
        BACKUP[Backup System]
        AUDIT[Audit Logging]
    end
    
    USER --> BROWSER
    BROWSER --> AUTH
    AUTH --> SESSION
    SESSION --> CSRF
    CSRF --> VALID
    VALID --> FLASK
    FLASK --> ROUTES
    ROUTES --> DB_CONN
    
    DB_CONN --> ENCRYPT
    ENCRYPT --> BACKUP
    BACKUP --> AUDIT
```

## 7. Deployment Architecture

```mermaid
graph TB
    subgraph "Production Environment"
        LB[Load Balancer]
        WEB1[Web Server 1]
        WEB2[Web Server 2]
        DB_MASTER[(Primary Database)]
        DB_SLAVE[(Replica Database)]
        REDIS[(Redis Cache)]
        FILES[File Storage]
    end
    
    subgraph "Development Environment"
        DEV_WEB[Dev Web Server]
        DEV_DB[(Dev Database)]
        DEV_FILES[Dev File Storage]
    end
    
    subgraph "Monitoring"
        MONITOR[Monitoring System]
        LOGS[Log Aggregation]
        ALERTS[Alert System]
    end
    
    LB --> WEB1
    LB --> WEB2
    WEB1 --> DB_MASTER
    WEB2 --> DB_MASTER
    DB_MASTER --> DB_SLAVE
    WEB1 --> REDIS
    WEB2 --> REDIS
    WEB1 --> FILES
    WEB2 --> FILES
    
    WEB1 --> MONITOR
    WEB2 --> MONITOR
    DB_MASTER --> MONITOR
    MONITOR --> LOGS
    LOGS --> ALERTS
```

## 8. Feature Module Interactions

```mermaid
mindmap
  root((ViroDB))
    Core Features
      Data Management
        Excel Import
        Data Validation
        Bulk Operations
      Query System
        SQL Editor
        Result Visualization
        Export Functions
      Reporting
        Custom Reports
        Excel Export
        Scheduled Reports
    Advanced Features
      Real-time Updates
        Live Dashboard
        Progress Tracking
        Notifications
      AI Integration
        Chat Assistant
        Auto-linking
        Pattern Recognition
      Analytics
        ML Models
        Trend Analysis
        Predictive Analytics
    Infrastructure
      Security
        Authentication
        Data Encryption
        Audit Logging
      Performance
        Caching
        Optimization
        Monitoring
      Scalability
        Load Balancing
        Database Replication
        File Storage
```

---

## Diagram Summary

These architectural diagrams provide visual representations of:

1. **High-Level System Architecture** - Overall system structure and component relationships
2. **Database Schema Architecture** - Entity relationships and data model structure
3. **Application Module Architecture** - Flask application organization and module dependencies
4. **Data Flow Architecture** - Sequence of operations for data processing
5. **Real-time Update Architecture** - WebSocket implementation for live updates
6. **Security Architecture** - Security layers and protection mechanisms
7. **Deployment Architecture** - Production deployment topology
8. **Feature Module Interactions** - Feature relationships and system capabilities

These diagrams complement the written design document and provide visual clarity for understanding the ViroDB system architecture.
