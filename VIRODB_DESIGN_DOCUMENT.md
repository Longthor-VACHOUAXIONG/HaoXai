# ViroDB System Design Document

---

## 1. System Overview

### 1.1 Purpose
ViroDB serves as a centralized platform for virology research data management, supporting:
- Specimen collection tracking (bats, rodents, environmental samples)
- Laboratory screening result management
- Sample storage and inventory management
- Data extraction and reporting
- Real-time data monitoring and analysis

### 1.2 Technology Stack
- **Backend**: Python 3.x with Flask framework
- **Frontend**: HTML5, CSS3, JavaScript with Bootstrap 5
- **Database**: SQLite (primary) with MySQL support
- **Real-time Communication**: Flask-SocketIO
- **Data Processing**: Pandas, NumPy
- **File Handling**: OpenPyXL for Excel operations

---

## 2. System Architecture

### 2.1 Application Structure
```
ViroDB/
├── app.py                    # Main Flask application entry point
├── config.py                 # Configuration management
├── requirements.txt          # Python dependencies
├── routes/                   # Application modules (Blueprints)
│   ├── auth.py              # Authentication and database connection
│   ├── main.py              # Dashboard and main views
│   ├── database.py          # Database operations and imports
│   ├── query.py             # SQL query interface
│   ├── extraction.py        # Data extraction and reporting
│   ├── sequence.py          # Sequence analysis
│   ├── chat.py              # AI-powered assistance
│   ├── excel_import.py      # Excel data import
│   ├── excel_merge.py       # Data merging utilities
│   ├── linking.py           # Data linking and relationships
│   ├── auto_linking.py      # Automated data linking
│   ├── ml.py                # Machine learning features
│   └── sample_management.py # Sample lifecycle management
├── database/                # Database layer
│   ├── db_manager.py        # Database connection management
│   ├── db_manager_flask.py  # Flask-integrated DB operations
│   ├── schema_comprehensive.sql  # Complete database schema
│   └── sample_manager.py    # Sample-specific operations
├── templates/               # HTML templates
│   ├── base.html           # Base template with navigation
│   ├── main/               # Main dashboard templates
│   ├── database/           # Database operation templates
│   ├── query/              # Query interface templates
│   └── extraction/         # Data extraction templates
├── static/                 # Static assets (CSS, JS, images)
├── uploads/                # File upload storage
└── utils/                  # Utility functions
```

### 2.2 Design Patterns
- **Blueprint Pattern**: Modular route organization
- **Factory Pattern**: Application creation with `create_app()`
- **Decorator Pattern**: Authentication and validation decorators
- **Observer Pattern**: Real-time updates via SocketIO

---

## 3. Database Design

### 3.1 Core Data Models

#### 3.1.1 Specimen Collection Tables
- **bat_data**: Bat specimen collection information
- **rodent_data**: Rodent specimen collection information
- **environmental_data**: Environmental sample collection
- **market_data**: Market sample collection data

#### 3.1.2 Sample Management Tables
- **swab_data**: Swab sample collection and tracking
- **tissue_data**: Tissue sample management
- **screening_data**: Laboratory screening results
- **storage_data**: Sample storage and inventory

#### 3.1.3 Supporting Tables
- **locations**: Geographic location data
- **taxonomy**: Species classification information
- **freezer_storage**: Freezer and storage unit management
- **screening_tests**: Available screening test types

### 3.2 Key Relationships
```
Host (bat/rodent/environmental) 
    ↓ (1:N)
Sample (swab/tissue/blood)
    ↓ (1:N)
Screening (PCR/qPCR results)
    ↓ (1:1)
Storage (freezer location)
```

### 3.3 Data Normalization
The database follows third normal form (3NF) with:
- Eliminated data redundancy
- Proper foreign key relationships
- Referential integrity constraints
- Indexing for performance optimization

---

## 4. User Interface Design

### 4.1 Layout Structure
- **Responsive Design**: Mobile-friendly interface using Bootstrap 5
- **Dark Theme**: Professional dark mode interface
- **Sidebar Navigation**: Fixed sidebar with module-based navigation
- **Main Content Area**: Dynamic content based on selected module

### 4.2 Key Interface Components

#### 4.2.1 Dashboard (`/main/dashboard`)
- Database statistics overview
- Recent activity monitoring
- Quick access to main functions
- Real-time data updates via SocketIO

#### 4.2.2 Database Management (`/database/`)
- Excel file import with real-time progress
- Data validation and error reporting
- Bulk data operations
- Data linking and relationship management

#### 4.2.3 Query Interface (`/query/`)
- SQL query editor with syntax highlighting
- Result visualization and export
- Query history and saved queries
- Performance monitoring

#### 4.2.4 Data Extraction (`/extraction/`)
- Custom report generation
- Excel export with formatting
- Filtered data extraction
- Automated report scheduling

### 4.3 User Experience Features
- **Real-time Updates**: Live progress indicators for long operations
- **Error Handling**: Comprehensive error messages and recovery options
- **Data Validation**: Client and server-side validation
- **Accessibility**: WCAG 2.1 compliant interface design

---

## 5. API Design

### 5.1 RESTful Endpoints

#### 5.1.1 Authentication (`/auth/`)
- `POST /auth/connect` - Database connection establishment
- `GET /auth/status` - Connection status check
- `POST /auth/disconnect` - Database disconnection

#### 5.1.2 Database Operations (`/database/`)
- `POST /database/import` - Excel data import
- `GET /database/import/progress` - Import progress tracking
- `POST /database/validate` - Data validation
- `POST /database/link` - Data relationship linking

#### 5.1.3 Query Operations (`/query/`)
- `POST /query/execute` - SQL query execution
- `GET /query/tables` - Available tables listing
- `GET /query/schema` - Database schema information

#### 5.1.4 Extraction (`/extraction/`)
- `POST /extraction/generate` - Report generation
- `GET /extraction/download` - Report download
- `POST /extraction/schedule` - Scheduled report creation

### 5.2 WebSocket Events
- `connect` - Client connection establishment
- `disconnect` - Client disconnection handling
- `subscribe_updates` - Real-time update subscription
- `progress_update` - Long-running operation progress

---

## 6. Core Features

### 6.1 Data Import System
- **Multi-format Support**: Excel (.xlsx, .xls), CSV, database files
- **Validation Engine**: Comprehensive data validation rules
- **Progress Tracking**: Real-time import progress with SocketIO
- **Error Recovery**: Detailed error reporting and correction guidance
- **Bulk Operations**: Efficient handling of large datasets

### 6.2 Query Engine
- **SQL Editor**: Feature-rich SQL query interface
- **Result Visualization**: Tabular and graphical result display
- **Export Options**: Multiple export formats (Excel, CSV, JSON)
- **Query Optimization**: Performance monitoring and suggestions
- **Saved Queries**: Query library for repeated use

### 6.3 Data Extraction & Reporting
- **Custom Reports**: Flexible report generation system
- **Template Engine**: Reusable report templates
- **Automated Scheduling**: Scheduled report generation
- **Advanced Filtering**: Complex data filtering capabilities
- **Export Formatting**: Professional Excel report formatting

### 6.4 Real-time Monitoring
- **Live Dashboard**: Real-time statistics and updates
- **Progress Indicators**: Visual progress for long operations
- **System Notifications**: Event-driven notifications
- **Activity Logging**: Comprehensive audit trail

### 6.5 AI-Powered Features
- **Chat Interface**: AI assistant for data analysis
- **Auto-linking**: Intelligent data relationship detection
- **Pattern Recognition**: ML-based data pattern analysis
- **Predictive Analytics**: Trend analysis and forecasting

---

## 7. Security Considerations

### 7.1 Authentication & Authorization
- **Session Management**: Secure session handling with expiration
- **Database Access**: Controlled database connection management
- **Role-based Access**: User role and permission system
- **Input Validation**: Comprehensive input sanitization

### 7.2 Data Protection
- **SQL Injection Prevention**: Parameterized queries
- **File Upload Security**: Secure file handling and validation
- **Data Encryption**: Sensitive data encryption at rest
- **Audit Logging**: Comprehensive access logging

### 7.3 System Security
- **CORS Configuration**: Proper cross-origin resource sharing
- **CSRF Protection**: Cross-site request forgery prevention
- **Secure Headers**: Security-focused HTTP headers
- **Error Handling**: Secure error message display

---

## 8. Performance Optimization

### 8.1 Database Optimization
- **Indexing Strategy**: Optimized database indexes
- **Query Optimization**: Efficient SQL query patterns
- **Connection Pooling**: Database connection management
- **Caching**: Query result caching for performance

### 8.2 Application Performance
- **Asynchronous Operations**: Background task processing
- **Memory Management**: Efficient memory usage patterns
- **File Handling**: Optimized file I/O operations
- **Response Compression**: Gzip compression for responses

### 8.3 Frontend Optimization
- **Lazy Loading**: On-demand content loading
- **Asset Optimization**: Minified CSS and JavaScript
- **Caching Strategy**: Browser caching implementation
- **Progressive Enhancement**: Graceful degradation support

---

## 9. Data Flow Architecture

### 9.1 Data Import Flow
```
Excel File → Validation → Processing → Database Insert → Confirmation
     ↓              ↓           ↓              ↓              ↓
  Upload        Rules Check  Transform    Transaction    Success/Error
```

### 9.2 Query Execution Flow
```
User Query → Parse → Validate → Execute → Format → Display
     ↓        ↓        ↓         ↓        ↓        ↓
  SQL Input  Syntax   Security  Database Results Visualization
            Check    Check     Engine
```

### 9.3 Real-time Update Flow
```
Database Change → Event Trigger → SocketIO → Client Update
       ↓               ↓              ↓           ↓
   Transaction    Event Queue    WebSocket   UI Refresh
```

---

## 10. Configuration Management

### 10.1 Environment Configuration
- **Development**: Debug mode, local database
- **Production**: Optimized settings, secure configuration
- **Testing**: Isolated test environment
- **Staging**: Pre-production validation

### 10.2 Database Configuration
- **Connection Settings**: Flexible database connection parameters
- **Backup Strategy**: Automated backup procedures
- **Migration Support**: Database version management
- **Performance Tuning**: Database optimization settings

---

## 11. Deployment Architecture

### 11.1 System Requirements
- **Python 3.8+**: Core runtime environment
- **Memory**: Minimum 4GB RAM (8GB recommended)
- **Storage**: 10GB minimum (scalable based on data volume)
- **Network**: Internet connectivity for updates and AI features

### 11.2 Deployment Options
- **Standalone**: Single-server deployment
- **Containerized**: Docker deployment support
- **Cloud**: Cloud platform compatibility
- **Hybrid**: Multi-environment deployment

---

## 12. Maintenance & Support

### 12.1 Monitoring
- **System Health**: Application performance monitoring
- **Database Health**: Database performance metrics
- **Error Tracking**: Comprehensive error logging
- **Usage Analytics**: User behavior and usage patterns

### 12.2 Backup & Recovery
- **Automated Backups**: Scheduled database backups
- **Point-in-time Recovery**: Granular recovery options
- **Data Validation**: Backup integrity verification
- **Disaster Recovery**: Business continuity planning

### 12.3 Updates & Maintenance
- **Version Management**: Controlled update deployment
- **Schema Migration**: Database schema updates
- **Dependency Management**: Package update procedures
- **Security Patches**: Timely security updates

---

## 13. Future Enhancements

### 13.1 Planned Features
- **Mobile Application**: Native mobile app support
- **API Gateway**: Enhanced API management
- **Advanced Analytics**: Extended ML capabilities
- **Integration Hub**: Third-party system integration

### 13.2 Scalability Improvements
- **Microservices Architecture**: Service decomposition
- **Load Balancing**: High availability deployment
- **Data Partitioning**: Horizontal scaling support
- **Caching Layer**: Redis integration

---

**Document Version**: 1.0  

