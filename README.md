# ViroDB - Virus Database Management System

A comprehensive web-based database management system for virology research data, including bat sample tracking, environmental monitoring, and market sample analysis.

## Features

- **Database Management**: SQLite/MySQL support with comprehensive schema
- **Bat Identification**: ML-powered bat species identification
- **Sample Tracking**: Track samples from collection to analysis
- **Excel Import/Export**: Bulk data import with validation
- **Real-time Analytics**: Live dashboard and reporting
- **AI Chat Interface**: Intelligent data analysis assistant
- **Security**: User authentication and session management
- **Backup System**: Automated database backups

## Tech Stack

- **Backend**: Flask (Python)
- **Database**: SQLite (default) / MySQL (optional)
- **Frontend**: HTML5, CSS3, JavaScript, Bootstrap
- **ML/AI**: Scikit-learn, OpenAI API
- **Deployment**: Docker support

## Quick Start

### Prerequisites

- Python 3.8+
- pip package manager

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/ViroDB.git
   cd ViroDB
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configuration**
   ```bash
   # Copy example configuration files
   cp config.example.py config.py
   cp db_settings.example.json db_settings.json
   cp backup_config.example.json backup_config.json
   cp sample_settings.example.json sample_settings.json
   
   # Edit configuration files with your settings
   # Set up your database path and API keys
   ```

4. **Initialize database**
   ```bash
   python app.py
   # The database will be automatically created on first run
   ```

5. **Run the application**
   ```bash
   python app.py
   ```

6. **Access the application**
   Open your browser and navigate to `http://localhost:5000`

## Configuration

### Database Setup

**SQLite (Default)**
- Database file: `DataExcel/CAN2-With-Referent-Key.db`
- No additional setup required

**MySQL (Optional)**
- Update `db_settings.json` with your MySQL credentials
- Set `DB_TYPE=mysql` environment variable

### Environment Variables

Create a `.env` file (optional):

```env
SECRET_KEY=your-secret-key-here
OPENAI_API_KEY=your-openai-api-key
DB_TYPE=sqlite  # or mysql
```

### API Keys

For AI chat functionality:
- Set `OPENAI_API_KEY` in your environment or `.env` file
- Get your key from [OpenAI Platform](https://platform.openai.com/)

## Project Structure

```
ViroDB/
├── app.py                 # Main Flask application
├── config.py             # Application configuration
├── requirements.txt      # Python dependencies
├── routes/               # Flask route handlers
├── templates/            # HTML templates
├── static/               # CSS, JS, images
├── database/             # Database schema and utilities
├── utils/                # Helper functions
├── models/               # ML models and encoders
├── DataExcel/            # Excel data templates
├── uploads/              # File upload directory
├── Ext-table/            # External table processing
└── Dockerfile           # Docker configuration
```

## Usage

### Data Import

1. Navigate to the Database section
2. Use the Excel Import feature
3. Follow the template format in `ViroDB_Import_Template.xlsx`
4. Validate data before import

### Bat Identification

1. Go to Bat Identification page
2. Upload bat sample data
3. Use ML models for species prediction
4. Review and export results

### AI Chat Assistant

1. Access the Chat interface
2. Ask questions about your data
3. Get insights and analysis
4. Export chat results

## Docker Deployment

### Build and Run

```bash
# Build the image
docker build -t virodb .

# Run the container
docker run -p 5000:5000 -v $(pwd)/data:/app/data virodb
```

### Docker Compose

```bash
docker-compose up -d
```

## Development

### Running Tests

```bash
python -m pytest tests/
```

### Code Style

Follow PEP 8 guidelines. Use linting tools:

```bash
flake8 .
black .
```

## Security

- Change default `SECRET_KEY` in production
- Use environment variables for sensitive data
- Enable HTTPS in production
- Regular database backups

## Backup and Recovery

Automatic backups are enabled by default:
- Backup location: `backups/` directory
- Schedule: Daily (configurable)
- Retention: 7 days (configurable)

Manual backup:
```bash
python backup_service.py
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
- Create an issue on GitHub
- Check the documentation in the `docs/` folder
- Review the troubleshooting guide

## Changelog

See `CHANGELOG.md` for version history and updates.

---

**Note**: This is a research database system. Ensure compliance with data protection regulations and institutional policies when handling sensitive biological data.
