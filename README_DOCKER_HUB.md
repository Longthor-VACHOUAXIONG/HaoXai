# HaoXai - HaoXai System

[![Docker Hub](https://img.shields.io/badge/docker-hub-HaoXai-blue.svg)](https://hub.docker.com/r/YOUR_DOCKER_USERNAME/HaoXai)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

A comprehensive **HaoXai System (HaoXai)** for virology research data management, supporting specimen tracking, screening results, storage management, and real-time data analysis.

## 🚀 Quick Start with Docker

### Option 1: Simple Installation
```bash
docker run -p 5000:5000 YOUR_DOCKER_USERNAME/HaoXai:latest
```
Then open http://localhost:5000 in your browser.

### Option 2: With Persistent Data
```bash
docker run -d \
  --name HaoXai \
  -p 5000:5000 \
  -v $(pwd)/uploads:/app/uploads \
  -v $(pwd)/data:/app/data \
  YOUR_DOCKER_USERNAME/HaoXai:latest
```

### Option 3: Production with Docker Compose
Create `docker-compose.yml`:
```yaml
version: '3.8'
services:
  HaoXai:
    image: YOUR_DOCKER_USERNAME/HaoXai:latest
    container_name: HaoXai
    ports:
      - "5000:5000"
    environment:
      - FLASK_ENV=production
      - SECRET_KEY=${SECRET_KEY:-change-this-secret-key}
    volumes:
      - ./uploads:/app/uploads
      - ./flask_session:/app/flask_session
      - lms_data:/app/data
    restart: unless-stopped

volumes:
  lms_data:
    driver: local
```

Then run:
```bash
docker-compose up -d
```

## ✨ Features

- 🔬 **Sample Management**: Track specimens, collection data, and metadata
- 🧪 **Laboratory Screening**: Manage test results and screening workflows
- 🗄️ **Storage Management**: Monitor sample storage conditions and locations
- 📊 **Data Analytics**: Real-time dashboards and visualization tools
- 🤖 **AI Assistant**: Intelligent help for data queries and analysis
- 📋 **SQL Interface**: Advanced database querying with autocomplete
- 📤 **Data Export**: Multiple formats (CSV, Excel, JSON)
- 🔄 **Real-time Updates**: Live data synchronization with WebSocket

## 🔧 Configuration

### Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `FLASK_ENV` | `production` | Flask environment |
| `SECRET_KEY` | - | Flask secret key (required in production) |
| `DB_TYPE` | `sqlite` | Database type (`sqlite` or `mysql`) |
| `MYSQL_HOST` | `mysql` | MySQL host (when using MySQL) |
| `MYSQL_USER` | `HaoXai` | MySQL username |
| `MYSQL_PASSWORD` | - | MySQL password |
| `MYSQL_DATABASE` | `HaoXai` | MySQL database name |

### Volume Mounts
| Path | Description |
|-------|-------------|
| `/app/uploads` | User uploaded files |
| `/app/flask_session` | Session data |
| `/app/data` | SQLite database and data files |

## 📚 Documentation

- **Full Documentation**: [Link to your documentation]
- **API Reference**: [Link to API docs]
- **Development Guide**: [Link to development setup]
- **Docker Hub**: https://hub.docker.com/r/YOUR_DOCKER_USERNAME/HaoXai

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Issues**: [Link to GitHub Issues]
- **Discussions**: [Link to GitHub Discussions]
- **Email**: your-email@example.com

## 🏷️ Version History

- **v1.0.0** - Initial release with core HaoXai functionality
- **v1.0.1** - Bug fixes and performance improvements
- **v1.1.0** - Added AI assistant and advanced analytics

---

**⚡ Get started in seconds:**
```bash
docker run -p 5000:5000 YOUR_DOCKER_USERNAME/HaoXai:latest
```

*Replace `YOUR_DOCKER_USERNAME` with your actual Docker Hub username.*
