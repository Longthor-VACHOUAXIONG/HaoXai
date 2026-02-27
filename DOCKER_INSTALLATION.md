# Docker Installation Required

Docker is not installed on your system. Please install Docker before proceeding with the containerization.

## Installation Instructions

### Windows
1. Download Docker Desktop from: https://www.docker.com/products/docker-desktop/
2. Run the installer and follow the setup wizard
3. Restart your computer after installation
4. Open Docker Desktop and wait for it to start
5. Verify installation by opening PowerShell/CMD and running:
   ```bash
   docker --version
   docker compose version
   ```

### Alternative: Docker without Docker Desktop
If you prefer not to use Docker Desktop, you can install Docker Engine directly:
1. Install WSL 2: `wsl --install`
2. Install Docker Engine: https://docs.docker.com/engine/install/windows/

## After Installation

Once Docker is installed, you can proceed with the HaoXai containerization:

1. **Build the Docker image:**
   ```bash
   docker build -t HaoXai-app .
   ```

2. **Run with SQLite (development):**
   ```bash
   docker compose -f docker-compose.dev.yml up --build
   ```

3. **Run with MySQL (production):**
   ```bash
   # Copy environment file first
   copy .env.example .env
   
   # Edit .env with your settings
   notepad .env
   
   # Start all services
   docker compose up --build -d
   ```

## Troubleshooting

### Docker Desktop Issues
- Ensure WSL 2 is enabled: `wsl --list --verbose`
- Restart Docker Desktop if services don't start
- Check system requirements (Windows 10/11, 64-bit)

### Permission Issues
- Run PowerShell as Administrator
- Ensure your user is in the docker-users group

### Resource Issues
- Allocate at least 4GB RAM to Docker Desktop
- Ensure enough disk space (20GB+ recommended)

## Next Steps

After Docker is installed and running:
1. Return to DOCKER_README.md for deployment instructions
2. Test the containerization setup
3. Deploy to your preferred environment
