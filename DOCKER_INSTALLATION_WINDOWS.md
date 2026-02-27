# Docker Installation for Windows - HaoXai Publishing Guide

## 🚨 Docker Not Found on Your System

The publishing script failed because Docker is not installed on your Windows system. Follow these steps to install Docker and then publish your HaoXai application.

## Step 1: Install Docker Desktop for Windows

### Option A: Docker Desktop (Recommended for most users)
1. **Download Docker Desktop**: https://www.docker.com/products/docker-desktop/
2. **Run the installer** and follow the setup wizard
3. **Restart your computer** when prompted
4. **Start Docker Desktop** from your Start menu

### Option B: Docker with WSL 2 (Advanced users)
1. **Enable WSL 2**: Open PowerShell as Administrator and run:
   ```powershell
   wsl --install
   ```
2. **Install Docker Engine**: Follow instructions at https://docs.docker.com/engine/install/windows/

## Step 2: Verify Docker Installation

Open PowerShell or Command Prompt and run:
```powershell
docker --version
docker-compose version
```

You should see output like:
```
Docker version 24.0.6, build ed223bc
Docker Compose version v2.21.0
```

## Step 3: Create Docker Hub Account

1. **Sign up** at https://hub.docker.com
2. **Verify your email address**
3. **Create a repository** named `HaoXai`

## Step 4: Login to Docker Hub

```powershell
docker login
```
Enter your Docker Hub username and password when prompted.

## Step 5: Build and Publish HaoXai

### Method A: Use the Publishing Script
```powershell
cd "d:\MyFiles\Program_Last_version\ViroDB_structure - Copy"
bash publish.sh
```

### Method B: Manual Commands (if script fails)
```powershell
# Navigate to project directory
cd "d:\MyFiles\Program_Last_version\ViroDB_structure - Copy"

# Build the Docker image
docker build -t longthor/HaoXai:latest .

# Test the image locally
docker run -d --name HaoXai-test -p 5000:5000 longthor/HaoXai:latest

# Wait 30 seconds, then test
Start-Sleep 30
curl http://localhost:5000

# Stop test container
docker stop HaoXai-test
docker rm HaoXai-test

# Push to Docker Hub
docker push longthor/HaoXai:latest
```

## Step 6: Verify Publication

1. **Check your Docker Hub repository**: https://hub.docker.com/r/longthor/HaoXai
2. **Test pulling your image**:
   ```powershell
   docker pull longthor/HaoXai:latest
   ```

## Troubleshooting

### Docker Desktop Issues
- **Make sure WSL 2 is enabled**: `wsl --install`
- **Check virtualization is enabled** in BIOS
- **Restart Docker Desktop** if it fails to start

### Build Issues
- **Clear Docker cache**: `docker system prune -a`
- **Check for port conflicts**: Make sure port 5000 is free
- **Verify Dockerfile exists** in the project directory

### Push Issues
- **Check login**: `docker login` again
- **Verify repository name** matches your Docker Hub username
- **Check internet connection**

## Alternative: Use GitHub Actions (Advanced)

If you can't install Docker locally, you can use GitHub Actions to build and publish:

1. **Create GitHub repository** with your code
2. **Create `.github/workflows/docker.yml`**:
   ```yaml
   name: Build and Publish Docker Image
   on:
     push:
       tags:
         - 'v*'
   jobs:
     build-and-push:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v3
         - name: Login to Docker Hub
           uses: docker/login-action@v2
           with:
             username: ${{ secrets.DOCKER_USERNAME }}
             password: ${{ secrets.DOCKER_PASSWORD }}
         - name: Build and push
           uses: docker/build-push-action@v4
           with:
             context: .
             push: true
             tags: longthor/HaoXai:latest
   ```

## After Installation

Once Docker is installed and working:
1. **Run the publishing script** again
2. **Your HaoXai will be available** at: https://hub.docker.com/r/longthor/HaoXai
3. **Users can install** with: `docker run -p 5000:5000 longthor/HaoXai:latest`

## Need Help?

- **Docker Documentation**: https://docs.docker.com/
- **Docker Desktop Issues**: https://docs.docker.com/desktop/troubleshoot/
- **Windows WSL Issues**: https://learn.microsoft.com/en-us/windows/wsl/troubleshooting

---

**Next Steps:**
1. Install Docker Desktop
2. Restart your computer
3. Run the publishing script again
4. Your HaoXai will be published to Docker Hub!
