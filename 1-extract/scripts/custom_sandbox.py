"""
Sandbox Environment Management
------------------------------
This module provides the SandboxEnv class, which handles the lifecycle of the 
Docker-based sandbox environment. It automates image building, container 
resource limitation, and environment provisioning.
"""

import os
import docker
from pathlib import Path

class SandboxEnv:
    """
    Manages a Docker-based sandbox environment for localized data processing.
    
    This class encapsulates the logic for:
    1. Connecting to the Docker daemon.
    2. Building the sandbox Docker image.
    3. Managing the lifecycle (start/stop/remove) of the sandbox container.
    4. Enforcing hardware resource limits (CPU, RAM).
    """

    def __init__(self, cores: int = 1, ram: int = 512, memory: int = 4096):
        """
        Initialize the Sandbox environment configuration.

        Args:
            cores (int): Number of CPU cores allowed (supports fractional values).
            ram (int): Memory limit in Megabytes.
            memory (int): Disk storage limit in Megabytes (driver dependent).
        """
        # Resolve absolute paths for reliable multi-platform execution
        script_dir = Path(__file__).resolve().parent
        self.root_dir = script_dir.parent  # Points to '1-extract'
        
        self.cores = cores
        self.ram = ram
        self.memory = memory
        
        # Automatically provision the environment upon object instantiation
        self.create_sandbox()

    def create_sandbox(self):
        """
        Provisions the sandbox by building the image and launching the container.
        Ensures a clean state by removing any existing 'sandbox' containers.
        """
        print(f"🛠️  Initializing Sandbox Environment...")

        try:
            # --- 1. DOCKER CLIENT INITIALIZATION ---
            # Attempt multi-platform connection strategies (Unix socket vs macOS socket)
            try:
                client = docker.from_env()
                client.ping()
            except:
                # Fallback for specific macOS configurations or custom socket paths
                client = docker.DockerClient(base_url="unix:///Users/anshalc/.docker/run/docker.sock")
                client.ping()

            # --- 2. IMAGE PROVISIONING ---
            # Build the image from the Dockerfile located in the root directory
            print("🏗️  Building Docker image 'sandbox'...")
            client.images.build(path=str(self.root_dir), tag="sandbox", rm=True)
            print("✅ Docker image 'sandbox' built successfully.")

            # --- 3. CLEANUP PREVIOUS STATE ---
            # Remove existing container to ensure fresh resource allocation and environment
            try:
                existing_container = client.containers.get("sandbox")
                print("🔄 Cleaning up existing sandbox container...")
                existing_container.stop()
                existing_container.remove()
            except docker.errors.NotFound:
                pass

            # --- 4. CONTAINER DEPLOYMENT ---
            # Launch the container with explicit resource limits and volume mounts
            print(f"🚀 Launching sandbox with {self.cores} core(s) and {self.ram}MB RAM...")
            
            client.containers.run(
                image="sandbox",
                detach=True,
                name="sandbox",
                mem_limit=f"{self.ram}m",
                nano_cpus=int(self.cores * 1e9),  # Convert unit cores to Docker nanocpus
                ports={
                    8501: 8501,  # Streamlit Dashboard Port
                    8000: 8000   # API Endpoint Port
                },
                volumes={
                    # Mount host paths for real-time monitoring and data persistence
                    "/Users/anshalc/.docker/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
                    str(self.root_dir / "data"): {"bind": "/sandbox/app/data", "mode": "rw"},
                    str(self.root_dir / "scripts"): {"bind": "/sandbox/app/scripts", "mode": "rw"}
                }
            )

            print("✨ Sandbox container 'sandbox' is now healthy and active!")
            print("🔗 Dashboard accessible at: http://localhost:8501")

        except docker.errors.DockerException as de:
            print(f"❌ Docker daemon error: {de}. Verification needed for Docker service status.")
        except Exception as e:
            print(f"❌ Sandbox Provisioning Failed: {e}")

if __name__ == "__main__":
    # Standard entry point for manual sandbox verification
    # Configured with 1 core and 512MB RAM by default
    instance = SandboxEnv(cores=1, ram=512)
