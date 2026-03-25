"""
Sandbox Environment Management
------------------------------
This module provides the SandboxEnv class, which handles the lifecycle of the 
Docker-based sandbox environment. It automates image building, container 
resource limitation, and environment provisioning.
"""

import os
import sys
import docker
from pathlib import Path

class SandboxEnv:
    def __init__(self, cores: int = 1, ram: int = 512):
        # Removed the unused 'memory' argument to keep the signature clean
        script_dir = Path(__file__).resolve().parent
        self.root_dir = script_dir.parent
        
        self.cores = cores
        self.ram = ram
        
        self.create_sandbox()

    def _get_docker_client(self):
        """Helper to get a Docker client dynamically across different OS environments."""
        try:
            # 1. Try standard environment variables (works for most Linux/Windows)
            return docker.from_env()
        except docker.errors.DockerException:
            # 2. Dynamic Fallback for macOS Desktop Docker
            mac_socket = f"unix://{os.path.expanduser('~')}/.docker/run/docker.sock"
            return docker.DockerClient(base_url=mac_socket)

    def _get_host_socket_path(self):
        """Determines the correct host socket path to mount based on the OS."""
        mac_socket = f"{os.path.expanduser('~')}/.docker/run/docker.sock"
        if os.path.exists(mac_socket):
            return mac_socket
        return "/var/run/docker.sock" # Default Linux path

    def create_sandbox(self):
        print(f"🛠️  Initializing Sandbox Environment...")

        try:
            client = self._get_docker_client()
            client.ping()

            # --- 1. IMAGE PROVISIONING (WITH LIVE LOGS) ---
            print("🏗️  Building Docker image 'sandbox' (this may take a moment)...")
            
            # Use low-level API to stream logs so the terminal doesn't freeze
            resp = client.api.build(path=str(self.root_dir), tag="sandbox", rm=True, decode=True)
            for chunk in resp:
                if 'stream' in chunk:
                    # Print the Dockerfile build steps directly to the terminal
                    sys.stdout.write(chunk['stream'])
                    sys.stdout.flush()
                elif 'error' in chunk:
                    raise Exception(chunk['error'])
                    
            print("\n✅ Docker image 'sandbox' built successfully.")

            # --- 2. CLEANUP PREVIOUS STATE ---
            try:
                existing_container = client.containers.get("sandbox")
                print("🔄 Cleaning up existing sandbox container...")
                existing_container.stop()
                existing_container.remove()
            except docker.errors.NotFound:
                pass

            # --- 3. CONTAINER DEPLOYMENT ---
            host_socket = self._get_host_socket_path()
            print(f"🚀 Launching sandbox with {self.cores} core(s) and {self.ram}MB RAM...")
            
            container = client.containers.run(
                image="sandbox",
                detach=True,
                name="sandbox",
                mem_limit=f"{self.ram}m",
                nano_cpus=int(self.cores * 1e9),
                ports={
                    8501: 8501,  
                    8000: 8000   
                },
                volumes={
                    # Dynamically mounts the correct socket for the current user/OS
                    host_socket: {"bind": "/var/run/docker.sock", "mode": "rw"},
                    str(self.root_dir / "data"): {"bind": "/sandbox/app/data", "mode": "rw"},
                    str(self.root_dir / "scripts"): {"bind": "/sandbox/app/scripts", "mode": "rw"}
                }
            )

            print("✨ Sandbox container 'sandbox' is now healthy and active!")
            print("🔗 Dashboard accessible at: http://localhost:8501")
            print("🔗 API accessible at: http://localhost:8000")

        except Exception as e:
            print(f"\n❌ Sandbox Provisioning Failed: {e}")

if __name__ == "__main__":
    # Simplified arguments matching the __init__ signature
    if len(sys.argv) < 3:
        print("Usage: python custom_sandbox.py <cores> <ram>")
        print("Creating a sandbox with default configurations (1 Core, 512MB RAM)")
        instance = SandboxEnv()
    else:
        instance = SandboxEnv(cores=int(sys.argv[1]), ram=int(sys.argv[2]))