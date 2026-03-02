import os
import shutil
from pathlib import Path
import docker

class SandboxEnv:
    def __init__(self, cores: int = 2, ram: int = 2048, memory: int = 4096):
        # Get the directory of this script and project root
        script_dir = Path(__file__).resolve().parent
        self.root_dir = script_dir.parent
        self.path = self.root_dir / "sandbox"
        
        self.cores = cores
        self.ram = ram
        self.memory = memory
        
        self.create_sandbox()

    def create_sandbox(self):
        """Cleans the sandbox directory and deploys the Docker environment."""
        # Clean up existing sandbox directory
        if self.path.exists():
            shutil.rmtree(self.path)
            print(f"Sandbox directory deleted at {self.path}")

        try:
            # Create the sandbox directory
            self.path.mkdir(parents=True, exist_ok=True)
            print(f"Sandbox directory created at {self.path}")

            # Initialize Docker client
            client = docker.from_env()

            # Build the Docker image
            print("Building Docker image 'sandbox'...")
            client.images.build(path=str(self.root_dir), tag="sandbox", rm=True)
            print("Docker image 'sandbox' built successfully.")

            # Remove existing container if it exists
            try:
                existing_container = client.containers.get("sandbox")
                print("Removing existing sandbox container...")
                existing_container.stop()
                existing_container.remove()
            except docker.errors.NotFound:
                pass

            # Run the container with resource limits
            client.containers.run(
                image="sandbox",
                detach=True,
                name="sandbox",
                storage_opt={"size": f"{self.memory}G"},
                mem_limit=f"{self.ram}m",
                nano_cpus=int(self.cores * 1e9),
                ports={8501: 8501, 8000: 8000},
                volumes={
                    str(self.path): {"bind": "/app/data", "mode": "rw"},
                    str(self.root_dir / "scripts"): {"bind": "/app/scripts", "mode": "rw"}
                }
            )
            print("🚀 Sandbox container 'sandbox' started successfully!")

            print("📁 Dashboard access: http://localhost:8501")

        except Exception as e:

            print(f"Error creating sandbox: {e}")
