# importing libraries
import shutil
import os
from pathlib import Path
import subprocess 
import docker

class sandbox_env():
    def __init__(self):
        # Get the directory of this script (scripts/)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # The project root is one level up
        self.root_dir = os.path.dirname(script_dir)
        self.path = os.path.join(self.root_dir, 'sandbox')
        self.cores = 2
        self.ram = 2048
        self.memory = 4096
        self.create_sandbox(self.cores, self.ram, self.memory)

    def create_sandbox(self, cores, ram, memory):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)
            print(f"Sandbox directory deleted at {self.path}")
        
        try:
            os.makedirs(self.path, exist_ok=True)
            print(f"Sandbox directory created at {self.path}")

            # Define the DMG path in the root directory
            dmg_path = os.path.join(self.root_dir, "sandbox.dmg")
            if os.path.exists(dmg_path):
                os.remove(dmg_path)
                print(f"Existing DMG deleted at {dmg_path}")

            cmd = ["hdiutil", "create", "-size", f"{memory}m", "-fs", "APFS", "-volname", "sandbox", dmg_path]
            subprocess.run(cmd, check=True)
            print(f"New DMG created at {dmg_path}")

            client = docker.from_env()
            # Build using the root_dir where the dockerfile is located
            # Note: Docker usually looks for 'Dockerfile' (Capital 'D')
            image, logs = client.images.build(path=self.root_dir, tag="sandbox", rm=True)
            print("Docker image 'sandbox' built successfully.")

            container = client.containers.run( 
                image="sandbox", 
                detach=True, 
                name="sandbox",
                mem_limit=f"{ram}m",
                nano_cpus=int(cores * 1e9),
                volumes={
                    self.path: {"bind": "/app/data", "mode": "rw"}
                }
            )

        except Exception as e:
            print(f"Error creating sandbox: {e}")

    

        
            



