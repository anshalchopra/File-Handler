# Project Scope

## 1. Project Overview

File Handler is a Python-based utility designed for optimized CRUD and transformation operations within a local file system. The project focuses on minimizing memory and storage footprints while maximizing performance under resource-constrained environments. It aims to simulate execution on low-resource cloud instances or legacy hardware, ensuring efficient data processing even when system resources are severely limited.

## 2. Task Objectives

- **Resource-Constrained Simulation**: Develop a sandboxed environment to strictly limit CPU, RAM, and storage availability, enabling performance benchmarking under severe hardware constraints.
- **Multi-Format Data Ingestion**: Implement a robust ingestion engine supporting diverse formats (CSV, JSON, Blobs) and processing modes (Batch, Streaming). Conduct stress testing via incremental load increases to identify system bottlenecks and ensure stability.
