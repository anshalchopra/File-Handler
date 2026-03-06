# Project Scope

## 1. Project Overview

File Handler is a Python-based utility designed for optimized CRUD and transformation operations within a local file system. The project focuses on minimizing memory and storage footprints while maximizing performance under resource-constrained environments. It aims to simulate execution on low-resource cloud instances or legacy hardware, ensuring efficient data processing even when system resources are severely limited.

## 2. Task Objectives

- **Resource-Constrained Simulation**: Develop a sandboxed environment to strictly limit CPU, RAM, and storage availability, enabling performance benchmarking under severe hardware constraints.
- **Multi-Format Data Ingestion**: Implement a robust ingestion engine supporting JSON REST APIs and Streaming Data. Conduct stress testing via incremental load increases to identify system bottlenecks and ensure stability.
- **Optimized Batch Storage**: Architect a local data lake using JSON for batch storage of streaming data, focusing on storage efficiency and rapid retrieval.
- **High-Throughput Data Processing**: Implement batch processing pipelines to transform and transmit data to a containerized Data Warehouse via optimized API endpoints.
- **Performance Visualization**: Integrate a dashboard to extract and visualize warehouse data through Power BI or Tableau, providing insights into processing efficiency and data trends.