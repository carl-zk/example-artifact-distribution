# example-artifact-distribution

Distributed Artifact Distribution System

# features

- big file upload with grpc streaming
- web UI for upload/download management
- hash verification for file integrity
- support for multiple agents to distribute files
- backpressure handling by agents to prevent overload
- resume support for interrupted uploads
- realtime progress updates via server-sent events (SSE)
- agent health monitoring and reporting
- agent auto connection management (retries, backoff, etc.)

# architecture

now it's only a simple version for development,

```
Web UI
 ↓ http    ↑ sse  
Server (single node)
 ↓ ↑ gRPC streaming
Agents
```

Future would be a distributed system with multiple nodes, and the architecture may look like this:

```
Web UI
 ↓ http
Gateway    ---> Redis (for coordination, state management like <agent, server> mapping, etc.)
 ↓ custom routing
Server (multiple nodes)
 ↓ gRPC streaming (long-lived connections, stateful)
Agents     ---> Redis (pub/sub for realtime progressess updates)
```

or task-scope living gRPC connections, with a service registry for routing:

```
Web UI
 ↓ http
Nginx (for load balancing)
 ↓ http
Server (multiple nodes) ---> Service Registry (for routing to correct agents based on tasks)
 ↓ gRPC streaming (short-lived connections, stateless)
Agents ---> Service Registry
```

[Web UI](https://github.com/carl-zk/artifact-distribute-manager)

## concerns

virtual threads + traditional jpa maybe also works, be careful.

## references

[https://www.baeldung.com/spring-webflux-upload-multiple-files](https://www.baeldung.com/spring-webflux-upload-multiple-files)