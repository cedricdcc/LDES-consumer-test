# LDES Consumer Docker Image

This Docker image reads and processes an `ldes-feeds.yaml` configuration file from a mounted volume and spawns child Docker containers for each feed using the `ldes2sparql` image.

## Features

- **YAML Configuration**: Reads feed configurations from `ldes-feeds.yaml`
- **Container Spawning**: Automatically spawns a Docker container for each feed
- **Docker Compose Integration**: Spawned containers inherit the parent's Docker Compose project labels
- **Logging**: Automatically configures state and performance logging to the `/data` volume
- **Environment Variables**: Passes feed-specific environment variables to child containers
- **Graceful Shutdown**: Handles cleanup of spawned containers on shutdown
- **Container Monitoring**: Monitors the status of spawned containers

## Logging

The LDES consumer automatically configures logging for all spawned containers:

- **State files**: Each feed gets its own state directory in `/data/ldes-consumer/state/{feed_name}/` (maps to `/state` in child containers)
- Each feed maintains its own LDES client state for resuming and tracking progress

### Log Directory Structure

```
data/
├── ldes-feeds.yaml
└── ldes-consumer/
    └── state/
        ├── feed-1/         # State files for feed-1
        ├── feed-2/         # State files for feed-2
        └── feed-n/         # State files for feed-n
```

## Requirements

The Docker socket must be mounted to allow the container to spawn child containers:
- Mount `/var/run/docker.sock:/var/run/docker.sock`

## Building the Docker Image

```bash
cd ldes-consumer
docker build -t ldes-consumer:latest .
```

## Running the Container

To run the container with a mounted volume containing your `ldes-feeds.yaml` file:

```bash
docker run -v /path/to/your/data:/data -v /var/run/docker.sock:/var/run/docker.sock ldes-consumer:latest
```

**Important**: The Docker socket (`/var/run/docker.sock`) must be mounted to allow the container to spawn child containers.

### Example with the data folder from this repository:

```bash
# From the root of the repository
docker run -v "$(pwd)/data:/data" -v /var/run/docker.sock:/var/run/docker.sock ldes-consumer:latest
```

### Windows PowerShell:

```powershell
docker run -v "${PWD}/data:/data" -v /var/run/docker.sock:/var/run/docker.sock ldes-consumer:latest
```

### Windows CMD:

```cmd
docker run -v "%cd%/data:/data" -v /var/run/docker.sock:/var/run/docker.sock ldes-consumer:latest
```

## Configuration

The application expects the configuration file at `/data/ldes-feeds.yaml` inside the container. You can customize this path by setting the `LDES_CONFIG_PATH` environment variable:

```bash
docker run -v /path/to/data:/my-data -e LDES_CONFIG_PATH=/my-data/ldes-feeds.yaml ldes-consumer:latest
```

### Environment Variables

- **`LDES_CONFIG_PATH`**: Path to the YAML configuration file (default: `/data/ldes-feeds.yaml`)
- **`LDES2SPARQL_IMAGE`**: Docker image to use for spawning child containers (default: `ghcr.io/maregraph-eu/ldes2sparql:latest`)
- **`DOCKER_NETWORK`**: Docker network to attach spawned containers to (optional)
- **`HOST_PWD`**: Host working directory for volume mounts (optional)
- **`DEFAULT_SPARQL_ENDPOINT`**: Default SPARQL endpoint for spawned containers (optional, but recommended)
- **`GRAPH_PREFIX`**: Prefix used in target graph URN pattern (default: `"ldes"`, used in `urn:kgap:{prefix}:{feedname}`)

### LDES2SPARQL Container Environment Variables

The LDES consumer automatically sets the following environment variables for each spawned ldes2sparql container. **All of these can be overridden** in the feed's `environment` section in your YAML configuration:

#### Default Environment Variables:

- **`FEED_NAME`**: The feed identifier from the YAML configuration
- **`FEED_URL`**: The feed URL from the YAML configuration
- **`LDES`**: The LDES feed URL (same as FEED_URL, required by ldes2sparql)
- **`PERF_NAME`**: Performance logging identifier (same as FEED_NAME)
- **`SPARQL_ENDPOINT`**: The target SPARQL endpoint URL (default: from `DEFAULT_SPARQL_ENDPOINT` env var)
- **`TARGET_GRAPH`**: The target named graph IRI (default: `urn:kgap:{GRAPH_PREFIX}:{feedname}`)
- **`OPERATION_MODE`**: Either "Sync" or "Replication" (default: `"Replication"`)
  - **Sync**: Uses SPARQL UPDATE protocol, supports Create/Update/Delete operations
  - **Replication**: Uses Graph Store Protocol, faster but only supports additions
- **`FOLLOW`**: Whether to continuously follow the feed (default: `"false"`)
- **`MEMBER_BATCH_SIZE`**: Number of members to process in each batch (default: `"500"`)
- **`SHAPE`**: SHACL shape for validation (default: `""` - empty string to prevent crashes)

#### Overriding Defaults

You can override **any** of these defaults for specific feeds in your `ldes-feeds.yaml`:

```yaml
feeds:
  my-feed:
    url: https://example.com/feed.ttl
    target_graph: urn:custom:graph:my-feed  # Alternative: set at feed level
    environment:
      # Override any default environment variable
      OPERATION_MODE: "Sync"  # Override to use Sync mode
      FOLLOW: "true"  # Enable continuous following
      MEMBER_BATCH_SIZE: "1000"  # Process 1000 members per batch
      SPARQL_ENDPOINT: "http://custom-endpoint:8890/sparql"  # Custom endpoint
      TARGET_GRAPH: "urn:custom:graph:my-feed"  # Custom target graph IRI
      # Add any additional custom variables
      MATERIALIZE: "true"
      CUSTOM_VAR: "value"
```

## Expected YAML Format

The `ldes-feeds.yaml` file should have the following structure:

```yaml
feeds:
  feed-name:
    url: https://example.com/feed.ttl
    target_graph: urn:custom:graph:feed-name  # Optional: custom target graph IRI
    environment:
      KEY: "value"
      MATERIALIZE: "true"
```

**Note**: The `target_graph` field can be set at the feed level (as shown above) or in the `environment` section as `TARGET_GRAPH`.

### How it Works

For each feed defined in the YAML file:
1. The application reads the feed configuration
2. Default environment variables are set for all required parameters
3. Any values in the feed's `environment` section override the defaults
4. A per-feed state directory is created at `/data/ldes-consumer/state/{feed_name}/`
5. A child Docker container is spawned using the `LDES2SPARQL_IMAGE` with the final environment variables
6. The feed-specific state directory is mounted to `/state` in the child container
7. If running in Docker Compose, child containers inherit the project labels and network

Example: For the `bodc-P02` feed with URL `https://kgfixed.github.io/vocab.nerc.ac.uk/P02/latest.ttl` and `MATERIALIZE: "false"`, the spawned container will have:
- `FEED_NAME=bodc-P02`
- `FEED_URL=https://kgfixed.github.io/vocab.nerc.ac.uk/P02/latest.ttl`
- `LDES=https://kgfixed.github.io/vocab.nerc.ac.uk/P02/latest.ttl`
- `PERF_NAME=bodc-P02`
- `SPARQL_ENDPOINT=<from DEFAULT_SPARQL_ENDPOINT>`
- `TARGET_GRAPH=urn:kgap:ldes:bodc-P02`
- `OPERATION_MODE=Replication`
- `FOLLOW=false`
- `MEMBER_BATCH_SIZE=500`
- `SHAPE=`
- `MATERIALIZE=false` (custom override from environment)
- Volume: `/data/ldes-consumer/state/bodc-P02` → `/state`

## Using with Docker Compose

The root `docker-compose.yaml` is already configured to run the LDES consumer with proper volume mounts:

```bash
# From the root of the repository
docker-compose up ldes-consumer
```

### Docker Compose Project Integration

When running via Docker Compose, the LDES consumer automatically:
- Detects it's part of a Docker Compose project
- Applies the same project labels to all spawned child containers
- This ensures spawned containers appear under the same project in `docker-compose ps` and similar commands

To add this service to your own `docker-compose.yaml`:

```yaml
services:
  ldes-consumer:
    build: ./ldes-consumer
    volumes:
      - ./data:/data
      - /var/run/docker.sock:/var/run/docker.sock  # Required!
    environment:
      - LDES_CONFIG_PATH=/data/ldes-feeds.yaml
      - LDES2SPARQL_IMAGE=ghcr.io/maregraph-eu/ldes2sparql:latest
      - DOCKER_NETWORK=${COMPOSE_PROJECT_NAME:-kgap}_default
```

**Note**: The Docker socket mount is crucial for spawning child containers.
