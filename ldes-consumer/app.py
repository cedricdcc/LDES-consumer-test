#!/usr/bin/env python3
"""
LDES Consumer Application
Reads and processes ldes-feeds.yaml from a mounted volume
Spawns Docker containers for each feed using ldes2sparql
"""

import yaml
import os
import sys
import signal
import time
from pathlib import Path
from typing import Dict, List

import docker
from docker.errors import DockerException, APIError


def get_compose_labels(docker_client):
    """
    Detect if the current container is part of a Docker Compose project
    and return its labels to apply to spawned containers.

    Args:
        docker_client: Docker client instance

    Returns:
        dict: Dictionary of compose-related labels, or empty dict if not in compose
    """
    try:
        # Get the current container's hostname (container ID)
        hostname = os.getenv("HOSTNAME")
        if not hostname:
            return {}

        # Get the current container
        container = docker_client.containers.get(hostname)
        labels = container.labels

        # Extract compose-related labels
        compose_labels = {}
        compose_keys = [
            "com.docker.compose.project",
            "com.docker.compose.project.working_dir",
            "com.docker.compose.project.config_files",
        ]

        for key in compose_keys:
            if key in labels:
                compose_labels[key] = labels[key]

        if compose_labels:
            print(
                f"Detected Docker Compose project: {compose_labels.get('com.docker.compose.project', 'unknown')}"
            )

        return compose_labels

    except Exception as e:
        print(f"Could not detect compose project: {e}")
        return {}


def get_parent_network(docker_client):
    """
    Detect the network that the current container is connected to.

    Args:
        docker_client: Docker client instance

    Returns:
        str: Network name, or None if detection fails
    """
    try:
        # Get the current container's hostname (container ID)
        hostname = os.getenv("HOSTNAME")
        if not hostname:
            return None

        # Get the current container
        container = docker_client.containers.get(hostname)
        networks = container.attrs.get("NetworkSettings", {}).get("Networks", {})

        # Get the first network (usually there's only one, or we pick the first)
        if networks:
            network_name = list(networks.keys())[0]
            print(f"Detected parent container network: {network_name}")
            return network_name

        return None

    except Exception as e:
        print(f"Could not detect parent network: {e}")
        return None


def load_ldes_feeds(config_path):
    """
    Load LDES feeds configuration from YAML file

    Args:
        config_path: Path to the ldes-feeds.yaml file

    Returns:
        Dictionary containing the feeds configuration
    """
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file: {e}", file=sys.stderr)
        sys.exit(1)


def process_feeds(feeds_config, docker_client, ldes2sparql_image):
    """
    Process the LDES feeds from configuration and spawn Docker containers

    Args:
        feeds_config: Dictionary containing feeds configuration
        docker_client: Docker client instance
        ldes2sparql_image: Docker image to use for spawning containers

    Returns:
        List of spawned container objects
    """
    if "feeds" not in feeds_config:
        print("Warning: No 'feeds' section found in configuration", file=sys.stderr)
        return []

    feeds = feeds_config["feeds"]
    print(f"Found {len(feeds)} feed(s) in configuration:\n")

    containers = []
    host_pwd = os.getenv("HOST_PWD", os.getcwd())
    graph_prefix = os.getenv("GRAPH_PREFIX", "ldes")

    # Detect compose project labels
    compose_labels = get_compose_labels(docker_client)

    # Detect parent container's network
    docker_network = get_parent_network(docker_client)
    if not docker_network:
        # Fall back to environment variable if detection fails
        docker_network = os.getenv("DOCKER_NETWORK", None)
        if docker_network:
            print(f"Using network from env var: {docker_network}")

    gdb_repo = os.getenv("GDB_REPO", "kgap")
    default_sparql_endpoint: str = os.getenv(
        "DEFAULT_SPARQL_ENDPOINT",
        f"http://graphdb:7200/repositories/{gdb_repo}/statements",
    )
    print(
        f"Default SPARQL endpoint (if not overridden by feed config): {default_sparql_endpoint}\n"
    )

    for feed_name, feed_data in feeds.items():
        print(f"Feed: {feed_name}")
        print(f"  URL: {feed_data.get('url', 'N/A')}")

        # Prepare per-feed volume mounts
        feed_state_dir = f"{host_pwd}/data/ldes-consumer/state/{feed_name}"

        # Ensure state directory exists
        Path(feed_state_dir).mkdir(parents=True, exist_ok=True)

        volumes = {feed_state_dir: {"bind": "/state", "mode": "rw"}}
        print(f"  State directory: {feed_state_dir} -> /state")

        # Get feed-specific configuration
        feed_url = feed_data.get("url", "")
        default_target_graph = f"urn:kgap:{graph_prefix}:{feed_name}"
        feed_env = feed_data.get("environment", {})

        target_graph = feed_env.get(
            "TARGET_GRAPH", feed_data.get("target_graph", default_target_graph)
        )
        print(f"  Target graph: {target_graph}")

        # Build environment variables by retrieving from feed config with defaults
        env_vars = {
            # Core feed identifiers
            "SPARQL_ENDPOINT": feed_env.get(
                "SPARQL_ENDPOINT", os.environ.get("DEFAULT_SPARQL_ENDPOINT", "")
            ),
            "TARGET_GRAPH": target_graph,
            "FOLLOW": feed_env.get("FOLLOW", "false"),
            "MEMBER_BATCH_SIZE": feed_env.get("MEMBER_BATCH_SIZE", "500"),
            "MATERIALIZE": feed_env.get("MATERIALIZE", "false"),
            "LOG_LEVEL": feed_env.get(
                "LOG_LEVEL",
                os.getenv("LDES_LOG_LEVEL", os.getenv("LOG_LEVEL", "DEBUG")),
            ).lower(),
            "LDES": feed_url,
            "POLLING_FREQUENCY": feed_env.get("POLLING_FREQUENCY", "60000"),  # in ms
            "FAILURE_IS_FATAL": feed_env.get("FAILURE_IS_FATAL", "false"),
            "OPERATION_MODE": feed_env.get("OPERATION_MODE", "Replication"),
            "SHAPE": feed_env.get("SHAPE", ""),
        }

        # Add any additional custom environment variables from feed configuration
        for env_key, env_value in feed_env.items():
            if env_key not in env_vars:  # Don't override already-set variables
                env_vars[env_key] = str(env_value)

        # Print all environment variables
        print("  Environment variables:")
        for env_key, env_value in env_vars.items():
            print(f"    {env_key}: {env_value}")

        # Spawn the Docker container
        try:
            print(f"  Spawning container with image: {ldes2sparql_image}")

            container_config = {
                "image": ldes2sparql_image,
                "name": f"ldes-consumer-{feed_name}",
                "environment": env_vars,
                "detach": True,  # Run in background to return container object
                "remove": False,  # Keep containers for inspection and debugging
                "volumes": volumes,  # Mount log directories
            }

            # Add compose labels if parent is in a compose project
            if compose_labels:
                # Add compose service label to identify spawned containers
                labels = compose_labels.copy()
                labels["com.docker.compose.service"] = f"ldes-consumer-{feed_name}"
                container_config["labels"] = labels

            # Add network if specified
            if docker_network:
                container_config["network"] = docker_network

            container = docker_client.containers.run(**container_config)
            containers.append(container)

            print(f"  ✓ Container started: {container.short_id}")
        except DockerException as e:
            print(f"  ✗ Error spawning container: {e}", file=sys.stderr)
        except Exception as e:
            print(f"  ✗ Unexpected error: {e}", file=sys.stderr)

        print()

    return containers


def cleanup_containers(containers: List):
    """
    Stop and remove spawned containers

    Args:
        containers: List of container objects to cleanup
    """
    if not containers:
        return

    print("\n" + "=" * 60)
    print("Cleaning up spawned containers...")
    print("=" * 60)

    for container in containers:
        try:
            container_info = container.attrs.get("Name", container.short_id)
            print(f"Stopping container: {container_info}")
            container.stop(timeout=10)
            print(f"  ✓ Stopped successfully")
            print(f"Removing container: {container_info}")
            container.remove()
            print(f"  ✓ Removed successfully")
        except Exception as e:
            print(f"  ✗ Error cleaning up container: {e}", file=sys.stderr)


def main():
    """Main entry point for the LDES consumer application"""

    # Default path for the configuration file (from mounted volume)
    config_path = os.getenv("LDES_CONFIG_PATH", "/data/ldes-feeds.yaml")

    # Docker image to use for spawning containers
    ldes2sparql_image = os.getenv(
        "LDES2SPARQL_IMAGE", "ghcr.io/maregraph-eu/ldes2sparql:latest"
    )

    print("=" * 60)
    print("LDES Consumer Application")
    print("=" * 60)
    print(f"Reading configuration from: {config_path}")
    print(f"LDES2SPARQL Image: {ldes2sparql_image}\n")

    # Initialize Docker client
    try:
        docker_client = docker.from_env()
        docker_client.ping()
        print("✓ Docker connection established\n")
    except Exception as e:
        print(f"✗ Error connecting to Docker: {e}", file=sys.stderr)
        print(
            "Make sure Docker socket is mounted at /var/run/docker.sock",
            file=sys.stderr,
        )
        sys.exit(1)

    # Load and process the feeds
    feeds_config = load_ldes_feeds(config_path)
    containers = process_feeds(feeds_config, docker_client, ldes2sparql_image)

    print("=" * 60)
    print(f"Configuration loaded successfully!")
    print(f"Spawned {len(containers)} container(s)")
    print("=" * 60)

    # Setup signal handlers for graceful shutdown
    def signal_handler(sig, frame):
        print("\n\nReceived shutdown signal...")
        cleanup_containers(containers)
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Keep the container running and monitor child containers
    print("\nApplication is running. Press Ctrl+C to stop.")
    print("Monitoring spawned containers...\n")

    try:
        while True:
            # instead of individual checking use the docker process sdk listener
            # per container that was spwnaed , it should be linstener based

            # Check status of spawned containers
            active_containers = []
            for container in containers:
                try:
                    container.reload()
                    if container.status == "running":
                        active_containers.append(container)
                except Exception:
                    # Container no longer exists
                    pass

            if active_containers:
                print(
                    f"\r{len(active_containers)}/{len(containers)} containers running",
                    end="",
                    flush=True,
                )
            else:
                print("\n\nAll spawned containers have stopped.")
                print("Continuing to run. Press Ctrl+C to stop.")

            time.sleep(10)

    except KeyboardInterrupt:
        print("\n\nShutting down gracefully...")
        cleanup_containers(containers)


if __name__ == "__main__":
    main()
