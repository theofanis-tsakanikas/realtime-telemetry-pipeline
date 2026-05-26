#!/bin/bash

# Define the path to the docker-compose file
COMPOSE_FILE="infra/docker-compose.yml"

# Function to display usage instructions
usage() {
    echo "Usage: $0 [command]"
    echo "Available commands:"
    echo "  up        - Start all containers in the background"
    echo "  down      - Stop and remove all containers"
    echo "  build     - Build or rebuild Docker images"
    echo "  restart   - Restart all containers"
    echo "  logs      - View live logs of the containers"
    echo "  ps        - Check status of the containers"
    exit 1
}

# Check if no arguments were passed
if [ $# -eq 0 ]; then
    usage
fi

# Read the first argument passed to the script
COMMAND=$1

case "$COMMAND" in
    up)
        echo "🐳 Starting Docker infrastructure..."
        docker compose -f $COMPOSE_FILE up -d
        ;;
    down)
        echo "🛑 Stopping and removing Docker infrastructure..."
        docker compose -f $COMPOSE_FILE down
        ;;
    build)
        echo "🛠️ Building custom Docker images..."
        docker compose -f $COMPOSE_FILE build
        ;;
    restart)
        echo "🔄 Restarting Docker infrastructure..."
        docker compose -f $COMPOSE_FILE restart
        ;;
    logs)
        echo "📜 Showing live container logs (Ctrl+C to exit)..."
        docker compose -f $COMPOSE_FILE logs -f
        ;;
    ps)
        echo "📊 Container Status:"
        docker compose -f $COMPOSE_FILE ps
        ;;
    *)
        echo "❌ Invalid command: $COMMAND"
        usage
        ;;
esac