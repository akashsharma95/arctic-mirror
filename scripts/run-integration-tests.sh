#!/bin/bash

# Integration Test Runner Script
# This script helps start Docker and run integration tests

set -e

echo "🚀 Arctic Mirror Integration Test Runner"
echo "========================================"

# Function to check if Docker is running
check_docker() {
    if docker ps >/dev/null 2>&1; then
        echo "✅ Docker is running"
        return 0
    else
        echo "❌ Docker is not running"
        return 1
    fi
}

# Function to start Docker
start_docker() {
    echo "🐳 Starting Docker daemon..."
    
    # Try different methods to start Docker
    if command -v systemctl >/dev/null 2>&1; then
        echo "   Using systemctl..."
        sudo systemctl start docker
        sleep 5
    else
        echo "   Using dockerd directly..."
        sudo dockerd --host=unix:///var/run/docker.sock --host=tcp://0.0.0.0:2376 &
        sleep 10
    fi
    
    # Wait for Docker to be ready
    echo "   Waiting for Docker to be ready..."
    for i in {1..30}; do
        if check_docker; then
            echo "✅ Docker started successfully"
            return 0
        fi
        echo "   Attempt $i/30 - waiting..."
        sleep 2
    done
    
    echo "❌ Failed to start Docker after 30 attempts"
    return 1
}

# Function to run integration tests
run_tests() {
    echo "🧪 Running integration tests..."
    
    # Check if we're in the right directory
    if [ ! -f "go.mod" ]; then
        echo "❌ Error: Please run this script from the project root directory"
        exit 1
    fi
    
    # Run the tests
    echo "   Running: go test ./tests/integration/... -v"
    go test ./tests/integration/... -v
    
    if [ $? -eq 0 ]; then
        echo "✅ All integration tests passed!"
    else
        echo "❌ Some integration tests failed"
        exit 1
    fi
}

# Function to run specific test
run_specific_test() {
    local test_name=$1
    echo "🧪 Running specific test: $test_name"
    
    go test ./tests/integration/... -run "$test_name" -v
}

# Function to show help
show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -h, --help              Show this help message"
    echo "  -t, --test TEST_NAME    Run a specific test (e.g., TestEndToEndIntegration)"
    echo "  -s, --start-docker      Only start Docker, don't run tests"
    echo "  -c, --check-docker      Only check Docker status"
    echo ""
    echo "Examples:"
    echo "  $0                      # Start Docker and run all integration tests"
    echo "  $0 -t TestEndToEndIntegration  # Run only end-to-end tests"
    echo "  $0 -s                   # Only start Docker"
    echo "  $0 -c                   # Only check Docker status"
}

# Main script logic
main() {
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_help
                exit 0
                ;;
            -t|--test)
                TEST_NAME="$2"
                shift 2
                ;;
            -s|--start-docker)
                START_DOCKER_ONLY=true
                shift
                ;;
            -c|--check-docker)
                CHECK_DOCKER_ONLY=true
                shift
                ;;
            *)
                echo "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    # Check Docker status
    if ! check_docker; then
        if [ "$CHECK_DOCKER_ONLY" = true ]; then
            echo "❌ Docker is not running"
            exit 1
        fi
        
        echo "🔄 Docker not running, attempting to start..."
        if ! start_docker; then
            echo "❌ Failed to start Docker. Please start it manually and try again."
            echo ""
            echo "Manual Docker start commands:"
            echo "  sudo systemctl start docker"
            echo "  # or"
            echo "  sudo dockerd --host=unix:///var/run/docker.sock"
            exit 1
        fi
    fi
    
    # If only starting Docker, exit here
    if [ "$START_DOCKER_ONLY" = true ]; then
        echo "✅ Docker is running. You can now run tests manually with:"
        echo "   go test ./tests/integration/... -v"
        exit 0
    fi
    
    # If only checking Docker, exit here
    if [ "$CHECK_DOCKER_ONLY" = true ]; then
        exit 0
    fi
    
    # Run tests
    if [ -n "$TEST_NAME" ]; then
        run_specific_test "$TEST_NAME"
    else
        run_tests
    fi
}

# Run main function with all arguments
main "$@"