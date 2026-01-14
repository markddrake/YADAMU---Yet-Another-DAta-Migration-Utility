@echo off
REM Build all YADAMU Docker images using Docker Compose
REM Build order matches dependency chain

set DOCKER_BUILDKIT=1
set COMPOSE_FILE=docker\dockerfiles\linux\build-yadamu.yml

REM Build in dependency order
docker compose build environment
docker compose build base
docker compose build regression
docker compose build secure
docker compose build commandline
docker compose build service

echo All images built successfully!
docker image prune -f
echo Build artifacts cleaned up!