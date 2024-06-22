#!/usr/bin/env bash

function test() {
    docker-compose down --remove-orphans
    docker-compose up -d
    sleep 0.3
    go mod tidy
    gotestsum -- -v -race -vet=all -json ./...
    docker-compose down --remove-orphans
}

$1;
