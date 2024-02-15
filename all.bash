#!/usr/bin/env bash

function test() {
    docker-compose down --remove-orphans
    docker-compose up -d
    sleep 1
    go mod tidy
    gotestsum -- -v -race -json ./...
    docker-compose down --remove-orphans
}

$1;
