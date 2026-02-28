#!/bin/bash

# Detect OS and set the correct stat command
if [[ "$OSTYPE" == "darwin"* ]]; then
    # MacOS
    GET_SIZE="stat -f%z"
else
    # Linux (Docker)
    GET_SIZE="stat -c%s"
fi

input_data() {
    local filename=$1
    if [ -z "$filename" ]; then
        echo "No filename provided. Enter one now:"
        read filename
    fi

    if [ -f "$filename" ]; then
        echo "File: $filename"
        # Use the dynamically detected command
        local size=$($GET_SIZE "$filename")
        echo "Initial Size: $size bytes"

        local limit=$(( 10 * 1024 * 1024 ))
        echo "Growing file to $limit bytes..."
        while [ $size -lt $limit ]; do
            echo "Adding a line of data at $(date "+%Y-%m-%d %H:%M:%S")" >> "$filename"
            size=$($GET_SIZE "$filename")
        done
        echo "Final Size: $size bytes"
    else
        # Use -p for Bash/Linux, or default for macOS zsh/sh
        echo -n "File '$filename' not found. Create it? (y/n): "
        read choice
        if [ "$choice" = "y" ]; then
            touch "$filename"
            input_data "$filename"
        fi
    fi
}

input_data "$1"
