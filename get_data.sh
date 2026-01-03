#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <remote_folder> <hosts_file> <local_destination>"
    exit 1
fi

# Input arguments
REMOTE_FOLDER=$1
HOSTS_FILE=$2
LOCAL_DEST=$3

# Check if the hosts file exists
if [ ! -f "$HOSTS_FILE" ]; then
    echo "Error: Hosts file '$HOSTS_FILE' does not exist."
    exit 1
fi

# Check if the local destination exists, if not create it
if [ ! -d "$LOCAL_DEST" ]; then
    echo "Local destination '$LOCAL_DEST' does not exist. Creating..."
    mkdir -p "$LOCAL_DEST"
fi

# Iterate through each host in the hosts file
while IFS= read -r HOST; do
    if [ -n "$HOST" ]; then
        echo "Fetching *.out files from $HOST:$REMOTE_FOLDER to $LOCAL_DEST"
        scp "$HOST:$REMOTE_FOLDER"/*.out "$LOCAL_DEST"/ 2>/dev/null

        if [ $? -eq 0 ]; then
            echo "Successfully fetched files from $HOST"
        else
            echo "Failed to fetch files from $HOST or no *.out files found."
        fi
    fi
done < "$HOSTS_FILE"

echo "Operation completed."