#!/usr/bin/env bash
FILE=/ycsb.config
# Re write this so that it checks if there's a YCSB directory and the executable file within.

if [ -f "$FILE" ]; then
	echo  "Client X has YCSB installed"
	exit 1 # True
else 
	echo "Client X does not have YCSB installed. Removing node..."
	exit 0
fi
