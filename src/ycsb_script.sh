FILE=./ycsb.config
# Re write this so that it checks if there's a YCSB directory and the executable file within.

if [ -f "$FILE" ]; then
	echo  "Client X has YCSB installed"
else 
	echo "Client X does not have YCSB installed. Removing node..."
fi
