#!/bin/bash

# Command to be executed
COMMAND="pytest tests/transforms"

# Loop to run the command 100 times
for ((i=1; i<=10000; i++))
do
    # Execute the command
    $COMMAND
done
