#!/bin/bash

success_count=0
run_count=0

while [ $run_count -lt 10 ]; do
    output=$(go test  -run 3B | tee output.log | grep -E "PASS|FAIL")
    if echo "$output" | grep -q "FAIL"; then
        echo "FAIL"
    else
        echo "PASS"
        ((success_count++)) 
    fi
    tail -n 1 output.log
    ((run_count++))
    sleep 1
done

echo "PASS: $success_count/$run_count"