#!/usr/bin/env bash

LOG_ROOT="/mount/log"

# Header
printf "%-32s %-32s %8s %8s %8s %8s %15s\n" \
       "folder" "run_id" "lines" "errors" "warnings" "failed" "elapsed"


# Build sorted list of top-level folders by creation time
top_folders=$(for d in "$LOG_ROOT"/*; do
    [ -d "$d" ] || continue
    ctime=$(stat -c %Y "$d" 2>/dev/null || echo 0)
    echo "$ctime|$d"
done | sort -n | cut -d'|' -f2-)


# Iterate sorted suites
for suite in $top_folders; do
    suite_name=$(basename "$suite")

    # Iterate timestamp subfolders (do *not* sort inside — keep your existing structure)
    for run in "$suite"/*; do
        [ -d "$run" ] || continue
        run_id=$(basename "$run")

        log_file="$run/$suite_name.log"
        [ -f "$log_file" ] || continue

        lines=$(wc -l < "$log_file")

        # Last non-empty line
        last_line=$(tac "$log_file" | sed '/./q')

        # Extract values
        errors=$(echo "$last_line"   | sed -n 's/.*Errors: \([0-9]*\).*/\1/p')
        warnings=$(echo "$last_line" | sed -n 's/.*Warnings: \([0-9]*\).*/\1/p')
        failed=$(echo "$last_line"   | sed -n 's/.*Failed: \([0-9]*\).*/\1/p')
        elapsed=$(echo "$last_line"  | sed -n 's/.*Elapsed Time: \([^ .]*\).*/\1/p')

        printf "%-32s %-32s %8s %8s %8s %8s %15s\n" \
            "$suite_name" "$run_id" "$lines" \
            "${errors:-}" "${warnings:-}" "${failed:-}" "${elapsed:-}"
    done
done
