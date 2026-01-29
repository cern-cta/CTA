#!/bin/bash

LOGFILE="var/log/cta/cta-taped.log"
# SEARCH_STRING="ArchiveMount::setJobBatchTransferred(): received a job to send to report queue"

for SEARCH_STRING in "ArchiveMount::setJobBatchTransferred(): received a job to send to report queue" "File successfully transmitted to drive"
do
    # Get first and last occurrences with timestamps
    FIRST=$(grep "$SEARCH_STRING" "$LOGFILE" | head -1)
    LAST=$(grep "$SEARCH_STRING" "$LOGFILE" | tail -1)

    # Count total occurrences
    TOTAL=$(grep -c "$SEARCH_STRING" "$LOGFILE")

    # Extract epoch times using grep and cut
    T_FIRST=$(echo "$FIRST" | grep -oP '"epoch_time":\K[0-9.]+')
    T_LAST=$(echo "$LAST" | grep -oP '"epoch_time":\K[0-9.]+')

    # Extract local times for display
    LOCAL_FIRST=$(echo "$FIRST" | grep -oP '"local_time":"\K[^"]+')
    LOCAL_LAST=$(echo "$LAST" | grep -oP '"local_time":"\K[^"]+')

    # Calculate time difference and average
    TIME_DIFF=$(echo "$T_LAST - $T_FIRST" | bc)
    AVG_TIME=$(echo "scale=6; $TIME_DIFF / $TOTAL" | bc)

    # Print results
    echo "Search String:        $SEARCH_STRING"
    echo "First occurrence:     $LOCAL_FIRST (epoch: $T_FIRST)"
    echo "Last occurrence:      $LOCAL_LAST (epoch: $T_LAST)"
    echo "Total occurrences:    $TOTAL"
    echo "Time span:            $TIME_DIFF seconds"
    echo "Average time between: $AVG_TIME seconds"
    echo ""
done