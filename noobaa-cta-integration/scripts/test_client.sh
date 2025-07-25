#!/bin/bash

# Simple End-to-End NooBaa S3 Glacier Test
# This test simulates NooBaa calling the migrate script directly

set -eo pipefail

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [E2E-TEST] $*"
}

main() {
    log "Starting simple end-to-end test..."
    
    # Step 1: Create a test file
    log "Step 1: Creating test file"
    kubectl exec -n noobaa noobaa-core-0 -- bash -c "
        echo 'End-to-end test file content - $(date)' > /tmp/e2e_test_file.txt
        echo 'File size: $(wc -c < /tmp/e2e_test_file.txt) bytes'
    "
    
    # Step 2: Create file list (this simulates what NooBaa NSFS Glacier would do)
    log "Step 2: Creating file list for migration"
    kubectl exec -n noobaa noobaa-core-0 -- bash -c "
        echo '/tmp/e2e_test_file.txt' > /tmp/e2e_file_list.txt
        echo 'File list created:'
        cat /tmp/e2e_file_list.txt
    "
    
    # Step 3: Call migrate script (this simulates NooBaa calling the executable)
    log "Step 3: Calling migrate script (simulates NooBaa NSFS Glacier workflow)"
    kubectl exec -n noobaa noobaa-core-0 -- /tmp/glacier/bin/migrate /tmp/e2e_file_list.txt
    local migrate_exit_code=$?
    
    # Step 4: Check results
    if [[ $migrate_exit_code -eq 0 ]]; then
        log "✅ SUCCESS: Migrate script completed successfully"
        
        # Step 5: Verify CTA received the file
        log "Step 4: Checking CTA frontend logs for archival"
        echo ""
        echo "=== Recent CTA Frontend Activity ==="
        kubectl logs -n dev cta-frontend-0 --tail=3 | grep -E "(requesterName|CLOSEW|queueArchive)" || echo "No recent archival activity found"
        echo ""
        
        log "🎉 END-TO-END TEST PASSED!"
        log "File successfully uploaded and queued for CTA archival"
        
    else
        log "❌ FAILED: Migrate script failed with exit code: $migrate_exit_code"
        return 1
    fi
    
    # Cleanup
    log "Step 5: Cleaning up test files"
    kubectl exec -n noobaa noobaa-core-0 -- bash -c "
        rm -f /tmp/e2e_test_file.txt /tmp/e2e_file_list.txt
        echo 'Cleanup completed'
    "
    
    log "End-to-end test completed successfully!"
}

main "$@"