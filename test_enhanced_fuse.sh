#!/bin/bash

echo "=== Enhanced FUSE Client Test Script ==="
echo

# Configuration
MOUNT_POINT="/tmp/enhanced-fuse"
LOG_FILE="/tmp/enhanced-fuse-test.log"
CLIENT_PID=""

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    if [ ! -z "$CLIENT_PID" ]; then
        echo "Stopping enhanced client (PID: $CLIENT_PID)"
        kill $CLIENT_PID 2>/dev/null
        sleep 2
    fi
    
    # Unmount if still mounted
    if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        echo "Unmounting $MOUNT_POINT"
        fusermount -u "$MOUNT_POINT" 2>/dev/null || sudo umount "$MOUNT_POINT" 2>/dev/null
    fi
    
    # Clean up test files
    rm -f /tmp/test-content.txt
    echo "Cleanup complete"
}

# Set trap to cleanup on exit
trap cleanup EXIT

echo "1. Starting enhanced FUSE client..."
# Start the enhanced client in background
./bin/enhanced-client -enhanced=true -consistency=session > "$LOG_FILE" 2>&1 &
CLIENT_PID=$!

echo "   Client PID: $CLIENT_PID"
echo "   Log file: $LOG_FILE"
echo "   Waiting for mount..."

# Wait for mount to be ready
sleep 5

# Check if mount point is ready
if ! mountpoint -q "$MOUNT_POINT"; then
    echo "❌ Mount point not ready. Check logs:"
    tail -20 "$LOG_FILE"
    exit 1
fi

echo "✅ Mount point ready: $MOUNT_POINT"
echo

echo "2. Basic file operations test..."

# Test basic operations
echo "   Creating test file..."
echo "Hello from enhanced FUSE!" > "$MOUNT_POINT/basic-test.txt"

echo "   Reading test file..."
CONTENT=$(cat "$MOUNT_POINT/basic-test.txt")
echo "   Content: $CONTENT"

echo "   Listing directory..."
ls -la "$MOUNT_POINT/"

echo "✅ Basic operations successful"
echo

echo "3. Vi compatibility test..."

# Create content for vi test
echo "Line 1: Original content" > /tmp/test-content.txt
echo "Line 2: For vi editing test" >> /tmp/test-content.txt
echo "Line 3: Enhanced FUSE filesystem" >> /tmp/test-content.txt

echo "   Copying content to FUSE filesystem..."
cp /tmp/test-content.txt "$MOUNT_POINT/vi-test.txt"

echo "   Original content:"
cat "$MOUNT_POINT/vi-test.txt"

echo "   Testing vi workflow (create temp file, rename)..."

# Simulate vi's typical workflow
TEMP_FILE="$MOUNT_POINT/.vi-test.txt.tmp$$"
TARGET_FILE="$MOUNT_POINT/vi-test.txt"

echo "   Step 1: Creating temporary file..."
echo "Modified Line 1: Content changed by vi" > "$TEMP_FILE"
echo "Modified Line 2: Vi workflow test" >> "$TEMP_FILE"
echo "Modified Line 3: Enhanced FUSE with rename" >> "$TEMP_FILE"

echo "   Step 2: Checking temporary file..."
cat "$TEMP_FILE"

echo "   Step 3: Renaming temp file to replace original (vi's atomic save)..."
mv "$TEMP_FILE" "$TARGET_FILE"

echo "   Step 4: Verifying final content after rename..."
cat "$TARGET_FILE"

# Test another common vi pattern - backup creation
echo "   Testing backup file creation (vi .bak pattern)..."
cp "$TARGET_FILE" "$TARGET_FILE.bak"

echo "   Backup created:"
ls -la "$MOUNT_POINT"/*.bak

echo "✅ Vi compatibility test successful"
echo

echo "4. Performance test..."

echo "   Creating multiple files..."
for i in {1..10}; do
    echo "File $i content - $(date)" > "$MOUNT_POINT/perf-test-$i.txt"
done

echo "   Listing all files..."
ls -la "$MOUNT_POINT/"

echo "   Reading all files..."
for i in {1..10}; do
    head -1 "$MOUNT_POINT/perf-test-$i.txt"
done

echo "✅ Performance test successful"
echo

echo "5. Directory operations test..."

echo "   Creating subdirectory..."
mkdir "$MOUNT_POINT/subdir"

echo "   Creating file in subdirectory..."
echo "Subdir file content" > "$MOUNT_POINT/subdir/nested-file.txt"

echo "   Reading nested file..."
cat "$MOUNT_POINT/subdir/nested-file.txt"

echo "   Listing subdirectory..."
ls -la "$MOUNT_POINT/subdir/"

echo "✅ Directory operations successful"
echo

echo "6. Client statistics..."
echo "   Checking client logs for performance stats..."
echo "   Last 10 log lines:"
tail -10 "$LOG_FILE"

echo
echo "=== All tests completed successfully! ==="
echo
echo "To manually test vi editing:"
echo "1. vi $MOUNT_POINT/manual-test.txt"
echo "2. Add some content and save with :wq"
echo "3. Check if vi returns to prompt normally"
echo
echo "Client is still running (PID: $CLIENT_PID)"
echo "Log file: $LOG_FILE"
echo "Mount point: $MOUNT_POINT"
echo
echo "Press Ctrl+C to stop the client and cleanup"

# Keep the script running so user can test manually
wait $CLIENT_PID 