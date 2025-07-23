#!/bin/bash

echo "=== Quick Vi Workflow Test ==="

# Start the client in background
echo "Starting enhanced client..."
./bin/enhanced-client -enhanced=true -consistency=session > /tmp/quick-test.log 2>&1 &
CLIENT_PID=$!

# Wait for mount
sleep 3

MOUNT_POINT="/tmp/enhanced-fuse"

if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    echo "❌ Mount failed. Check logs:"
    tail -10 /tmp/quick-test.log
    kill $CLIENT_PID 2>/dev/null
    exit 1
fi

echo "✅ Mount successful: $MOUNT_POINT"

# Test vi's typical workflow
echo "Testing vi workflow:"

# 1. Create original file
echo "Original content" > "$MOUNT_POINT/test.txt"
echo "1. Created original file: $(cat "$MOUNT_POINT/test.txt")"

# 2. Create temp file (what vi does)
echo "Modified by vi editor" > "$MOUNT_POINT/.test.txt.swp"
echo "2. Created temp file: $(cat "$MOUNT_POINT/.test.txt.swp")"

# 3. Rename temp to original (atomic save)
echo "3. Renaming temp file to original..."
mv "$MOUNT_POINT/.test.txt.swp" "$MOUNT_POINT/test.txt"

if [ $? -eq 0 ]; then
    echo "✅ Rename successful!"
    echo "4. Final content: $(cat "$MOUNT_POINT/test.txt")"
else
    echo "❌ Rename failed!"
fi

# Check if vi can now work
echo ""
echo "Vi should now work! Try:"
echo "vi $MOUNT_POINT/manual-test.txt"
echo ""
echo "Client PID: $CLIENT_PID"
echo "Log file: /tmp/quick-test.log"
echo ""
echo "Press Enter to stop the client..."
read

# Cleanup
kill $CLIENT_PID 2>/dev/null
sleep 2

if mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
    fusermount -u "$MOUNT_POINT" 2>/dev/null
fi

echo "Test complete!" 