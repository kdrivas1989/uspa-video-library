#!/bin/bash
# Quick status check script

echo "=== VIDEO PROCESSING STATUS ==="
echo ""

echo "RUNNING PROCESSES:"
ps aux | grep -E "convert_and_upload|batch_upload" | grep -v grep | awk '{print "  PID:", $2, "-", $12, $13}' || echo "  None"
echo ""

echo "2016 MONDIAL (Google Drive):"
MKV1=$(rclone lsf "gdrive:1 - Skydiving Competitions/2016 Mondial - Organized" -R 2>/dev/null | grep -ic "\.mkv$")
MP41=$(rclone lsf "gdrive:1 - Skydiving Competitions/2016 Mondial - Organized" -R 2>/dev/null | grep -ic "\.mp4$")
echo "  MKV remaining: $MKV1"
echo "  MP4 remaining: $MP41"
echo ""

echo "2018 WPC (Google Drive):"
MKV2=$(rclone lsf "gdrive:1 - Skydiving Competitions/2018WPC - Organized" -R 2>/dev/null | grep -ic "\.mkv$")
MP42=$(rclone lsf "gdrive:1 - Skydiving Competitions/2018WPC - Organized" -R 2>/dev/null | grep -ic "\.mp4$")
echo "  MKV remaining: $MKV2"
echo "  MP4 remaining: $MP42"
echo ""

echo "RECENT LOGS:"
echo "--- 2016 Upload (last 8 lines) ---"
tail -8 /tmp/upload_log_2016_retry.txt 2>/dev/null || tail -8 /tmp/upload_log.txt 2>/dev/null || echo "  No log"
echo ""
echo "--- 2018 Conversion (last 8 lines) ---"
tail -8 /tmp/conversion_log_2018.txt 2>/dev/null || echo "  No log"
echo ""
echo "==============================="
