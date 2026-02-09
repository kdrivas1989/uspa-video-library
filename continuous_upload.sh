#!/bin/bash
# Continuously upload MP4 files as they're converted

cd /Users/kevindrivas/Desktop/projects/video-library
LOG="/tmp/continuous_upload.log"

echo "$(date): Starting continuous upload" >> $LOG

while true; do
    # Check if conversions are still running
    CONVERTING=$(pgrep -f "convert_and_upload.py" | wc -l)

    # Count MP4s on each folder
    MP4_2016=$(rclone lsf "gdrive:1 - Skydiving Competitions/2016 Mondial - Organized" -R 2>/dev/null | grep -ic "\.mp4$")
    MP4_2018=$(rclone lsf "gdrive:1 - Skydiving Competitions/2018WPC - Organized" -R 2>/dev/null | grep -ic "\.mp4$")

    echo "$(date): Converting=$CONVERTING, 2016 MP4=$MP4_2016, 2018 MP4=$MP4_2018" >> $LOG

    # Upload 2016 if there are MP4s
    if [ "$MP4_2016" -gt 0 ]; then
        echo "$(date): Uploading 2016 Mondial ($MP4_2016 files)" >> $LOG
        python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2016 Mondial - Organized" "2016 Mondial" >> /tmp/upload_log_2016.txt 2>&1
    fi

    # Upload 2018 if there are MP4s
    if [ "$MP4_2018" -gt 0 ]; then
        echo "$(date): Uploading 2018 WPC ($MP4_2018 files)" >> $LOG
        python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2018WPC - Organized" "2018 WPC" >> /tmp/upload_log_2018.txt 2>&1
    fi

    # If no conversions running and no MP4s left, we're done
    if [ "$CONVERTING" -eq 0 ] && [ "$MP4_2016" -eq 0 ] && [ "$MP4_2018" -eq 0 ]; then
        echo "$(date): ALL DONE!" >> $LOG
        break
    fi

    # Wait before next check
    sleep 120
done

echo "$(date): Continuous upload finished" >> $LOG
