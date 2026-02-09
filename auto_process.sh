#!/bin/bash
# Auto-process script - monitors and continues processing folders

LOG="/tmp/auto_process.log"
cd /Users/kevindrivas/Desktop/projects/video-library

echo "$(date): Auto-process started" >> $LOG

# Wait for current upload to finish
while pgrep -f "batch_upload_to_library.py" > /dev/null; do
    sleep 60
done

echo "$(date): 2016 Mondial upload completed" >> $LOG

# Check if there are remaining MP4s (from ongoing conversion)
REMAINING=$(rclone lsf "gdrive:1 - Skydiving Competitions/2016 Mondial - Organized" -R 2>/dev/null | grep -ic "\.mp4$")
if [ "$REMAINING" -gt 0 ]; then
    echo "$(date): Found $REMAINING more MP4s, running upload again" >> $LOG
    python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2016 Mondial - Organized" "2016 Mondial" >> /tmp/upload_log.txt 2>&1
fi

# Wait for conversion to finish
while pgrep -f "convert_and_upload.py.*2016 Mondial" > /dev/null; do
    sleep 60
done

echo "$(date): 2016 Mondial conversion completed" >> $LOG

# Final upload pass for any remaining MP4s
REMAINING=$(rclone lsf "gdrive:1 - Skydiving Competitions/2016 Mondial - Organized" -R 2>/dev/null | grep -ic "\.mp4$")
if [ "$REMAINING" -gt 0 ]; then
    echo "$(date): Final pass - $REMAINING MP4s remaining" >> $LOG
    python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2016 Mondial - Organized" "2016 Mondial" >> /tmp/upload_log.txt 2>&1
fi

echo "$(date): 2016 Mondial fully complete!" >> $LOG

# Start 2018WPC conversion
echo "$(date): Starting 2018WPC conversion" >> $LOG
python3 -u convert_and_upload.py "1 - Skydiving Competitions/2018WPC - Organized" >> /tmp/conversion_log_2018.txt 2>&1 &

sleep 120  # Wait for some conversions

# Start 2018WPC upload
echo "$(date): Starting 2018WPC upload" >> $LOG
python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2018WPC - Organized" "2018 WPC" >> /tmp/upload_log_2018.txt 2>&1

echo "$(date): 2018WPC upload pass completed" >> $LOG

# Continue until all done
while true; do
    MKV=$(rclone lsf "gdrive:1 - Skydiving Competitions/2018WPC - Organized" -R 2>/dev/null | grep -ic "\.mkv$")
    MP4=$(rclone lsf "gdrive:1 - Skydiving Competitions/2018WPC - Organized" -R 2>/dev/null | grep -ic "\.mp4$")

    if [ "$MKV" -eq 0 ] && [ "$MP4" -eq 0 ]; then
        echo "$(date): All done!" >> $LOG
        break
    fi

    if [ "$MP4" -gt 0 ]; then
        echo "$(date): Uploading $MP4 remaining MP4s" >> $LOG
        python3 -u batch_upload_to_library.py "1 - Skydiving Competitions/2018WPC - Organized" "2018 WPC" >> /tmp/upload_log_2018.txt 2>&1
    fi

    sleep 300
done

echo "$(date): ALL PROCESSING COMPLETE!" >> $LOG
