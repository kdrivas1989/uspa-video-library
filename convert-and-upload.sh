#!/bin/bash
# Convert video files in a folder to MP4 for uploading

if [ -z "$1" ]; then
    echo "Usage: ./convert-and-upload.sh /path/to/folder"
    exit 1
fi

FOLDER="$1"
OUTPUT_FOLDER="${FOLDER}/converted"

# Create output folder
mkdir -p "$OUTPUT_FOLDER"

echo "Converting videos in: $FOLDER"
echo "Output folder: $OUTPUT_FOLDER"
echo ""

# Count files
TOTAL=$(find "$FOLDER" -maxdepth 2 -type f \( -iname "*.mkv" -o -iname "*.avi" -o -iname "*.mts" -o -iname "*.m2ts" -o -iname "*.wmv" -o -iname "*.flv" \) | wc -l | tr -d ' ')
echo "Found $TOTAL files to convert"
echo ""

COUNT=0
find "$FOLDER" -maxdepth 2 -type f \( -iname "*.mkv" -o -iname "*.avi" -o -iname "*.mts" -o -iname "*.m2ts" -o -iname "*.wmv" -o -iname "*.flv" \) | while read FILE; do
    COUNT=$((COUNT + 1))
    FILENAME=$(basename "$FILE")
    BASENAME="${FILENAME%.*}"
    OUTPUT="$OUTPUT_FOLDER/${BASENAME}.mp4"

    echo "[$COUNT/$TOTAL] Converting: $FILENAME"

    # Fast conversion using copy if possible, otherwise re-encode
    ffmpeg -i "$FILE" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k -y "$OUTPUT" -loglevel error

    if [ $? -eq 0 ]; then
        echo "  ✓ Done: ${BASENAME}.mp4"
    else
        echo "  ✗ Failed: $FILENAME"
    fi
    echo ""
done

echo ""
echo "Conversion complete!"
echo "Converted files are in: $OUTPUT_FOLDER"
echo ""
echo "Now upload the 'converted' folder to: http://videos.kd-evolution.com/videoupload"
