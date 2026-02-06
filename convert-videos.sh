#!/bin/bash
# Video Converter - Converts MKV, AVI, MTS files to MP4
# Usage: ./convert-videos.sh [folder_path]
# If no folder specified, uses current directory

FOLDER="${1:-.}"

echo "========================================"
echo "  Video Converter (MKV/AVI/MTS -> MP4)"
echo "========================================"
echo "Folder: $FOLDER"
echo ""

# Check if ffmpeg is installed
if ! command -v ffmpeg &> /dev/null; then
    echo "ERROR: ffmpeg is not installed."
    echo "Install with: brew install ffmpeg"
    exit 1
fi

# Count files to convert
count=0
for ext in mkv MKV avi AVI mts MTS m2ts M2TS; do
    count=$((count + $(ls -1 "$FOLDER"/*.$ext 2>/dev/null | wc -l)))
done

if [ "$count" -eq 0 ]; then
    echo "No MKV, AVI, or MTS files found in $FOLDER"
    exit 0
fi

echo "Found $count file(s) to convert"
echo ""

converted=0
failed=0

# Convert each file
for ext in mkv MKV avi AVI mts MTS m2ts M2TS; do
    for file in "$FOLDER"/*.$ext 2>/dev/null; do
        [ -f "$file" ] || continue

        filename=$(basename "$file")
        dirname=$(dirname "$file")
        name="${filename%.*}"
        output="$dirname/$name.mp4"

        # Skip if output already exists
        if [ -f "$output" ]; then
            echo "SKIP: $output already exists"
            continue
        fi

        echo "Converting: $filename"
        echo "       To: $name.mp4"

        if ffmpeg -i "$file" -c:v libx264 -preset fast -crf 23 -c:a aac -b:a 128k -movflags +faststart "$output" -y -loglevel warning -stats; then
            echo "SUCCESS: $name.mp4"
            converted=$((converted + 1))
        else
            echo "FAILED: $filename"
            failed=$((failed + 1))
        fi
        echo ""
    done
done

echo "========================================"
echo "Complete! Converted: $converted, Failed: $failed"
echo "========================================"
