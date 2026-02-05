#!/usr/bin/env bash
# Build script for Render - installs ffmpeg static binary

set -e

# Install Python dependencies
pip install -r requirements.txt

# Download and install static ffmpeg binary
mkdir -p /opt/render/project/src/bin
cd /opt/render/project/src/bin
curl -L https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz | tar xJ --strip-components=1
chmod +x ffmpeg ffprobe

echo "ffmpeg installed at /opt/render/project/src/bin/ffmpeg"
