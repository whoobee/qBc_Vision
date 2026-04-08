# qBc_Vision

MQTT-based Vision subsystem for the qB-Companion robot, providing camera capture, video recording, and display services.

![Python](https://img.shields.io/badge/Python-3.13-blue) ![Camera](https://img.shields.io/badge/Camera-PiCamera2-red) ![Platform](https://img.shields.io/badge/Platform-Raspberry%20Pi-lightgrey)

## Overview

`qBc_Vision` leverages the official `picamera2` library to interface with the Raspberry Pi Camera. It runs as a headless MQTT service, listening for commands to take pictures, record H.264 video, stream hardware previews, and display media locally using external utilities (`feh`, `mpv`).

## Quick Start

```bash
# Setup
cd qBc_Vision
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run
python3 vision_service.py --mqtt-broker localhost
```

### System Dependencies
The service relies on system-level packages for media handling:
```bash
sudo apt install ffmpeg feh mpv
```

## Architecture

The service connects to the MQTT broker and maintains a state machine for the camera.

### Subscribed Topics

- `robot/vision/cmd`: Expects JSON payloads commanding the vision system.

### Published Topics

- `robot/vision/state`: (Retained) Overall service status (`{"status": "online", "camera_ready": true, ...}`).
- `robot/vision/current_state`: Human-readable current state (e.g., `ready`, `recording_video`, `streaming`).
- `robot/vision/error_info`: Last error encountered.
- `robot/vision/frame_ready`: Published when a picture is captured (`{"file": "/path/to/pic.jpg"}`).
- `robot/vision/video_ready`: Published when a video finishes recording.
- `robot/system/heartbeat/vision`: 1 Hz keepalive.

## Command API (`robot/vision/cmd`)

Send JSON payloads to execute camera functions.

| Command | Payload Example | Description |
|---|---|---|
| **Capture Frame** | `{"command": "capture_frame"}` | Takes a 1080p picture and saves it to `resources/pictures/`. |
| **Capture Video** | `{"command": "capture_video", "duration": 5}` | Records an H.264 video for the specified duration (seconds). |
| **Stop Video** | `{"command": "stop_video"}` | Stops an ongoing video recording early. |
| **Display Picture** | `{"command": "display_picture", "file": "frame_xyz.jpg"}` | Displays an image fullscreen using `feh`. |
| **Display Video** | `{"command": "display_video", "file": "video_xyz.mp4"}` | Plays a video fullscreen using `mpv`. |
| **Start Stream** | `{"command": "start_stream"}` | Starts a live hardware preview on the connected display (DRM/QT). |
| **Stop Stream** | `{"command": "stop_stream"}` | Stops the live preview. |
| **Stop Display** | `{"command": "stop_display"}` | Kills the currently active `feh` or `mpv` process. |

## Resources

All captured media is saved inside the `qBc_Vision/resources/` directory:
- Pictures: `resources/pictures/`
- Videos: `resources/video/`
