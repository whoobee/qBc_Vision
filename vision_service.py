#!/usr/bin/env python3
"""
qBc_Vision MQTT Service

Provides camera capture, video recording, and display services via MQTT.

MQTT topics:
    Subscribe:
        robot/vision/cmd         General commands (JSON):
                                 {"command": "capture_frame"}
                                 {"command": "capture_video", "duration": 5}
                                 {"command": "stop_video"}
                                 {"command": "display_picture", "file": "path"}
                                 {"command": "display_video", "file": "path"}
                                 {"command": "start_stream"}
                                 {"command": "stop_stream"}
                                 {"command": "stop_display"}

    Publish:
        robot/vision/state              (RETAIN) {"status":"online", ...}
        robot/vision/frame_ready        {"file": "/path/to/pic.jpg"}
        robot/vision/video_ready        {"file": "/path/to/video.mp4"}
        robot/system/heartbeat/vision   keepalive (1 Hz)

Usage:
    python3 vision_service.py [--mqtt-broker localhost] [--mqtt-port 1883]

System dependencies:
    - picamera2  (system-wide on Raspberry Pi OS)
    - ffmpeg     (for video muxing: apt install ffmpeg)
    - feh        (for picture display: apt install feh)
    - mpv        (for video playback: apt install mpv)
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

import paho.mqtt.client as mqtt

try:
    from picamera2 import Picamera2, Preview
    from picamera2.encoders import H264Encoder
    from picamera2.outputs import FfmpegOutput

    HAS_CAMERA = True
except ImportError:
    HAS_CAMERA = False

logger = logging.getLogger("qBc_Vision")

BASE_DIR = Path(__file__).parent
PICTURES_DIR = BASE_DIR / "resources" / "pictures"
VIDEO_DIR = BASE_DIR / "resources" / "video"

# Camera settings
CAPTURE_WIDTH = 1920
CAPTURE_HEIGHT = 1080
VIDEO_BITRATE = 10_000_000  # 10 Mbps

# Shared-memory frame path for low-latency navigation stream
SHM_NAV_FRAME = "/dev/shm/qb_nav_frame.jpg"
SHM_NAV_FRAME_TMP = "/dev/shm/qb_nav_frame.tmp.jpg"
NAV_STREAM_FPS = 15  # Target capture rate for navigation stream

# MQTT topics
TOPIC_CMD = "robot/vision/cmd"
TOPIC_STATE = "robot/vision/state"
TOPIC_HEARTBEAT = "robot/system/heartbeat/vision"
TOPIC_FRAME_READY = "robot/vision/frame_ready"
TOPIC_VIDEO_READY = "robot/vision/video_ready"
TOPIC_CURRENT_STATE = "robot/vision/current_state"
TOPIC_ERROR_INFO = "robot/vision/error_info"


class VisionService:
    def __init__(self, broker="localhost", port=1883):
        self.broker = broker
        self.port = port

        # Camera
        self._camera = None
        self._camera_lock = threading.Lock()

        # State
        self._streaming = False
        self._recording = False
        self._displaying = False
        self._lock = threading.Lock()
        self._running = False

        # Subprocess / thread management
        self._display_proc = None
        self._record_stop = threading.Event()

        # Navigation stream (continuous capture to shared memory)
        self._nav_streaming = False
        self._nav_stream_stop = threading.Event()

        # MQTT client
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id="qbc_vision",
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message
        self._client.will_set(
            TOPIC_STATE,
            json.dumps({"status": "offline"}),
            qos=1,
            retain=True,
        )
        self.connected = False
        self._error_info = "E_OK"

    # ------------------------------------------------------------------
    # Camera management
    # ------------------------------------------------------------------

    def _init_camera(self):
        if not HAS_CAMERA:
            logger.error("picamera2 not available — camera disabled")
            return False
        try:
            self._camera = Picamera2()
            config = self._camera.create_video_configuration(
                main={"size": (CAPTURE_WIDTH, CAPTURE_HEIGHT)},
            )
            self._camera.configure(config)
            self._camera.start()
            time.sleep(2)  # allow auto-exposure / white-balance to settle
            logger.info("Camera initialised (%dx%d)", CAPTURE_WIDTH, CAPTURE_HEIGHT)
            return True
        except Exception as e:
            logger.error("Camera init failed: %s", e)
            self._set_error("camera init: " + str(e)[:80])
            self._camera = None
            return False

    def _close_camera(self):
        if self._camera:
            try:
                self._camera.stop()
                self._camera.close()
            except Exception:
                pass
            self._camera = None

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    def _get_state(self):
        with self._lock:
            return {
                "streaming": self._streaming,
                "recording": self._recording,
                "displaying": self._displaying,
                "camera_ready": self._camera is not None,
            }

    def _derive_current_state(self):
        """Derive human-readable current state from internal flags."""
        with self._lock:
            if self._recording:
                return "recording_video"
            if self._streaming:
                return "streaming"
            if self._displaying:
                return "displaying"
            if self._camera is not None:
                return "ready"
            return "no_camera"

    def _set_error(self, error):
        """Set and publish error info."""
        self._error_info = error
        if self.connected:
            self._client.publish(TOPIC_ERROR_INFO, error, qos=1, retain=True)

    def _publish_state(self):
        state = {"status": "online", **self._get_state()}
        self._client.publish(TOPIC_STATE, json.dumps(state), qos=1, retain=True)
        self._client.publish(TOPIC_CURRENT_STATE, self._derive_current_state(), qos=1, retain=True)
        self._client.publish(TOPIC_ERROR_INFO, self._error_info, qos=1, retain=True)

    # ------------------------------------------------------------------
    # MQTT callbacks
    # ------------------------------------------------------------------

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties):
        if reason_code.is_failure:
            logger.error("MQTT connection failed: %s", reason_code)
            return
        self.connected = True
        logger.info("Connected to MQTT broker %s:%d", self.broker, self.port)
        client.subscribe([(TOPIC_CMD, 1)])
        self._publish_state()

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.connected = False
        if reason_code.is_failure:
            logger.warning("Disconnected from MQTT broker: %s", reason_code)

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("Invalid JSON on %s", msg.topic)
            return

        cmd = data.get("command", "")
        if cmd == "capture_frame":
            resp = self.handle_capture_frame()
        elif cmd == "capture_video":
            resp = self.handle_capture_video(data.get("duration", 5))
        elif cmd == "stop_video":
            resp = self.handle_stop_video()
        elif cmd == "display_picture":
            resp = self.handle_display_picture(data.get("file"))
        elif cmd == "display_video":
            resp = self.handle_display_video(data.get("file"))
        elif cmd == "start_stream":
            resp = self.handle_start_stream()
        elif cmd == "stop_stream":
            resp = self.handle_stop_stream()
        elif cmd == "start_nav_stream":
            resp = self.handle_start_nav_stream()
        elif cmd == "stop_nav_stream":
            resp = self.handle_stop_nav_stream()
        elif cmd == "stop_display":
            resp = self.handle_stop_display()
        elif cmd == "get_state":
            self._publish_state()
            return
        else:
            logger.warning("Unknown vision command: %s", cmd)
            return

        if resp and resp.get("status") == "error":
            logger.warning("Vision command error: %s", resp.get("message"))

    # ------------------------------------------------------------------
    # Capture frame
    # ------------------------------------------------------------------

    def handle_capture_frame(self):
        if self._camera is None:
            return {"status": "error", "message": "Camera not available"}

        PICTURES_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = PICTURES_DIR / f"frame_{timestamp}.jpg"

        try:
            with self._camera_lock:
                self._camera.capture_file(str(filepath))
        except Exception as e:
            return {"status": "error", "message": f"Capture failed: {e}"}

        logger.info("Frame captured: %s", filepath)
        self._client.publish(
            TOPIC_FRAME_READY,
            json.dumps({"file": str(filepath)}),
            qos=1,
        )
        return {"status": "ok", "file": str(filepath)}

    # ------------------------------------------------------------------
    # Video recording
    # ------------------------------------------------------------------

    def handle_capture_video(self, duration=5):
        with self._lock:
            if self._recording:
                return {"status": "error", "message": "Already recording"}
            self._recording = True

        if self._camera is None:
            with self._lock:
                self._recording = False
            return {"status": "error", "message": "Camera not available"}

        VIDEO_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = VIDEO_DIR / f"video_{timestamp}.mp4"

        self._record_stop.clear()
        thread = threading.Thread(
            target=self._record_worker,
            args=(filepath, duration),
            daemon=True,
        )
        thread.start()

        self._publish_state()
        logger.info("Recording started: %s (%ds)", filepath, duration)
        return {"status": "ok", "file": str(filepath), "duration": duration}

    def _record_worker(self, filepath, duration):
        try:
            with self._camera_lock:
                encoder = H264Encoder(bitrate=VIDEO_BITRATE)
                output = FfmpegOutput(str(filepath))
                self._camera.start_recording(encoder, output)

            self._record_stop.wait(timeout=duration)

            with self._camera_lock:
                self._camera.stop_recording()

            logger.info("Video saved: %s", filepath)
            self._client.publish(
                TOPIC_VIDEO_READY,
                json.dumps({"file": str(filepath)}),
                qos=1,
            )
        except Exception as e:
            logger.error("Recording error: %s", e)
            self._set_error("recording: " + str(e)[:80])
        finally:
            with self._lock:
                self._recording = False
            self._publish_state()

    def handle_stop_video(self):
        with self._lock:
            if not self._recording:
                return {"status": "error", "message": "Not recording"}
        self._record_stop.set()
        logger.info("Recording stop requested")
        return {"status": "ok", "message": "Recording stopping"}

    # ------------------------------------------------------------------
    # Display picture
    # ------------------------------------------------------------------

    def handle_display_picture(self, file_path):
        if file_path is None:
            return {"status": "error", "message": "No file specified"}

        # Resolve relative paths against pictures directory
        if not os.path.isabs(file_path):
            resolved = (PICTURES_DIR / file_path).resolve()
            if not str(resolved).startswith(str(PICTURES_DIR.resolve())):
                return {"status": "error", "message": "Invalid file path"}
            file_path = str(resolved)

        if not os.path.isfile(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}

        self._stop_display_proc()

        try:
            self._display_proc = subprocess.Popen(
                ["feh", "--fullscreen", "--hide-pointer", file_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            with self._lock:
                self._displaying = True
        except FileNotFoundError:
            return {"status": "error", "message": "feh not installed (apt install feh)"}

        self._publish_state()
        logger.info("Displaying picture: %s", file_path)
        return {"status": "ok", "file": file_path}

    # ------------------------------------------------------------------
    # Display video
    # ------------------------------------------------------------------

    def handle_display_video(self, file_path):
        if file_path is None:
            return {"status": "error", "message": "No file specified"}

        # Resolve relative paths against video directory
        if not os.path.isabs(file_path):
            resolved = (VIDEO_DIR / file_path).resolve()
            if not str(resolved).startswith(str(VIDEO_DIR.resolve())):
                return {"status": "error", "message": "Invalid file path"}
            file_path = str(resolved)

        if not os.path.isfile(file_path):
            return {"status": "error", "message": f"File not found: {file_path}"}

        self._stop_display_proc()

        # Try mpv first, fall back to ffplay
        for player_cmd in (
            ["mpv", "--fullscreen", "--no-terminal", file_path],
            ["ffplay", "-fs", "-autoexit", "-loglevel", "quiet", file_path],
        ):
            try:
                self._display_proc = subprocess.Popen(
                    player_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                with self._lock:
                    self._displaying = True
                threading.Thread(
                    target=self._display_monitor, daemon=True
                ).start()
                self._publish_state()
                logger.info("Displaying video: %s", file_path)
                return {"status": "ok", "file": file_path}
            except FileNotFoundError:
                continue

        return {
            "status": "error",
            "message": "No video player found (apt install mpv or ffmpeg)",
        }

    def _display_monitor(self):
        """Wait for display subprocess to finish and update state."""
        proc = self._display_proc
        if proc:
            proc.wait()
            # Only clear state if this is still the active process
            if self._display_proc is proc:
                self._display_proc = None
                with self._lock:
                    self._displaying = False
                self._publish_state()

    # ------------------------------------------------------------------
    # Camera streaming (live preview on display)
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Navigation stream (low-latency continuous capture to shared memory)
    # ------------------------------------------------------------------

    def handle_start_nav_stream(self):
        """Start continuous capture to /dev/shm for navigation servoing."""
        if self._nav_streaming:
            return {"status": "ok", "message": "Already streaming"}
        if self._camera is None:
            return {"status": "error", "message": "Camera not available"}

        self._nav_stream_stop.clear()
        self._nav_streaming = True
        thread = threading.Thread(target=self._nav_stream_worker, daemon=True)
        thread.start()
        logger.info("Navigation stream started → %s @ %d fps", SHM_NAV_FRAME, NAV_STREAM_FPS)
        return {"status": "ok", "message": "Navigation stream started"}

    def handle_stop_nav_stream(self):
        """Stop the navigation continuous capture stream."""
        if not self._nav_streaming:
            return {"status": "ok", "message": "Not streaming"}
        self._nav_stream_stop.set()
        self._nav_streaming = False
        # Clean up shared memory file
        for p in (SHM_NAV_FRAME, SHM_NAV_FRAME_TMP):
            try:
                os.remove(p)
            except OSError:
                pass
        logger.info("Navigation stream stopped")
        return {"status": "ok", "message": "Navigation stream stopped"}

    def _nav_stream_worker(self):
        """Continuously capture frames to shared memory at target FPS."""
        period = 1.0 / NAV_STREAM_FPS
        while not self._nav_stream_stop.is_set():
            start = time.monotonic()
            try:
                with self._camera_lock:
                    self._camera.capture_file(SHM_NAV_FRAME_TMP)
                # Atomic rename so readers never see a partial file
                os.replace(SHM_NAV_FRAME_TMP, SHM_NAV_FRAME)
            except Exception as e:
                logger.debug("Nav stream capture error: %s", e)
            elapsed = time.monotonic() - start
            if elapsed < period:
                self._nav_stream_stop.wait(period - elapsed)
        self._nav_streaming = False

    # ------------------------------------------------------------------
    # Display stream (preview)
    # ------------------------------------------------------------------

    def handle_start_stream(self):
        with self._lock:
            if self._streaming:
                return {"status": "error", "message": "Already streaming"}

        if self._camera is None:
            return {"status": "error", "message": "Camera not available"}

        self._stop_display_proc()

        # Try DRM preview (hardware overlay, works without X11),
        # then fall back to QT preview
        for preview_type in (Preview.DRM, Preview.QT):
            try:
                with self._camera_lock:
                    self._camera.start_preview(preview_type)
                with self._lock:
                    self._streaming = True
                self._publish_state()
                logger.info("Stream started (preview=%s)", preview_type.name)
                return {"status": "ok", "message": "Streaming started"}
            except Exception as e:
                logger.debug("Preview %s failed: %s", preview_type.name, e)
                continue

        return {"status": "error", "message": "No preview backend available"}

    def handle_stop_stream(self):
        with self._lock:
            if not self._streaming:
                return {"status": "error", "message": "Not streaming"}

        try:
            with self._camera_lock:
                self._camera.stop_preview()
        except Exception as e:
            logger.warning("Preview stop error: %s", e)

        with self._lock:
            self._streaming = False
        self._publish_state()
        logger.info("Stream stopped")
        return {"status": "ok", "message": "Streaming stopped"}

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    def handle_stop_display(self):
        self._stop_display_proc()
        return {"status": "ok", "message": "Display stopped"}

    def _stop_display_proc(self):
        """Terminate any active display subprocess."""
        if self._display_proc is not None:
            self._display_proc.terminate()
            try:
                self._display_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._display_proc.kill()
            self._display_proc = None
        with self._lock:
            self._displaying = False
        self._publish_state()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self):
        self._running = True

        # Ensure resource dirs exist
        PICTURES_DIR.mkdir(parents=True, exist_ok=True)
        VIDEO_DIR.mkdir(parents=True, exist_ok=True)

        # Initialise camera
        self._init_camera()

        # Connect MQTT
        self._client.connect(self.broker, self.port)
        self._client.loop_start()

        logger.info("qBc_Vision service on MQTT %s:%d", self.broker, self.port)

        # Block until signal, publish heartbeat every second
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())

        while not stop.is_set():
            self._client.publish(TOPIC_HEARTBEAT, b"1", qos=0)
            stop.wait(1.0)

        logger.info("Shutting down...")
        self._running = False

        # Stop active operations
        self._record_stop.set()
        if self._streaming:
            self.handle_stop_stream()
        self._stop_display_proc()

        self._client.publish(
            TOPIC_STATE, json.dumps({"status": "offline"}), qos=1, retain=True
        )
        self._client.loop_stop()
        self._client.disconnect()
        self._close_camera()


def main():
    parser = argparse.ArgumentParser(description="qBc_Vision MQTT Service")
    parser.add_argument(
        "--mqtt-broker", default="localhost", help="MQTT broker address"
    )
    parser.add_argument(
        "--mqtt-port", type=int, default=1883, help="MQTT broker port"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    service = VisionService(broker=args.mqtt_broker, port=args.mqtt_port)
    service.run()


if __name__ == "__main__":
    main()
