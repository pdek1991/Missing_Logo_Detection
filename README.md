# Missing Logo Detector

Production-grade logo monitoring for broadcast streams with a built-in web dashboard.

## Features

- Monitors up to **50 streams**.
- Default polling interval **10 seconds per stream**.
- Retry and timeout handling for unavailable streams.
- 24x7-friendly logging with rotation/compression.
- REST API for integration.
- Real-time dashboard (PWA) with RED/YELLOW/GREEN sorting and alarms.
- Alarm popup + audio for new incidents.

## Supported

- OS: Windows 10/11 (primary), Linux (source mode).
- Python: 3.9+
- FFmpeg required in `PATH`.
- Streams: UDP multicast/unicast sources supported by FFmpeg.
- APIs:
  - `GET /health`
  - `GET /status`
  - `GET /status/{channel}`
  - `GET /api/logo_status`
  - `GET /api/logo_status/{channel}`

## Project Layout

```text
logo_detector/
|- main.py
|- api.py
|- detector.py
|- ffmpeg_reader.py
|- state_machine.py
|- status_store.py
|- logger.py
|- utils.py
|- config.yaml              # External runtime config (not bundled in exe)
|- logos/                  # External template logos (not bundled in exe)
|- logs/                   # Runtime output
|- roi/                    # Runtime ROI captures
|- web/                    # Bundled into exe
   |- index.html
   |- styles.css
   |- app.js
   |- manifest.json
   |- service-worker.js
   |- assets/
      |- icons/
      |- media/
```

## Run From Source

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Start app:

```bash
python main.py
```

3. Open dashboard:

- `http://localhost:8000/`

## EXE Build (PyInstaller)

Use this command from project root (Windows PowerShell):

```powershell
pyinstaller --noconfirm --clean --onefile --name MissingLogoDetector --add-data "web;web" --collect-all fastapi --collect-all starlette --collect-all pydantic --collect-all uvicorn main.py
```

What goes inside EXE:

- Python application code
- `web/` dashboard files (HTML/CSS/JS/PWA assets)

What stays outside EXE (by design):

- `config.yaml`
- `logos/`
- `logs/`
- `roi/`
- non-runtime docs/files (`README.md`, `requirements.txt`, etc.)

## Run EXE

1. Build command creates: `dist\\MissingLogoDetector.exe`
2. Keep these next to the EXE:
   - `config.yaml`
   - `logos/` folder
3. Run `MissingLogoDetector.exe`
4. Dashboard URL: `http://localhost:8000/`

`logs/` and `roi/` are created automatically if missing.

## Config Notes (`config.yaml`)

Scheduler defaults:

- `max_streams: 50`
- `default_poll_interval_seconds: 10`
- `default_ffmpeg_timeout_seconds: 4`
- `default_retry_attempts: 1`
- `max_workers: 50`

Per-stream key fields:

- `name`
- `url`
- `template_path` (from external `logos/`)
- `display_logo` (dashboard logo)
- `threshold_yellow`
- `threshold_red`
- `poll_interval_seconds`
- `ffmpeg_timeout_seconds`
- `retry_attempts`
- `roi`

## FFmpeg Sample Streams

```bash
ffmpeg -re -i videoplayback.mp4 -c:v libx264 -preset veryfast -tune zerolatency -b:v 4M -maxrate 4M -bufsize 8M -c:a aac -b:a 128k -mpegts_flags resend_headers -f mpegts "udp://239.1.1.1:1234?pkt_size=1316&ttl=16&fifo_size=1000000&overrun_nonfatal=1"

ffmpeg -re -i withlogo.mp4 -c:v libx264 -preset veryfast -tune zerolatency -b:v 4M -maxrate 4M -bufsize 8M -c:a aac -b:a 128k -mpegts_flags resend_headers -f mpegts "udp://239.1.1.1:1235?pkt_size=1316&ttl=16&fifo_size=1000000&overrun_nonfatal=1"
```
