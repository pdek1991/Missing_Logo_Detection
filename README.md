# Missing Logo Detector

Real-time NOC-grade logo monitoring for up to 50 channels (24x7 operation).

## What It Does

- Detects missing channel logo using template matching only
- Runs fast checks at 1-second cadence (configurable)
- Applies sports secondary ROI fallback when primary ROI score is weak
- Uses 5-second confirmation window (1 frame/second) before RED escalation
- Pushes immediate YELLOW during recheck so operators get early visibility
- Updates dashboard in real-time via SSE with HTTP polling fallback

## Detection and Alert Logic

1. Frame is captured from persistent FFmpeg stream worker.
2. Logo score is computed from configured ROI.
3. If score indicates likely missing:
   - Status immediately becomes `LOGO MAYBE MISSING` (YELLOW)
   - Non-blocking recheck starts (5 checks in 5 seconds)
4. Final result after recheck:
   - `MISSING DETECTED` (RED) only if logo remains missing for full window
   - `STREAM TIMEOUT` if all recheck captures fail
   - `LOGO PRESENT` if false alarm

## Reliability and Auto-Heal

- One persistent FFmpeg process per stream (no per-cycle process churn)
- FFmpeg watchdog restarts stalled readers automatically
- Rawvideo (`bgr24`) pipeline removes PNG encode/decode overhead
- Recheck stale-lock recovery: forced reset after 15s
- Scheduler autonomous restart loop on fatal failure
- API watchdog restarts API thread if it dies
- Daily report generation is queue-based with a single background writer

## Dashboard (10x5 Grid)

- Fixed 10x5 tile layout optimized for 50 channels
- Per-tile mini timeline + stability % for flicker visibility
- Non-blocking toast notifications (bottom-right)
- Persistent `Mute Alarms` and `Silence 15m` controls
- Status counters shown only when any RED/YELLOW/WHITE exists
- Header title and browser tab title: `Missing Logo Detector`
- Runtime branding uses root `logo.png` via `/runtime-logo`

## API Endpoints

- `GET /health`
- `GET /api/config`
- `GET /api/logo_status`
- `GET /api/logo_status/{channel}`
- `GET /api/logo_status/stream` (SSE stream)

## Status Semantics

- `GREEN`: logo present
- `YELLOW`: warmup / stream issue / recheck-in-progress / ambiguous state
- `RED`: confirmed missing logo
- `WHITE`: intentional/scheduled international logo-off window

## Build (PyInstaller)

Use this optimized command (small launcher executable in `--onedir` output):

```powershell
pyinstaller --noconfirm --clean --onefile --name MissingLogoDetector --icon=logo.ico --add-data "web;web" --add-data "logo.png;." --exclude-module matplotlib --exclude-module tkinter --exclude-module PyQt5 --exclude-module IPython --exclude-module jupyter main.py
```

## FFmpeg Command

Use this to validate stream readability and frame extraction:

# Sony Pal
``` 
ffmpeg -stream_loop -1 -re -fflags +genpts -i withlogo.mp4 -c:v libx264 -preset veryfast -tune zerolatency -profile:v high -g 50 -keyint_min 50 -sc_threshold 0 -b:v 4M -maxrate 4M -minrate 4M -bufsize 2M -pix_fmt yuv420p -c:a aac -b:a 128k -ar 48000 -f mpegts -mpegts_flags resend_headers -muxrate 5M "udp://239.1.1.1:1235?pkt_size=1316&ttl=16&buffer_size=10000000&fifo_size=1000000&overrun_nonfatal=1"


```

# Sony SAB HD
``` 
ffmpeg -stream_loop -1 -re -fflags +genpts -i videoplayback.mp4 -c:v libx264 -preset veryfast -tune zerolatency -profile:v high -g 50 -keyint_min 50 -sc_threshold 0 -b:v 4M -maxrate 4M -minrate 4M -bufsize 2M -pix_fmt yuv420p -c:a aac -b:a 128k -ar 48000 -f mpegts -mpegts_flags resend_headers -muxrate 5M "udp://239.1.1.1:1234?pkt_size=1316&ttl=16&buffer_size=10000000&fifo_size=1000000&overrun_nonfatal=1"

```

# Sports
``` 
ffmpeg -stream_loop -1 -re -i output_02_04.mp4 -c:v libx264 -preset veryfast -tune zerolatency -b:v 4M -maxrate 4M -bufsize 8M -c:a aac -b:a 128k -mpegts_flags resend_headers -f mpegts "udp://239.1.1.1:1236?pkt_size=1316&ttl=16&fifo_size=1000000&overrun_nonfatal=1"

```

## Runtime Assets (External)

Keep these next to the executable for field updates:

- `config.yaml`
- `logo.png`
- `logos/`
- `channel xml/`
- `ffmpeg.exe` (or ffmpeg available in `PATH`)
