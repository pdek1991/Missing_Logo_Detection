import glob
import json
import os
import queue
import threading
import time
from datetime import datetime

from loguru import logger


class ReportManager:
    def __init__(self, logs_dir="logs"):
        self.logs_dir = logs_dir
        self.data_file = os.path.join(logs_dir, "report_data.json")
        self.active_alerts = {}
        self._lock = threading.Lock()

        self._report_queue = queue.Queue(maxsize=2000)
        self._report_stop = threading.Event()
        self._report_worker = threading.Thread(
            target=self._report_worker_loop,
            daemon=True,
            name="report-writer",
        )
        self._report_worker.start()

        self._load_data()

    def _load_data(self):
        with self._lock:
            if os.path.exists(self.data_file):
                try:
                    with open(self.data_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        self.active_alerts = data.get("active_alerts", {})
                except Exception as e:
                    logger.error("Failed to load report_data.json: {}", e)
                    self.active_alerts = {}

    def _save_data(self):
        os.makedirs(self.logs_dir, exist_ok=True)
        try:
            with open(self.data_file, "w", encoding="utf-8") as f:
                json.dump({"active_alerts": self.active_alerts}, f)
        except Exception as e:
            logger.error("Failed to save report_data.json: {}", e)

    def record_event(self, channel, status):
        """
        Records incidents only on state transition to avoid heavy I/O every cycle.
        Returns True if a transition occurred (and thus a report update is needed).
        """
        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        date_today = now.strftime("%Y-%m-%d")

        updated = False
        with self._lock:
            if status == "MISSING DETECTED":
                if channel not in self.active_alerts:
                    self.active_alerts[channel] = now_str
                    self._save_data()
                    updated = True
                    logger.info("Incident recorded for channel '{}'", channel)

            elif status == "LOGO PRESENT":
                if channel in self.active_alerts:
                    start_time = self.active_alerts.pop(channel)
                    self._save_data()
                    self._append_to_history(date_today, channel, start_time, now_str)
                    updated = True
                    logger.info("Incident recovered for channel '{}'", channel)

        return updated

    @staticmethod
    def _format_duration(seconds):
        seconds = max(0, int(seconds))
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def _append_to_history(self, date_str, channel, start, end):
        history_file = os.path.join(self.logs_dir, f"history_{date_str}.json")

        history = []
        if os.path.exists(history_file):
            try:
                with open(history_file, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception as e:
                logger.error("Failed to read history file {}: {}", history_file, e)
                history = []

        duration_seconds = 0
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            duration_seconds = int((end_dt - start_dt).total_seconds())
        except ValueError:
            duration_seconds = 0

        history.append(
            {
                "channel": channel,
                "off_time": start,
                "recover_time": end,
                "duration_seconds": max(0, duration_seconds),
                "duration": self._format_duration(duration_seconds),
            }
        )

        try:
            with open(history_file, "w", encoding="utf-8") as f:
                json.dump(history, f, indent=2)
        except Exception as e:
            logger.error("Failed to write history file {}: {}", history_file, e)

    def generate_daily_report(self, force=False):
        """
        Generates the HTML report.
        In production, this should only be called when record_event returns True.
        """
        del force
        now = datetime.now()
        date_today = now.strftime("%Y-%m-%d")
        history_file = os.path.join(self.logs_dir, f"history_{date_today}.json")
        report_file = os.path.join(self.logs_dir, f"report_{date_today}.html")

        with self._lock:
            history = []
            if os.path.exists(history_file):
                try:
                    with open(history_file, "r", encoding="utf-8") as f:
                        history = json.load(f)
                except Exception as e:
                    logger.error("Failed to read history during report generation: {}", e)

            active_alerts = dict(self.active_alerts)
            html = self._build_html_report(date_today, history, active_alerts=active_alerts)

            try:
                with open(report_file, "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception as e:
                logger.error("Failed to write HTML report: {}", e)

            self._cleanup_old_reports()

    def generate_daily_report_async(self):
        try:
            self._report_queue.put_nowait(1)
        except queue.Full:
            # Skip if writer is already backlogged; next batch will include latest state.
            pass

    def _report_worker_loop(self):
        pending = False
        last_flush = time.monotonic()

        while True:
            if self._report_stop.is_set() and self._report_queue.empty() and not pending:
                break

            try:
                self._report_queue.get(timeout=0.5)
                pending = True
            except queue.Empty:
                pass

            now = time.monotonic()
            should_flush = pending and ((now - last_flush) >= 1.0 or self._report_stop.is_set())
            if should_flush:
                try:
                    self.generate_daily_report()
                except Exception as exc:
                    logger.error("Report writer loop failed: {}", exc)
                pending = False
                last_flush = now

    def shutdown(self, timeout=2.0):
        self._report_stop.set()
        try:
            self._report_queue.put_nowait(1)
        except queue.Full:
            pass

        if self._report_worker.is_alive():
            self._report_worker.join(timeout=max(0.1, float(timeout)))

    def _build_html_report(self, date_str, history, active_alerts=None):
        active_alerts = active_alerts or {}
        rows = ""
        active_rows = ""
        now_dt = datetime.now()

        if active_alerts:
            for channel in sorted(active_alerts.keys()):
                start = active_alerts.get(channel, "")
                duration_text = "00:00:00"
                try:
                    start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
                    duration_text = self._format_duration((now_dt - start_dt).total_seconds())
                except ValueError:
                    pass

                active_rows += f"""
                <tr>
                    <td>{channel}</td>
                    <td style='color: #e74c3c; font-weight: bold;'>{start}</td>
                    <td style='color: #e67e22; font-weight: bold;'>ACTIVE</td>
                    <td style='font-weight: 600;'>{duration_text}</td>
                </tr>
                """

        if not history and not active_rows:
            rows = "<tr><td colspan='4' style='text-align:center;'>No incidents recorded today.</td></tr>"
        else:
            for item in history:
                rows += f"""
                <tr>
                    <td>{item.get('channel', '')}</td>
                    <td style='color: #e74c3c; font-weight: bold;'>{item.get('off_time', '')}</td>
                    <td style='color: #27ae60; font-weight: bold;'>{item.get('recover_time', '')}</td>
                    <td style='font-weight: 600;'>{item.get('duration', '00:00:00')}</td>
                </tr>
                """

        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Logo Detection Daily Report - {date_str}</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f7f6; padding: 20px; }}
                h1 {{ color: #2c3e50; text-align: center; }}
                table {{ width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                th {{ background-color: #2980b9; color: white; padding: 15px; text-align: left; text-transform: uppercase; font-size: 14px; }}
                td {{ padding: 12px 15px; border-bottom: 1px solid #eee; font-size: 14px; color: #333; }}
                tr:last-child td {{ border-bottom: none; }}
                tr:hover {{ background-color: #f9f9f9; }}
                .container {{ max-width: 1000px; margin: 0 auto; }}
                .footer {{ margin-top: 20px; text-align: center; font-size: 12px; color: #7f8c8d; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Logo Absence Report ({date_str})</h1>
                <table>
                    <thead>
                        <tr>
                            <th>Channel Name</th>
                            <th>Logo Missing Time</th>
                            <th>Recovery Time</th>
                            <th>Duration (HH:MM:SS)</th>
                        </tr>
                    </thead>
                    <tbody>
                        {active_rows}
                        {rows}
                    </tbody>
                </table>
                <div class="footer">Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
            </div>
        </body>
        </html>
        """
        return html

    def _cleanup_old_reports(self):
        report_files = glob.glob(os.path.join(self.logs_dir, "report_*.html"))
        history_files = glob.glob(os.path.join(self.logs_dir, "history_*.json"))

        for files in (report_files, history_files):
            if len(files) <= 7:
                continue

            files.sort()
            to_delete = files[:-7]
            for item in to_delete:
                try:
                    os.remove(item)
                except Exception as e:
                    logger.error("Failed to delete old file {}: {}", item, e)
