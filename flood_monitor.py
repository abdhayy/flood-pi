"""Monitor an active-high flood sensor and report flood events to SHE Digital."""

import json
import logging
import os
import signal
import socket
from datetime import datetime, timezone
from queue import Queue
from threading import Event, Thread
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4


POLL_INTERVAL = 0.05
STABLE_SECONDS = 0.25
RETRY_SECONDS = 10


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def send_alerts(outbox, stop, url, token):
    opener = build_opener(NoRedirect)
    while True:
        alert = outbox.get()
        if stop.is_set():
            return
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = Request(url, json.dumps(alert).encode("utf-8"), headers, method="POST")
        while not stop.is_set():
            try:
                with opener.open(request, timeout=5) as response:
                    if not 200 <= response.status < 300:
                        raise OSError(f"Unexpected response status {response.status}")
                logging.info("SHE Digital accepted flood alert %s", alert["event_id"])
                break
            except HTTPError as error:
                status = error.code
                error.close()
                logging.warning("Flood alert %s received HTTP %s", alert["event_id"], status)
                if 300 <= status < 500 and status not in (408, 429):
                    break
            except (URLError, OSError) as error:
                logging.warning("Flood alert %s could not be sent: %s", alert["event_id"], error)
            stop.wait(RETRY_SECONDS)


def monitor(sensor, siren, stop, outbox, device_id):
    flooded = False
    last_reading = False
    changed_at = monotonic()
    while not stop.is_set():
        reading = sensor.is_active
        now = monotonic()
        if reading != last_reading:
            last_reading = reading
            changed_at = now
        if reading != flooded and now - changed_at >= STABLE_SECONDS:
            flooded = reading
            if flooded:
                siren.on()
                logging.warning("Flood detected. Siren on.")
                if outbox is not None:
                    outbox.put({
                        "event_id": str(uuid4()),
                        "event_type": "flood_detected",
                        "device_id": device_id,
                        "detected_at": datetime.now(timezone.utc).isoformat(),
                    })
            else:
                siren.off()
                logging.info("Sensor dry. Siren off.")
        stop.wait(POLL_INTERVAL)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    sensor_pin = int(os.getenv("FLOOD_SENSOR_GPIO", "17"))
    siren_pin = int(os.getenv("FLOOD_SIREN_GPIO", "27"))
    if sensor_pin == siren_pin:
        raise SystemExit("Choose different BCM pins for the sensor and siren.")

    url = os.getenv("SHE_FLOOD_ALERT_URL", "").strip()
    token = os.getenv("SHE_FLOOD_ALERT_TOKEN", "").strip()
    endpoint = urlsplit(url)
    if url and (endpoint.scheme not in ("http", "https") or not endpoint.hostname):
        raise SystemExit("Set SHE_FLOOD_ALERT_URL to an absolute HTTP or HTTPS URL.")
    if url and token and endpoint.scheme != "https":
        raise SystemExit("Use an HTTPS alert URL when sending an authentication token.")

    from gpiozero import DigitalInputDevice, OutputDevice

    stop = Event()

    def handle_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    outbox = Queue() if url else None
    siren_active_high = os.getenv("FLOOD_SIREN_ACTIVE_LOW", "0") != "1"
    with DigitalInputDevice(sensor_pin, pull_up=False) as sensor, OutputDevice(
        siren_pin, active_high=siren_active_high, initial_value=False
    ) as siren:
        if outbox is None:
            logging.info("Running in siren-only mode. Set SHE_FLOOD_ALERT_URL to send alerts.")
        else:
            Thread(target=send_alerts, args=(outbox, stop, url, token), daemon=True).start()
        try:
            monitor(sensor, siren, stop, outbox, os.getenv("FLOOD_DEVICE_ID") or socket.gethostname())
        finally:
            stop.set()
            siren.off()


if __name__ == "__main__":
    main()
