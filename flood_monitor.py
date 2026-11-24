"""Monitor an active-high flood sensor and report flood events to SHE Digital."""

import argparse
import json
import logging
import os
import signal
import socket
from datetime import datetime, timezone
from queue import Empty, Queue
from threading import Event, Thread
from time import monotonic
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4


POLL_INTERVAL = 0.05
STABLE_SECONDS = 0.25
RETRY_SECONDS = 10
HEARTBEAT_SECONDS = 15


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


def device_request(url, token, payload):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return Request(url, json.dumps(payload).encode("utf-8"), headers, method="POST")


def simulate_alert(url, token, device_id):
    if not url or not token:
        raise SystemExit("Set SHE_FLOOD_ALERT_URL and SHE_FLOOD_ALERT_TOKEN before sending a test alert.")
    event = {
        "event_id": str(uuid4()),
        "event_type": "flood_detected",
        "device_id": device_id,
        "location": os.getenv("FLOOD_DEVICE_LOCATION", "").strip(),
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "simulated": True,
    }
    try:
        with build_opener(NoRedirect).open(device_request(url, token, event), timeout=5):
            pass
    except HTTPError as error:
        status = error.code
        error.close()
        raise SystemExit(f"SHE Digital rejected the test alert (HTTP {status}).") from None
    except OSError as error:
        raise SystemExit(f"Could not send the test alert: {error}.") from None
    print(f"Test flood alert sent ({event['event_id']}).")


def send_alerts(outbox, stop, url, token):
    opener = build_opener(NoRedirect)
    while True:
        alert = outbox.get()
        if stop.is_set():
            return
        request = device_request(url, token, alert)
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


def send_status(updates, stop, url, token):
    opener = build_opener(NoRedirect)
    current = None
    while not stop.is_set():
        try:
            current = updates.get(timeout=HEARTBEAT_SECONDS)
            while True:
                current = updates.get_nowait()
        except Empty:
            pass
        if current is None or stop.is_set():
            continue
        try:
            with opener.open(device_request(url, token, current), timeout=5):
                pass
        except HTTPError as error:
            logging.warning("Flood sensor status received HTTP %s", error.code)
            error.close()
        except (URLError, OSError) as error:
            logging.warning("Flood sensor status could not be sent: %s", error)


def monitor(sensor, siren, stop, outbox, device_id, status_updates=None):
    device = {"device_id": device_id, "location": os.getenv("FLOOD_DEVICE_LOCATION", "").strip()}
    flooded = None
    last_reading = None
    changed_at = monotonic()
    while not stop.is_set():
        reading = sensor.is_active
        now = monotonic()
        if reading != last_reading:
            last_reading = reading
            changed_at = now
        if reading != flooded and now - changed_at >= STABLE_SECONDS:
            flooded = reading
            status_since = datetime.now(timezone.utc).isoformat()
            event_id = None
            if flooded:
                siren.on()
                logging.warning("Flood detected. Siren on.")
                event_id = str(uuid4())
                if outbox is not None:
                    outbox.put({
                        **device,
                        "event_id": event_id,
                        "event_type": "flood_detected",
                        "detected_at": status_since,
                    })
            else:
                siren.off()
                logging.info("Sensor dry. Siren off.")
            if status_updates is not None:
                status_updates.put({
                    **device,
                    "status": "flooded" if flooded else "normal",
                    "status_since": status_since,
                    "event_id": event_id,
                })
        stop.wait(POLL_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--simulate-alert", action="store_true",
                        help="Send one labeled web test alert without using GPIO or changing live sensor status.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = os.getenv("SHE_FLOOD_ALERT_URL", "").strip()
    token = os.getenv("SHE_FLOOD_ALERT_TOKEN", "").strip()
    endpoint = urlsplit(url)
    if url and (endpoint.scheme not in ("http", "https") or not endpoint.hostname):
        raise SystemExit("Set SHE_FLOOD_ALERT_URL to an absolute HTTP or HTTPS URL.")
    if url and token and endpoint.scheme != "https":
        raise SystemExit("Use an HTTPS alert URL when sending an authentication token.")

    device_id = os.getenv("FLOOD_DEVICE_ID") or socket.gethostname()
    if args.simulate_alert:
        simulate_alert(url, token, device_id)
        return

    sensor_pin = int(os.getenv("FLOOD_SENSOR_GPIO", "17"))
    siren_pin = int(os.getenv("FLOOD_SIREN_GPIO", "27"))
    if sensor_pin == siren_pin:
        raise SystemExit("Choose different BCM pins for the sensor and siren.")

    from gpiozero import DigitalInputDevice, OutputDevice

    stop = Event()

    def handle_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    outbox = Queue() if url else None
    status_updates = Queue() if url else None
    siren_active_high = os.getenv("FLOOD_SIREN_ACTIVE_LOW", "0") != "1"
    with DigitalInputDevice(sensor_pin, pull_up=False) as sensor, OutputDevice(
        siren_pin, active_high=siren_active_high, initial_value=False
    ) as siren:
        if outbox is None:
            logging.info("Running in siren-only mode. Set SHE_FLOOD_ALERT_URL to send alerts.")
        else:
            Thread(target=send_alerts, args=(outbox, stop, url, token), daemon=True).start()
            status_url = endpoint._replace(path=endpoint.path.rstrip("/") + "/status").geturl()
            Thread(target=send_status, args=(status_updates, stop, status_url, token), daemon=True).start()
        try:
            monitor(sensor, siren, stop, outbox, device_id, status_updates)
        finally:
            stop.set()
            siren.off()


if __name__ == "__main__":
    main()
