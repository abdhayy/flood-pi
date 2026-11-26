"""Monitor active-low flood and active-high relay feedback inputs for SHE Digital."""

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
SSH_PREVIEW_URL = "http://127.0.0.1:13610/api/flood-alerts"


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


def simulate_sensor(url, token, device_id):
    stop = Event()
    statuses = Queue()
    alerts = Queue()
    device = {"device_id": device_id[:115] + "-simulation", "simulated": True}
    status_url = url.rstrip("/") + "/status"
    Thread(target=send_status, args=(statuses, stop, status_url, token), daemon=True).start()
    Thread(target=send_alerts, args=(alerts, stop, url, token), daemon=True).start()
    current = None

    def publish(status):
        nonlocal current
        if status == current:
            return
        current = status
        timestamp = datetime.now(timezone.utc).isoformat()
        event_id = str(uuid4()) if status == "flooded" else None
        if event_id:
            alerts.put({**device, "event_id": event_id, "event_type": "flood_detected",
                        "detected_at": timestamp})
        statuses.put({**device, "status": status, "status_since": timestamp, "event_id": event_id})

    publish("normal")
    try:
        while True:
            command = input("Simulated sensor [dry / wet / quit]: ").strip().lower()
            if command == "quit":
                break
            if command in ("dry", "wet"):
                publish("normal" if command == "dry" else "flooded")
            else:
                print("Enter dry, wet, or quit.")
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        stop.set()


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


class DebouncedInput:
    def __init__(self):
        self.value = None
        self.last_reading = None
        self.changed_at = monotonic()

    def update(self, reading, now):
        if reading != self.last_reading:
            self.last_reading = reading
            self.changed_at = now
        if reading != self.value and now - self.changed_at >= STABLE_SECONDS:
            self.value = reading
            return True
        return False


def monitor(sensor, relay_feedback, stop, outbox, device_id, status_updates=None):
    device = {"device_id": device_id, "location": os.getenv("FLOOD_DEVICE_LOCATION", "").strip()}
    flood = DebouncedInput()
    relay = DebouncedInput()
    status_since = None
    event_id = None
    while not stop.is_set():
        now = monotonic()
        flood_changed = flood.update(sensor.is_active, now)
        # GPIO Zero's pull-up input is active-low. Invert it to report physical HIGH.
        relay_changed = relay.update(not relay_feedback.is_active, now)
        if relay_changed:
            logging.info("Siren relay feedback: %s.", "ON" if relay.value else "OFF")
        if flood_changed:
            status_since = datetime.now(timezone.utc).isoformat()
            event_id = None
            if flood.value:
                logging.warning("Flood detected.")
                event_id = str(uuid4())
                if outbox is not None:
                    outbox.put({
                        **device,
                        "event_id": event_id,
                        "event_type": "flood_detected",
                        "detected_at": status_since,
                    })
            else:
                logging.info("Sensor dry.")
        if status_updates is not None and status_since is not None and (flood_changed or relay_changed):
            status_updates.put({
                **device,
                "status": "flooded" if flood.value else "normal",
                "status_since": status_since,
                "event_id": event_id,
                "relay_on": relay.value,
            })
        stop.wait(POLL_INTERVAL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    simulation = parser.add_mutually_exclusive_group()
    simulation.add_argument("--simulate-alert", action="store_true",
                            help="Send one labeled web test alert without using GPIO or changing live sensor status.")
    simulation.add_argument("--simulate-sensor", action="store_true",
                            help="Interactively send dry or wet readings to the laptop preview, without using GPIO.")
    parser.add_argument("--laptop-preview", action="store_true",
                        help="Send to the laptop preview through the SSH tunnel on local port 13610.")
    args = parser.parse_args()
    if args.simulate_sensor and not args.laptop_preview:
        parser.error("--simulate-sensor requires --laptop-preview.")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    url = SSH_PREVIEW_URL if args.laptop_preview else os.getenv("SHE_FLOOD_ALERT_URL", "").strip()
    token = os.getenv("SHE_FLOOD_ALERT_TOKEN", "").strip()
    if args.laptop_preview and not token:
        parser.error("Set SHE_FLOOD_ALERT_TOKEN to the laptop preview token.")
    endpoint = urlsplit(url)
    if url and (endpoint.scheme not in ("http", "https") or not endpoint.hostname):
        raise SystemExit("Set SHE_FLOOD_ALERT_URL to an absolute HTTP or HTTPS URL.")
    if url and token and endpoint.scheme != "https" and not args.laptop_preview:
        raise SystemExit("Use an HTTPS alert URL when sending an authentication token.")

    device_id = os.getenv("FLOOD_DEVICE_ID") or socket.gethostname()
    if args.simulate_alert:
        simulate_alert(url, token, device_id)
        return
    if args.simulate_sensor:
        simulate_sensor(url, token, device_id)
        return

    sensor_pin = int(os.getenv("FLOOD_SENSOR_GPIO", "17"))
    feedback_pin = int(os.getenv("FLOOD_SIREN_STATUS_GPIO", os.getenv("FLOOD_SIREN_GPIO", "27")))
    if sensor_pin == feedback_pin:
        raise SystemExit("Choose different BCM pins for the sensor and siren relay feedback.")

    from gpiozero import DigitalInputDevice

    stop = Event()

    def handle_stop(signum, frame):
        stop.set()

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    outbox = Queue() if url else None
    status_updates = Queue() if url else None
    with DigitalInputDevice(sensor_pin, pull_up=True) as sensor, DigitalInputDevice(
        feedback_pin, pull_up=True
    ) as relay_feedback:
        logging.info("Sensor GPIO%d: LOW means wet. Relay feedback GPIO%d: HIGH means ON. Both pins are inputs.",
                     sensor_pin, feedback_pin)
        if outbox is None:
            logging.info("Monitoring locally. Set SHE_FLOOD_ALERT_URL to report readings to SHE Digital.")
        else:
            Thread(target=send_alerts, args=(outbox, stop, url, token), daemon=True).start()
            status_url = endpoint._replace(path=endpoint.path.rstrip("/") + "/status").geturl()
            Thread(target=send_status, args=(status_updates, stop, status_url, token), daemon=True).start()
        try:
            monitor(sensor, relay_feedback, stop, outbox, device_id, status_updates)
        finally:
            stop.set()


if __name__ == "__main__":
    main()
