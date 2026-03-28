# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads the last N events from Event Hubs and serves a live dashboard
#
# Routes:
#   GET  /              → serves the demo e-commerce store (client.html)
#   POST /track         → receives a click event and publishes it to Event Hubs
#   GET  /dashboard     → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent events as JSON (polled by the dashboard)
#   GET  /api/analytics → returns processed analytics from Stream Analytics (NEW)

import os
import json
import threading
from datetime import datetime, timezone
from collections import defaultdict

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

# Azure Event Hubs SDK
from azure.eventhub import EventHubProducerClient, EventData, EventHubConsumerClient

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream")

# NEW: Configuration for analytics event hub
ANALYTICS_EVENT_HUB = os.environ.get("ANALYTICS_EVENT_HUB", "clickstream-analytics")
ANALYTICS_CONSUMER_GROUP = "$Default"

# In-memory buffer: stores the last 50 events received by the consumer thread.
_event_buffer = []
_buffer_lock = threading.Lock()
MAX_BUFFER = 50

# NEW: Data structures for analytics results
_device_breakdown = defaultdict(lambda: {"counts": {}, "last_update": None})
_spike_detection = {"current_spike": None, "history": [], "last_update": None}
_analytics_lock = threading.Lock()

# Store last 5 minutes of spike history (60 entries = 5 minutes with 5-second windows)
MAX_SPIKE_HISTORY = 60


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – skipping Event Hubs publish")
        return

    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME,
    )
    with producer:
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer thread – reads events from Event Hubs and buffers them
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    """Callback invoked by the consumer client for each incoming event."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _event_buffer.append(data)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    partition_context.update_checkpoint(event)


def start_consumer():
    """Start the Event Hubs consumer in a background daemon thread."""
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – consumer thread not started")
        return

    def run():
        """Run the sync consumer in a thread."""
        try:
            consumer = EventHubConsumerClient.from_connection_string(
                conn_str=CONNECTION_STR,
                consumer_group="$Default",
                eventhub_name=EVENT_HUB_NAME,
            )
            with consumer:
                consumer.receive(
                    on_event=_on_event,
                    starting_position="-1",
                    max_wait_time=5
                )
        except Exception as e:
            app.logger.error(f"Consumer thread error: {e}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Event Hubs consumer thread started")


# ---------------------------------------------------------------------------
# Background consumer for Stream Analytics output
# ---------------------------------------------------------------------------
def start_analytics_consumer():
    """Start consumer for Stream Analytics output in background thread."""
    app.logger.info("🔧 start_analytics_consumer() called")
    app.logger.info(f"🔧 CONNECTION_STR set: {bool(CONNECTION_STR)}")
    app.logger.info(f"🔧 ANALYTICS_EVENT_HUB: {ANALYTICS_EVENT_HUB}")
    
    if not CONNECTION_STR:
        app.logger.warning("Connection string not set - analytics consumer not started")
        return
    
    def on_event_sync(partition_context, event):
        """Sync callback for events"""
        try:
            body = event.body_as_str(encoding="UTF-8")
            app.logger.info(f"🔥 ANALYTICS CONSUMER GOT: {body[:200]}")
            
            data = json.loads(body)
            app.logger.info(f"📊 PARSED: analytics_type={data.get('analytics_type')}")
            
            with _analytics_lock:
                if data.get("analytics_type") == "device_breakdown":
                    dimension = data.get("dimension")
                    event_count = data.get("event_count")
                    percentage = data.get("percentage", 0)
                    timestamp = data.get("timestamp")
                    
                    app.logger.info(f"📱 DEVICE: {dimension} = {event_count} ({percentage}%)")
                    
                    _device_breakdown["counts"][dimension] = {
                        "count": event_count,
                        "percentage": percentage
                    }
                    _device_breakdown["total"] = sum(v["count"] for v in _device_breakdown["counts"].values())
                    _device_breakdown["last_update"] = timestamp
                    
                elif data.get("analytics_type") == "spike_detection":
                    current_events = data.get("current_events")
                    avg_events = data.get("avg_events_1min")
                    timestamp = data.get("timestamp")
                    
                    app.logger.info(f"⚠️ SPIKE: {current_events} events (avg: {avg_events})")
                    
                    spike_info = {
                        "timestamp": timestamp,
                        "current_events": current_events,
                        "avg_events": avg_events,
                        "severity": data.get("spike_severity"),
                        "percent_of_average": data.get("percent_of_average")
                    }
                    
                    _spike_detection["current_spike"] = spike_info
                    if len(_spike_detection["history"]) >= MAX_SPIKE_HISTORY:
                        _spike_detection["history"].pop(0)
                    _spike_detection["history"].append(spike_info)
                    _spike_detection["last_update"] = timestamp
                    
                else:
                    app.logger.info(f"❓ Unknown analytics type: {data.get('analytics_type')}")
            
            partition_context.update_checkpoint(event)
            
        except Exception as e:
            app.logger.error(f"❌ Error processing event: {e}")
    
    def run_consumer():
        """Run the sync consumer"""
        try:
            app.logger.info("🔧 Creating EventHubConsumerClient...")
            consumer = EventHubConsumerClient.from_connection_string(
                conn_str=CONNECTION_STR,
                consumer_group=ANALYTICS_CONSUMER_GROUP,
                eventhub_name=ANALYTICS_EVENT_HUB,
            )
            app.logger.info(f"🔧 Consumer client created for {ANALYTICS_EVENT_HUB}")
            
            with consumer:
                app.logger.info("🔧 Starting consumer.receive()...")
                consumer.receive(
                    on_event=on_event_sync,
                    starting_position="-1",
                    max_wait_time=5
                )
        except Exception as e:
            app.logger.error(f"🔧 Analytics consumer failed: {e}")
    
    thread = threading.Thread(target=run_consumer, daemon=True)
    thread.start()
    app.logger.info("✅ Analytics consumer thread started (sync version)")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    """Health check – used by Azure App Service to verify the app is running."""
    return jsonify({"status": "healthy"}), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.
    Now includes deviceType, browser, and os fields.
    """
    if not request.json:
        abort(400)

    # Enrich the event with server-side timestamp and device info
    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "deviceType": request.json.get("deviceType", "unknown"),
        "browser":    request.json.get("browser", "unknown"),
        "os":         request.json.get("os", "unknown"),
        "client_timestamp": request.json.get("timestamp"),
        "server_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    send_to_event_hubs(event)

    # Also buffer locally so the dashboard works even without a consumer thread
    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return the buffered events as JSON.
    The dashboard polls this endpoint every 2 seconds.
    """
    try:
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])

    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify({"events": recent, "summary": summary, "total": len(recent)}), 200


@app.route("/api/analytics", methods=["GET"])
def get_analytics():
    """Return processed analytics from Stream Analytics."""
    with _analytics_lock:
        return jsonify({
            "device_breakdown": {
                "data": dict(_device_breakdown.get("counts", {})),
                "total_events": _device_breakdown.get("total", 0),
                "last_update": _device_breakdown.get("last_update")
            },
            "spike_detection": {
                "current": _spike_detection.get("current_spike"),
                "recent_history": _spike_detection.get("history", [])[-20:],
                "last_update": _spike_detection.get("last_update")
            }
        }), 200


@app.route("/debug/consumer", methods=["GET"])
def debug_consumer():
    """Check analytics consumer status"""
    return jsonify({
        "device_breakdown": dict(_device_breakdown.get("counts", {})),
        "spike_detection": _spike_detection.get("current_spike"),
        "consumer_should_be_running": True,
        "analytics_hub": ANALYTICS_EVENT_HUB
    })


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background consumers
    start_consumer()
    start_analytics_consumer()
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
