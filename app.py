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
import asyncio  # NEW for async consumer
from collections import defaultdict  # NEW for analytics storage

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

# Azure Event Hubs SDK
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.aio import EventHubConsumerClient  # NEW async consumer

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream")

# NEW: Configuration for analytics event hub
ANALYTICS_EVENT_HUB = os.environ.get("ANALYTICS_EVENT_HUB", "clickstream")
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

    async def on_event_async(partition_context, event):
        """Async callback for events."""
        if event:
            body = event.body_as_str(encoding="UTF-8")
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                data = {"raw": body}

            with _buffer_lock:
                _event_buffer.append(data)
                if len(_event_buffer) > MAX_BUFFER:
                    _event_buffer.pop(0)

            await partition_context.update_checkpoint(event)

    async def receive_events():
        """Async function to receive events."""
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            consumer_group="$Default",
            eventhub_name=EVENT_HUB_NAME,
        )
        
        async with consumer:
            await consumer.receive(
                on_event=on_event_async,
                starting_position="-1",
                on_error=lambda e: app.logger.error(f"Consumer error: {e}")
            )

    def run():
        """Run the async consumer in a thread."""
        try:
            asyncio.run(receive_events())
        except Exception as e:
            app.logger.error(f"Consumer thread error: {e}")

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Event Hubs consumer thread started")

# ---------------------------------------------------------------------------
# NEW: Background consumer for Stream Analytics output
# ---------------------------------------------------------------------------
async def on_analytics_event(partition_context, event):
    """Process events from Stream Analytics output."""
    body = event.body_as_str(encoding="UTF-8")
    
    # ADD THIS DEBUG LINE
    app.logger.info(f"📨 RAW ANALYTICS EVENT: {body}")
    
    try:
        data = json.loads(body)
        
        # ADD THIS DEBUG LINE
        app.logger.info(f"📊 PARSED DATA: {data}")
        app.logger.info(f"🔍 ANALYTICS TYPE: {data.get('analytics_type')}")
        
    except json.JSONDecodeError:
        app.logger.warning(f"Failed to parse analytics event: {body}")
        return
    
    with _analytics_lock:
        if data.get("analytics_type") == "device_breakdown":
            # ADD THIS DEBUG LINE
            app.logger.info(f"📱 PROCESSING DEVICE BREAKDOWN: dimension={data.get('dimension')}, count={data.get('event_count')}")
            
            timestamp = data.get("timestamp")
            dimension = data.get("dimension")
            count = data.get("event_count")
            percentage = data.get("percentage", 0)
            
            _device_breakdown["counts"][dimension] = {
                "count": count,
                "percentage": percentage
            }
            _device_breakdown["last_update"] = timestamp
            _device_breakdown["total"] = sum(
                v["count"] for v in _device_breakdown["counts"].values()
            )
            
            # ADD THIS DEBUG LINE
            app.logger.info(f"✅ DEVICE BREAKDOWN UPDATED: {_device_breakdown['counts']}")
            
        elif data.get("analytics_type") == "spike_detection":
            # ADD THIS DEBUG LINE
            app.logger.info(f"⚠️ PROCESSING SPIKE DETECTION: current_events={data.get('current_events')}")
            
            spike_info = {
                "timestamp": data.get("timestamp"),
                "current_events": data.get("current_events"),
                "avg_events": data.get("avg_events_1min"),
                "severity": data.get("spike_severity"),
                "percent_of_average": data.get("percent_of_average")
            }
            
            _spike_detection["current_spike"] = spike_info
            _spike_detection["history"].append(spike_info)
            
            if len(_spike_detection["history"]) > MAX_SPIKE_HISTORY:
                _spike_detection["history"].pop(0)
            
            _spike_detection["last_update"] = data.get("timestamp")
            
            # ADD THIS DEBUG LINE
            app.logger.info(f"✅ SPIKE DETECTION UPDATED: {spike_info}")
        else:
            # ADD THIS DEBUG LINE
            app.logger.warning(f"❓ UNKNOWN ANALYTICS TYPE: {data.get('analytics_type')}")
    
    await partition_context.update_checkpoint(event)


def start_analytics_consumer():
    """Start consumer for Stream Analytics output in background thread."""
    if not CONNECTION_STR:
        app.logger.warning("Connection string not set - analytics consumer not started")
        return
    
    async def run_consumer():
        consumer = EventHubConsumerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            consumer_group=ANALYTICS_CONSUMER_GROUP,
            eventhub_name=ANALYTICS_EVENT_HUB,
        )
        
        async with consumer:
            await consumer.receive(
                on_event=on_analytics_event,
                starting_position="-1",
                on_error=lambda e: app.logger.error(f"Analytics consumer error: {e}")
            )
    
    def run_in_thread():
        asyncio.run(run_consumer())
    
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    app.logger.info("Analytics consumer thread started")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.route("/debug/peek-analytics", methods=["GET"])
def peek_analytics():
    """Peek messages from analytics Event Hub"""
    from azure.eventhub import EventHubConsumerClient
    import asyncio
    
    conn_str = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
    analytics_hub = os.environ.get("ANALYTICS_EVENT_HUB", "clickstream-analytics")
    
    if not conn_str:
        return jsonify({"error": "EVENT_HUB_CONNECTION_STR not set"}), 500
    
    messages = []
    error = None
    
    async def receive_messages():
        nonlocal messages, error
        try:
            client = EventHubConsumerClient.from_connection_string(
                conn_str=conn_str,
                consumer_group="$Default",
                eventhub_name=analytics_hub,
            )
            
            async with client:
                partition_ids = await client.get_partition_ids()
                
                for partition_id in partition_ids:
                    try:
                        props = await client.get_partition_properties(partition_id)
                        
                        if props['last_enqueued_sequence_number'] > 0:
                            start_seq = max(0, props['last_enqueued_sequence_number'] - 5)
                            
                            # Create a list to collect events
                            events_received = []
                            
                            # Define callback to collect events
                            def on_event(partition_context, event):
                                if event:
                                    events_received.append({
                                        "partition": partition_id,
                                        "body": event.body_as_str(),
                                        "sequence": event.sequence_number,
                                        "enqueued_time": str(event.enqueued_time)
                                    })
                            
                            # Receive events
                            await client.receive(
                                on_event=on_event,
                                partition_id=partition_id,
                                starting_position=start_seq,
                                max_wait_time=5
                            )
                            
                            messages.extend(events_received)
                            
                    except Exception as e:
                        messages.append({"error": f"Partition {partition_id}: {str(e)}"})
                        
        except Exception as e:
            error = str(e)
    
    # Run async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(receive_messages())
    loop.close()
    
    return jsonify({
        "messages": messages,
        "count": len(messages),
        "hub": analytics_hub,
        "error": error
    })
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
        "deviceType": request.json.get("deviceType", "unknown"),      # NEW
        "browser":    request.json.get("browser", "unknown"),         # NEW
        "os":         request.json.get("os", "unknown"),              # NEW
        "client_timestamp": request.json.get("timestamp"),            # NEW
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


# ---------------------------------------------------------------------------
# NEW: Analytics endpoint
# ---------------------------------------------------------------------------
@app.route("/api/analytics", methods=["GET"])
def get_analytics():
    """Return processed analytics from Stream Analytics."""
    with _analytics_lock:
        return jsonify({
            "device_breakdown": {
                "data": dict(_device_breakdown["counts"]),
                "total_events": _device_breakdown.get("total", 0),
                "last_update": _device_breakdown.get("last_update")
            },
            "spike_detection": {
                "current": _spike_detection.get("current_spike"),
                "recent_history": _spike_detection.get("history")[-20:],  # Last 20 entries
                "last_update": _spike_detection.get("last_update")
            }
        }), 200


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background consumers
    start_consumer()              # Original consumer for raw events
    start_analytics_consumer()    # NEW: Consumer for Stream Analytics output
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
