# CST8916 Assignment 2

**Student Name**: Usama Iqbal
**Student ID**: 040777763
**Course**: CST8916 Remote Data and RT Applications
**Semester**: Winter 2026

---

## Demo Video

🎥 Watch Demo Video    https://youtu.be/dWifqxvrlmU


# Real-Time Clickstream Analytics Platform

A real-time clickstream analytics platform that tracks user interactions on an e-commerce store and displays live analytics on a dashboard. The solution uses Azure Event Hubs for data ingestion and Azure Stream Analytics for real-time processing.

---

##  Data Flow

1. User clicks on e-commerce store → Click event sent to Flask API (`/track`)
2. Flask app enriches event with server timestamp & device info → Sends to Event Hub (Input)
3. Stream Analytics processes events in real-time → Calculates device breakdown & spike detection
4. Processed data sent to Event Hub (Output)
5. Flask app consumes processed data → Stores in memory
6. Dashboard polls Flask API (`/api/analytics`) every 2 seconds → Displays live charts

---

##  Design Decisions

### 1. Event Enrichment
**Decision:** Enrich click events with server-side timestamp, device type, browser, and OS at the API layer.

**Why:**
- Client-side timestamps can be manipulated or inaccurate
- Device info from browser allows meaningful device breakdown analytics
- Server enrichment ensures consistent, trustworthy data

---

### 2. Two Event Hub Architecture
**Decision:** Use separate Event Hubs for raw events and processed analytics.

**Why:**
- Decouples data ingestion from analytics processing
- Allows independent scaling
- Processed data is pre-aggregated, reducing dashboard query complexity
- Clean separation of concerns

---

### 3. In-Memory Buffer with Polling Dashboard
**Decision:** Store last 50 events in memory and use polling (not WebSockets).

**Why:**
- Simpler implementation for learning purposes
- Reduces infrastructure complexity
- 2-second polling is sufficient for near real-time analytics
- Avoids maintaining persistent connections

---

### 4. Gunicorn with Threading for Consumers
**Decision:** Run Event Hub consumers as background threads in the Flask app.

**Why:**
- Simplifies deployment (single process)
- Automatic restart if consumer fails
- Works within Azure App Service constraints
- No need for additional services

---

##  Setup Instructions

### Prerequisites
- Azure subscription
- Python 3.11+
- Git
- Azure CLI (optional)

---

##  Azure Resources Needed

| Resource                     | Purpose                     | SKU/Tier        |
|-----------------------------|-----------------------------|-----------------|
| Azure Event Hubs Namespace  | Message ingestion           | Standard tier   |
| Event Hub (Input)           | Raw clickstream data        | `clickstream`   |
| Event Hub (Output)          | Processed analytics         | `clickstream-analytics` |
| Azure Stream Analytics Job  | Real-time processing        | 1 SU            |
| Azure App Service           | Flask app hosting           | Free (F1) or Basic (B1) |

---

##  Environment Variables

Create these in your Azure App Service Configuration:

| Variable                    | Description                          | Example           |
|----------------------------|--------------------------------------|-------------------|
| EVENT_HUB_CONNECTION_STR   | Event Hubs namespace connection string | `Endpoint=sb://...` |
| EVENT_HUB_NAME             | Input event hub name                 | `clickstream`     |
| ANALYTICS_EVENT_HUB        | Output event hub name                | `clickstream-analytics` |

---

##  Key Endpoints

| Endpoint           | Method | Purpose                     |
|-------------------|--------|-----------------------------|
| `/`               | GET    | E-commerce store            |
| `/dashboard`      | GET    | Analytics dashboard         |
| `/track`          | POST   | Send click events           |
| `/api/events`     | GET    | Get recent events           |
| `/api/analytics`  | GET    | Get processed analytics     |
| `/health`         | GET    | Health check                |

---
