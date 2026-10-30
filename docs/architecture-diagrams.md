# Rocket Ground Station — Comprehensive Architectural Diagrams & Deep-Dive Reference

> **Language / Idioma:** [ 🇬🇧 English ](architecture-diagrams.md)
> **Companion Document:** [System Architecture](architecture.md) | [Binary Protocol](protocol.md) | [Frame Schema](frame-schema.md)

This document provides a complete visual and technical walkthrough of the **Rocket Telemetry Ground Station** (`estacion-terrena-pc`) codebase. It details the internal software architecture, asynchronous event loop, binary decoding engine, fault tolerance mechanisms, persistence layers, and frontend rendering pipelines through structured Mermaid diagrams.

---

## Table of Contents
1. [End-to-End System & Component Topology](#1-end-to-end-system--component-topology)
2. [Data Ingestion & Binary Frame Parsing Engine](#2-data-ingestion--binary-frame-parsing-engine)
3. [Asynchronous Concurrency & Task Lifecycle](#3-asynchronous-concurrency--task-lifecycle)
4. [Backend Object-Oriented Domain Model](#4-backend-object-oriented-domain-model)
5. [Rocket Flight State Machine & Telemetry Mapping](#5-rocket-flight-state-machine--telemetry-mapping)
6. [Pre-Flight Go / No-Go Qualification & Audit Pipeline](#6-pre-flight-go--no-go-qualification--audit-pipeline)
7. [Offline GIS Tile Ingestion & Fallback Hierarchy](#7-offline-gis-tile-ingestion--fallback-hierarchy)
8. [Frontend Multi-View Architecture & Decoupled RAF Loop](#8-frontend-multi-view-architecture--decoupled-raf-loop)
9. [Source Code Map & Direct References](#9-source-code-map--direct-references)

---

## 1. End-to-End System & Component Topology

The system is architected as an asynchronous, single-process micro-service written in Python 3.11+ using **FastAPI** and **uvicorn**, communicating with hardware via **pyserial-asyncio** and with client browsers via **WebSockets**.

```mermaid
flowchart TD
    subgraph HW["Hardware & Physical Transceiver Layer"]
        ROCKET["Rocket Flight Avionics (BMP280, MPU6050, GPS)"]
        LORA_AIR["LoRa 433 MHz Air Link (SF7, BW 250kHz, CR 4/5)"]
        GS_RADIO["Ground Station LoRa Modem (SX1278 / SiK)"]
        UART["USB-UART Serial Bus (/dev/ttyACM0 / COMx @ 115200 8N1)"]
        
        ROCKET -->|RF Telemetry| LORA_AIR
        LORA_AIR -->|RF Packets| GS_RADIO
        GS_RADIO -->|Transparent Serial Stream| UART
    end

    subgraph BACKEND["FastAPI Backend (main.py)"]
        subgraph INGESTION["Serial Acquisition & Protocol"]
            SER_PROTO["SerialProtocol (asyncio.Protocol)"]
            FRAME_BUF["FrameBuffer (Sliding Byte Window)"]
            FRAME_PARSER["FrameParser Engine (Declarative Unpacker)"]
        end

        subgraph CONFIG["Declarative Configuration (YAML)"]
            CFG_APP["config/app.yaml (Serial, GoNoGo, Charts)"]
            CFG_FRAME["config/frame.yaml (Fields, Endianness, CRC-16)"]
        end

        subgraph PIPELINE["process_frame() Dispatcher"]
            STAT["PacketStatistics (Rx, CRC Errors, Loss, Health)"]
            WS_MGR["WebSocketManager (Concurrent Broadcaster)"]
            DB_STORAGE["Database (SQLite3 WAL Mode)"]
            CSV_STORAGE["CSVLogger (Immediate Flush Flat-File)"]
            AUDIT_STORAGE["GoNoGoCSVLogger & DB Audit Table"]
        end

        subgraph HTTP_SRV["FastAPI Web & REST Endpoints"]
            REST_API["REST Router (/api/status, /api/history, /api/config)"]
            WS_ENDPOINT["WebSocket Endpoint (/ws/telemetry)"]
            TILE_SERVER["Tile Server (/tiles/{z}/{x}/{y}.png)"]
            STATIC_SRV["StaticFiles Mounter (/frontend)"]
        end

        UART -->|data_received()| SER_PROTO
        SER_PROTO -->|feed() bytes| FRAME_BUF
        CFG_FRAME -.->|Loads Rules| FRAME_PARSER
        CFG_APP -.->|Loads Settings| BACKEND
        FRAME_BUF -->|Raw 65B Candidate| FRAME_PARSER
        FRAME_PARSER -->|Decoded Dict| PIPELINE
        
        PIPELINE --> STAT
        PIPELINE --> WS_MGR
        PIPELINE --> DB_STORAGE
        PIPELINE --> CSV_STORAGE
        REST_API --> DB_STORAGE
        TILE_SERVER --> TILE_CACHE[("data/tiles/ (Local GIS Cache)")]
    end

    subgraph CLIENTS["Frontend Web Application Suite (HTML5 / ES6)"]
        INDEX_HTML["Primary Mission Dashboard (frontend/index.html)"]
        GONOGO_HTML["Pre-Flight Go/No-Go Console (frontend/gonogo.html)"]
        RECOVERY_HTML["Tactical Search & Recovery View (frontend/recovery.html)"]
        COMMON_JS["Shared Utility Suite (frontend/js/common.js)"]
        DEMO_ENGINE["Embedded Synthetic Simulator (Demo Mode)"]

        WS_MGR -->|ws://host:8000/ws/telemetry| INDEX_HTML
        WS_MGR -->|ws://host:8000/ws/telemetry| GONOGO_HTML
        WS_MGR -->|ws://host:8000/ws/telemetry| RECOVERY_HTML
        REST_API -->|HTTP GET /api/history| INDEX_HTML
        REST_API -->|HTTP POST /api/gonogo/event| GONOGO_HTML
        TILE_SERVER -->|Map Imagery| INDEX_HTML
        TILE_SERVER -->|Map Imagery| RECOVERY_HTML
        COMMON_JS -.->|Provides State & Formats| INDEX_HTML
        COMMON_JS -.->|Provides State & Formats| GONOGO_HTML
        COMMON_JS -.->|Provides State & Formats| RECOVERY_HTML
        DEMO_ENGINE -.->|Offline Fallback Simulation| COMMON_JS
    end
```

---

## 2. Data Ingestion & Binary Frame Parsing Engine

The telemetry acquisition loop avoids thread-blocking polling by utilizing Python's `asyncio.Protocol`. The sliding buffer inspects byte streams dynamically, re-synchronizing cleanly when noise or dropped bytes occur.

```mermaid
sequenceDiagram
    autonumber
    participant UART as OS Serial Port (/dev/ttyACM0)
    participant PROTO as SerialProtocol (main.py)
    participant BUF as FrameBuffer (main.py)
    participant CRC as validate_crc() (main.py)
    participant PARSER as FrameParser (main.py)
    participant PROC as process_frame() (main.py)
    participant WS as WebSocketManager (main.py)
    participant DB as SQLite & CSV Storage (main.py)
    participant UI as Frontend Clients (WebSockets)

    UART->>PROTO: data_received(chunk: bytes)
    PROTO->>BUF: feed(chunk)
    
    loop FrameBuffer Parsing Window
        alt Buffer has < 65 bytes
            BUF-->>PROTO: Wait for more bytes
        else Buffer has >= 65 bytes
            alt First byte != 0xAA (Sync Byte)
                BUF->>BUF: Discard leading byte, slide window +1
            else First byte == 0xAA
                BUF->>CRC: Validate CRC-16/CCITT-FALSE on slice[0:65]
                alt CRC Validation Failed
                    CRC-->>BUF: Invalid CRC
                    BUF->>BUF: Discard 0xAA byte, slide window +1
                    BUF->>PROC: Register CRC Error in PacketStatistics
                else CRC Validation Passed
                    CRC-->>BUF: Valid CRC
                    BUF->>PARSER: parse(slice[0:65])
                    PARSER->>PARSER: struct.unpack(endian, fields)
                    PARSER->>PARSER: Apply scaling & offsets (Raw * Scale + Offset)
                    PARSER->>PARSER: Map discrete status to Phase Enums
                    PARSER-->>BUF: Decoded telemetry dict
                    BUF->>BUF: Consume full 65 bytes from buffer
                    BUF->>PROC: Enqueue background task: process_frame(parsed_dict)
                end
            end
        end
    end

    par Dual Persistence
        PROC->>DB: INSERT INTO telemetry (timestamp, sequence, payload)
        PROC->>DB: telemetry.csv.write(row) & file.flush()
    and Real-Time Client Broadcast
        PROC->>WS: broadcast(JSON envelope)
        WS->>UI: WebSocket frame transmission
    end
```

---

## 3. Asynchronous Concurrency & Task Lifecycle

The backend operates entirely on a single-threaded cooperative event loop (`asyncio`). To prevent garbage collection of pending coroutines and handle hardware disconnects seamlessly, it implements explicit background task tracking and a continuous reconnection state machine.

```mermaid
flowchart TD
    START(["Application Startup"]) --> LOAD_CFG["Load YAML Configurations (app.yaml, frame.yaml)"]
    LOAD_CFG --> INIT_DB["Initialize SQLite WAL Mode & Open CSV Handles"]
    INIT_DB --> SPAWN_SERIAL["asyncio.create_task(serial_loop)"]

    subgraph SERIAL_FSM["serial_loop() Lifecycle & Auto-Reconnect"]
        TRY_CONN["Resolve Serial Port (/dev/ttyACM0 or COMx)"] --> OPEN_CONN["serial_asyncio.create_serial_connection()"]
        OPEN_CONN -->|Success| CONN_ACTIVE["Protocol Active & Listening"]
        OPEN_CONN -->|Failure / Port Busy| WAIT_RETRY["Sleep reconnect_delay (3s)"]
        WAIT_RETRY --> TRY_CONN

        CONN_ACTIVE --> WAIT_DISC["await protocol.disconnected.wait()"]
        WAIT_DISC -->|Port Disconnected / Cable Pulled| ON_LOST["connection_lost() Callback Triggered"]
        ON_LOST --> BROADCAST_DISC["Broadcast {'connected': false} via WebSockets"]
        BROADCAST_DISC --> WAIT_RETRY
    end

    subgraph TASK_MANAGER["create_background_task() GC Guard"]
        ENQUEUE_TASK["Frame Received: process_frame() Coroutine"] --> CREATE_TASK["task = asyncio.create_task(coro)"]
        CREATE_TASK --> ADD_SET["background_tasks.add(task)"]
        ADD_SET --> ADD_CALLBACK["task.add_done_callback(background_tasks.discard)"]
        ADD_CALLBACK --> EXEC_TASK["Execute Frame Processing & Broadcasting"]
        EXEC_TASK --> ON_DONE["Task Completed: Set Cleanly Discards Task"]
    end

    subgraph SHUTDOWN_HANDLER["Graceful Shutdown (@app.on_event('shutdown'))"]
        STOP_SIGNAL(["SIGINT / SIGTERM / System Exit"]) --> CANCEL_TASKS["Cancel pending background_tasks"]
        CANCEL_TASKS --> GATHER_WAIT["await asyncio.gather(*tasks, return_exceptions=True)"]
        GATHER_WAIT --> CLOSE_DB["Close SQLite connection (Commit WAL)"]
        CLOSE_DB --> FLUSH_CSV["Flush and close telemetry.csv & gonogo_audit.csv"]
        FLUSH_CSV --> TERMINATE(["Process Exit 0"])
    end
```

---

## 4. Backend Object-Oriented Domain Model

The core architecture in `main.py` is cleanly segregated into immutable dataclass configurations, stateful buffers, protocol handlers, and persistent storage abstractions.

```mermaid
classDiagram
    direction TB

    class AppConfig {
        +SerialConfig serial
        +WebSocketConfig websocket
        +StorageConfig storage
        +GoNoGoConfig gonogo
        +ChartsConfig charts
    }

    class FrameConfig {
        +str endian
        +list~FieldConfig~ fields
        +bytes sync
        +str crc_type
        +int frame_length
        +int crc_offset
        +int payload_size
        +int frame_size
    }

    class FieldConfig {
        +str name
        +str type
        +float scale
        +float value_offset
        +int byte_offset
        +int byte_length
        +dict values
        +int expected_value
    }

    class SerialProtocol {
        +asyncio.Event disconnected
        +bytes rx_buffer
        +connection_made(transport)
        +data_received(data)
        +connection_lost(exc)
    }

    class FrameBuffer {
        +bytearray buffer
        +int max_size
        +FrameParser parser
        +feed(data) list~dict~
    }

    class FrameParser {
        +FrameConfig config
        +parse(raw_frame) dict
    }

    class PacketStatistics {
        +int total_received
        +int total_valid
        +int total_corrupted
        +int total_lost
        +int last_sequence
        +register(parsed, raw, valid)
        +snapshot() dict
    }

    class Database {
        +sqlite3.Connection conn
        +insert(timestamp, sequence, payload)
        +insert_gonogo_audit(entry)
        +get_gonogo_audit_logs(limit)
        +get_recent(limit)
        +close()
    }

    class CSVLogger {
        +TextIO file
        +csv.writer writer
        +write(fields)
        +close()
    }

    class WebSocketManager {
        +set~WebSocket~ active_connections
        +connect(websocket)
        +disconnect(websocket)
        +broadcast(message)
    }

    class RuntimeState {
        +bool connected
        +str current_port
        +float last_packet_time
        +dict last_telemetry
        +deque recent_packets
    }

    AppConfig --> FrameConfig
    FrameConfig *-- FieldConfig
    SerialProtocol --> FrameBuffer
    FrameBuffer --> FrameParser
    FrameParser --> FrameConfig
    FrameBuffer ..> PacketStatistics
    FrameBuffer ..> Database
    FrameBuffer ..> CSVLogger
    FrameBuffer ..> WebSocketManager
    RuntimeState ..> PacketStatistics
```

---

## 5. Rocket Flight State Machine & Telemetry Mapping

The 65-byte definitive telemetry frame encodes flight status into byte 7 as a discrete state machine identifier. Both backend and frontend map these codes into standardized operational flight phases.

```mermaid
stateDiagram-v2
    [*] --> STANDBY: 0x00 (Launchpad Readiness)
    
    STANDBY --> POWERED: 0x01 (Motor Ignition & Liftoff)
    note right of POWERED
        Thrust Active
        Acceleration > 2.5g
        Roll stabilization
    end note

    POWERED --> BURNOUT: 0x02 / 0x03 (Motor Burnout)
    note right of BURNOUT
        Ballistic Ascent
        Velocity > 0 m/s
        Decreasing axial acceleration
    end note

    BURNOUT --> APOGEE: 0x04 / 0x07 (Apogee Reached)
    note right of APOGEE
        Peak Altitude Reached
        Vertical Velocity = 0 m/s
    end note

    APOGEE --> DROGUE_ORD: 0x08 / 0x0F (Drogue Pyro Commanded)
    note right of DROGUE_ORD
        Pyro Channel 1 Fired
        Pin Frenado = 1
    end note

    DROGUE_ORD --> DROGUE_CONF: 0x10 / 0x1F (Drogue Parachute Deployed)
    note right of DROGUE_CONF
        High-speed stabilized descent
        Target sink rate: ~48 m/s
    end note

    DROGUE_CONF --> MAIN_ORD: 0x20 / 0x3F (Main Pyro Commanded @ ~450m)
    note right of MAIN_ORD
        Pyro Channel 2 Fired
        Pin Principal = 1
    end note

    MAIN_ORD --> MAIN_CONF: 0x40 / 0x7F (Main Parachute Deployed)
    note right of MAIN_CONF
        Low-speed terminal descent
        Target sink rate: ~5.5 m/s
    end note

    MAIN_CONF --> LANDED: 0x80 / 0xFF (Touchdown on Ground)
    note right of LANDED
        Zero vertical velocity
        Stable GPS coordinates
        Beacon transmitting for recovery
    end note

    LANDED --> [*]
```

---

## 6. Pre-Flight Go / No-Go Qualification & Audit Pipeline

Prior to motor ignition, `frontend/gonogo.html` enforces a strict launch criteria evaluation protocol. It continuously audits live launchpad sensor telemetry against `config/app.yaml` bounds, verifies ground crew manual checklists, requires a continuous 15-second stability hold, and audits every decision to persistent storage.

```mermaid
sequenceDiagram
    autonumber
    participant OP as Mission Director / Flight Controller
    participant UI as Go/No-Go Console (frontend/gonogo.html)
    participant WS as Telemetry Stream (/ws/telemetry)
    participant API as REST API (/api/gonogo/event)
    participant AUDIT as Database & CSV Audit Logs

    Note over UI: Step 1: Automated Baseline Verification
    WS->>UI: Live Telemetry (Altitude, Accel X/Y/Z, GPS)
    UI->>UI: Check Barometric Altitude (~0.0 m ± 5.0m)
    UI->>UI: Check Earth Gravity (Accel Z = 1.0g ± 0.15g)
    UI->>UI: Check Pad Sway (Accel X/Y < 0.10g)
    UI->>UI: Check GPS 3D Fix & Radius (< 5.0m from pad)

    Note over UI: Step 2: Manual Station Checklists
    OP->>UI: Complete Communications Check (Antenna GS, RSSI)
    OP->>UI: Complete Propulsion & Mechanical Check (Igniters, Airframe)
    OP->>UI: Complete Range Safety & Weather Check (< 15 km/h wind)

    alt Any Automated or Manual Check Fails
        UI->>UI: Reset 15-second Stability Hold Timer to 0s
        UI->>UI: Set Status = NO-GO (Red Alarm)
    else All Criteria Satisfied
        UI->>UI: Increment Stability Hold Timer (0s -> 15s)
        alt Stability Timer reaches 15.0s
            UI->>UI: Play Audio Annunciation ("GO FOR LAUNCH")
            UI->>UI: Enable GO Decision Authorization
            OP->>UI: Click "CONFIRMAR DECISIÓN GO"
            UI->>API: POST /api/gonogo/event {decision: "GO", operator: "DIRECTOR", checks: {...}}
            API->>AUDIT: INSERT INTO gonogo_audit & Append gonogo_audit.csv
            AUDIT-->>API: 200 OK Confirmed
            API-->>UI: Audit Confirmed Badge Displayed
        end
    end
```

---

## 7. Offline GIS Tile Ingestion & Fallback Hierarchy

Ground operations typically occur in arid, unpopulated launch sites with zero cellular reception. The ground station guarantees 100% offline geospatial tracking via a multi-tiered tile caching and fallback mechanism.

```mermaid
flowchart TD
    MAP_REQ["Leaflet Map requests: /tiles/{z}/{x}/{y}.png"] --> CHECK_LOCAL{"Does tile exist in data/tiles/{z}/{x}/{y}.png?"}

    CHECK_LOCAL -->|YES: Cached Locally| SERVE_LOCAL["Read from disk & return HTTP 200 (image/png)"]
    SERVE_LOCAL --> RENDER_MAP["Leaflet Map renders high-res offline tile"]

    CHECK_LOCAL -->|NO: Cache Miss| CHECK_NET{"Internet Available?"}
    
    CHECK_NET -->|YES: Online Ground Station| FETCH_OSM["Fetch from OpenStreetMap tile server"]
    FETCH_OSM --> SAVE_CACHE["Save tile to data/tiles/{z}/{x}/{y}.png"]
    SAVE_CACHE --> SERVE_LOCAL

    CHECK_NET -->|NO: Remote Field Site| CHECK_FALLBACK{"Does tile_offline.png exist in frontend/assets/?"}
    CHECK_FALLBACK -->|YES| SERVE_GRID["Return tile_offline.png (Subtle grid placeholder)"]
    CHECK_FALLBACK -->|NO| SERVE_EMPTY["Return 1x1 transparent PNG fallback"]
    
    SERVE_GRID --> RENDER_FALLBACK["Leaflet renders grid tile, GPS track & bearing line intact"]
    SERVE_EMPTY --> RENDER_FALLBACK

    subgraph PRE_DOWNLOAD["Pre-Mission Preparation (scripts/predownload_tiles.py)"]
        ADMIN["Launch Coordinator"] --> RUN_SCRIPT["python scripts/predownload_tiles.py --lat -34.5 --lon -58.4 --radius 15 --min-zoom 11 --max-zoom 17"]
        RUN_SCRIPT --> BULK_FETCH["Bulk download OSM Slippy map tiles into data/tiles/"]
    end
```

---

## 8. Frontend Multi-View Architecture & Decoupled RAF Loop

To maintain 60 FPS fluid rendering on field laptops without browser freezing or memory leaks, the frontend completely separates high-frequency WebSocket message ingestion from screen rendering using tiered `requestAnimationFrame` budgets.

```mermaid
flowchart TD
    WS_MSG["WebSocket Telemetry Frame (10 Hz)"] --> INGEST["ingest(packet) in JavaScript"]
    
    subgraph MEM_BUFFERS["Sliding In-Memory Ring Buffers"]
        INGEST --> B_HIST["state.hist: 60s Historical Window (Splice > 60s)"]
        INGEST --> B_LOSS["state.seqWindow: 5s Packet Loss Window"]
        INGEST --> B_RATE["state.packetTimes: 5s RX Rate Window"]
        INGEST --> B_LOG["state.events: Max 200 FIFO Flight Events"]
        INGEST --> B_ROUTE["state.routePoints: Max 2000 FIFO Coordinate Pairs"]
    end

    subgraph RAF_PIPELINE["Decoupled requestAnimationFrame(loop) Execution"]
        RAF_START(["Screen VSync Refresh (~60 Hz)"]) --> TIER_60HZ{"Every VSync Frame (~60 Hz)"}
        
        TIER_60HZ --> DRAW_TAPE["drawTape(): Exponential Smooth Altimeter (0.35 EMA)"]
        TIER_60HZ --> DRAW_CHARTS["renderCharts(): HTML5 Canvas Oscilloscopes (Window Extrapolation)"]

        TIER_60HZ --> TIER_30HZ{"Interval >= 33 ms (~30 Hz)?"}
        TIER_30HZ -->|YES| DRAW_NUMS["renderText(): Hi-Contrast Big Numeric Metrics"]
        TIER_30HZ -->|NO| SKIP_30HZ["Pass"]

        TIER_60HZ --> TIER_4HZ{"Interval >= 250 ms (~4 Hz)?"}
        TIER_4HZ -->|YES| DRAW_MAP["updateMap(): Leaflet Polyline & Pan + ISA-18.2 Alarm Classification"]
        TIER_4HZ -->|NO| SKIP_4HZ["Pass"]

        TIER_60HZ --> TIER_2HZ{"Interval >= 500 ms (~2 Hz)?"}
        TIER_2HZ -->|YES| DRAW_HDR["renderHeader(): System Clock, MET T+, Stage Badge, Radio Signal Dot"]
        TIER_2HZ -->|NO| SKIP_2HZ["Pass"]
    end
```

---

## 9. Source Code Map & Direct References

| Component / Subsystem | Source File | Line Range | Architectural Responsibility |
| :--- | :--- | :--- | :--- |
| **Declarative Config Data Structures** | [`main.py`](../main.py#L106-L251) | `106–251` | Typed dataclasses for serial, framing, CRC, Go/No-Go limits, and chart scaling rules. |
| **CRC-16/CCITT-FALSE Engine** | [`main.py`](../main.py#L509-L552) | `509–552` | Fast arithmetic bit-shifting CRC calculation without look-up tables over bytes 0–62. |
| **Dynamic Frame Parser Engine** | [`main.py`](../main.py#L555-L675) | `555–675` | Schema-driven binary unpacker supporting variable endianness, linear scaling, and phase mapping. |
| **Sliding Window Byte Accumulator** | [`main.py`](../main.py#L678-L754) | `678–754` | Sliding byte buffer with sync-byte searching (`0xAA`) and stream resynchronization. |
| **Statistical Metrics Tracker** | [`main.py`](../main.py#L757-L794) | `757–794` | Real-time tracking of received frames, sequence increments, missing packets, and CRC failures. |
| **Relational Storage & GoNoGo Audit** | [`main.py`](../main.py#L797-L982) | `797–982` | SQLite 3 database manager with Write-Ahead Logging (WAL) and indexed timestamp queries. |
| **Flat-File Immediate CSV Loggers** | [`main.py`](../main.py#L985-L1050) | `985–1050` | Unbuffered flat-file loggers with immediate disk `flush()` on every row to prevent power-cut data loss. |
| **WebSocket Broadcast Hub** | [`main.py`](../main.py#L1053-L1090) | `1053–1090` | Non-blocking broadcast distributor to multiple simultaneous dashboard clients with auto-cleanup. |
| **Background GC Guard & Processor** | [`main.py`](../main.py#L1121-L1190) | `1121–1190` | Prevents Python garbage collection of active async coroutines and coordinates frame persistence. |
| **Asynchronous Serial Protocol** | [`main.py`](../main.py#L1193-L1236) | `1193–1236` | `asyncio.Protocol` implementation binding OS UART hardware interrupts to Python coroutines. |
| **Serial Auto-Reconnect Loop** | [`main.py`](../main.py#L1278-L1338) | `1278–1338` | Continuous reconnection state machine managing hardware disconnects and port recovery. |
| **REST & WebSocket Endpoints** | [`main.py`](../main.py#L1343-L1731) | `1343–1731` | FastAPI HTTP/WebSocket routes for dashboard HTML, offline tiles, status, logs, and telemetry stream. |
| **Shared Frontend Utilities** | [`frontend/js/common.js`](../frontend/js/common.js#L1-L367) | `1–367` | Argentine number formatting, sliding loss/rate tracking (`PacketTracker`), alarms, and theme manager. |
| **Embedded Client Demo Engine** | [`frontend/js/common.js`](../frontend/js/common.js#L369-L1272) | `369–1272` | Intercepts `fetch` and `WebSocket` in standalone/demo mode to simulate realistic 290s flight physics. |
| **Primary Mission Dashboard** | [`frontend/index.html`](../frontend/index.html#L1-L1828) | `1–1828` | Multi-tiered RAF rendering, Canvas altimeter tape, oscilloscope charts, and Leaflet tracking. |
| **Pre-Flight Go / No-Go Console** | [`frontend/gonogo.html`](../frontend/gonogo.html#L1-L2167) | `1–2167` | Automated sensor validation, manual station checklists, 15s stability hold, and persistent audit trail. |
| **Search & Recovery GIS View** | [`frontend/recovery.html`](../frontend/recovery.html#L1-L1230) | `1–1230` | Geodesic bearing and distance calculations, dynamic compass arrow, and offline map dispatch. |
| **Offline Map Pre-Downloader** | [`scripts/predownload_tiles.py`](../scripts/predownload_tiles.py#L1-L226) | `1–226` | CLI utility converting bounding coordinates to Slippy map tiles for zero-connectivity field missions. |
| **Hardware-in-the-Loop Simulator** | [`telemetry_simulator.ino`](../arduino/telemetry_simulator/telemetry_simulator.ino#L1-L655) | `1–655` | Microcontroller firmware generating 65-byte LoRa frames at 10 Hz with injected fault anomalies. |
