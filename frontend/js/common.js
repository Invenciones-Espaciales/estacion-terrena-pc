/**
 * ESTACIÓN TERRENA — COMMON TELEMETRY UTILITIES & GLOBAL HEADER MANAGER
 * High-contrast aerospace ground station frontend suite
 */

// Apply saved theme immediately to prevent any white flash / FOUC
(function applyEarlyTheme() {
    try {
        const saved = localStorage.getItem("ground_station_theme") || "light";
        document.documentElement.setAttribute("data-theme", saved);
    } catch (e) {}
})();

// Fast element selector
window.$ = window.$ || (id => document.getElementById(id));
var $ = window.$;

// Numeric formatting with Argentine / Spanish locale (comma decimals)
window.fmt = window.fmt || function fmt(n, dec = 2) {
    const num = Number(n);
    return Number.isFinite(num)
        ? new Intl.NumberFormat("es-AR", { minimumFractionDigits: dec, maximumFractionDigits: dec }).format(num)
        : "---";
};
var fmt = window.fmt;

// Strict fixed-width signed tabular numeric formatting (+00.00 / -00.00)
window.fmtSigned = window.fmtSigned || function fmtSigned(n, dec = 2) {
    const num = Number(n);
    if (!Number.isFinite(num)) return " --- ";
    const sign = num > 0 ? "+" : (num < 0 ? "−" : "+");
    const absVal = Math.abs(num);
    const parts = absVal.toFixed(dec).split(".");
    const intPart = parts[0].padStart(2, "0");
    return `${sign}${intPart}.${parts[1] || ""}`;
};
var fmtSigned = window.fmtSigned;

// Format milliseconds into HH:MM:SS string
window.elapsed = window.elapsed || function elapsed(ms) {
    if (!Number.isFinite(ms) || ms < 0) return "00:00:00";
    const s = Math.floor(ms / 1000);
    const p = v => String(v).padStart(2, "0");
    return `${p(Math.floor(s / 3600))}:${p(Math.floor((s % 3600) / 60))}:${p(s % 60)}`;
};
var elapsed = window.elapsed;

// Canonical Spanish flight phase dictionary
window.PHASE_NAMES = window.PHASE_NAMES || {
    0: "PLATAFORMA",
    1: "MOTOR ENCENDIDO",
    2: "FIN DE EMPUJE",
    3: "FIN DE EMPUJE",
    4: "APOGEO",
    7: "APOGEO",
    8: "ORDEN DROGUE",
    15: "ORDEN DROGUE",
    16: "DROGUE DESPLEGADO",
    31: "DROGUE DESPLEGADO",
    32: "ORDEN PRINCIPAL",
    63: "ORDEN PRINCIPAL",
    64: "PRINCIPAL DESPLEGADO",
    127: "PRINCIPAL DESPLEGADO",
    128: "ATERRIZADO",
    255: "ATERRIZADO",
    STANDBY: "PLATAFORMA",
    POWERED: "MOTOR ENCENDIDO",
    BURNOUT: "FIN DE EMPUJE",
    APOGEE: "APOGEO",
    DROGUE_ORD: "ORDEN DROGUE",
    DROGUE_CONF: "DROGUE DESPLEGADO",
    MAIN_ORD: "ORDEN PRINCIPAL",
    MAIN_CONF: "PRINCIPAL DESPLEGADO",
    LANDED: "ATERRIZADO"
};
var PHASE_NAMES = window.PHASE_NAMES;

window.getPhaseName = window.getPhaseName || function getPhaseName(status) {
    if (status === null || status === undefined) return "---";
    return window.PHASE_NAMES[status] || String(status);
};
var getPhaseName = window.getPhaseName;

/**
 * Sliding 5-second packet loss and RX rate tracker
 */
window.PacketTracker = window.PacketTracker || class PacketTracker {
    constructor(windowMs = 5000) {
        this.windowMs = windowMs;
        this.lastSeq = null;
        this.seqWindow = [];     // Array of { t: epochMs, rec: int, lost: int }
        this.packetTimes = [];   // Array of epochMs
        this.lossPercent = 0.0;
        this.rxRate = 0.0;
        this.rxCount = 0;
    }

    recordPacket(seq, timestampMs = Date.now(), isHistory = false) {
        this.rxCount++;
        const now = timestampMs;

        let lostInStep = 0;
        if (Number.isFinite(seq)) {
            if (this.lastSeq !== null) {
                const expected = (this.lastSeq + 1) & 0xFFFF;
                if (seq !== expected) {
                    const diff = (seq - expected) & 0xFFFF;
                    if (diff < 1000) lostInStep = diff;
                }
            }
            this.lastSeq = seq;
        }

        this.seqWindow.push({ t: now, rec: 1, lost: lostInStep });

        if (!isHistory) {
            this.packetTimes.push(now);
        }
    }

    prune(now = Date.now()) {
        const cutoff = now - this.windowMs;

        // Prune packet rates
        let cutTimes = 0;
        while (cutTimes < this.packetTimes.length && this.packetTimes[cutTimes] < cutoff) {
            cutTimes++;
        }
        if (cutTimes > 0) this.packetTimes.splice(0, cutTimes);

        const span = this.packetTimes.length > 1 ? (this.packetTimes.at(-1) - this.packetTimes[0]) / 1000 : 0;
        this.rxRate = span > 0 ? (this.packetTimes.length - 1) / span : (this.packetTimes.length > 0 ? 1.0 : 0.0);

        // Prune packet loss window
        let cutSeq = 0;
        while (cutSeq < this.seqWindow.length && this.seqWindow[cutSeq].t < cutoff) {
            cutSeq++;
        }
        if (cutSeq > 0) this.seqWindow.splice(0, cutSeq);

        let recTotal = 0, lostTotal = 0;
        for (let i = 0; i < this.seqWindow.length; i++) {
            recTotal += this.seqWindow[i].rec;
            lostTotal += this.seqWindow[i].lost;
        }
        const total = recTotal + lostTotal;
        this.lossPercent = total > 0 ? (lostTotal / total) * 100 : 0.0;
    }
};
var PacketTracker = window.PacketTracker;

/**
 * Updates the unified 6-cell global header
 *
 * @param {Object} options
 * @param {number} [options.missionTimeMs] - Milliseconds since liftoff (or 0)
 * @param {string|number} [options.status] - Flight stage key or label
 * @param {number|null} [options.rssi] - RF RSSI in dBm
 * @param {number} [options.lossPercent] - Recent packet loss percentage
 * @param {boolean} [options.connected] - Link status boolean
 * @param {number|null} [options.lastPacketAt] - Epoch timestamp of last packet
 */
window.updateGlobalHeader = window.updateGlobalHeader || function updateGlobalHeader({
    missionTimeMs = 0,
    status = null,
    rssi = null,
    lossPercent = 0.0,
    connected = false,
    lastPacketAt = null,
    tempImu = null
} = {}) {
    const now = Date.now();
    const nowDate = new Date();
    const pad = n => String(n).padStart(2, "0");

    // 1. Local Time
    const localTimeEl = window.$("localTime");
    if (localTimeEl) {
        localTimeEl.textContent = `${pad(nowDate.getHours())}:${pad(nowDate.getMinutes())}:${pad(nowDate.getSeconds())}`;
    }

    // 2. Mission Clock
    const clockEl = window.$("clock");
    if (clockEl) {
        clockEl.textContent = window.elapsed(missionTimeMs);
    }

    // 3. Flight Phase
    const phaseEl = window.$("phase");
    if (phaseEl) {
        phaseEl.textContent = window.getPhaseName(status);
    }

    // 4. RF RSSI
    const rssiEl = window.$("rssi");
    if (rssiEl) {
        if (rssi !== null && Number.isFinite(Number(rssi))) {
            rssiEl.textContent = `${window.fmt(rssi, 0)} dBm`;
            rssiEl.className = "v num";
        } else {
            rssiEl.textContent = "N/D";
            rssiEl.className = "v num nd";
        }
    }

    // 5. Packet Loss
    const lossEl = window.$("lossPercent");
    if (lossEl) {
        lossEl.textContent = `${window.fmt(lossPercent, 1)} %`;
        if (lossPercent > 15) {
            lossEl.style.color = "var(--bad)";
        } else if (lossPercent > 5) {
            lossEl.style.color = "var(--warn)";
        } else {
            lossEl.style.color = "var(--ink)";
        }
    }

    // 6. IMU Temperature Pill (Cockpit Header Badge)
    const tempPillEl = window.$("tempPill");
    if (tempPillEl) {
        if (tempImu !== null && Number.isFinite(Number(tempImu))) {
            const t = Number(tempImu);
            tempPillEl.textContent = `IMU ${window.fmt(t, 1)} °C`;
            if (t >= 60) {
                tempPillEl.className = "temp-pill crit";
            } else if (t >= 45) {
                tempPillEl.className = "temp-pill warn";
            } else {
                tempPillEl.className = "temp-pill";
            }
        } else {
            tempPillEl.textContent = "IMU N/D";
            tempPillEl.className = "temp-pill";
        }
    }

    // 7. Link Status & Dot with >450ms Freeze Degradation
    const dotEl = window.$("linkDot");
    const linkStatusEl = window.$("linkStatus");
    const silenceMs = lastPacketAt !== null ? (now - lastPacketAt) : 0;
    const isFrozen = connected && lastPacketAt !== null && silenceMs > 450;
    const isStale = connected && lastPacketAt !== null && silenceMs > 2000;

    if (dotEl) {
        if (!connected) {
            dotEl.className = "dot";
        } else if (isFrozen) {
            dotEl.className = "dot warn";
        } else {
            dotEl.className = "dot online";
        }
    }

    if (linkStatusEl) {
        if (!connected) {
            linkStatusEl.textContent = "DESCONECTADO";
            linkStatusEl.style.color = "var(--bad)";
        } else if (isStale) {
            linkStatusEl.textContent = `SIN SEÑAL (+${(silenceMs / 1000).toFixed(1)}s)`;
            linkStatusEl.style.color = "var(--bad)";
        } else if (isFrozen) {
            linkStatusEl.textContent = `SILENCIO (+${(silenceMs / 1000).toFixed(1)}s)`;
            linkStatusEl.style.color = "var(--warn)";
        } else {
            linkStatusEl.textContent = "CONECTADO";
            linkStatusEl.style.color = "var(--ink)";
        }
    }
};
var updateGlobalHeader = window.updateGlobalHeader;

/**
 * Fetches the last 60 seconds of telemetry from the server
 * and feeds each frame to the provided callback
 */
window.fetchHistory60s = window.fetchHistory60s || async function fetchHistory60s(onFrame, onDone) {
    try {
        const response = await fetch("/api/history?seconds=60", { cache: "no-store" });
        if (!response.ok) return null;
        const data = await response.json();
        const frames = data.frames || [];

        if (typeof onFrame === "function") {
            for (let i = 0; i < frames.length; i++) {
                onFrame(frames[i], true);
            }
        }

        if (typeof onDone === "function") {
            onDone(data);
        }
        return data;
    } catch (err) {
        console.warn("Failed to fetch 60-second telemetry history:", err);
        return null;
    }
};
var fetchHistory60s = window.fetchHistory60s;

/**
 * Aerospace Cockpit Binary Annunciator updater (DROGUE & MAIN)
 * @param {string} cardId - DOM id of the annunciator card element
 * @param {string} stateId - DOM id of the annunciator state text element
 * @param {number|boolean} val - 0 (idle) or 1 (deployed)
 */
window.setAnnunciator = window.setAnnunciator || function setAnnunciator(cardId, stateId, val) {
    const card = window.$(cardId);
    const stateEl = window.$(stateId);
    if (!card || !stateEl) return;

    const isActive = Number(val) === 1;
    const targetClass = isActive ? "annun-card state-active" : "annun-card state-idle";
    if (card.className !== targetClass) {
        card.className = targetClass;
        if (isActive) {
            stateEl.innerHTML = '<span class="annun-icon">[OK]</span>[1: DESPLEGADO]';
        } else {
            stateEl.textContent = "[0: EN ESPERA]";
        }
    }
};
var setAnnunciator = window.setAnnunciator;
 
/**
 * Theme toggle manager (light/dark)
 */
window.initThemeManager = window.initThemeManager || function initThemeManager() {
    const updateThemeUI = () => {
        const isDark = document.documentElement.getAttribute("data-theme") === "dark";
        const icon = window.$("themeIcon");
        const label = window.$("themeLabel");
        if (icon) icon.textContent = isDark ? "☀️" : "🌙";
        if (label) label.textContent = isDark ? "CLARO" : "OSCURO";
    };

    const toggleTheme = () => {
        const cur = document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
        const next = cur === "dark" ? "light" : "dark";
        document.documentElement.setAttribute("data-theme", next);
        try {
            localStorage.setItem("ground_station_theme", next);
        } catch (e) {}
        updateThemeUI();
        window.dispatchEvent(new CustomEvent("themechange", { detail: { theme: next } }));
    };

    updateThemeUI();

    const btn = window.$("btnThemeToggle");
    if (btn && !btn._themeBound) {
        btn._themeBound = true;
        btn.addEventListener("click", toggleTheme);
    }
};
var initThemeManager = window.initThemeManager;

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", window.initThemeManager);
} else {
    window.initThemeManager();
}

/* ============================================================================
   CLIENT-SIDE DEMO FLIGHT SIMULATOR & GITHUB PAGES INTERCEPTOR
   Faithful 290-second 10 Hz physics & telemetry port of telemetry_simulator.ino
   ============================================================================ */

(function initDemoModeEngine() {
    const CYCLE_DURATION_S = 290.0;
    const BASE_LAT = -34.6037;
    const BASE_LON = -58.3816;
    const BASE_PRESS = 1013.25;

    const STATUS_MAP = {
        0: "STANDBY",
        1: "POWERED",
        2: "BURNOUT",
        4: "APOGEE",
        8: "DROGUE_ORD",
        16: "DROGUE_CONF",
        32: "MAIN_ORD",
        64: "MAIN_CONF",
        128: "LANDED"
    };

    function detectDemoMode() {
        try {
            const params = new URLSearchParams(window.location.search);
            const demoParam = params.get("demo") || params.get("mode");
            if (demoParam === "0" || demoParam === "false" || demoParam === "live") {
                sessionStorage.removeItem("gs_demo_mode");
                return false;
            }
            if (demoParam === "1" || demoParam === "true" || demoParam === "demo" || window.location.hash === "#demo") {
                sessionStorage.setItem("gs_demo_mode", "1");
                return true;
            }
            if (sessionStorage.getItem("gs_demo_mode") === "1") {
                return true;
            }
            const host = (window.location.hostname || "").toLowerCase();
            if (
                window.location.protocol === "file:" ||
                host.endsWith(".github.io") ||
                host.endsWith(".pages.dev") ||
                host.endsWith(".netlify.app") ||
                host.endsWith(".vercel.app")
            ) {
                sessionStorage.setItem("gs_demo_mode", "1");
                return true;
            }
        } catch (e) {}
        return false;
    }

    const IS_DEMO = detectDemoMode();
    window.IS_DEMO_MODE = IS_DEMO;

    // Rewrite navigation links on static hosts or when demo query param is active
    function adaptNavigationLinks() {
        const isStaticFilePath = window.location.pathname.endsWith(".html") ||
            (window.location.hostname || "").toLowerCase().endsWith(".github.io") ||
            window.location.protocol === "file:";
        const hasDemoQuery = new URLSearchParams(window.location.search).get("demo") === "1";

        if (!isStaticFilePath && !hasDemoQuery) return;

        const links = document.querySelectorAll("a.nav-tab");
        links.forEach(a => {
            const rawHref = a.getAttribute("href") || "";
            let target = rawHref;
            if (isStaticFilePath) {
                if (rawHref === "/" || rawHref === "/index.html" || rawHref === "index.html") target = "index.html";
                else if (rawHref === "/gonogo" || rawHref === "/gonogo.html" || rawHref === "gonogo.html") target = "gonogo.html";
                else if (rawHref === "/recovery" || rawHref === "/recovery.html" || rawHref === "recovery.html") target = "recovery.html";
            }
            if (hasDemoQuery && !target.includes("demo=1")) {
                target += (target.includes("?") ? "&" : "?") + "demo=1";
            }
            a.setAttribute("href", target);
        });
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", adaptNavigationLinks);
    } else {
        adaptNavigationLinks();
    }

    if (!IS_DEMO) return;

    // Determine current view to pick smart default start time if not already in sessionStorage
    function getDefaultSimTime() {
        const path = (window.location.pathname || "").toLowerCase();
        if (path.includes("gonogo")) {
            return 125.0; // Phase 2: Nominal pad standby (>65s of stable sensors, all 8 subsystems GO)
        }
        if (path.includes("recovery")) {
            return 222.0; // Final descent approaching touchdown (lands in 8 seconds at t=230s!)
        }
        return 152.0; // Telemetry view: T-3 seconds before liftoff (immediate action + 60s pad history)
    }

    let simTimeSec = getDefaultSimTime();
    try {
        const savedT = parseFloat(sessionStorage.getItem("gs_demo_sim_time"));
        if (Number.isFinite(savedT) && savedT >= 0 && savedT <= CYCLE_DURATION_S) {
            simTimeSec = savedT;
        }
    } catch (e) {}

    const demoEngine = {
        simTimeSec,
        speed: 1,
        paused: false,
        minimized: false,
        frameSeq: Math.floor(simTimeSec * 10),
        invalidCrcCount: 0,
        sockets: new Set()
    };
    window.demoEngine = demoEngine;

    // Deterministic pseudo-noise helper so history frames and live frames match smoothly
    function detNoise(seed, minVal, maxVal) {
        const x = Math.sin(seed * 12.9898 + 78.233) * 43758.5453;
        const frac = x - Math.floor(x);
        return minVal + frac * (maxVal - minVal);
    }

    /**
     * Port of calculate_flight(float t, TelemetryFrame &f) from telemetry_simulator.ino
     */
    function calculateDemoFrame(tSec, seqNum, epochMs) {
        const t = ((tSec % CYCLE_DURATION_S) + CYCLE_DURATION_S) % CYCLE_DURATION_S;
        const seed = Math.round(t * 10);

        let status = 0;
        let altura_actual = detNoise(seed + 1, -0.35, 0.35);
        let altitud_gps = detNoise(seed + 2, -0.6, 0.6);
        let ground_speed = detNoise(seed + 3, 0.0, 0.12);
        let latitude = BASE_LAT + detNoise(seed + 4, -0.000004, 0.000004);
        let longitude = BASE_LON + detNoise(seed + 5, -0.000004, 0.000004);
        let satellites = 9;
        let rssi = -54 + Math.round(detNoise(seed + 6, -2, 2));
        let temp_imu = 28.5 + detNoise(seed + 7, -0.2, 0.2);

        let accel_x = detNoise(seed + 8, -0.02, 0.02);
        let accel_y = detNoise(seed + 9, -0.02, 0.02);
        let accel_z = 1.0 + detNoise(seed + 10, -0.02, 0.02);
        let gyro_x = detNoise(seed + 11, -0.4, 0.4);
        let gyro_y = detNoise(seed + 12, -0.4, 0.4);
        let gyro_z = detNoise(seed + 13, -0.4, 0.4);
        let pin_frenado = 0;
        let pin_principal = 0;

        if (t < 20.0) {
            // Phase 0: Cold boot & GPS lock acquisition (0s - 20s)
            status = 0;
            if (t < 8.0) {
                satellites = Math.floor((t / 8.0) * 3.0);
                latitude = 0.0;
                longitude = 0.0;
                altitud_gps = 0.0;
            } else {
                satellites = 4 + Math.floor(((t - 8.0) / 12.0) * 5.0);
                const settle = 1.0 - ((t - 8.0) / 12.0);
                latitude = BASE_LAT + detNoise(seed + 14, -0.00008, 0.00008) * settle;
                longitude = BASE_LON + detNoise(seed + 15, -0.00008, 0.00008) * settle;
                altitud_gps = detNoise(seed + 16, -6.0, 6.0) * settle;
            }
            altura_actual = (t < 10.0) ? (8.0 * (1.0 - t / 10.0) + detNoise(seed + 17, -1.5, 1.5)) : detNoise(seed + 17, -0.5, 0.5);
            rssi = -62 + Math.round(detNoise(seed + 18, -2, 2));
        } else if (t < 60.0) {
            // Phase 1: Pad inspection & handling disturbances (20s - 60s)
            status = 0;
            satellites = 8;
            rssi = -56 + Math.round(detNoise(seed + 19, -2, 2));
            if (t >= 28.0 && t <= 33.0) {
                accel_x = 0.28 * Math.sin((t - 28.0) * 4.0) + detNoise(seed + 20, -0.05, 0.05);
                accel_y = 0.22 * Math.cos((t - 28.0) * 3.5) + detNoise(seed + 21, -0.05, 0.05);
                accel_z = 0.94 + detNoise(seed + 22, -0.08, 0.08);
                gyro_y = 14.0 * Math.sin((t - 28.0) * 3.0);
                gyro_z = 10.0 * Math.cos((t - 28.0) * 2.5);
            }
            if (t >= 40.0 && t <= 44.0) {
                altura_actual = 7.4 * Math.sin(((t - 40.0) / 4.0) * Math.PI) + detNoise(seed + 23, -0.4, 0.4);
            }
            if (t >= 48.0 && t <= 52.0) {
                latitude = BASE_LAT + 0.000075;
                longitude = BASE_LON + 0.000060;
            }
        } else if (t < 155.0) {
            // Phase 2: Nominal stable Go/No-Go standby on launch pad (60s - 155s)
            status = 0;
            satellites = 10;
            rssi = -52 + Math.round(detNoise(seed + 24, -1, 1));
            altura_actual = detNoise(seed + 25, -0.35, 0.35);
            altitud_gps = detNoise(seed + 26, -0.5, 0.5);
            accel_x = detNoise(seed + 27, -0.015, 0.015);
            accel_y = detNoise(seed + 28, -0.015, 0.015);
            accel_z = 1.0 + detNoise(seed + 29, -0.015, 0.015);
            latitude = BASE_LAT + detNoise(seed + 30, -0.000003, 0.000003);
            longitude = BASE_LON + detNoise(seed + 31, -0.000003, 0.000003);
        } else if (t < 161.0) {
            // Phase 3: Powered ascent / motor burn (155s - 161s)
            const dt = t - 155.0;
            status = 1; // POWERED
            altura_actual = 0.5 * 82.0 * dt * dt + detNoise(seed + 32, -1.2, 1.2);
            altitud_gps = altura_actual + detNoise(seed + 33, -4.0, 4.0);
            ground_speed = 15.0 + dt * 4.5 + detNoise(seed + 34, -0.8, 0.8);

            // Axial acceleration driven on accel_x and accel_y for cockpit G-meter & G-load charts
            const boostG = 9.5 - dt * 0.45 + detNoise(seed + 35, -0.25, 0.25);
            accel_x = boostG;
            accel_y = boostG;
            accel_z = detNoise(seed + 36, -0.35, 0.35);

            gyro_x = 120.0 + dt * 25.0 + detNoise(seed + 37, -5.0, 5.0);
            gyro_y = detNoise(seed + 38, -8.0, 8.0);
            gyro_z = detNoise(seed + 39, -8.0, 8.0);

            latitude = BASE_LAT + dt * 0.00012;
            longitude = BASE_LON + dt * 0.00018;
            satellites = 9;
            rssi = -62 - Math.round(dt * 2.0) + Math.round(detNoise(seed + 40, -2, 2));
            temp_imu = 29.0 + dt * 1.1;
        } else if (t < 177.0) {
            // Phase 4: Coasting after burnout up to apogee (161s - 177s)
            const dt = t - 161.0;
            status = 2; // BURNOUT
            const norm = dt / 16.0;
            altura_actual = 1476.0 + 374.0 * (1.0 - (1.0 - norm) * (1.0 - norm)) + detNoise(seed + 41, -1.5, 1.5);
            altitud_gps = altura_actual + detNoise(seed + 42, -5.0, 5.0);
            ground_speed = 42.0 * (1.0 - 0.5 * norm) + detNoise(seed + 43, -1.0, 1.0);

            const dragG = -1.2 * (1.0 - norm * 0.8) + detNoise(seed + 44, -0.08, 0.08);
            accel_x = dragG;
            accel_y = dragG;
            accel_z = detNoise(seed + 45, -0.10, 0.10);

            gyro_x = 270.0 * (1.0 - norm * 0.7) + detNoise(seed + 46, -3.0, 3.0);
            gyro_y = detNoise(seed + 47, -4.0, 4.0);
            gyro_z = detNoise(seed + 48, -4.0, 4.0);

            latitude = BASE_LAT + 0.00072 + dt * 0.00015;
            longitude = BASE_LON + 0.00108 + dt * 0.00022;
            satellites = 10;
            rssi = -74 - Math.round(norm * 14.0) + Math.round(detNoise(seed + 49, -2, 2));
            temp_imu = 35.6 + norm * 3.5;
        } else if (t < 180.0) {
            // Phase 5: Apogee & Drogue Pyro Order (177s - 180s)
            const dt = t - 177.0;
            status = (dt < 1.5) ? 4 : 8; // APOGEE -> DROGUE_ORD
            altura_actual = 1850.0 - 2.0 * dt * dt + detNoise(seed + 50, -1.0, 1.0);
            altitud_gps = altura_actual + detNoise(seed + 51, -4.0, 4.0);
            ground_speed = 21.0 + detNoise(seed + 52, -0.8, 0.8);

            const pyroShock = (dt >= 1.5) ? (2.4 + detNoise(seed + 53, -0.4, 0.4)) : detNoise(seed + 53, -0.05, 0.05);
            accel_x = pyroShock;
            accel_y = pyroShock;
            accel_z = (dt >= 1.5) ? detNoise(seed + 54, -1.2, 1.2) : detNoise(seed + 54, -0.05, 0.05);

            gyro_x = 45.0 + detNoise(seed + 55, -10.0, 10.0);
            gyro_y = (dt >= 1.5) ? 65.0 * Math.sin(dt * 8.0) : detNoise(seed + 56, -5.0, 5.0);
            gyro_z = (dt >= 1.5) ? 50.0 * Math.cos(dt * 7.0) : detNoise(seed + 57, -5.0, 5.0);

            pin_frenado = (dt >= 1.5) ? 1 : 0;
            pin_principal = 0;

            latitude = BASE_LAT + 0.00312 + dt * 0.00010;
            longitude = BASE_LON + 0.00460 + dt * 0.00015;
            satellites = 10;
            rssi = -88 + Math.round(detNoise(seed + 58, -2, 2));
            temp_imu = 39.1;
        } else if (t < 207.0) {
            // Phase 6: Drogue descent (~48 m/s descent rate) (180s - 207s)
            const dt = t - 180.0;
            status = 16; // DROGUE_CONF
            altura_actual = 1835.0 - 47.6 * dt + detNoise(seed + 59, -1.5, 1.5);
            altitud_gps = altura_actual + detNoise(seed + 60, -4.0, 4.0);
            ground_speed = 14.0 + 3.0 * Math.sin(dt * 0.4) + detNoise(seed + 61, -0.5, 0.5);

            const swayG = 1.0 + 0.25 * Math.sin(dt * 1.5) + detNoise(seed + 62, -0.08, 0.08);
            accel_x = swayG;
            accel_y = swayG;
            accel_z = 0.20 * Math.cos(dt * 1.2) + detNoise(seed + 63, -0.08, 0.08);

            gyro_x = 25.0 * Math.sin(dt * 0.9);
            gyro_y = 18.0 * Math.cos(dt * 1.1);
            gyro_z = 15.0 * Math.sin(dt * 0.7);

            pin_frenado = 1;
            pin_principal = 0;

            latitude = BASE_LAT + 0.00342 + dt * 0.00009;
            longitude = BASE_LON + 0.00505 + dt * 0.00014;
            satellites = 10;
            rssi = -86 + Math.round((dt / 27.0) * 12.0) + Math.round(detNoise(seed + 64, -2, 2));
            temp_imu = 39.0 - (dt / 27.0) * 2.5;
        } else if (t < 210.0) {
            // Phase 7: Main Chute Pyro Order at ~550m (207s - 210s)
            const dt = t - 207.0;
            status = 32; // MAIN_ORD
            altura_actual = 550.0 - 22.0 * dt + detNoise(seed + 65, -1.0, 1.0);
            altitud_gps = altura_actual + detNoise(seed + 66, -3.0, 3.0);
            ground_speed = 9.5 + detNoise(seed + 67, -0.5, 0.5);

            const mainShock = 3.2 * Math.exp(-dt * 0.8) + 1.0 + detNoise(seed + 68, -0.2, 0.2);
            accel_x = mainShock;
            accel_y = mainShock;
            accel_z = detNoise(seed + 69, -0.5, 0.5);

            gyro_x = 40.0 * Math.sin(dt * 5.0);
            gyro_y = 55.0 * Math.cos(dt * 4.5);
            gyro_z = 35.0 * Math.sin(dt * 6.0);

            pin_frenado = 1;
            pin_principal = 1;

            latitude = BASE_LAT + 0.00585 + dt * 0.00007;
            longitude = BASE_LON + 0.00883 + dt * 0.00010;
            satellites = 10;
            rssi = -74 + Math.round(detNoise(seed + 70, -2, 2));
            temp_imu = 36.5;
        } else if (t < 230.0) {
            // Phase 8: Main parachute gentle descent (~24 m/s -> 4.8 m/s touchdown) (210s - 230s)
            const dt = t - 210.0;
            status = 64; // MAIN_CONF
            altura_actual = Math.max(0.0, 484.0 * (1.0 - dt / 20.0) + detNoise(seed + 71, -0.5, 0.5));
            altitud_gps = Math.max(0.0, altura_actual + detNoise(seed + 72, -2.0, 2.0));
            ground_speed = 5.2 + 1.2 * Math.sin(dt * 0.5) + detNoise(seed + 73, -0.3, 0.3);

            const chuteG = 1.0 + 0.08 * Math.sin(dt * 0.8) + detNoise(seed + 74, -0.04, 0.04);
            accel_x = chuteG;
            accel_y = chuteG;
            accel_z = 0.08 * Math.cos(dt * 0.8) + detNoise(seed + 75, -0.04, 0.04);

            gyro_x = 8.0 * Math.sin(dt * 0.8);
            gyro_y = 6.0 * Math.cos(dt * 0.7);
            gyro_z = 5.0 * Math.sin(dt * 0.6);

            pin_frenado = 1;
            pin_principal = 1;

            latitude = BASE_LAT + 0.00606 + dt * 0.00005;
            longitude = BASE_LON + 0.00913 + dt * 0.00007;
            satellites = 10;
            rssi = -72 + Math.round((dt / 20.0) * 8.0) + Math.round(detNoise(seed + 76, -2, 2));
            temp_imu = 36.0 - (dt / 20.0) * 3.0;
        } else {
            // Phase 9: Landed & Recovery Beacon (230s - 290s)
            const dt = t - 230.0;
            status = 128; // LANDED
            altura_actual = detNoise(seed + 77, -0.3, 0.3);
            altitud_gps = detNoise(seed + 78, -0.8, 0.8);
            ground_speed = detNoise(seed + 79, 0.0, 0.08);

            accel_x = 0.12 + detNoise(seed + 80, -0.01, 0.01);
            accel_y = 0.99 + detNoise(seed + 81, -0.01, 0.01);
            accel_z = -0.05 + detNoise(seed + 82, -0.01, 0.01);

            gyro_x = detNoise(seed + 83, -0.2, 0.2);
            gyro_y = detNoise(seed + 84, -0.2, 0.2);
            gyro_z = detNoise(seed + 85, -0.2, 0.2);

            pin_frenado = 1;
            pin_principal = 1;

            latitude = BASE_LAT + 0.00706 + detNoise(seed + 86, -0.000003, 0.000003);
            longitude = BASE_LON + 0.01053 + detNoise(seed + 87, -0.000003, 0.000003);
            satellites = 11;
            rssi = -64 + Math.round(detNoise(seed + 88, -1, 1));
            temp_imu = 33.0 - (dt / 60.0) * 3.0;
        }

        // Anomaly 1: High lateral G / thermal spike at t = 166.0s .. 167.2s
        if (t >= 166.0 && t <= 167.2) {
            accel_y = 10.4 + detNoise(seed + 89, -0.3, 0.3);
            temp_imu = 64.5 + detNoise(seed + 90, -0.8, 0.8);
        }

        const pressure = BASE_PRESS * Math.pow(1.0 - (Math.max(0.0, altura_actual) / 44330.0), 5.255);
        const status_label = STATUS_MAP[status] || "STANDBY";

        return {
            timestamp: epochMs / 1000.0,
            frame_length: 65,
            crc_valid: true,
            crc_value: 0xA5C3,
            raw_hex: "AA55" + String(seqNum & 0xFFFF).padStart(4, "0"),
            data: {
                frame_number: seqNum & 0xFFFF,
                status,
                status_label,
                timestamp: Math.round(t * 1000),
                pressure: Number(pressure.toFixed(2)),
                altura_actual: Number(altura_actual.toFixed(2)),
                altitud_gps: Number(altitud_gps.toFixed(2)),
                latitude: Number(latitude.toFixed(6)),
                longitude: Number(longitude.toFixed(6)),
                ground_speed: Number(ground_speed.toFixed(2)),
                satellites,
                rssi,
                temp_imu: Number(temp_imu.toFixed(1)),
                accel_x: Number(accel_x.toFixed(3)),
                accel_y: Number(accel_y.toFixed(3)),
                accel_z: Number(accel_z.toFixed(3)),
                gyro_x: Number(gyro_x.toFixed(2)),
                gyro_y: Number(gyro_y.toFixed(2)),
                gyro_z: Number(gyro_z.toFixed(2)),
                pin_frenado,
                pin_principal
            },
            statistics: {
                received: seqNum,
                invalid: demoEngine.invalidCrcCount,
                lost: 0,
                loss_percent: 0.0
            }
        };
    }

    function generateDemoHistory(endTimeSec, seconds = 60) {
        const count = Math.min(600, Math.max(10, Math.round(seconds * 10)));
        const nowMs = Date.now();
        const frames = [];
        const baseSeq = Math.max(1, demoEngine.frameSeq - count);

        for (let i = 0; i < count; i++) {
            const offsetSec = (count - 1 - i) * 0.1;
            let tSample = endTimeSec - offsetSec;
            if (tSample < 0) tSample = 0;
            const sampleEpochMs = nowMs - Math.round(offsetSec * 1000);
            const seq = baseSeq + i;
            frames.push(calculateDemoFrame(tSample, seq, sampleEpochMs));
        }

        return {
            count: frames.length,
            seconds,
            frames_received: demoEngine.frameSeq,
            connected: true,
            last_timestamp: nowMs / 1000.0,
            frames
        };
    }

    // Default configuration matching config/app.yaml
    const DEMO_CONFIG = {
        charts: {
            scaling_mode: "ratchet",
            altitude: { pad_max_m: 50.0, expected_apogee_m: 1500.0, step_m: 500.0 },
            acceleration: { min_g: -2.0, max_g: 12.0 },
            velocity: { min_vz_ms: -40.0, max_vz_ms: 160.0, step_ms: 50.0 }
        },
        gonogo: {
            required_hold_s: 30,
            altitude_tol_m: 5.0,
            accel_xy_tol_g: 0.10,
            accel_z_nominal_g: 1.0,
            accel_z_tol_g: 0.15,
            gps_radius_m: 5.0,
            weather_limits: {
                max_wind_speed_ms: 8.5,
                max_cloud_cover_pct: 50.0,
                min_visibility_km: 5.0
            },
            manual_checklist: [
                { id: "estructura", item: "Integridad estructural de fuselaje y aletas verificada", responsible: "Mecánica" },
                { id: "motor", item: "Motor propulsor asegurado e ignitor instalado (sin armar)", responsible: "Propulsión" },
                { id: "paracaidas", item: "Paracaídas Drogue y Principal plegados y líneas libres", responsible: "Recuperación" },
                { id: "pirotecnia", item: "Cargas pirotécnicas conectadas y llave maestra en SAFE", responsible: "Seguridad" },
                { id: "rango", item: "Perímetro de seguridad despejado y espacio aéreo libre", responsible: "RSO" },
                { id: "baterias", item: "Baterías de aviónica y estación terrena al 100% de carga", responsible: "Aviónica" }
            ]
        }
    };

    function getDemoAuditLogs() {
        try {
            const raw = localStorage.getItem("gs_demo_gonogo_audit_logs");
            if (raw) {
                const parsed = JSON.parse(raw);
                if (Array.isArray(parsed) && parsed.length > 0) return parsed;
            }
        } catch (e) {}

        const nowSec = Date.now() / 1000.0;
        const seedLogs = [
            {
                id: 3,
                timestamp: nowSec - 45,
                iso_time: new Date((nowSec - 45) * 1000).toISOString(),
                action: "VERIFICACION_OPERADOR",
                item_id: "estacion_terrena",
                item_label: "Estación Terrena y Subsistemas",
                state: "GO",
                operator: "OPERADOR_DEMO",
                details: null
            },
            {
                id: 2,
                timestamp: nowSec - 90,
                iso_time: new Date((nowSec - 90) * 1000).toISOString(),
                action: "ANCLAJE_GPS",
                item_id: "gps_origin",
                item_label: "Fijar Origen GPS Rampa",
                state: "ORIGEN_FIJADO",
                operator: "OPERADOR_DEMO",
                details: JSON.stringify({ lat: BASE_LAT, lon: BASE_LON })
            },
            {
                id: 1,
                timestamp: nowSec - 140,
                iso_time: new Date((nowSec - 140) * 1000).toISOString(),
                action: "CHECKLIST_MANUAL",
                item_id: "baterias",
                item_label: "Baterías de aviónica y estación terrena al 100% de carga",
                state: "GO",
                operator: "OPERADOR_DEMO",
                details: JSON.stringify({ responsible: "Aviónica" })
            }
        ];
        try {
            localStorage.setItem("gs_demo_gonogo_audit_logs", JSON.stringify(seedLogs));
        } catch (e) {}
        return seedLogs;
    }

    function appendDemoAuditLog(body) {
        const logs = getDemoAuditLogs();
        const nowSec = Date.now() / 1000.0;
        const entry = {
            id: logs.length ? (logs[0].id + 1) : 1,
            timestamp: nowSec,
            iso_time: new Date(nowSec * 1000).toISOString(),
            action: body.action || "UPDATE",
            item_id: body.item_id || null,
            item_label: body.item_label || null,
            state: body.state || "UNKNOWN",
            operator: body.operator || "OPERADOR_DEMO",
            details: body.details ? JSON.stringify(body.details) : null
        };
        logs.unshift(entry);
        if (logs.length > 200) logs.pop();
        try {
            localStorage.setItem("gs_demo_gonogo_audit_logs", JSON.stringify(logs));
        } catch (e) {}
        return entry;
    }

    // Intercept window.fetch for /api/* routes
    const nativeFetch = window.fetch ? window.fetch.bind(window) : null;
    window.fetch = async function demoFetch(input, init) {
        const urlStr = typeof input === "string" ? input : (input && input.url ? input.url : String(input));

        if (urlStr.includes("/api/status")) {
            const now = new Date();
            const pad = n => String(n).padStart(2, "0");
            const payload = {
                connected: true,
                demo_mode: true,
                frames_received: demoEngine.frameSeq,
                last_timestamp: Date.now() / 1000.0,
                serial_port: "SIMULADOR_DEMO (10 Hz)",
                baudrate: 115200,
                statistics: {
                    received: demoEngine.frameSeq,
                    invalid: demoEngine.invalidCrcCount,
                    lost: 0,
                    loss_percent: 0.0
                },
                station: {
                    serial_port: "SIMULADOR_DEMO (10 Hz)",
                    baudrate: 115200,
                    connected: true,
                    database_active: true,
                    csv_active: true,
                    disk_free_pct: 78.4,
                    disk_free_mb: 42500.0,
                    min_required_mb_1h: 50.0,
                    disk_go_1h: true,
                    storage_db: "demo_memory.db",
                    storage_csv: "demo_telemetry.csv",
                    server_time: `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`
                },
                gonogo: DEMO_CONFIG.gonogo,
                charts: DEMO_CONFIG.charts
            };
            return new Response(JSON.stringify(payload), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (urlStr.includes("/api/config")) {
            return new Response(JSON.stringify(DEMO_CONFIG), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (urlStr.includes("/api/history")) {
            let secs = 60;
            try {
                const u = new URL(urlStr, window.location.origin);
                secs = parseFloat(u.searchParams.get("seconds")) || 60;
            } catch (e) {}
            const histPayload = generateDemoHistory(demoEngine.simTimeSec, secs);
            return new Response(JSON.stringify(histPayload), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (urlStr.includes("/api/telemetry")) {
            const frame = calculateDemoFrame(demoEngine.simTimeSec, demoEngine.frameSeq, Date.now());
            return new Response(JSON.stringify({ connected: true, ...frame }), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (urlStr.includes("/api/gonogo/logs")) {
            return new Response(JSON.stringify({ logs: getDemoAuditLogs() }), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (urlStr.includes("/api/gonogo/log")) {
            let body = {};
            try {
                if (init && init.body) body = JSON.parse(init.body);
            } catch (e) {}
            const entry = appendDemoAuditLog(body);
            return new Response(JSON.stringify({ status: "ok", entry }), {
                status: 200,
                headers: { "Content-Type": "application/json" }
            });
        }

        if (nativeFetch) {
            return nativeFetch(input, init);
        }
        throw new Error("Fetch unavailable");
    };

    // Intercept window.WebSocket for /ws and /ws/telemetry
    const NativeWebSocket = window.WebSocket;
    class DemoWebSocket {
        static CONNECTING = 0;
        static OPEN = 1;
        static CLOSING = 2;
        static CLOSED = 3;

        constructor(url, protocols) {
            this.url = url;
            this.protocols = protocols;
            this.readyState = DemoWebSocket.CONNECTING;
            this.onopen = null;
            this.onmessage = null;
            this.onerror = null;
            this.onclose = null;
            this._listeners = { open: [], message: [], error: [], close: [] };

            demoEngine.sockets.add(this);

            setTimeout(() => {
                if (this.readyState !== DemoWebSocket.CONNECTING) return;
                this.readyState = DemoWebSocket.OPEN;
                this._emit("open", {});
                this._sendJson({ type: "connection", connected: true });
                const initialFrame = calculateDemoFrame(demoEngine.simTimeSec, demoEngine.frameSeq, Date.now());
                this._sendJson({ type: "telemetry", ...initialFrame });
            }, 40);
        }

        addEventListener(type, fn) {
            if (this._listeners[type]) this._listeners[type].push(fn);
        }

        removeEventListener(type, fn) {
            if (this._listeners[type]) {
                this._listeners[type] = this._listeners[type].filter(cb => cb !== fn);
            }
        }

        send() {}

        close() {
            if (this.readyState === DemoWebSocket.CLOSED) return;
            this.readyState = DemoWebSocket.CLOSED;
            demoEngine.sockets.delete(this);
            this._emit("close", {});
        }

        _emit(type, evt) {
            const handler = this["on" + type];
            if (typeof handler === "function") {
                try { handler(evt); } catch (e) { console.error(e); }
            }
            const list = this._listeners[type] || [];
            for (const fn of list) {
                try { fn(evt); } catch (e) { console.error(e); }
            }
        }

        _sendJson(obj) {
            if (this.readyState !== DemoWebSocket.OPEN) return;
            this._emit("message", { data: JSON.stringify(obj) });
        }
    }

    window.WebSocket = function PatchedWebSocket(url, protocols) {
        const u = String(url || "");
        if (u.includes("/ws")) {
            return new DemoWebSocket(url, protocols);
        }
        return new NativeWebSocket(url, protocols);
    };
    window.WebSocket.CONNECTING = 0;
    window.WebSocket.OPEN = 1;
    window.WebSocket.CLOSING = 2;
    window.WebSocket.CLOSED = 3;

    // 10 Hz Simulation Loop
    setInterval(() => {
        if (demoEngine.paused) return;

        const dtSec = 0.1 * demoEngine.speed;
        demoEngine.simTimeSec = (demoEngine.simTimeSec + dtSec) % CYCLE_DURATION_S;
        demoEngine.frameSeq = (demoEngine.frameSeq + 1) & 0xFFFF;

        try {
            sessionStorage.setItem("gs_demo_sim_time", demoEngine.simTimeSec.toFixed(1));
        } catch (e) {}

        const frame = calculateDemoFrame(demoEngine.simTimeSec, demoEngine.frameSeq, Date.now());
        const msg = { type: "telemetry", ...frame };

        for (const ws of demoEngine.sockets) {
            ws._sendJson(msg);
        }

        updateDemoHudUI();
    }, 100);

    // Jump to specific simulation time and update page telemetry in-place smoothly
    function jumpToSimTime(targetSec, reload = false) {
        const clamped = Math.max(0, Math.min(CYCLE_DURATION_S - 0.1, Number(targetSec) || 0));
        demoEngine.simTimeSec = clamped;
        demoEngine.frameSeq = Math.max(100, Math.floor(clamped * 10));
        try {
            sessionStorage.setItem("gs_demo_sim_time", clamped.toFixed(1));
        } catch (e) {}

        // Reset flight liftoff flags if rewinding before launch (t < 155s)
        if (clamped < 155.0) {
            if (window.state) {
                window.state.liftoffDetected = false;
                window.state.liftoffAt = null;
            }
        } else {
            if (window.state && !window.state.liftoffDetected) {
                window.state.liftoffDetected = true;
                window.state.liftoffAt = Date.now() - (clamped - 155.0) * 1000;
            }
        }

        if (reload) {
            window.location.reload();
            return;
        }

        // Broadcast telemetry frame immediately over all open WebSockets
        const frame = calculateDemoFrame(demoEngine.simTimeSec, demoEngine.frameSeq, Date.now());
        const msg = { type: "telemetry", ...frame };
        for (const ws of demoEngine.sockets) {
            ws._sendJson(msg);
        }

        updateDemoHudUI();
    }
    window.jumpToDemoTime = jumpToSimTime;

    function getPhaseBadgeLabel(t) {
        if (t < 20) return "BOOT GPS";
        if (t < 60) return "PRUEBA RAMPA";
        if (t < 155) return "RAMPA NOMINAL";
        if (t < 161) return "PROPULSIÓN";
        if (t < 177) return "COASTING";
        if (t < 180) return "APOGEO / DROGUE";
        if (t < 207) return "DESCENSO DROGUE";
        if (t < 210) return "ORDEN MAIN";
        if (t < 230) return "DESCENSO MAIN";
        return "ATERRIZADO";
    }

    function updateDemoHudUI() {
        const t = demoEngine.simTimeSec;
        const timeEl = document.getElementById("demoHudTime");
        const sliderEl = document.getElementById("demoHudSlider");
        const phaseEl = document.getElementById("demoHudPhase");
        const minBtn = document.getElementById("demoBtnMin");
        if (timeEl) {
            timeEl.textContent = `${t.toFixed(1)}s / 290s`;
        }
        if (sliderEl && document.activeElement !== sliderEl) {
            sliderEl.value = t.toFixed(1);
        }
        if (phaseEl) {
            phaseEl.textContent = getPhaseBadgeLabel(t);
        }
        if (minBtn) {
            minBtn.textContent = demoEngine.minimized ? "▢" : "▼";
            minBtn.title = demoEngine.minimized ? "Expandir barra de control" : "Minimizar barra de control";
        }

        const presets = document.querySelectorAll(".demo-preset-btn");
        presets.forEach(btn => {
            const start = parseFloat(btn.getAttribute("data-start"));
            const end = parseFloat(btn.getAttribute("data-end"));
            btn.classList.toggle("active", t >= start && t < end);
        });
    }

    function mountDemoHud() {
        if (document.getElementById("demoControlHud")) return;

        const hud = document.createElement("div");
        hud.id = "demoControlHud";
        hud.className = "demo-hud";
        hud.innerHTML = `
            <div class="demo-hud-header">
                <span class="demo-hud-badge" title="Simulador de telemetría en vivo">
                    🚀 DEMO <span id="demoHudPhase" style="opacity:0.9; font-weight:800;"></span>
                </span>
                <button type="button" class="demo-hud-btn play-btn" id="demoBtnPlay" title="Pausar / Reanudar simulación">⏸</button>
                <span class="demo-hud-time" id="demoHudTime">${demoEngine.simTimeSec.toFixed(1)}s / 290s</span>
                <div class="demo-hud-header-actions">
                    <button type="button" class="demo-hud-btn" id="demoBtnSpeed" title="Velocidad de reproducción">1x</button>
                    <button type="button" class="demo-hud-btn min-btn" id="demoBtnMin" title="Minimizar / Expandir control demo">▼</button>
                </div>
            </div>
            <div class="demo-hud-body" id="demoHudBody">
                <div class="demo-hud-slider-wrap">
                    <input type="range" class="demo-hud-slider" id="demoHudSlider" min="0" max="290" step="0.5" value="${demoEngine.simTimeSec.toFixed(1)}" title="Línea de tiempo (0s - 290s)">
                </div>
                <div class="demo-hud-presets">
                    <button type="button" class="demo-hud-btn demo-preset-btn" data-time="125" data-start="60" data-end="150" title="Rampa Estabilizada">Rampa GO</button>
                    <button type="button" class="demo-hud-btn demo-preset-btn" data-time="152" data-start="150" data-end="176" title="Despegue (T-3s)">Despegue</button>
                    <button type="button" class="demo-hud-btn demo-preset-btn" data-time="176" data-start="176" data-end="206" title="Apogeo y Drogue">Apogeo</button>
                    <button type="button" class="demo-hud-btn demo-preset-btn" data-time="206" data-start="206" data-end="226" title="Paracaídas Principal">Main Chute</button>
                    <button type="button" class="demo-hud-btn demo-preset-btn" data-time="227" data-start="226" data-end="291" title="Aterrizaje">Aterrizaje</button>
                </div>
            </div>
        `;

        document.body.appendChild(hud);

        const playBtn = document.getElementById("demoBtnPlay");
        playBtn?.addEventListener("click", () => {
            demoEngine.paused = !demoEngine.paused;
            playBtn.textContent = demoEngine.paused ? "▶" : "⏸";
            playBtn.classList.toggle("paused", demoEngine.paused);
        });

        const slider = document.getElementById("demoHudSlider");
        slider?.addEventListener("input", e => {
            jumpToSimTime(parseFloat(e.target.value));
        });
        slider?.addEventListener("change", e => {
            jumpToSimTime(parseFloat(e.target.value));
        });

        hud.querySelectorAll(".demo-preset-btn").forEach(btn => {
            btn.addEventListener("click", () => {
                jumpToSimTime(parseFloat(btn.getAttribute("data-time")));
            });
        });

        const speedBtn = document.getElementById("demoBtnSpeed");
        speedBtn?.addEventListener("click", () => {
            const speeds = [1, 2, 5];
            const nextIdx = (speeds.indexOf(demoEngine.speed) + 1) % speeds.length;
            demoEngine.speed = speeds[nextIdx];
            speedBtn.textContent = `${demoEngine.speed}x`;
            speedBtn.classList.toggle("active", demoEngine.speed > 1);
        });

        const minBtn = document.getElementById("demoBtnMin");
        const bodyWrap = document.getElementById("demoHudBody");
        minBtn?.addEventListener("click", () => {
            demoEngine.minimized = !demoEngine.minimized;
            hud.classList.toggle("minimized", demoEngine.minimized);
            if (bodyWrap) bodyWrap.style.display = demoEngine.minimized ? "none" : "";
            minBtn.textContent = demoEngine.minimized ? "▢" : "▼";
            minBtn.title = demoEngine.minimized ? "Expandir barra de control" : "Minimizar barra de control";
        });

        updateDemoHudUI();
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", mountDemoHud);
    } else {
        mountDemoHud();
    }
})();

