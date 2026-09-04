import asyncio
import struct
import time
from aiohttp import web, WSMsgType
from pathlib import Path
import json

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

LAPS = {}
WS_CLIENTS = set()
UDP_PORT = 9000

# session tracker for the currently-active ping/rssi session (one at a time)
_session_counter = 0
current_ping = {"id": None, "task": None, "proc": None, "rssi_task": None, "lap": None}

def load_whitelist():
    path = BASE_DIR / "whitelist.json"
    by_lap = {}
    if path.exists():
        raw = json.loads(path.read_text())
        for addr, name in raw.items():
            parts = addr.split(":")
            lap = (int(parts[3], 16) << 16) | (int(parts[4], 16) << 8) | int(parts[5], 16)
            by_lap[lap] = name
    return by_lap

WHITELIST_BY_LAP = load_whitelist()

# -----------------------------
# Bluetooth device info extraction
# -----------------------------
async def get_device_info(bt_addr):
    info = {}
    try:
        proc = await asyncio.create_subprocess_exec(
            'hcitool', 'name', bt_addr,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        name = stdout.decode().strip()
        if name:
            info['name'] = name
    except Exception:
        pass

    try:
        proc = await asyncio.create_subprocess_exec(
            'hcitool', 'info', bt_addr,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        output = stdout.decode()
        for line in output.split('\n'):
            if 'Device Name:' in line:
                info['name'] = line.split(':', 1)[1].strip()
            elif 'Class:' in line:
                info['class'] = line.split(':', 1)[1].strip()
            elif 'Manufacturer:' in line:
                info['manufacturer'] = line.split(':', 1)[1].strip()
            elif 'LMP Version:' in line:
                info['lmp_version'] = line.split(':', 1)[1].strip()
    except Exception:
        pass

    return info


def lap_uap_to_addr(lap, uap, nap=0x0000):
    return "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}".format(
        (nap >> 8) & 0xFF,
        nap & 0xFF,
        uap & 0xFF,
        (lap >> 16) & 0xFF,
        (lap >> 8) & 0xFF,
        lap & 0xFF,
    )

# -----------------------------
# UDP receiver
# -----------------------------
class UDPProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        if len(data) < 11:
            return

        lap, uap, rssi, channel, flags, pdu_len = struct.unpack_from("<IBbBBH", data, 0)
        now = time.time()

        state = LAPS.setdefault(lap, {
            "lap": lap, "uap": 0xFF, "last_seen": 0, "rssi": 0,
            "channels": {}, "count": 0, "name": None,
            "color": f"hsl({lap % 360},70%,50%)"
        })

        state["last_seen"] = now
        state["rssi"] = rssi
        state["count"] += 1
        state["channels"][channel] = rssi
        uap = uap if uap is not None else 0xFF
        if flags & 0x02:
            state["uap"] = uap
        if lap in WHITELIST_BY_LAP:
            state["name"] = WHITELIST_BY_LAP[lap]
        msg = {
            "type": "packet", "lap": lap, "uap": state["uap"], "rssi": rssi,
            "channel": channel, "count": state["count"], "name": state.get("name")
        }
        for ws in list(WS_CLIENTS):
            asyncio.create_task(ws.send_json(msg))

# -----------------------------
# WebSocket handler
# -----------------------------
async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    WS_CLIENTS.add(ws)
    try:
        await ws.send_json({"type": "init", "laps": list(LAPS.values())})
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
    finally:
        WS_CLIENTS.discard(ws)
    return ws


async def broadcast(msg):
    dead = set()
    for ws in list(WS_CLIENTS):
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    WS_CLIENTS.difference_update(dead)

# -----------------------------
# RSSI polling (runs for the life of the session)
# -----------------------------
async def poll_rssi(bt_addr, lap, interval=0.5):
    while True:
        try:
            proc = await asyncio.create_subprocess_exec(
                'hcitool', 'rssi', bt_addr,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            out, _ = await proc.communicate()
            if proc.returncode == 0 and out:
                rssi = int(out.decode().strip().split()[-1])
                if lap in LAPS:
                    LAPS[lap]['rssi'] = rssi
                await broadcast({'type': 'hci_rssi', 'lap': lap, 'rssi': rssi})
        except Exception:
            pass
        await asyncio.sleep(interval)

# -----------------------------
# Ping session management
# -----------------------------
async def stop_current_ping():
    """Cleanly tear down whatever ping/rssi session is currently active."""
    proc = current_ping.get("proc")
    rssi_task = current_ping.get("rssi_task")
    task = current_ping.get("task")

    if rssi_task:
        rssi_task.cancel()

    if proc and proc.returncode is None:
        try:
            proc.terminate()
        except ProcessLookupError:
            pass

    if task and not task.done():
        task.cancel()
        try:
            await task
        except Exception:
            pass

    current_ping.update({"id": None, "task": None, "proc": None, "rssi_task": None, "lap": None})


async def run_l2ping_session(bt_addr, lap, uap, session_id):
    try:
        info = await get_device_info(bt_addr)
        if lap in WHITELIST_BY_LAP:
            info['name'] = WHITELIST_BY_LAP[lap]
        if info.get('name') and lap in LAPS:
            LAPS[lap]['name'] = info['name']
        await broadcast({'type': 'device_info', 'lap': lap, 'uap': uap, **info})

        # continuous, uncounted ping - keeps the ACL link alive like blue_sonar does
        proc = await asyncio.create_subprocess_exec(
            'l2ping', '-t', '1', '-d', '1', bt_addr,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )

        if current_ping.get("id") != session_id:
            # we got superseded/stopped while starting up - bail out cleanly
            proc.terminate()
            return
        current_ping["proc"] = proc

        rssi_task = asyncio.create_task(poll_rssi(bt_addr, lap))
        current_ping["rssi_task"] = rssi_task

        await broadcast({'type': 'ping_status', 'status': 'running', 'lap': lap})

        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            await broadcast({'type': 'ping_output', 'data': line.decode('utf-8', errors='ignore')})

        await proc.wait()
        await broadcast({'type': 'ping_complete', 'success': proc.returncode == 0, 'lap': lap})

    except Exception as e:
        await broadcast({'type': 'ping_output', 'data': f'\nError: {str(e)}\n'})
        await broadcast({'type': 'ping_complete', 'success': False, 'lap': lap})
    finally:
        if current_ping.get("id") == session_id:
            rt = current_ping.get("rssi_task")
            if rt:
                rt.cancel()
            current_ping.update({"id": None, "task": None, "proc": None, "rssi_task": None, "lap": None})


async def l2ping_handler(request):
    global _session_counter
    data = await request.json()
    lap = data.get('lap')
    uap = data.get('uap')

    if uap == 0xFF:
        return web.json_response({'success': False, 'message': 'UAP unknown, cannot ping'})

    bt_addr = lap_uap_to_addr(lap, uap)

    await stop_current_ping()  # replace any session already running

    _session_counter += 1
    session_id = _session_counter
    task = asyncio.create_task(run_l2ping_session(bt_addr, lap, uap, session_id))
    current_ping.update({"id": session_id, "task": task, "lap": lap})

    return web.json_response({'success': True})


async def l2ping_stop_handler(request):
    await stop_current_ping()
    return web.json_response({'success': True})

# -----------------------------
# HTTP handlers
# -----------------------------
async def index(request):
    return web.FileResponse(STATIC_DIR / "index_mobile_full.html")

# -----------------------------
# Main
# -----------------------------
async def main():
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(UDPProtocol, local_addr=("0.0.0.0", UDP_PORT))

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/l2ping", l2ping_handler)
    app.router.add_post("/l2ping/stop", l2ping_stop_handler)
    app.router.add_static("/static", STATIC_DIR)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

    print("Listening UDP :9000, Web :8080")
    while True:
        await asyncio.sleep(3600)

asyncio.run(main())