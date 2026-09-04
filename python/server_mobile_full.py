import asyncio
import struct
import time
import subprocess
from aiohttp import web, WSMsgType
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"

LAPS = {}
WS_CLIENTS = set()
UDP_PORT = 9000

# -----------------------------
# Bluetooth device info extraction
# -----------------------------
async def get_device_info(bt_addr):
    """
    Get device info using hcitool and sdptool
    Returns dict with name, class, manufacturer, etc.
    """
    info = {}
    
    try:
        # Get device name
        proc = await asyncio.create_subprocess_exec(
            'hcitool', 'name', bt_addr,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        name = stdout.decode().strip()
        if name:
            info['name'] = name
    except:
        pass
    
    try:
        # Get device info (class, clock offset, etc)
        proc = await asyncio.create_subprocess_exec(
            'hcitool', 'info', bt_addr,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
        output = stdout.decode()
        
        # Parse output
        for line in output.split('\n'):
            if 'Device Name:' in line:
                info['name'] = line.split(':', 1)[1].strip()
            elif 'Class:' in line:
                info['class'] = line.split(':', 1)[1].strip()
            elif 'Manufacturer:' in line:
                info['manufacturer'] = line.split(':', 1)[1].strip()
            elif 'LMP Version:' in line:
                info['lmp_version'] = line.split(':', 1)[1].strip()
    except:
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

        lap, uap, rssi, channel, flags, pdu_len = struct.unpack_from(
            "<IBbBBH", data, 0
        )

        now = time.time()

        state = LAPS.setdefault(lap, {
            "lap": lap,
            "uap": 0xFF,
            "last_seen": 0,
            "rssi": 0,
            "channels": {},
            "count": 0,
            "name": None,
            "color": f"hsl({lap % 360},70%,50%)"
        })

        state["last_seen"] = now
        state["rssi"] = rssi
        state["count"] += 1
        state["channels"][channel] = rssi
        uap = uap if uap is not None else 0xFF
        if flags & 0x02:
            state["uap"] = uap

        msg = {
            "type": "packet",
            "lap": lap,
            "uap": state["uap"],
            "rssi": rssi,
            "channel": channel,
            "count": state["count"],
            "name": state.get("name")
        }

        for ws in WS_CLIENTS:
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

# -----------------------------
# L2Ping handler with device info
# -----------------------------
async def l2ping_handler(request):
    data = await request.json()
    lap = data.get('lap')
    uap = data.get('uap')
    
    if uap == 0xFF:
        return web.json_response({
            'success': False,
            'message': 'UAP unknown, cannot ping'
        })
    
    # Construct Bluetooth address (you'll need NAP - Network Access Point)
    # For now using 00:00 as NAP placeholder
    #bt_addr = f"00:00:{uap:02X}:{lap:06X}"
    bt_addr = lap_uap_to_addr(lap, uap)
    # More realistic: extract from LAP/UAP if you have full address
    # bt_addr = f"{(lap >> 16) & 0xFF:02X}:{(lap >> 8) & 0xFF:02X}:{lap & 0xFF:02X}:{uap:02X}:XX:XX"
    
    asyncio.create_task(run_l2ping(bt_addr, lap, uap))
    
    return web.json_response({'success': True})

async def run_l2ping(bt_addr, lap, uap):
    try:
        # First, try to get device info
        info = await get_device_info(bt_addr)
        
        # Store device name if found
        if info.get('name') and lap in LAPS:
            LAPS[lap]['name'] = info['name']
        
        # Send device info to clients
        for ws in WS_CLIENTS:
            await ws.send_json({
                'type': 'device_info',
                'lap': lap,
                'uap': uap,
                **info
            })
        rssi_task = asyncio.create_task(poll_rssi(bt_addr, lap))
        # Now run l2ping
        # Use sudo if needed, or set capabilities as discussed
        process = await asyncio.create_subprocess_exec(
             'l2ping', '-c', '8', bt_addr,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            output = line.decode('utf-8', errors='ignore')
            await broadcast({
                    'type': 'ping_output',
                    'data': output
                })
            """
            for ws in WS_CLIENTS:
                await ws.send_json({
                    'type': 'ping_output',
                    'data': output
                })
            """
        await process.wait()
        
        for ws in WS_CLIENTS:
            await ws.send_json({
                'type': 'ping_complete',
                'success': process.returncode == 0
            })
            
    except Exception as e:
        await broadcast({
                'type': 'ping_output',
                'data': f'\nError: {str(e)}\n'
            })
        await broadcast({
                'type': 'ping_complete',
                'success': False
            })
        """
        for ws in WS_CLIENTS:
            await ws.send_json({
                'type': 'ping_output',
                'data': f'\nError: {str(e)}\n'
            })
            await ws.send_json({
                'type': 'ping_complete',
                'success': False
            })
        """
    finally:
        rssi_task.cancel()
        
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

async def run_l2ping_bof(bt_addr, lap, interval=0.5):
    # keep a persistent ping running to hold the link open
    ping_proc = await asyncio.create_subprocess_exec(
        'l2ping', '-i', 'hci0', '-t', '1', '-d', '1', bt_addr,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL
    )
    
    try:
        while True:
            proc = await asyncio.create_subprocess_exec(
                'hcitool', '-i', 'hci0', 'rssi', bt_addr,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL
            )
            out, _ = await proc.communicate()
            rssi = None
            if proc.returncode == 0 and out:
                # "RSSI return value: -58"
                rssi = int(out.decode().strip().split()[-1])
                if lap in LAPS:
                    LAPS[lap]['rssi'] = rssi
            await broadcast({'type': 'rssi', 'lap': lap, 'rssi': rssi})
            #for ws in WS_CLIENTS:
            #    await ws.send_json({'type': 'rssi', 'lap': lap, 'rssi': rssi})
            await asyncio.sleep(interval)  # control cadence yourself, not via l2ping -d
    finally:
        ping_proc.terminate()
        

async def broadcast(msg):
    dead = set()
    for ws in list(WS_CLIENTS):
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    WS_CLIENTS.difference_update(dead)
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

    await loop.create_datagram_endpoint(
        UDPProtocol,
        local_addr=("0.0.0.0", UDP_PORT)
    )

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_post("/l2ping", l2ping_handler)
    app.router.add_static("/static", STATIC_DIR)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

    print("Listening UDP :9000, Web :8080")
    while True:
        await asyncio.sleep(3600)

asyncio.run(main())