import subprocess
import asyncio
import struct
import time
from aiohttp import web, WSMsgType

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"



LAPS = {}  # lap -> state
WS_CLIENTS = set()

UDP_PORT = 9000

PING_PROCESSES = {}  # Track running ping processes


async def l2ping_handler(request):
    data = await request.json()
    lap = data.get('lap')
    uap = data.get('uap')
    
    if uap == 0xFF:
        return web.json_response({
            'success': False,
            'message': 'UAP unknown, cannot ping'
        })
    
    # Construct Bluetooth address
    bt_addr = f"{lap:06X}:{uap:02X}:00:00:00:00"
    
    # Start l2ping in background
    asyncio.create_task(run_l2ping(bt_addr, lap))
    
    return web.json_response({'success': True})

async def run_l2ping(bt_addr, lap):
    try:
        process = await asyncio.create_subprocess_exec(
            'l2ping', '-c', '4', bt_addr,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            
            output = line.decode('utf-8', errors='ignore')
            
            # Send to all WebSocket clients
            for ws in WS_CLIENTS:
                await ws.send_json({
                    'type': 'ping_output',
                    'data': output
                })
        
        await process.wait()
        
        # Send completion status
        for ws in WS_CLIENTS:
            await ws.send_json({
                'type': 'ping_complete',
                'success': process.returncode == 0
            })
            
    except Exception as e:
        for ws in WS_CLIENTS:
            await ws.send_json({
                'type': 'ping_output',
                'data': f'\nError: {str(e)}\n'
            })
            await ws.send_json({
                'type': 'ping_complete',
                'success': False
            })



# -----------------------------
# UDP receiver
# -----------------------------
class UDPProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data, addr):
        # Parse header
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
            "color": f"hsl({lap % 360},70%,50%)"
        })

        state["last_seen"] = now
        state["rssi"] = rssi
        state["count"] += 1
        state["channels"][channel] = rssi
        uap = uap if uap is not None else 0xFF
        if flags & 0x02:
            state["uap"] = uap

        # Push to websocket clients
        msg = {
            "type": "packet",
            "lap": lap,
            "uap": state["uap"],
            "rssi": rssi,
            "channel": channel,
            "count": state["count"]
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

    # send full state on connect
    await ws.send_json({
        "type": "init",
        "laps": list(LAPS.values())
    })

    async for msg in ws:
        if msg.type == WSMsgType.ERROR:
            break

    WS_CLIENTS.remove(ws)
    return ws


# -----------------------------
# HTTP handlers
# -----------------------------
async def index(request):
    #return web.FileResponse("static/index.html")
    return web.FileResponse(STATIC_DIR / "index_mobile.html")


# -----------------------------
# Main
# -----------------------------
async def main():
    loop = asyncio.get_running_loop()

    # UDP server
    await loop.create_datagram_endpoint(
        UDPProtocol,
        local_addr=("0.0.0.0", UDP_PORT)
    )

    # Web server
    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_get("/ws", ws_handler)
    app.router.add_static("/static", STATIC_DIR)
    app.router.add_post("/l2ping", l2ping_handler)
    #app.router.add_static("/static", "static")

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()

    print("Listening UDP :9000, Web :8080")
    while True:
        await asyncio.sleep(3600)


asyncio.run(main())
