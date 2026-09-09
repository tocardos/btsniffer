"""
radar_server / app.py

Thin relay between one or more ZMQ PUB telemetry streams (e.g. btsniffer and
ubertooth_newscan) and a browser-based radar UI. Deliberately dumb on the
backend: it does no per-LAP state tracking or role/RSSI logic itself -- it
just forwards each JSON message from ZMQ to every connected WebSocket client,
tagging it with the name of the endpoint it arrived on so the frontend can
keep the sources separate (and compare them) instead of blending them.

Each connected source gets its own ZMQ SUB socket and listener thread; every
message is validated as JSON, stamped with {"src": <endpoint-name>}, and
broadcast verbatim to all browser clients. All radar placement, source
coloring, master/slave pairing, and staleness logic lives in static/radar.js.

Run:
    pip install -r requirements.txt
    python app.py \
        --zmq btsniffer=127.0.0.1:9000 \
        --zmq ubertooth=127.0.0.1:9001 \
        --http-port 8080

Then open http://localhost:8080 in a browser.

Endpoint specs are "name=host:port" (the name is what radar.js uses for
coloring/legend; stick to short lowercase ids like "btsniffer"/"ubertooth").
A bare "host:port" (no name=) is accepted and gets an auto-generated name.
"""

import argparse
import asyncio
import json
import logging
import threading
from pathlib import Path
from typing import List, Set, Tuple

import zmq
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("radar_server")

STATIC_DIR = Path(__file__).parent / "static"

from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(app: FastAPI):
    loop = asyncio.get_event_loop()
    for name, host, port in app.state.zmq_endpoints:
        t = threading.Thread(
            target=zmq_listener_thread,
            args=(name, host, port, loop),
            daemon=True,
            name=f"zmq-sub-{name}",
        )
        t.start()
    yield


app = FastAPI(title="Bluetooth CFO Radar", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# All currently-connected browser WebSockets.
clients: Set[WebSocket] = set()
clients_lock = asyncio.Lock()


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    async with clients_lock:
        clients.add(ws)
    log.info("client connected (%d total)", len(clients))
    try:
        while True:
            # We don't expect the browser to send anything meaningful; just
            # keep the connection open and drain whatever arrives (e.g.
            # ping frames some browsers send as text).
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        async with clients_lock:
            clients.discard(ws)
        log.info("client disconnected (%d total)", len(clients))


async def broadcast(message: str):
    """Send `message` to every connected client, dropping dead ones."""
    async with clients_lock:
        targets = list(clients)
    dead = []
    for ws in targets:
        try:
            await ws.send_text(message)
        except Exception:
            dead.append(ws)
    if dead:
        async with clients_lock:
            for ws in dead:
                clients.discard(ws)


def zmq_listener_thread(name: str, host: str, port: int, loop: asyncio.AbstractEventLoop):
    """
    One thread per ZMQ endpoint (each source needs its own SUB socket so we
    can stamp messages with which source they came from). Each received
    message is validated as JSON, tagged with {"src": name}, and handed off
    to the asyncio event loop for broadcasting.
    """
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    endpoint = f"tcp://{host}:{port}"
    sub.connect(endpoint)
    sub.setsockopt_string(zmq.SUBSCRIBE, "")  # subscribe to everything
    log.info("ZMQ SUB [%s] connected to %s", name, endpoint)

    while True:
        try:
            raw_bytes = sub.recv()
        except zmq.ZMQError as e:
            log.error("zmq recv error on [%s]: %s", name, e)
            continue

        try:
            # Decode explicitly (rather than sub.recv_string(), which
            # decodes internally and raises UnicodeDecodeError *out of*
            # recv() itself). That exception was previously unhandled
            # here, so a single malformed/garbage frame from the sender
            # permanently killed this thread -- silently, since it's a
            # daemon thread with no supervisor -- and no further messages
            # were ever broadcast again for the rest of the run. One bad
            # frame should be dropped, not fatal.
            try:
                raw = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                log.warning("[%s] dropped non-UTF8 message from zmq (%d bytes): %r",
                            name, len(raw_bytes), raw_bytes[:200])
                continue

            # Validate JSON here (cheaply) so a malformed message can't
            # wedge every connected browser's parser; log and skip instead.
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("[%s] dropped non-JSON message from zmq: %r", name, raw[:200])
                continue

            if not isinstance(obj, dict):
                log.warning("[%s] dropped non-object JSON message from zmq: %r", name, raw[:200])
                continue

            # Stamp the source. The endpoint name is authoritative for
            # identity on this server: even if a sender includes its own
            # "src" field, the relay decides which source the message
            # actually arrived from. This is what lets radar.js keep
            # btsniffer and ubertooth readings of the same LAP apart
            # instead of overwriting each other.
            obj["src"] = name

            asyncio.run_coroutine_threadsafe(broadcast(json.dumps(obj)), loop)
        except Exception:
            # Belt-and-braces: nothing above should reach here, but this
            # thread has no supervisor to restart it, so an unanticipated
            # exception must never be allowed to end the loop silently.
            log.exception("unexpected error handling zmq message on [%s]; continuing", name)


def parse_endpoints(args) -> List[Tuple[str, str, int]]:
    """
    Build the (name, host, port) endpoint list.

    --zmq is repeatable: --zmq btsniffer=127.0.0.1:9000 --zmq ubertooth=127.0.0.1:9001
    For backward compatibility, if no --zmq is given we fall back to the old
    --zmq-host/--zmq-port pair, named "btsniffer" (the historical default).
    """
    endpoints: List[Tuple[str, str, int]] = []
    if args.zmq:
        for i, spec in enumerate(args.zmq):
            if "=" in spec:
                name, hostport = spec.split("=", 1)
                name = name.strip()
            else:
                name, hostport = f"src{i}", spec
            if not name:
                raise SystemExit(f"invalid --zmq spec {spec!r}: empty source name")
            try:
                host, port = hostport.rsplit(":", 1)
                endpoints.append((name, host.strip(), int(port)))
            except ValueError:
                raise SystemExit(
                    f"invalid --zmq spec {spec!r}: expected name=host:port "
                    f"(e.g. ubertooth=127.0.0.1:9001)"
                )
    else:
        endpoints.append(("btsniffer", args.zmq_host, args.zmq_port))

    names = [n for n, _, _ in endpoints]
    dupes = {n for n in names if names.count(n) > 1}
    if dupes:
        raise SystemExit(f"duplicate --zmq source name(s): {sorted(dupes)}")
    return endpoints


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--zmq", action="append", default=None, metavar="NAME=HOST:PORT",
                   help="ZMQ PUB source, repeatable. Example: "
                        "--zmq btsniffer=127.0.0.1:9000 --zmq ubertooth=127.0.0.1:9001. "
                        "If omitted, falls back to --zmq-host/--zmq-port.")
    p.add_argument("--zmq-host", default="127.0.0.1",
                    help="(legacy, used only when --zmq is not given) "
                         "Host running the ZMQ PUB socket (default: 127.0.0.1)")
    p.add_argument("--zmq-port", type=int, default=9000,
                    help="(legacy, used only when --zmq is not given) ZMQ PUB port (default: 9000)")
    p.add_argument("--http-host", default="0.0.0.0",
                    help="Address to bind the web server to (default: 0.0.0.0)")
    p.add_argument("--http-port", type=int, default=8080,
                    help="Port to serve the radar UI on (default: 8080)")
    return p.parse_args()


if __name__ == "__main__":
    import uvicorn

    cli_args = parse_args()
    app.state.zmq_endpoints = parse_endpoints(cli_args)
    log.info("sources: %s", ", ".join(f"{n}=tcp://{h}:{p}" for n, h, p in app.state.zmq_endpoints))
    uvicorn.run(app, host=cli_args.http_host, port=cli_args.http_port, log_level="info")
