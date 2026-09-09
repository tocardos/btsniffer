/*
 * zmq_sender.hpp
 *
 * Drop-in replacement for the old udp.h fire-and-forget telemetry link.
 * Uses a ZeroMQ PUB socket (matches the old UDP "just broadcast it" style --
 * no connection handshake, subscribers can come and go). One or more
 * subscribers connect with SUB sockets and (optionally) filter on topic;
 * this publisher uses a single empty topic prefix so a plain
 * zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0) on the subscriber receives
 * everything.
 *
 * Requires libzmq (the C library, not cppzmq) -- link with -lzmq.
 */

#ifndef ZMQ_SENDER_HPP
#define ZMQ_SENDER_HPP

#include <cstdint>
#include <cstddef>

// Bind a PUB socket to tcp://<bind_addr>:<port>. Mirrors the old
// udp_init(ip, port) call site -- call once at startup.
void zmq_sender_init(const char *bind_addr, int port);

// Close the socket/context. Mirrors the old udp_close().
void zmq_sender_close(void);

// Publish one telemetry event as a JSON object.
//   role: 0 = unknown, 1 = master, 2 = slave (see bt_role_t in lapnode.hpp)
//   cfo_hz: CFO estimate for THIS packet's role (0.0 if role is unknown)
//   cfo_diff_hz: running master-slave average CFO delta for this LAP
//                (0.0 until both a master and a slave sample exist)
//   pdu/pdu_len: optional payload (hex-encoded in the JSON); pass
//                pdu=nullptr, pdu_len=0 when there's no payload for this
//                event (e.g. a brute-force-failure notification).
void zmq_send_lap_uap_pdu(uint32_t lap, uint8_t uap, int channel, int pkt_flag,
                           int role,
                           double rssi_dbm, double snr_db, double cfo_hz,
                           double cfo_diff_hz,
                           const uint8_t *pdu, size_t pdu_len);

#endif // ZMQ_SENDER_HPP
