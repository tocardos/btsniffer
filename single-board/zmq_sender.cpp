/*
 * zmq_sender.cpp
 *
 * See zmq_sender.hpp. Hand-rolled JSON (no external JSON dependency assumed)
 * so this compiles against just libzmq. Swap the message-building section
 * for nlohmann::json or msgpack if you'd rather use a real library --
 * the socket plumbing below doesn't need to change either way.
 */

#include "zmq_sender.hpp"

#include <zmq.h>

#include <sstream>
#include <iomanip>
#include <mutex>
#include <iostream>
#include <cstring>

static void *zctx = nullptr;
static void *zpub = nullptr;
static std::mutex zsend_mtx;

void zmq_sender_init(const char *bind_addr, int port)
{
	zctx = zmq_ctx_new();
	if (!zctx) {
		std::cerr << "zmq_sender: zmq_ctx_new() failed" << std::endl;
		return;
	}

	zpub = zmq_socket(zctx, ZMQ_PUB);
	if (!zpub) {
		std::cerr << "zmq_sender: zmq_socket() failed" << std::endl;
		return;
	}

	std::ostringstream ep;
	ep << "tcp://" << bind_addr << ":" << port;

	if (zmq_bind(zpub, ep.str().c_str()) != 0) {
		std::cerr << "zmq_sender: failed to bind " << ep.str()
		          << ": " << zmq_strerror(zmq_errno()) << std::endl;
		return;
	}

	std::cout << "zmq_sender: PUB socket bound to " << ep.str() << std::endl;
}

void zmq_sender_close(void)
{
	std::lock_guard<std::mutex> lock(zsend_mtx);
	if (zpub) {
		zmq_close(zpub);
		zpub = nullptr;
	}
	if (zctx) {
		zmq_ctx_term(zctx);
		zctx = nullptr;
	}
}

static std::string hex_encode(const uint8_t *data, size_t len)
{
	static const char *hexd = "0123456789abcdef";
	std::string out;
	out.reserve(len * 2);
	for (size_t i = 0; i < len; i++) {
		out.push_back(hexd[(data[i] >> 4) & 0xF]);
		out.push_back(hexd[data[i] & 0xF]);
	}
	return out;
}

void zmq_send_lap_uap_pdu(uint32_t lap, uint8_t uap, int channel, int pkt_flag,
                           int role,
                           double rssi_dbm, double snr_db, double cfo_hz,
                           double cfo_diff_hz,
                           const uint8_t *pdu, size_t pdu_len)
{
	if (!zpub) return;

	std::ostringstream j;
	j << std::fixed << std::setprecision(3);
	j << "{"
	  << "\"lap\":" << lap << ","
	  << "\"uap\":" << (unsigned) uap << ","
	  << "\"channel\":" << channel << ","
	  << "\"pkt_flag\":" << pkt_flag << ","
	  << "\"role\":" << role << ","
	  << "\"rssi_dbm\":" << rssi_dbm << ","
	  << "\"snr_db\":" << snr_db << ","
	  << "\"cfo_hz\":" << cfo_hz << ","
	  << "\"cfo_diff_hz\":" << cfo_diff_hz << ","
	  << "\"pdu_len\":" << pdu_len << ","
	  << "\"pdu_hex\":\"" << (pdu && pdu_len ? hex_encode(pdu, pdu_len) : std::string())
	  << "\""
	  << "}";

	std::string msg = j.str();

	std::lock_guard<std::mutex> lock(zsend_mtx);
	if (!zpub) return; // may have been closed concurrently
	zmq_send(zpub, msg.data(), msg.size(), ZMQ_DONTWAIT);
}
