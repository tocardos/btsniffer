#ifndef UDP_REPORT_H
#define UDP_REPORT_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

struct bt_udp_packet {
    uint32_t lap;
    uint8_t  uap;        // 0xFF if unknown
    int8_t   rssi;       // dBm
    uint8_t  channel;    // 0–39
    uint8_t  flags;      // bit0=crc_ok, bit1=known_uap
    uint16_t pdu_len;
    uint8_t  pdu[];
};
int udp_init(const char *dst_ip, uint16_t dst_port);
//void udp_send_lap_uap_pdu(uint32_t lap, uint8_t uap,
//                          const uint8_t *pdu, int pdu_len);
void udp_send_lap_uap_pdu(uint32_t lap,
    uint8_t  uap,       // 0xFF if unknown
    int8_t   rssi,       // dBm
    uint8_t  channel,    // 0–39
    uint8_t  flags,      // bit0=crc_ok, bit1=known_uap
    uint16_t pdu_len,
    const  uint8_t*  pdu
);
void udp_close(void);

#ifdef __cplusplus
}
#endif

#endif
