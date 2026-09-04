#include "udp.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <cstdlib>

static int udp_sock = -1;
static struct sockaddr_in udp_addr;

int udp_init(const char *dst_ip, uint16_t dst_port)
{
    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) {
        perror("socket");
        return -1;
    }

    memset(&udp_addr, 0, sizeof(udp_addr));
    udp_addr.sin_family = AF_INET;
    udp_addr.sin_port   = htons(dst_port);

    if (inet_pton(AF_INET, dst_ip, &udp_addr.sin_addr) != 1) {
        perror("inet_pton");
        close(udp_sock);
        udp_sock = -1;
        return -1;
    }

    return 0;
}

//void udp_send_lap_uap_pdu(uint32_t lap, uint8_t uap,
//                          const uint8_t *pdu, int pdu_len)
void udp_send_lap_uap_pdu(uint32_t lap,
    uint8_t  uap,       // 0xFF if unknown
    int8_t   rssi,       // dBm
    uint8_t  channel,    // 0–39
    uint8_t  flags,      // bit0=crc_ok, bit1=known_uap
    uint16_t pdu_len,
    const uint8_t*  pdu
)
{
    if (udp_sock < 0) return;

    char buffer[1024];
    int offset = 0;
    
    size_t total_len = sizeof(struct bt_udp_packet) + pdu_len;
    struct bt_udp_packet *pkt = (struct bt_udp_packet *)malloc(total_len);
    
    if (!pkt) return;
    pkt->lap = lap;
    pkt->uap = uap;
    pkt->rssi = rssi;
    pkt->channel = channel;
    pkt->flags = flags;
    pkt->pdu_len = pdu_len;
    memcpy(pkt->pdu, pdu, pdu_len);

    offset += snprintf(buffer + offset, sizeof(buffer) - offset,
                       "LAP=0x%06X UAP=0x%02X Lpdu = %d PDU=",
                       lap & 0xFFFFFF, uap, pdu_len);

    for (int i = 0; i < pdu_len && offset < (int)sizeof(buffer) - 3; i++) {
        offset += snprintf(buffer + offset,
                           sizeof(buffer) - offset,
                           "%02X", pdu[i]);
    }
    // Add newline
    if (offset < (int)sizeof(buffer) - 1) {
        buffer[offset++] = '\n';
    }

    //sendto(udp_sock, buffer, offset, 0,
    //       (struct sockaddr *)&udp_addr,
    //       sizeof(udp_addr));
    sendto(udp_sock, pkt, total_len, 0,
           (struct sockaddr *)&udp_addr,
           sizeof(udp_addr));
    free(pkt);
}

void udp_close(void)
{
    if (udp_sock >= 0) {
        close(udp_sock);
        udp_sock = -1;
    }
}
