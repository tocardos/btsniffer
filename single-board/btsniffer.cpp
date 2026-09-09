/* 
 * Copyright 2019-2020 Marco Cominelli
 * Copyright 2019-2020 Francesco Gringoli
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <uhd/utils/safe_main.hpp>
#include <uhd/utils/thread.hpp>
#include <uhd/types/tune_request.hpp>
#include <uhd/usrp/multi_usrp.hpp>
#include <boost/program_options.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <iostream>
#include <cstdlib>
#include <pthread.h>
#include <csignal>
#include <stdio.h>
#define _USE_MATH_DEFINES
#include <math.h>
#include <unordered_map>
#include "lapnode.hpp"
#include "zmq_sender.hpp"
#include <cstdint>
#include <map>

namespace po = boost::program_options;

//typedef std::complex<double> iqsamp_t;
typedef std::complex<float> iqsamp_t;  // Changed from double to float

/*** Select number of channels ***/
#define CHAN_40
#include "btsniffer.hpp"

const unsigned int decfactor = (int) raw_srate/srate;

unsigned long total_num_samps = 0;

volatile unsigned int bufselect;
pthread_spinlock_t lock[2];

volatile static bool stopsig = false;
void sigint_handler(int) {stopsig = true;}

typedef struct ProcPars {
	iqsamp_t *bankA;
	iqsamp_t *bankB;
	size_t bufsize;
} proc_pars_t;


// vde added to keep UAPs
struct uap_candidate {
    uint8_t uap;
    uint32_t hits;
};

struct lap_state {
    uint32_t lap;
    std::map<uint8_t, uap_candidate> uaps;
    uint32_t packets_seen;
    bool uap_locked;
    uint8_t locked_uap;

    lap_state(uint32_t l)
        : lap(l),
          packets_seen(0),
          uap_locked(false),
          locked_uap(0) {}
};

std::map<uint32_t, lap_state> lap_db;
static uint16_t bt_crc16(const uint8_t *data, size_t len);

static int bt_payload_len(uint8_t type)
{
    switch (type) {
    case 0x0: return 0;    // NULL
    case 0x1: return 0;    // POLL
    case 0x2: return 18;   // FHS
    case 0x3: return 17;   // DM1
    case 0x4: return 27;   // DH1
    case 0x8: return 121;  // DM3
    case 0x9: return 183;  // DH3
    case 0xA: return 224;  // DM5
    case 0xB: return 339;  // DH5
    case 0xC: return 9;    // DV (Data field is 1 byte header + 9 bytes body)
    // SCO/eSCO Packets (Voice/Data)
    // HV packets carry voice fields (10, 20, 30 bits). 
    // Represented here as 0 as they don't carry ACL data payloads.
    case 0x5: return 0;    // HV1
    case 0x6: return 0;    // HV2
    case 0x7: return 0;    // HV3
    // eSCO packets (Added in Bluetooth v1.2)
    case 0xD: return 30;   // EV3 (Max payload 30 bytes)
    case 0xE: return 120;  // EV4 (Max payload 120 bytes. Note: Collides with AUX1)
    case 0xF: return 180;  // EV5 (Max payload 180 bytes)
    default:  return -1;
    }
}
/*===============================================================================*/
/**
 * Enhanced debugging for PDU extraction
 */
// First, let's verify the header is correctly dewhitened
void debug_header(uint32_t header_dewhitened) {
    uint8_t lt_addr = header_dewhitened & 0x7;        // bits 0-2
    uint8_t type = (header_dewhitened >> 3) & 0xF;    // bits 3-6
    uint8_t flow = (header_dewhitened >> 7) & 0x1;    // bit 7
    uint8_t arqn = (header_dewhitened >> 8) & 0x1;    // bit 8
    uint8_t seqn = (header_dewhitened >> 9) & 0x1;    // bit 9
    uint8_t hec = (header_dewhitened >> 10) & 0xFF;   // bits 10-17
    
    std::cout << boost::format("  Header breakdown:") << std::endl;
    std::cout << boost::format("    LT_ADDR: 0x%X") % (int)lt_addr << std::endl;
    std::cout << boost::format("    TYPE: 0x%X") % (int)type;
    
    const char* type_name = "UNKNOWN";
    switch(type) {
        case 0x0: type_name = "NULL"; break;
        case 0x1: type_name = "POLL"; break;
        case 0x2: type_name = "FHS"; break;
        case 0x3: type_name = "DM1"; break;
        case 0x4: type_name = "DH1"; break;
        case 0x8: type_name = "DM3"; break;
        case 0x9: type_name = "DH3"; break;
        case 0xA: type_name = "DM5"; break;
        case 0xB: type_name = "DH5"; break;
        case 0x5: type_name = "HV1"; break;
        case 0x6: type_name = "HV2"; break;
        case 0x7: type_name = "HV3"; break;
        case 0xC: type_name = "DV"; break;
        case 0xE: type_name = "AUX1"; break; // EV4 is eSCO, AUX1 is ACL
        // Added in Bluetooth v1.2 (eSCO packets)
        case 0xD: type_name = "EV3"; break;
        case 0xF: type_name = "EV5"; break;
    }
    std::cout << " (" << type_name << ")" << std::endl;
    std::cout << boost::format("    FLOW: %d, ARQN: %d, SEQN: %d") % (int)flow % (int)arqn % (int)seqn << std::endl;
    std::cout << boost::format("    HEC: 0x%02X") % (int)hec << std::endl;
}
/**
 * Decodes a Bluetooth (15,10) 2/3 FEC block.
 * Input: 15-bit codeword (LSB first).
 * Output: 10-bit data (LSB first).
 */
static uint16_t decode_2_3_fec(uint16_t codeword) {
    // Bluetooth Hamming (15,10) parameters
    // Data bits are at positions 5-14 (in 0-based indexing logic where 0 is LSB)
    // Parity bits are at positions 0-4
    
    // Calculate syndrome
    // Based on Bluetooth Core Spec Vol 2, Part B, Section 14.2.2
    uint8_t s0 = ((codeword >> 0) & 1) ^ ((codeword >> 5) & 1) ^ ((codeword >> 7) & 1) ^ 
                 ((codeword >> 9) & 1)  ^ ((codeword >> 11) & 1) ^ ((codeword >> 13) & 1);
    uint8_t s1 = ((codeword >> 1) & 1) ^ ((codeword >> 6) & 1) ^ ((codeword >> 7) & 1) ^ 
                 ((codeword >> 10) & 1) ^ ((codeword >> 11) & 1) ^ ((codeword >> 14) & 1);
    uint8_t s2 = ((codeword >> 2) & 1) ^ ((codeword >> 5) & 1) ^ ((codeword >> 6) & 1) ^ 
                 ((codeword >> 9) & 1)  ^ ((codeword >> 12) & 1) ^ ((codeword >> 13) & 1);
    uint8_t s3 = ((codeword >> 3) & 1) ^ ((codeword >> 7) & 1) ^ ((codeword >> 8) & 1) ^ 
                 ((codeword >> 9) & 1)  ^ ((codeword >> 10) & 1) ^ ((codeword >> 11) & 1);
    uint8_t s4 = ((codeword >> 4) & 1) ^ ((codeword >> 5) & 1) ^ ((codeword >> 8) & 1) ^ 
                 ((codeword >> 9) & 1)  ^ ((codeword >> 12) & 1) ^ ((codeword >> 13) & 1);

    uint16_t syndrome = (s4 << 4) | (s3 << 3) | (s2 << 2) | (s1 << 1) | s0;

    // Correct error based on syndrome
    // This mapping corrects single-bit errors in the 15-bit block
    if (syndrome != 0) {
        uint16_t error_mask = 0;
        switch (syndrome) {
            // Parity bits errors
            case 0x01: error_mask = 0x0001; break; // p0
            case 0x02: error_mask = 0x0002; break; // p1
            case 0x04: error_mask = 0x0004; break; // p2
            case 0x08: error_mask = 0x0008; break; // p3
            case 0x10: error_mask = 0x0010; break; // p4
            // Data bits errors
            case 0x17: error_mask = 0x0020; break; // d0 (bit 5)
            case 0x1B: error_mask = 0x0040; break; // d1 (bit 6)
            case 0x07: error_mask = 0x0080; break; // d2 (bit 7)
            case 0x19: error_mask = 0x0100; break; // d3 (bit 8)
            case 0x0F: error_mask = 0x0200; break; // d4 (bit 9)
            case 0x16: error_mask = 0x0400; break; // d5 (bit 10)
            case 0x1A: error_mask = 0x0800; break; // d6 (bit 11)
            case 0x0E: error_mask = 0x1000; break; // d7 (bit 12)
            case 0x1C: error_mask = 0x2000; break; // d8 (bit 13)
            case 0x0D: error_mask = 0x4000; break; // d9 (bit 14)
            default: break; // Uncorrectable or no error
        }
        codeword ^= error_mask;
    }

    // Extract only the 10 data bits (bits 5 to 14) and shift them down
    return (codeword >> 5) & 0x3FF;
}
// CRITICAL FIX: The whitener initialization was WRONG!
// The Bluetooth spec says the whitener is initialized with {clk[1:6], 1} 
// then the UAP is XORed bit-by-bit into the feedback
// CRITICAL FIX: Whitener must be CONTINUOUS from header through payload
// Don't reinitialize - continue from where header whitening left off!
// CRITICAL: We need to reconstruct the LFSR state after header dewhitening
// The header has already been dewhitened by extract_header_bf, so we need to 
// figure out where the LFSR would be after processing those 18 header bits

bool extract_pdu_debug(const uint8_t *binbuf,
                       size_t access_code_start,
                       uint32_t *headers,      // Changed: now array of headers
                       uint32_t *clk_table,    // Added: corresponding CLK values
                       int num_clks,           // Added: number of valid CLK/header pairs
                       uint32_t clk,           // The CLK we want to use
                       uint32_t uap,
                       uint8_t *pdu,
                       size_t *pdu_len,
                       bool verbose = false,
                       size_t binbuf_size = 0,
                       bool allow_null_poll = true)  // Added: whether to accept NULL/POLL as valid
{
    if (!binbuf || !pdu || !pdu_len || !headers || !clk_table) return false;

    // Find the header that corresponds to this CLK value
    uint32_t header = 0;
    bool clk_found = false;
    for (int i = 0; i < num_clks; i++) {
        if (clk_table[i] == clk) {
            header = headers[i];
            clk_found = true;
            break;
        }
    }
    
    if (!clk_found) {
        if (verbose) {
            std::cout << boost::format("  ✗ CLK %d not found in clk_table") % clk << std::endl;
        }
        return false;
    }

    if (verbose) {
        std::cout << boost::format("  Attempting UAP=0x%02X, CLK=%d") % uap % clk << std::endl;
        debug_header(header);
    }

    // Extract packet type from header (bits 3-6)
    uint8_t packet_type = (header >> 3) & 0x0F;
    
    // If we're brute-forcing and don't allow NULL/POLL, reject them
    // (they always "succeed" but prove nothing)
    if (!allow_null_poll && (packet_type == 0x0 || packet_type == 0x1)) {
        if (verbose) {
            std::cout << "  ⊘ Skipping NULL/POLL (can't validate CLK)" << std::endl;
        }
        return false;
    }
    
    // Get expected payload length based on packet type
    int expected_payload_bytes = bt_payload_len(packet_type);
    if (expected_payload_bytes < 0) {
        if (verbose) {
            std::cout << "  ✗ Unknown packet type, skipping" << std::endl;
        }
        return false;
    }
    
    // Check if this packet type uses FEC
    bool has_fec = (packet_type == 0x3 || packet_type == 0x8 || packet_type == 0xA);  // DM1, DM3, DM5
    
    if (verbose) {
        std::cout << boost::format("  Expected payload: %d bytes, FEC: %s") 
            % expected_payload_bytes % (has_fec ? "YES" : "NO") << std::endl;
    }
    
    // For now, skip FEC packets - they need additional decoding
    if (has_fec) {
        if (verbose) {
            std::cout << "  ⚠ FEC packets not yet supported, skipping" << std::endl;
        }
        return false;
    }
    
    // For packets with payload, add 2 bytes for CRC
    int total_bytes = expected_payload_bytes;
    bool has_crc = (packet_type != 0x0 && packet_type != 0x1);
    if (has_crc) {
        total_bytes += 2; // Add CRC bytes
    }

    // Start of payload (after access code + 18-bit header)
    size_t payload_start = access_code_start + 90 * srate;
    
    // BOUNDS CHECK
    size_t payload_end = payload_start + (total_bytes * 8 * srate);
    if (binbuf_size > 0 && payload_end > binbuf_size) {
        if (verbose) {
            std::cout << boost::format("  ✗ BOUNDS ERROR: payload_end=%d > binbuf_size=%d") 
                % payload_end % binbuf_size << std::endl;
        }
        return false;
    }

    // Initialize whitener with CLK and UAP
    uint8_t lfsr = ((clk & 0x3F) | 0x40);
    
    // Clock in UAP bits  
    for (int i = 0; i < 8; i++) {
        uint8_t uap_bit = (uap >> i) & 0x1;
        uint8_t lfsr_msb = (lfsr >> 6) & 0x1;
        uint8_t feedback = lfsr_msb ^ uap_bit;
        lfsr = (lfsr << 1) & 0x7F;
        if (feedback) {
            lfsr ^= 0x11;
        }
    }
    
    if (verbose) {
        std::cout << boost::format("  Initial LFSR (after UAP): 0x%02X") % (int)lfsr << std::endl;
    }
    
    // Clock through the 18 header bits to get to payload state
    for (int i = 0; i < 18; i++) {
        uint8_t lfsr_msb = (lfsr >> 6) & 0x1;
        lfsr = (lfsr << 1) & 0x7F;
        if (lfsr_msb) {
            lfsr ^= 0x11;
        }
    }
    
    if (verbose) {
        std::cout << boost::format("  Whitener state at payload start: 0x%02X") % (int)lfsr << std::endl;
    }

    // Dewhiten the payload
    *pdu_len = 0;
    for (int byte_idx = 0; byte_idx < total_bytes; byte_idx++) {
        uint8_t byte_acc = 0;
        for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
            size_t buf_idx = payload_start + (byte_idx * 8 + bit_idx) * srate;
            
            if (binbuf_size > 0 && buf_idx >= binbuf_size) {
                if (verbose) {
                    std::cout << boost::format("  ✗ BOUNDS ERROR at byte %d, bit %d") 
                        % byte_idx % bit_idx << std::endl;
                }
                return false;
            }
            
            uint8_t bit = binbuf[buf_idx];

            // Generate whitening bit
            uint8_t wbit = (lfsr >> 6) & 0x1;
            
            // Clock the LFSR
            lfsr = (lfsr << 1) & 0x7F;
            if (wbit) {
                lfsr ^= 0x11;
            }

            // Dewhiten
            bit ^= wbit;
            byte_acc |= (bit << bit_idx);
        }
        pdu[*pdu_len] = byte_acc;
        (*pdu_len)++;
    }

    if (verbose) {
        std::cout << "  Dewhitened PDU (first " << std::min(*pdu_len, (size_t)16) << " bytes): ";
        for (size_t i = 0; i < std::min(*pdu_len, (size_t)16); i++) {
            std::cout << boost::format("%02X ") % (int)pdu[i];
        }
        std::cout << std::endl;
    }

    // Validate CRC if packet has payload
    if (!has_crc) {
        *pdu_len = 0;
        if (verbose) std::cout << "  ✓ NULL/POLL packet (no CRC)" << std::endl;
        return true;
    }

    if (*pdu_len < 2) {
        if (verbose) std::cout << "  ✗ PDU too short for CRC" << std::endl;
        return false;
    }
    
    // Last 2 bytes are CRC
    uint16_t received_crc = (pdu[*pdu_len - 2] << 8) | pdu[*pdu_len - 1];
    size_t payload_len = *pdu_len - 2;

    // Calculate CRC over payload (without CRC bytes)
    uint16_t calc_crc = bt_crc16(pdu, payload_len);
    
    if (verbose) {
        std::cout << boost::format("  Received CRC: 0x%04X") % received_crc << std::endl;
        std::cout << boost::format("  Calculated CRC: 0x%04X") % calc_crc << std::endl;
        
        // Show how close we are
        uint16_t xor_val = received_crc ^ calc_crc;
        int bit_errors = __builtin_popcount(xor_val);
        std::cout << boost::format("  CRC bit errors: %d") % bit_errors << std::endl;
    }
    
    // Remove CRC from pdu_len
    *pdu_len = payload_len;

    bool valid = (calc_crc == received_crc);
    if (verbose) {
        if (valid) {
            std::cout << "  ✓ CRC VALID!" << std::endl;
        } else {
            std::cout << "  ✗ CRC mismatch" << std::endl;
        }
    }
    
    return valid;
}

// Updated test_uap_combinations to pass headers array
bool test_uap_combinations(const uint8_t *binbuf,
                           size_t access_code_start,
                           uint32_t *headers,      // Changed: array of headers
                           uint32_t *clk_table,    // Added: CLK values from extract_header_bf
                           int num_clks,           // Added: number of valid entries
                           lap_node &node,
                           uint8_t *pdu,
                           size_t *pdu_len,
                           size_t binbuf_size = 0)
{
    std::cout << "=== Testing all UAP/CLK combinations ===" << std::endl;
    std::cout << boost::format("  two_uap_stage=%d, uap_locked=%d, num_clks=%d") 
        % node.two_uap_stage % node.uap_locked % num_clks << std::endl;
    
    for (int uap_idx = 0; uap_idx < 2; uap_idx++) {
        uint8_t uap = node.uap_pair[uap_idx];
        uint32_t clk0, clk1;
        int clock_index;
        
        if (node.get_uap_data_by_uap(uap, &clk0, &clk1, &clock_index)) {
            uint32_t clks[2] = {clk0, clk1};
            
            for (int clk_idx = 0; clk_idx < 2; clk_idx++) {
                std::cout << boost::format("\n--- Testing UAP 0x%02X with CLK %d ---") 
                    % (int)uap % clks[clk_idx] << std::endl;
                
                // Allow NULL/POLL when initially locking UAP
                if (extract_pdu_debug(binbuf, access_code_start, headers, clk_table, num_clks,
                                     clks[clk_idx], uap, pdu, pdu_len, true, binbuf_size, true)) {
                    std::cout << "*** SUCCESS! UAP locked to 0x" << std::hex 
                             << (int)uap << std::dec << " ***" << std::endl;
                    node.uap_locked = true;
                    node.locked_uap = uap;
                    node.last_clk = clks[clk_idx];  // Save the CLK!
                    node.clk_idx = clk_idx;         // Save which CLK index worked
                    
                    // DEBUG: Verify the lock
                    std::cout << boost::format("  DEBUG: Set node.uap_locked=%d, locked_uap=0x%02X, last_clk=%d") 
                        % node.uap_locked % (int)node.locked_uap % node.last_clk << std::endl;
                    
                    return true;  // Return true on success
                }
            }
        }
    }
    
    std::cout << "\n=== No valid UAP/CLK combination found ===" << std::endl;
    return false;  // Return false on failure
}

// Helper function to dewhiten header for a specific CLK (without HEC validation)
uint32_t dewhiten_header_with_clk(const uint8_t *header_bits, uint32_t clk) {
    uint32_t header = 0;
    
    // Extract the 18 header bits (already FEC decoded in extract_header_bf)
    for (int i = 0; i < 18; i++) {
        int s0 = 0, s1 = 0;
        for (int j = 0; j < 3; j++) {
            if (header_bits[(i*3 + j) * srate]) s1++;
            else s0++;
        }
        header >>= 1;
        if (s1 > s0) header |= 0x20000;
    }
    
    // Dewhiten with this CLK
    uint32_t whitener = (clk & 0x3f) | 0x40;
    uint32_t header_dewhiten = header;
    
    for (int i = 0; i < 18; i++) {
        uint32_t whitener_out = (whitener >> 6) & 0x1;
        uint32_t whitener_shifted = (whitener << 1) & 0x7f;
        whitener = whitener_shifted ^ (whitener_out | (whitener_out << 4));
        header_dewhiten = header_dewhiten ^ (whitener_out << i);
    }
    
    return header_dewhiten;
}

// Updated extract_pdu_with_locked_uap to use headers array
bool extract_pdu_with_locked_uap(const uint8_t *binbuf,
                                  size_t access_code_start,
                                  uint32_t *headers,      // Changed: array
                                  uint32_t *clk_table,    // Added: CLK values
                                  int num_clks,           // Added: number of valid entries
                                  lap_node &node,
                                  long long current_time_us,
                                  uint8_t *pdu,
                                  size_t *pdu_len,
                                  size_t binbuf_size = 0)
{
    if (!node.uap_locked) return false;
    
    // Calculate time difference from last packet
    long long time_diff = current_time_us - node.get_ts();
    
    // Each Bluetooth slot is 625 microseconds
    int slots_elapsed = (int)round(time_diff / 625.0);
    
    // Predict CLK: advance by slots_elapsed (mod 64 for 6-bit CLK)
    uint32_t predicted_clk = (node.last_clk + slots_elapsed) % 64;
    
    std::cout << boost::format("UAP locked (0x%02X), predicting CLK %d (last=%d, +%d slots)") 
        % (int)node.locked_uap % predicted_clk % node.last_clk % slots_elapsed << std::endl;
    
    // Try the predicted CLK (allow NULL/POLL here since it's the prediction)
    if (extract_pdu_debug(binbuf, access_code_start, headers, clk_table, num_clks,
                         predicted_clk, node.locked_uap, pdu, pdu_len, false, binbuf_size, true)) {
        node.last_clk = predicted_clk;  // Update for next prediction
        node.set_ts(current_time_us);
        std::cout << "  ✓ PDU extracted successfully" << std::endl;
        return true;
    }
    
    // If prediction failed, try nearby CLK values (±5 slots for drift/jitter)
    // Increased from ±2 to ±5 to handle more clock drift
    //for (int offset = -5; offset <= 5; offset++) {
    // vde check odd
    for (int offset = -4; offset <= 4; offset += 2) {
        if (offset == 0) continue;  // Already tried
        uint32_t clk_try = (predicted_clk + offset + 64) % 64;
        
        // Allow NULL/POLL in the nearby range since we're close to prediction
        if (extract_pdu_debug(binbuf, access_code_start, headers, clk_table, num_clks,
                             clk_try, node.locked_uap, pdu, pdu_len, false, binbuf_size, true)) {
            node.last_clk = clk_try;
            node.set_ts(current_time_us);
            std::cout << boost::format("  ✓ PDU extracted with CLK %d (offset %+d)") 
                % clk_try % offset << std::endl;
            return true;
        }
    }
    
    // Fallback: brute force all 64 CLK values if prediction completely failed
    // We need to generate headers for ALL CLK values, not just the ones that passed HEC
    std::cout << "  ⚠ Prediction failed, generating headers for all 64 CLK values..." << std::endl;
    
    uint32_t all_headers[64];
    uint32_t all_clks[64];
    
    // Generate dewhitened header for each possible CLK value
    const uint8_t *header_start = binbuf + access_code_start + 72 * srate;
    for (uint32_t clk = 0; clk < 64; clk++) {
        all_clks[clk] = clk;
        all_headers[clk] = dewhiten_header_with_clk(header_start, clk);
    }
    
    // Now try all CLK values with their corresponding headers
    // CRITICAL: Don't accept NULL/POLL when brute forcing - they always succeed!
    int attempts = 0;
    for (uint32_t clk = 0; clk < 64; clk++) {
        if (abs((int)clk - (int)predicted_clk) <= 5) continue;  // Already tried these
        
        // Reject NULL/POLL packets when brute-forcing (allow_null_poll = false)
        if (extract_pdu_debug(binbuf, access_code_start, all_headers, all_clks, 64,
                             clk, node.locked_uap, pdu, pdu_len, false, binbuf_size, false)) {
            node.last_clk = clk;
            node.set_ts(current_time_us);
            std::cout << boost::format("  ✓ PDU extracted with CLK %d (brute forced, diff=%d)") 
                % clk % (int)(clk - predicted_clk) << std::endl;
            return true;
        }
        attempts++;
    }
    
    std::cout << boost::format("  ✗ Could not extract PDU with any CLK value (%d attempts, no valid payload found)") 
        % attempts << std::endl;
    return false;
}
/*===============================================================================*/
int extract_pdu(uint8_t *binbuf,
                size_t access_code_start,
                uint32_t header,
                uint32_t clk,
                uint8_t *pdu,
                size_t *pdu_len);

static inline uint8_t bt_whiten_bit(uint8_t *lfsr)
{
    uint8_t out = (*lfsr >> 6) & 1;
    *lfsr = ((*lfsr << 1) & 0x7f) ^ (out | (out << 4));
    return out;
}

// Bluetooth CRC-16 polynomial: x^16 + x^12 + x^5 + 1 (0x11021)
static uint16_t bt_crc16(const uint8_t *data, size_t len)
{
    uint16_t crc = 0xFFFF; // initial value
    for (size_t i = 0; i < len; i++) {
        crc ^= (uint16_t)data[i] << 8;
        for (int j = 0; j < 8; j++) {
            if (crc & 0x8000)
                crc = (crc << 1) ^ 0x11021;
            else
                crc <<= 1;
        }
    }
    return crc & 0xFFFF;
}

/**
 * Extract PDU from binbuffer
 * Returns true if payload is successfully dewhitened and CRC is valid
 */
bool extract_pdu(const uint8_t *binbuf,
                 size_t access_code_start,
                 uint32_t header,
                 uint32_t clk,
                 uint32_t uap,
                 uint8_t *pdu,
                 size_t *pdu_len)
{
    if (!binbuf || !pdu || !pdu_len) return false;

    // Extract packet type from header (bits 3-6)
    uint8_t packet_type = (header >> 3) & 0x0F;
    
    // Get expected payload length based on packet type
    int expected_payload_bytes = bt_payload_len(packet_type);
    if (expected_payload_bytes < 0) {
        // Unknown packet type
        return false;
    }
    
    // For packets with CRC, add 2 bytes
    int total_bytes = expected_payload_bytes;
    if (packet_type != 0x0 && packet_type != 0x1) {
        total_bytes += 2; // Add CRC bytes
    }

    // Start of payload (after access code + 18-bit header)
    // Access code is 72 bits, header is 18 bits = 90 bits total
    size_t payload_start = access_code_start + 90 * srate;

    // Initialize whitener with CLK[1:6] and UAP
    // The whitener is a 7-bit LFSR initialized as: {CLK[1:6], 1}
    // Then clocked 8 times with UAP bits
    uint8_t lfsr = ((clk & 0x3F) | 0x40); // bits [6:0] = {CLK[1:6], 1}
    
    // Clock in UAP bits (8 bits)
    for (int i = 0; i < 8; i++) {
        uint8_t uap_bit = (uap >> i) & 0x1;
        uint8_t feedback = ((lfsr >> 6) & 0x1) ^ uap_bit;
        lfsr = ((lfsr << 1) & 0x7F) | feedback;
        // XOR with bit 4 as well
        if (feedback) {
            lfsr ^= 0x10; // bit 4
        }
    }

    // Now dewhiten the payload
    *pdu_len = 0;
    uint8_t byte_acc = 0;
    int bit_pos = 0;

    for (int byte_idx = 0; byte_idx < total_bytes; byte_idx++) {
        for (int bit_idx = 0; bit_idx < 8; bit_idx++) {
            size_t buf_idx = payload_start + (byte_idx * 8 + bit_idx) * srate;
            uint8_t bit = binbuf[buf_idx];

            // Generate whitening bit
            uint8_t wbit = (lfsr >> 6) & 0x1;
            uint8_t feedback = wbit;
            lfsr = ((lfsr << 1) & 0x7F) | feedback;
            if (feedback) {
                lfsr ^= 0x10; // XOR with bit 4
            }

            // Dewhiten
            bit ^= wbit;
            byte_acc |= (bit << bit_pos);
            bit_pos++;

            if (bit_pos == 8) {
                pdu[*pdu_len] = byte_acc;
                (*pdu_len)++;
                bit_pos = 0;
                byte_acc = 0;
            }
        }
    }

    // Validate CRC if packet has payload
    if (packet_type == 0x0 || packet_type == 0x1) {
        // NULL or POLL packets have no payload or CRC
        *pdu_len = 0;
        return true;
    }

    if (*pdu_len < 2) return false; // Need at least CRC
    
    // Last 2 bytes are CRC
    uint16_t received_crc = (pdu[*pdu_len - 2] << 8) | pdu[*pdu_len - 1];
    size_t payload_len = *pdu_len - 2;

    // Calculate CRC over payload (without CRC bytes)
    uint16_t calc_crc = bt_crc16(pdu, payload_len);
    
    // Remove CRC from pdu_len
    *pdu_len = payload_len;

    return (calc_crc == received_crc);
}
// Calculate signal power from IQ samples
double calculate_signal_power(const iqsamp_t *samples, size_t start, size_t length, size_t stride = 1) {
    double power = 0.0;
    size_t count = 0;
    
    for (size_t i = 0; i < length; i++) {
        size_t idx = start + i * stride;
        float real = samples[idx].real();
        float imag = samples[idx].imag();
        power += (real * real) + (imag * imag);
        count++;
    }
    
    return (count > 0) ? (power / count) : 0.0;
}

// Calculate RSSI in dBm (approximate, needs calibration)
double calculate_rssi_dbm(double power) {
    if (power <= 0) return -120.0;
    return 10.0 * log10(power);
}

// Estimate SNR by comparing packet energy to surrounding noise
double estimate_snr(const iqsamp_t *samples, size_t packet_start, size_t packet_len, 
                    size_t noise_offset, size_t stride = 1) {
    // Calculate signal power during packet
    double signal_power = calculate_signal_power(samples, packet_start, packet_len, stride);
    
    // Calculate noise power before/after packet (at noise_offset samples away)
    size_t noise_len = packet_len / 4; // Use 1/4 of packet length for noise sample
    double noise_power = 0.0;
    
    if (packet_start >= noise_offset + noise_len) {
        // Sample noise before packet
        noise_power = calculate_signal_power(samples, packet_start - noise_offset - noise_len, 
                                            noise_len, stride);
    } else {
        // Sample noise after packet
        noise_power = calculate_signal_power(samples, packet_start + packet_len + noise_offset,
                                            noise_len, stride);
    }
    
    if (noise_power <= 0) return 0.0;
    
    // SNR in dB
    return 10.0 * log10(signal_power / noise_power);
}

// Structure to hold signal metrics
struct signal_metrics {
    double rssi_dbm;
    double snr_db;
    double signal_power;
    
    signal_metrics() : rssi_dbm(-120.0), snr_db(0.0), signal_power(0.0) {}
};

// Calculate signal metrics for a detected packet
signal_metrics calculate_packet_metrics(const iqsamp_t *chan_samples, size_t bufsize,
                                        size_t packet_start, size_t srate) {
    signal_metrics metrics;
    
    // Packet spans access code (72 bits) + header (18 bits) = 90 bits minimum
    size_t packet_samples = 90 * srate;
    
    // Make sure we don't go out of bounds
    if (packet_start + packet_samples > bufsize) {
        packet_samples = bufsize - packet_start;
    }
    
    // Calculate signal power over the packet
    metrics.signal_power = calculate_signal_power(chan_samples, packet_start, 
                                                  packet_samples, srate);
    
    // Convert to RSSI (dBm)
    metrics.rssi_dbm = calculate_rssi_dbm(metrics.signal_power);
    
    // Estimate SNR (compare to noise 1000 samples before packet)
    metrics.snr_db = estimate_snr(chan_samples, packet_start, packet_samples, 
                                  1000, srate);
    
    return metrics;
}



// ---------------------------------------------------------------------------
// NEW: CFO estimation via phase-derivative over the known access code +
// sync word.
//
// The access word is fully known once a LAP has been confirmed via the CRC
// check a few lines up (aw == awfinal), because awfinal deterministically
// encodes the 64-bit sync word (the same value used to build `aw` from
// barker/lap/code). For each of the known bits we know the expected GFSK
// frequency-deviation sign (+1 for bit=1, -1 for bit=0). Averaging
// (measured instantaneous frequency - expected deviation) over every
// bit-slot in the access word cancels the data modulation and leaves the
// CFO.
//
// TODO: CFO_SAMPLE_RATE_HZ must equal the *decimated channel* sample rate
// in Hz (raw_srate / decfactor, from btsniffer.hpp -- which wasn't part of
// what you gave me). Please confirm/adjust; everything else here is
// independent of that constant.
// ---------------------------------------------------------------------------
#ifndef CFO_SAMPLE_RATE_HZ
#define CFO_SAMPLE_RATE_HZ (2000000.0)  // TODO: set to raw_srate/decfactor
#endif

// Nominal GFSK peak frequency deviation for BR (spec calls for >=140 kHz
// @ 1 Msym/s). Only used to weight known-bit polarity for the subtraction
// below; a value that's somewhat off mainly costs precision, not bias,
// since it's applied symmetrically to +1/-1 bits. Adjust if you have a
// calibrated figure for your radio/front-end.
#ifndef GFSK_DEVIATION_HZ
#define GFSK_DEVIATION_HZ (160000.0)
#endif

// Rebuild the known access-code bits (4 preamble + 64 sync word = 68 bit
// slots) from the verified access word, in the same bit-slot ordering
// btsniffer.cpp already uses to assemble `aw` from barker/lap/code above.
static void ac_known_bits(uint64_t awfinal, uint32_t lap, uint8_t known_bits[68])
{
    // Preamble (bit-slots 0-3): alternating 1010/0101, polarity set by the
    // LAP's LSB -- mirrors the barker_true selection used above.
    bool lap_lsb = (lap & 0x1) != 0;
    known_bits[0] = lap_lsb ? 0 : 1;
    known_bits[1] = lap_lsb ? 1 : 0;
    known_bits[2] = lap_lsb ? 0 : 1;
    known_bits[3] = lap_lsb ? 1 : 0;

    // Sync word (bit-slots 4-67): awfinal bit i (0 = LSB) maps to bit-slot
    // (67 - i), i.e. MSB-first, matching how `aw` was assembled from
    // (barker << 58) | (lap << 34) | code.
    for (int i = 0; i < 64; i++) {
        known_bits[4 + i] = (uint8_t) ((awfinal >> (63 - i)) & 0x1);
    }
}

// Phase-derivative CFO estimate (Hz) over the access-code span starting at
// sample offset `access_code_start` in `chan`.
double estimate_cfo_hz(const iqsamp_t *chan, size_t access_code_start,
                        uint64_t awfinal, uint32_t lap)
{
    uint8_t known_bits[68];
    ac_known_bits(awfinal, lap, known_bits);

    double sum_freq_hz = 0.0;
    int count = 0;

    for (int b = 0; b < 68; b++) {
        size_t center = access_code_start + (size_t)(b * srate);
        if (center < 1) continue;

        iqsamp_t x0 = chan[center - 1];
        iqsamp_t x1 = chan[center];

        // Instantaneous phase step between consecutive samples, via
        // atan2 of the complex product x1 * conj(x0).
        double re = (double) x1.real() * x0.real() + (double) x1.imag() * x0.imag();
        double im = (double) x1.imag() * x0.real() - (double) x1.real() * x0.imag();
        double inst_phase = std::atan2(im, re);

        double inst_freq_hz = inst_phase * CFO_SAMPLE_RATE_HZ / (2.0 * M_PI);
        double expected_dev_hz = (known_bits[b] ? +1.0 : -1.0) * GFSK_DEVIATION_HZ;

        sum_freq_hz += (inst_freq_hz - expected_dev_hz);
        count++;
    }

    return (count > 0) ? (sum_freq_hz / count) : 0.0;
}

// NEW: telemetry wrapper. Pulls the current role/RSSI/SNR/CFO for `lap`
// out of lap_map (populated a few lines up in the main loop via
// lap_node::role_from_clk() + update_signal_metrics()) and publishes over
// ZMQ. Signature-compatible drop-in for the old udp_send_lap_uap_pdu()
// call sites (minus the rssi argument, which is now looked up internally).

std::unordered_map<uint32_t, lap_node> lap_map;


static void send_telemetry(uint32_t lap, uint8_t uap, int ch, int pkt_flag,
                            size_t pdu_len, const uint8_t *pdu)
{
    auto it = lap_map.find(lap);
    if (it == lap_map.end()) {
        zmq_send_lap_uap_pdu(lap, uap, ch, pkt_flag, ROLE_UNKNOWN,
                              -120.0, 0.0, 0.0, 0.0, pdu, pdu_len);
        return;
    }

    lap_node &node = it->second;
    bt_role_t role = node.last_role;
    double rssi = node.last_rssi;
    double snr  = node.last_snr;
    double cfo  = (role == ROLE_MASTER) ? node.last_cfo_master :
                  (role == ROLE_SLAVE)  ? node.last_cfo_slave  : 0.0;

    zmq_send_lap_uap_pdu(lap, uap, ch, pkt_flag, (int) role,
                          rssi, snr, cfo, node.cfo_diff_hz, pdu, pdu_len);
}

/*=========================================================================*/
inline bool is_valid_preamble(uint8_t *binbuf, unsigned int k)
{
	uint8_t preamble1 = 0, preamble2 = 0;
	preamble1 = binbuf[k] + binbuf[k+2*srate];
	preamble2 = binbuf[k+1*srate] + binbuf[k+3*srate];
	
	if ((preamble1 == 2 && preamble2 == 0) or (preamble1 == 0 && preamble2 == 2))
		return true;
	else
		return false;
}

inline uint8_t extract_byte(uint8_t *binbuf, unsigned int start)
{
	uint8_t result = 0x00;
	for (int b = 0; b < 8; b++) result |= binbuf[start + b*srate] << b;
	return result;
}

struct timeval t_start;

void compute_tsdiff(struct timeval *x, struct timeval *y, struct timeval *diff)
{
	// x must be bigger than y
	int x_usec = x->tv_usec;
	int x_sec = x->tv_sec;
	int y_usec = y->tv_usec;
	int y_sec = y->tv_sec;
	
	if (x_usec < y_usec) {
		x_sec --;
		x_usec += 1000000;
	}
	
	diff->tv_sec = x_sec - y_sec;
	diff->tv_usec = x_usec - y_usec;
	
  return;
}


int _length (uint64_t word, int left, int right)
{
  if (left == right)
    return left;
  
  int mid = (left + right) / 2;
  if (right == mid)
    return mid;
  
  if (word >= (1LLU << mid))
    return (_length (word, left, mid));
  
  return (_length (word, mid, right));
}


int length (uint64_t word)
{
  return (_length (word, 64, 0));
}


uint64_t compute_remainder (uint64_t input, uint64_t divisor)
{
  int divisor_length = length (divisor);
  int input_length = length (input);
  
  if (divisor_length + input_length > 63)
    return input;
  
  input = input << divisor_length;
  
  while (length (input) >= divisor_length) {
    uint64_t tmp = divisor << (length (input) - divisor_length);
    input = input ^ tmp;
  }
  
  return input;
}

/* Extracts the header if FEC is perfect, then it tries (bruteforce)
 * all possible clk values to dewhiten the header and finally
 * tries all possible UAP until a valid HEC code is found.
 */

//int extract_header_bf (uint8_t *buf, uint32_t* head, uint32_t *clks, int *clk_found, uint32_t uap)
int extract_header_bf( uint8_t *buf, uint32_t *headers, uint32_t *clks, int *clk_found, uint32_t uap)
{
  int perfect_rx = 0;
  uint32_t header = 0;
  
  for (int i = 0; i < 54; i += 3) {
    int s0 = 0, s1 = 0;
    for (int j = 0; j < 3; j++) {
      if ( *(buf+(i+j)*srate) ) s1++;
      else s0++;
    }
    header >>= 1;
    if (s1 == 0 || s0 == 0) perfect_rx++;
    if (s1 > s0) header |= 0x20000;
  }
  // vde modified 
  //*head = header;
  // vde modified to decode only perfect headers
  if (perfect_rx != 18) return -1;
  //if (perfect_rx < 16) return -1;

  // first bit received is the LSB, so header is composed by
  //     +-----+---------+
  // MSB | HEC | LT_ADDR | LSB
  //     +-----+---------+
  //      8 bit   10 bit

  // Brute force the clock
  *clk_found = 0;
  for (uint32_t clk = 0; clk < 64; clk++) {
    uint32_t header_dewhiten = header;
    uint32_t whitener;
    whitener = (clk & 0x3f) | 0x40;

    // Dewhiten header using this clk value
    for (int i = 0; i < 18; i++) {
      uint32_t whitener_out = (whitener >> 6) & 0x1;
      uint32_t whitener_shifted = (whitener << 1) & 0x7f;
      whitener = whitener_shifted ^ (whitener_out | (whitener_out << 4));
      header_dewhiten = header_dewhiten ^ (whitener_out << i);
    }
    /*
    // vde added fro debug
    uint8_t packet_type = (header_dewhiten >> 3) & 0xF;
    int slots_used = 1;  // Default

    switch(packet_type) {
      case 0x8: case 0x9: case 0xC: case 0xE:  // DM3, DH3, DV, AUX1
        slots_used = 3;
        std::cout << boost::format("Delta in slots: %d ") % slots_used << std::endl;
        break;
      case 0xA: case 0xB:  // DM5, DH5
        slots_used = 5;
        std::cout << boost::format("Delta in slots: %d ") % slots_used << std::endl;
        break;
      default:
        slots_used = 1;
        std::cout << boost::format("Delta in slots: %d ") % slots_used << std::endl;
    }
        */
    //
    // Re-compute the HEC over the dewhitened header
    uint32_t lfsr = uap;
    for (int i = 0; i < 10; i++) {
      uint32_t lfsr_out = (lfsr >> 7) & 0x1;
      uint32_t data_in = (header_dewhiten >> i) & 0x1;
      uint32_t lfsr_in = (lfsr_out ^ data_in);
      uint32_t lfsr_adder =
	(lfsr_in << 7) |
	(lfsr_in << 5) |
	(lfsr_in << 2) |
	(lfsr_in << 1) |
	(lfsr_in << 0);
      lfsr = (lfsr << 1) & 0xff;
      lfsr = lfsr ^ lfsr_adder;
    }

    // Compare HEC computed and HEC received.
    // First bit received is in header_dewhiten[10], last is in header_dewhiten[17].
    // First bit to be transmitted is in position 7, last one is in position 0.
    int kk = 0;
    while (kk < 8) {
      uint32_t bit_rx = (header_dewhiten >> (10 + kk)) & 0x1;
      uint32_t bit_tx = (lfsr >> (7 - kk)) & 0x1;
      if (bit_rx != bit_tx) break;
      kk++;
    }

    if (kk == 8) {
      clks[*clk_found] = clk;
      headers[*clk_found] = header_dewhiten;  // Store matching header
      //if (*clk_found == 0) {  // Only store first match
      //  *head = header_dewhiten;
    //}
      
      (*clk_found)++;
    }
  }

  return *clk_found;
}


void* proc_routine(void *routine_params)
{
  // Set priority on current thread
  uhd::set_thread_priority_safe(1, true);
  
  // Read parameters.
  proc_pars_t *pars = (proc_pars_t *) routine_params;
  size_t bufsize = pars->bufsize;
  iqsamp_t *bankA = pars->bankA;
  iqsamp_t *bankB = pars->bankB;

  iqsamp_t *curbuf;
  size_t samples_processed = 0;
  FILE *fptrout = fopen("results.txt","w");
  int local_bufselect;

  // Allocate buffers and auxiliary pointers.
  uint8_t *binbuffer = (uint8_t*) malloc(decfactor * bufsize * sizeof(uint8_t));
  iqsamp_t *sigbuf = (iqsamp_t*) malloc(decfactor * bufsize * sizeof(iqsamp_t));
  iqsamp_t *chanbuf = (iqsamp_t*) malloc(decfactor * bufsize * sizeof(iqsamp_t));

  // Make filter polyphase!
  //std::vector<std::vector<double>> poly(decfactor);
  std::vector<std::vector<float>> poly(decfactor);
  size_t components_length = (size_t) ceil(FILTER_TAP_NUM/decfactor);
  for (size_t i = 0; i < components_length * decfactor; i++) {
	  if (i < FILTER_TAP_NUM) poly[i%decfactor].push_back(filter_taps[i]);
	  else poly[i%decfactor].push_back(0.0);
  }
  
  // Create twiddle matrix for float processing.
  std::complex<float> i_unit(0.0f, 1.0f);
  std::vector<std::vector<iqsamp_t>> twiddle(decfactor);

  for (size_t row = 0; row < decfactor; row++) {
      for (size_t col = 0; col < decfactor; col++) {
          float phase = -2.0f * M_PI * (float)(col * row) / (float)decfactor;
          twiddle[row].push_back(std::polar(1.0f, phase));
      }
  }


  while(stopsig == false) {
    local_bufselect = bufselect;
    
    if (local_bufselect) {
      curbuf = bankB;
      pthread_spin_lock(&lock[1]);
    } else {
      curbuf = bankA;
      pthread_spin_lock(&lock[0]);
    }

    // Filtering
    //memset(sigbuf, 0, decfactor * bufsize * sizeof(iqsamp_t));
    std::fill_n(sigbuf, decfactor * bufsize, iqsamp_t(0.0f, 0.0f));
	// moving unlock here to allow overlapping of filtering and data acquisition
	if (local_bufselect) pthread_spin_unlock(&lock[1]);
    else pthread_spin_unlock(&lock[0]); 

	
	// adapted for float processing
	for (size_t i = FILTER_TAP_NUM-1; i < bufsize*decfactor; i += decfactor) {
	    for (size_t k = 0; k < components_length; k++) {
		    size_t idx = i/decfactor;
		    sigbuf[idx-1] += (curbuf[i-k*decfactor+0] * poly[0].at(k))/((float) components_length);
		    for (size_t ch = 1; ch < decfactor; ch++) {
			    sigbuf[ch*bufsize + idx] += (curbuf[i-k*decfactor+(decfactor-ch)] * poly[ch].at(k)/((float) components_length-1));
		    }
	    }
    }

    const size_t blocksize = 1000; // us
    const size_t blocknsamps = blocksize*srate; // samples per block
    const size_t nblocks = floor(bufsize/blocknsamps);

    // Channelize using DFT.
    //memset(chanbuf, 0, decfactor * bufsize * sizeof(iqsamp_t));
    std::fill_n(chanbuf, decfactor * bufsize, iqsamp_t(0.0f, 0.0f));
    for (unsigned int i = 0; i < bufsize; i++) {
	    for (size_t ch = 0; ch < decfactor; ch++)
		    for (size_t k = 0; k < decfactor; k++)
			    chanbuf[ch*bufsize + i] += sigbuf[k*bufsize+i] * twiddle[ch].at(k);
    }
    
    for (unsigned int ch=0; ch<decfactor; ch++) {
	    iqsamp_t *chan = chanbuf + ch * bufsize;
	    uint8_t *tmpbinbuf = binbuffer + ch * bufsize;

	    // Discriminate bits without using atan2.
	    for (size_t i = 1; i < bufsize; i++) {
		    double tmp = chan[i-1].real() * chan[i].imag() - chan[i-1].imag() * chan[i].real();
		    tmpbinbuf[i] = (tmp > 0) ? 1 : 0;
	    }
    }

    for (size_t block = 1; block < nblocks-1; block++) {
	    for (unsigned int ch = 0; ch < decfactor; ch++) {
		    iqsamp_t *chan = chanbuf + ch * bufsize;
		    uint8_t *binbuf = binbuffer + ch * bufsize;

		    for (unsigned int i = block*blocknsamps; i < (block+1)*blocknsamps; i++) {
			    if (is_valid_preamble(binbuf, i) == false) continue;

			    uint64_t barker = extract_byte(binbuf, i + 62*srate);
			    barker = barker & 0x3f;
			    if (barker != 0x13 && barker != 0x2c) continue;
	    
			    uint64_t lap =
				    (uint64_t) extract_byte(binbuf, i + 54*srate) << 16 |
				    (uint64_t) extract_byte(binbuf, i + 46*srate) << 8  |
				    (uint64_t) extract_byte(binbuf, i + 38*srate);
			    
			    uint64_t code =
				    ((uint64_t) extract_byte(binbuf, i +  4*srate) <<  0) |
				    ((uint64_t) extract_byte(binbuf, i + 12*srate) <<  8) |
				    ((uint64_t) extract_byte(binbuf, i + 20*srate) << 16) |
				    ((uint64_t) extract_byte(binbuf, i + 28*srate) << 24) |
				    ((uint64_t) extract_byte(binbuf, i + 36*srate) << 32);
			    code = code & 0x3FFFFFFFFLLU;
			    
			    uint64_t aw = ((uint64_t) barker << 58) | (lap << 34) | code;
			    
			    // use lap to rebuild access word from scratch, do not use barker
			    // set barker accordingly to extracted lap.
			    uint64_t barker_true =  ((lap & 0x800000) != 0) ? 0x13 : 0x2c;
			    
			    uint64_t x = (barker_true << 24) | lap;
			    uint64_t p = 0x83848D96BBCC54FC;
			    uint64_t xtilde = (p >> 34) ^ x;
			    uint64_t gp = 0157464165547;
			    uint64_t g = (gp << 1) ^ gp;
			    uint64_t ctilde = compute_remainder (xtilde, g);
			    uint64_t stilde = ctilde | (xtilde << 34);
			    uint64_t awfinal = stilde ^ p;
			    
			    uint32_t _lap = (uint32_t) lap;
            
                uint64_t ble_access_code = 0x8E89BED6;
                uint64_t extracted_code = 
                        ((uint64_t) extract_byte(binbuf, i +  4*srate) <<  0) |
                        ((uint64_t) extract_byte(binbuf, i + 12*srate) <<  8) |
                        ((uint64_t) extract_byte(binbuf, i + 20*srate) << 16) |
                        ((uint64_t) extract_byte(binbuf, i + 28*srate) << 24);

                if (extracted_code == ble_access_code) {
                    std::cout << boost::format("[%2d]--BLE packet detected! (not supported) -- ")
                        % ch  << std::endl;
                    //std::cout << "BLE packet detected! (not supported)" << std::endl;
                    continue;
                }
            
                // Role (master/slave) is NOT determined here. It requires a
            // HEC-validated CLK, which the state machine below may or may not
            // resolve for this packet -- role is derived from clk parity at
            // each point in the switch() below where a CLK is actually
            // confirmed (see lap_node::role_from_clk). `telemetry_recorded`
            // tracks whether one of those branches already logged this
            // packet; if none did, the fallback right after the switch()
            // records it with ROLE_UNKNOWN rather than dropping it silently.
            bool telemetry_recorded = false;

            double pkt_cfo_hz = 0.0;  // CFO estimate for this packet, if available
            signal_metrics metrics;  // Signal metrics for this packet, if available
            


			    if (aw == awfinal) {
				    if (lap_map.find(_lap) == lap_map.end()) {
					    lap_map[_lap] = lap_node(_lap);
				    }

            // Calculate signal metrics for this packet
            //signal_metrics metrics = calculate_packet_metrics(chan, bufsize, i, srate);
            metrics = calculate_packet_metrics(chan, bufsize, i, srate);

            // --- NEW: CFO -----------------------------------------------------------
            // CFO only needs the access code itself (known once aw==awfinal passed),
            // not a resolved CLK, so it's safe to compute here for every packet.
            //double pkt_cfo_hz = estimate_cfo_hz(chan, i, awfinal, _lap);
            pkt_cfo_hz = estimate_cfo_hz(chan, i, awfinal, _lap);


            
            // --------------------------------------------------------------------------
			    } else {
				    continue;
			    }
	    
			    if (stopsig) break;
	    
#define INVALID_CLK_INDEX -1
#define DELTA_TS_SAME_THRESHOLD 40 // this should depend on frame length!
#define DELTA_TS_SLOT_THRESHOLD 620 // should be 625, give margin
#define SLOT_DURATION 625.0
#define ERROR_THRESHOLD 0.05
			    /*
          std::cout << boost::format("DEBUG: ch=%d, i=%d, bufsize=%d, binbuf offset=%d") 
              % ch % i % bufsize % (ch * bufsize) << std::endl;
          std::cout << boost::format("  access_code_start would be: %d") % i << std::endl;
          std::cout << boost::format("  payload_start would be: %d") % (i + 90 * srate) << std::endl;
          std::cout << boost::format("  FHS payload_end would be: %d") % (i + 90 * srate + 20 * 8 * srate) << std::endl;
          std::cout << boost::format("  Available buffer: %d to %d") % (ch * bufsize) % ((ch + 1) * bufsize) << std::endl;
          */
			    //uint32_t header = 0;
          uint32_t headers[64];  // Array of dewhitened headers
			    uint32_t clk_table[64];
          // vde added
          uint8_t pdu[400];
          size_t pdu_len;
			    long long timenow_sec_us = (samples_processed + i)/srate;

                
			    
			    if (stopsig == false) {
				    //std::cout << boost::format("[%2d] %12lld us -- %06X -- ")
					  //  % ch % (timenow_sec_us) % (_lap);
              //udp_send_lap_uap_pdu(lap, 0, nullptr, 0);
            std::cout << boost::format("[%2d] %12lld us -- %06X -- RSSI: %+6.1f dBm, SNR: %5.1f dB -- ")
                % ch % (timenow_sec_us) % (_lap) 
                % lap_map[_lap].last_rssi 
                % lap_map[_lap].last_snr;
			    }
			    
			    lap_map[_lap].increase_processed_packets();
			    switch (lap_map[_lap].get_status()) {
			    case LAP_STATE_NEW:
				    {
					    int a;
					    uint32_t uap;
					    lap_map[_lap].set_ts(timenow_sec_us);
					    lap_map[_lap].set_tstart(timenow_sec_us);
                        //std::cout << boost::format("started %lld -- ") % timenow_sec_us;
					    int valid_uaps = 0;
					    for (uap = 0; uap < 256; uap ++) {
						    int clk_found = extract_header_bf(binbuf+i+72*srate,
						                                      headers, clk_table, &a, uap);
						    if (clk_found <= 0) {
							    lap_map[_lap].bf_failed(); // don't log but keep trace of LAP
							    continue;
						    }
						    else if (clk_found != 2) {
							    // This should never happen.
							    std::cerr << "Invalid number of clk values " << clk_found << std::endl;
							    stopsig = true;
							    continue;
						    }
						    lap_map[_lap].set_uap_data(valid_uaps, true, uap,
						                               clk_table[0], clk_table[1], INVALID_CLK_INDEX);
						    valid_uaps++;
					    }
					    if (valid_uaps != 32) {
						    std::cout << boost::format("Init failed") << std::endl;
						    lap_map[_lap].bf_cannot_init();
                             send_telemetry(lap, 0xFF, ch, 0,0,0);
					    } else {
						    std::cout << boost::format("Initialized") << std::endl;
                
						    lap_map[_lap].set_status(LAP_STATE_BRUTE_FORCING);
					    }
				    }
				    break;
			    case LAP_STATE_BRUTE_FORCING:
				    {
					    long long tmpdeltats = timenow_sec_us - lap_map[_lap].get_ts();
					    if (tmpdeltats < 0) {
						    // Skip packet
						    std::cout << "Skip packet in the past" << std::endl;
						    lap_map[_lap].count_packet_inthepast ();
						    continue;
					    }
					    
					    if (llabs(lap_map[_lap].get_ts() - timenow_sec_us) < DELTA_TS_SAME_THRESHOLD) {
						    // This might happen with packet received on side channels.
						    std::cout << "Skip packet (too close to another one) ";
						    int a, clk_index;
						    int confirmed_uap = 0;
						    int valid_uap = 0;
						    for (int jj = 0; jj < 32; jj ++) {
							    uint32_t clks[2];
							    uint32_t uap;
							    bool uap_valid;
							    lap_map[_lap].get_uap_data(jj, &uap_valid, &uap,
							                               &clks[0], &clks[1], &clk_index);
							    if(uap_valid == false) continue;
							    valid_uap ++;
							    int clk_found = extract_header_bf(binbuf+i+72*srate,
							                                      headers, clk_table, &a, uap);
							    if (clk_found != 2) continue;
							    if (clk_table[0] != clks[0] || clk_table[1] != clks[1]) continue;
							    confirmed_uap ++;
						    }
						    
						    if (confirmed_uap == valid_uap) lap_map[_lap].new_packet_too_close (true);
						    else lap_map[_lap].new_packet_too_close (false);
						    
						    std::cout <<
							    boost::format("Confirmed %d out of %d UAPs for LAP %0X. Skipping")
							    % confirmed_uap % valid_uap % _lap << std::endl;
						    lap_map[_lap].set_ts(timenow_sec_us);
						    continue;
					    } else if (llabs(lap_map[_lap].get_ts() - timenow_sec_us) < DELTA_TS_SLOT_THRESHOLD) {
						    // we cannot handle such situation at the moment, so simply exit
						    std::cerr << "Cannot handle packet type, removing LAP" << std::endl;
						    lap_map.erase(_lap);
						    continue;
					    } else {
						    // measure how far away we are from being a multiple of 625
						    long long prevts = lap_map[_lap].get_ts();
						    long long deltats = timenow_sec_us - prevts;
						    float deltats_float = (float) deltats;
						    float periods = deltats_float / 625.0;
						    float periods_round = roundf(periods);
						    float error = fabsf(periods - periods_round);
						    if (error > ERROR_THRESHOLD) {
							    std::cerr << boost::format("Error too big (%f), LAP removed") % error
							              << std::endl;
                                 send_telemetry(lap, 0xFF, ch, 0,0,0);
							    lap_map.erase(_lap);
							    continue;
						    }
						    
						    int a, clk_index;
						    int count_valid_uap = 0;
						    int count_broken_uap = 0;
						    for (int jj = 0; jj < 32; jj ++) {
							    uint32_t clks[2];
							    uint32_t uap;
							    bool uap_valid;
							    lap_map[_lap].get_uap_data(jj, &uap_valid, &uap,
							                               &clks[0], &clks[1], &clk_index);
							    if(uap_valid == false)
								    continue;
							    int clk_found = extract_header_bf(binbuf+i+72*srate,
							                                      headers, clk_table, &a, uap);
							    if (clk_found != 2) {
								    // if we end up here it means either
								    // 1. the connection changed, we should remove the old one and restart
								    // 2. this frame is corrupt
								    // At the moment we handle case 2 only, so we simply ignore this uap
								    // and we make sure at the end that all were broken
								    count_broken_uap++;
								    continue;
							    }
							    if (clk_table[0] == clks[0] && clk_table[1] == clks[1]) {
								    count_valid_uap++;
								    continue;
							    }
							    int slot = ((int) periods_round) % 64;
							    // test only the valid one
							    int old_to_check = 2;
							    if (clk_index != INVALID_CLK_INDEX) {
								    clks[0] = clks[clk_index];
								    old_to_check = 1;
							    }
							    int qnew, qold;
							    int qnew_valid = INVALID_CLK_INDEX;
							    int matched = 0;
							    for (qold = 0; qold < old_to_check; qold ++) {
								    for (qnew = 0; qnew < 2; qnew ++) {
									    int slot_guessed = (clk_table[qnew] - clks[qold]) % 64;
									    if (slot == slot_guessed) {
										    qnew_valid = qnew;
										    matched ++;
									    }
								    }
							    }
							    if (matched > 1) {
								    lap_map[_lap].set_uap_data(jj, true, uap, clk_table[0], clk_table[1],
								                               INVALID_CLK_INDEX);
								    count_valid_uap ++;
							    } else if (matched > 0) {
								    lap_map[_lap].set_uap_data(jj, true, uap, clk_table[0], clk_table[1],
								                               qnew_valid);
								    count_valid_uap ++;
							    } else {
								    lap_map[_lap].set_uap_data(jj, false, 0, 0, 0, INVALID_CLK_INDEX);
							    }
						    } // for all uap
						    if (count_valid_uap == 0 && count_broken_uap > 0) {
							    lap_map[_lap].count_broken_uap ();
							    std::cerr << "Frame likely broken, skipped" << std::endl;
                                send_telemetry(lap, 0xFF, ch, 0,0,0);
							    continue;
						    } else if (count_valid_uap == 0) {
							    std::cout << "No valid UAP remaining, LAP removed" << std::endl;
                                send_telemetry(lap, 0xFF, ch, 0,0,0);
							    lap_map.erase(_lap);
							    continue;
						    } else if (count_valid_uap <= 2) {
							    uint32_t clks[2];
							    uint32_t uap;
							    bool isvalid;
							    int clock_index;
							    uint32_t uap_found[2];
							    int uap_idx = 0;
							    
 							    struct timeval ts;
 							    gettimeofday(&ts, NULL);
 							    
							    for (int i = 0; i < 32; i++) {
								    lap_map[_lap].get_uap_data(i, &isvalid, &uap,
								                               &clks[0], &clks[1], &clock_index);
								    if (isvalid) {
									    uap_found[uap_idx++] = uap;
								    }
							    }
							    send_telemetry(lap, 0xFF, ch, 0,0,0);
							    std::cout << boost::format("Only two UAP left (%X and %X) - ")
								    % uap_found[0] % uap_found[1];
							    if (!lap_map[_lap].two_uap_stage && uap_idx == 2) {
                                    lap_map[_lap].two_uap_stage = true;
                                    lap_map[_lap].uap_pair[0] = uap_found[0];
                                    lap_map[_lap].uap_pair[1] = uap_found[1];

                                    std::cout << boost::format("Two UAP stage entered - ");
                                }
                                //lap_node &node = lap_map[_lap];
                                if (lap_map[_lap].two_uap_stage && !lap_map[_lap].uap_locked) {
                                    std::cout << boost::format("DEBUG before test: uap_locked=%d") 
                                        % lap_map[_lap].uap_locked << std::endl;
    
                                // We need to test both UAP candidates
                                // For each UAP, we need to call extract_header_bf to get its specific headers/clks
    
                                for (int uap_idx = 0; uap_idx < 2; uap_idx++) {
                                    uint8_t test_uap = lap_map[_lap].uap_pair[uap_idx];
        
                                    std::cout << boost::format("Extracting headers for UAP candidate 0x%02X") 
                                        % (int)test_uap << std::endl;
        
                                    // Extract headers for this specific UAP
                                    int clk_found = 0;
                                    uint32_t test_headers[64];
                                    uint32_t test_clks[64];
        
                                    int result = extract_header_bf(binbuf + i + 72*srate,
                                      test_headers, test_clks, &clk_found, test_uap);
        
                                    if (result <= 0 || clk_found <= 0) {
                                        std::cout << boost::format("  No valid headers found for UAP 0x%02X") 
                                            % (int)test_uap << std::endl;
                                        continue;
                                    }
        
                                    std::cout << boost::format("  Found %d valid CLK values for UAP 0x%02X") 
                                        % clk_found % (int)test_uap << std::endl;
        
                                    // Now get the expected CLK values from lap_node for this UAP
                                    uint32_t expected_clk0, expected_clk1;
                                    int clock_index;
                                    if (!lap_map[_lap].get_uap_data_by_uap(test_uap, &expected_clk0, &expected_clk1, &clock_index)) {
                                        std::cout << boost::format("  Could not get UAP data for 0x%02X") 
                                            % (int)test_uap << std::endl;
                                        continue;
                                    }
        
                                    std::cout << boost::format("  Expected CLKs for UAP 0x%02X: %d, %d") 
                                        % (int)test_uap % expected_clk0 % expected_clk1 << std::endl;
        
                                    // Try both expected CLK values
                                    uint32_t expected_clks[2] = {expected_clk0, expected_clk1};
        
                                    for (int clk_idx = 0; clk_idx < 2; clk_idx++) {
                                        uint32_t try_clk = expected_clks[clk_idx];
            
                                        std::cout << boost::format("\n--- Testing UAP 0x%02X with CLK %d ---") 
                                            % (int)test_uap % try_clk << std::endl;
            
                                        // Check if this CLK is in our extracted headers
                                        bool clk_in_table = false;
                                        for (int i = 0; i < clk_found; i++) {
                                            if (test_clks[i] == try_clk) {
                                                clk_in_table = true;
                                                break;
                                            }
                                        }
            
                                        if (!clk_in_table) {
                                            std::cout << boost::format("  ⚠ CLK %d not in extracted table, skipping") 
                                                % try_clk << std::endl;
                                            continue;
                                        }
            
            // Try to extract PDU with this UAP/CLK combination
            if (extract_pdu_debug(binbuf, i, test_headers, test_clks, clk_found,
                                 try_clk, test_uap, pdu, &pdu_len, true, bufsize)) {
                std::cout << "*** SUCCESS! UAP locked to 0x" << std::hex 
                         << (int)test_uap << std::dec << " ***" << std::endl;
                
                lap_map[_lap].uap_locked = true;
                lap_map[_lap].locked_uap = test_uap;
                lap_map[_lap].last_clk = try_clk;
                lap_map[_lap].clk_idx = clk_idx;
                
                std::cout << boost::format("  DEBUG: Set node.uap_locked=%d, locked_uap=0x%02X, last_clk=%d") 
                    % lap_map[_lap].uap_locked % (int)lap_map[_lap].locked_uap % lap_map[_lap].last_clk << std::endl;
                
                // UAP locked! Transition to tracking state
                lap_map[_lap].set_status(LAP_STATE_UAP_LOCKED);

                // NEW: role from clk parity, cross-checked against packet
                // type/LT_ADDR. Broadcast (LT_ADDR==0) and POLL (type==0x1)
                // packets can only ever come from the master, so seeing one
                // lets us confirm (or correct) which clk parity means
                // "master" for this LAP, instead of just assuming CLK1=0.
                {
                    const uint8_t *header_start = binbuf + i + 72 * srate;
                    uint32_t header = dewhiten_header_with_clk(header_start, try_clk);
                    uint8_t lt_addr = header & 0x7;
                    uint8_t ptype = (header >> 3) & 0xF;
                    if (lt_addr == 0 || ptype == 0x1) {
                        lap_map[_lap].confirm_master_parity(try_clk);
                    }
                }
                bt_role_t pkt_role = lap_map[_lap].role_from_clk(try_clk);
                lap_map[_lap].update_signal_metrics(pkt_role, metrics.rssi_dbm,
                                                     metrics.snr_db, pkt_cfo_hz);
                telemetry_recorded = true;

                // Export the first PDU
                //udp_send_lap_uap_pdu(_lap, lap_map[_lap].locked_uap, pdu, pdu_len);
                send_telemetry(lap, lap_map[_lap].locked_uap, ch, 0,pdu_len,pdu);

                // Update timestamp
                lap_map[_lap].set_ts(timenow_sec_us);
                
                // Break out of all loops - we found it!
                goto uap_locked_success;
            }
        }
    }
    
    uap_locked_success:
    if (!lap_map[_lap].uap_locked) {
        std::cout << "WARNING: Could not lock UAP with any combination!" << std::endl;
    }
}
                 

							    // Check that only 2 UAPs have been found.
							    if (uap_idx > 2)
								    fprintf(fptrout, "ERROR\n");
							    
							    long long tsfirst = lap_map[_lap].get_tstart();
							    long long tsresolv = (samples_processed + i)/srate;
							    long long tsdiff = tsresolv - tsfirst;

                                /* Compute energy of last pkt */
                                double energy = 0;
                                for (size_t k = i; k < i + 64*srate; k++) {
                                    energy += (chan[k].real() * chan[k].real()) + (chan[k].imag() * chan[k].imag());
                                }

                                std::cout << boost::format("first: %lld -- ") % tsfirst;
							    
                                std::cout << boost::format("solved in %lld us")  % tsdiff << std::endl;

                                fprintf(fptrout,
                                    "%ld.%06ld %06X -- %3X %3x -- ts %lld -- tdiff %lld -- energy %lf\n",
 								    ts.tv_sec, ts.tv_usec, _lap, uap_found[0], uap_found[1], tsresolv,
                                    tsdiff, energy);
 							    fflush (fptrout);

                                // LAP resolved, remove LAP to restart.
                                //lap_map.erase(_lap);
							    
						    } else {
							    std::cout << boost::format("%d possible UAPs remaining") % count_valid_uap
							              << std::endl;
						    }
						    
						    lap_map[_lap].set_ts(timenow_sec_us);
					    }
				    }
				    break;
            case LAP_STATE_UAP_LOCKED:
            {
                // This LAP has a locked UAP - extract PDU from this packet
                std::cout << boost::format("Locked UAP (0x%02X) - ") 
                    % (int)lap_map[_lap].locked_uap;
    
                // Extract headers using the locked UAP
                int clk_found;
                uint32_t lock_headers[64];
                uint32_t lock_clks[64];
    
                clk_found = extract_header_bf(binbuf + i + 72*srate,
                                  lock_headers, lock_clks, &clk_found, 
                                  lap_map[_lap].locked_uap);
    
                if (clk_found > 0) {
                    if (extract_pdu_with_locked_uap(binbuf, i, lock_headers, lock_clks, clk_found,
                                       lap_map[_lap], timenow_sec_us,
                                       pdu, &pdu_len, bufsize)) {
                        std::cout << "✓ PDU extracted" << std::endl;
                        lap_map[_lap].pdu_ok++;

                        // NEW: role from clk parity (extract_pdu_with_locked_uap
                        // already updated last_clk to whichever CLK it resolved
                        // this packet with), cross-checked against packet
                        // type/LT_ADDR the same way as the brute-force path.
                        {
                            uint32_t resolved_clk = lap_map[_lap].last_clk;
                            const uint8_t *header_start = binbuf + i + 72 * srate;
                            uint32_t header = dewhiten_header_with_clk(header_start, resolved_clk);
                            uint8_t lt_addr = header & 0x7;
                            uint8_t ptype = (header >> 3) & 0xF;
                            if (lt_addr == 0 || ptype == 0x1) {
                                lap_map[_lap].confirm_master_parity(resolved_clk);
                            }
                            bt_role_t pkt_role = lap_map[_lap].role_from_clk(resolved_clk);
                            lap_map[_lap].update_signal_metrics(pkt_role, metrics.rssi_dbm,
                                                                 metrics.snr_db, pkt_cfo_hz);
                            telemetry_recorded = true;
                        }

                        //udp_send_lap_uap_pdu(_lap, lap_map[_lap].locked_uap, pdu, pdu_len);
                        send_telemetry(lap, lap_map[_lap].locked_uap, ch, 2,pdu_len,pdu);
                    } else {
                        std::cout << "✗ PDU extraction failed" << std::endl;
                        lap_map[_lap].pdu_fail++;
                        send_telemetry(lap, lap_map[_lap].locked_uap, ch, 2,0,0);
                    }
                } else {
                    std::cout << "✗ extract_header_bf failed" << std::endl;
                    send_telemetry(lap, lap_map[_lap].locked_uap, ch, 2,0,0);
                    lap_map[_lap].pdu_fail++;
                }
            }
            break;
			    default:
				    {
				    }
				    break;
			    }
			    

            // NEW: if none of the branches above resolved a CLK for this
            // packet (brute-force miss, prediction miss, LAP_STATE_NEW,
            // etc.), still record it -- with ROLE_UNKNOWN -- so it isn't
            // silently dropped from the telemetry/radar view.
            if (!telemetry_recorded) {
                lap_map[_lap].update_signal_metrics(ROLE_UNKNOWN, metrics.rssi_dbm,
                                                     metrics.snr_db, pkt_cfo_hz);
            }
			    i += 100;
		    }
	    }
		    
    }

    // Increment sample count.
    samples_processed += bufsize;
    
    
    // Release buffer.
    //if (local_bufselect) pthread_spin_unlock(&lock[1]);
    //else pthread_spin_unlock(&lock[0]); 
  }
  
  free(chanbuf);
  free(sigbuf);
  free(binbuffer); 
  fclose(fptrout);
  
  return NULL;
}


// Entry point.
int UHD_SAFE_MAIN(int argc, char *argv[])
{
  // Set highest priority on current thread.
  uhd::set_thread_priority_safe(1, true);

  std::cout << "Realtime Bluetooth Sniffer";
  std::cout << std::endl << std::endl;

  std::string args, subdev, antname, channel_list;
  double rate, freq, gain;
  
  // Initialise program options.
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h",
      "print this help message")
    ("args", po::value<std::string>(&args)->default_value(""),
     "USRP device arguments")
    ("channels", po::value<std::string>(&channel_list)->default_value("0"),
     "select channel to use (e.g. \"0\", \"0,1\")")
    ("subdev", po::value<std::string>(&subdev)->default_value("A:A"),
     "set frontend specification (e.g. \"A:A\", \"A:A A:B\")")
    ("rate", po::value<double>(&rate)->default_value(8e6),
     "set sampling rate")
    ("freq", po::value<double>(&freq)->default_value(2420e6),
     "set central frequency")
    ("gain", po::value<double>(&gain)->default_value(40),
     "set RF gain of the receiving chains")
    ("ant", po::value<std::string>(&antname)->default_value("TX/RX"),
     "select antenna on the frontend");
  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  // Print help message.
  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return ~0;
  }

  // Create a multi-USRP device.
  std::cout << "Creating USRP device..." << std::endl;
  std::string enhanced_args = args + ",recv_buff_size=8388608"  +",num_recv_frames=256";
    //",recv_buff_size=10000000" +  // 100 MB buffer (was ~2-8 MB)
    //",num_recv_frames=512";
  //uhd::usrp::multi_usrp::sptr usrp = uhd::usrp::multi_usrp::make(args);
  uhd::usrp::multi_usrp::sptr usrp = uhd::usrp::multi_usrp::make(enhanced_args);
  std::cout << "USRP device created." << std::endl;

  // Select the RX subdevice first; this mapping affects all the settings.
  if (vm.count("subdev")) usrp->set_rx_subdev_spec(subdev);

  // Set the RX sample rate on all channels.
  std::cout << boost::format("-- Asking for RX rate %f MHz...") % (rate/1e6);
  std::cout << std::endl;
  usrp->set_rx_rate(rate);
  std::cout << boost::format("-- Actually got RX rate %f MHz") % (usrp->get_rx_rate()/1e6);
  std::cout << std::endl;

  // Lock motherboard clock to internal reference source and reset time register.
  std::cout << "-- Setting device timestamp to 0.0 s... ";
  usrp->set_clock_source("internal");
  usrp->set_time_now(uhd::time_spec_t(0.0));
  std::cout << "done" << std::endl;
  
  // Set the RX center frequency.
  std::cout << boost::format("-- Asking for freq %f MHz...") % (freq/1e6) << std::endl;
  uhd::tune_request_t tunereq(freq);
  usrp->set_rx_freq(tunereq);
  std::cout << boost::format("-- Actually got freq %f MHz") % (usrp->get_rx_freq()/1e6) << std::endl;

  // Set the RX gain.
  std::cout << boost::format("-- Asking for gain %f dB...") % gain << std::endl;
  usrp->set_rx_gain(gain);
  std::cout << boost::format("-- Actually got gain %f dB") % usrp->get_rx_gain() << std::endl;

  // Set the RX antennas.
  std::cout << boost::format("-- Asking for antenna \"%s\"...") % antname << std::endl;
  usrp->set_rx_antenna(antname);
  std::cout << boost::format("-- Actually got antenna \"%s\"...") % antname << std::endl;

  // Select the RX channels.
  std::vector<std::string> channel_strings;
  std::vector<size_t> channel_nums;
  boost::split(channel_strings, channel_list, boost::is_any_of("\"',"));
  for (size_t c = 0; c < channel_strings.size(); c++) {
    size_t chan = boost::lexical_cast<int>(channel_strings[c]);
    if (chan < usrp->get_rx_num_channels()) {
      channel_nums.push_back(boost::lexical_cast<int>(channel_strings[c]));
    } else {
      throw std::runtime_error("Invalid channel(s) specified.");
    }
  }
  const size_t nchannels = usrp->get_rx_num_channels();
  
  // Create a receive streamer.
  //uhd::stream_args_t stream_args("fc64","sc16");
  // for smaller cpu, use float instead of double
  uhd::stream_args_t stream_args("fc32","sc16");
  stream_args.channels = channel_nums;
  uhd::rx_streamer::sptr rx_stream = usrp->get_rx_stream(stream_args);

  // Allocate data buffers.
  const double nseconds = 0.1;
  const double bufsize_us = nseconds * rate;
  const size_t usrp_bufsize = rx_stream->get_max_num_samps();
  const size_t usrp_nbuffers = (size_t) ceil(bufsize_us / (float) usrp_bufsize);
  //std::vector<iqsamp_t> bankA(usrp_bufsize * usrp_nbuffers);
  //std::vector<iqsamp_t> bankB(usrp_bufsize * usrp_nbuffers);
  //iqsamp_t *bankptr;
  std::vector<iqsamp_t> bankA_ch0(usrp_bufsize * usrp_nbuffers);
  std::vector<iqsamp_t> bankB_ch0(usrp_bufsize * usrp_nbuffers);

  // RX1 banks only used if nchannels == 2
  std::vector<iqsamp_t> bankA_ch1;
  std::vector<iqsamp_t> bankB_ch1;

  if (nchannels == 2) {
    bankA_ch1.resize(usrp_bufsize * usrp_nbuffers);
    bankB_ch1.resize(usrp_bufsize * usrp_nbuffers);
  }

  // Active bank pointers
  iqsamp_t *bankptr_ch0;
  iqsamp_t *bankptr_ch1 = nullptr;

  // Auxiliary data for recv().
  uhd::rx_metadata_t md;
  double timeout = 10.0;
  unsigned long num_acc_samps = 0;
  unsigned long num_rx_samps = 0;

  // Setup streaming options.
  double stream_delay = 1.0;
  uhd::stream_cmd_t stream_cmd(uhd::stream_cmd_t::STREAM_MODE_START_CONTINUOUS);
  stream_cmd.num_samps = total_num_samps;
  stream_cmd.stream_now = false;
  stream_cmd.time_spec = uhd::time_spec_t(stream_delay);

  // Initialise frontend spinlocks.
  pthread_spin_init(&lock[0], PTHREAD_PROCESS_PRIVATE);
  pthread_spin_init(&lock[1], PTHREAD_PROCESS_PRIVATE);

  // Create thread for processing data.
  pthread_t proc_thread;
  //proc_pars_t proc_pars = {&bankA.front(), &bankB.front(), usrp_bufsize/decfactor * usrp_nbuffers};
  
	proc_pars_t proc_pars = {&bankA_ch0.front(), &bankB_ch0.front(), usrp_bufsize/decfactor * usrp_nbuffers};
  pthread_create(&proc_thread, NULL, proc_routine, &proc_pars);

  std::signal(SIGINT, &sigint_handler);
  // setup udp streaming
  zmq_sender_init("0.0.0.0", 9000);
  std::cout << boost::format("\n buffer size %d samples.") % usrp_nbuffers;
        std::cout << std::endl;
  // Start streaming.
  rx_stream->issue_stream_cmd(stream_cmd);
  std::cout << std::endl << "Streaming... Press CTRL+C to stop." << std::endl;
  std::cout << std::endl << "      Timestamp       LAP    Info " << std::endl;
  
  while (stopsig == false) {

    bufselect = (bufselect + 1) % 2;
    
    // Lock buffer.
    pthread_spin_lock(&lock[bufselect]);
    
    // Claim buffer for writing.
    //if (bufselect) {
    //  bankptr = &bankA.front();
    //} else {
    //  bankptr = &bankB.front();
    //}
    if (bufselect) {
      bankptr_ch0 = &bankA_ch0.front();
    } else {
      bankptr_ch0 = &bankB_ch0.front();
    }
      
    // Receive data.
    for (unsigned int n = 0; n < usrp_nbuffers; n++) {
      //num_rx_samps = rx_stream->recv(bankptr + usrp_bufsize*n, usrp_bufsize, md, timeout, false);
      num_rx_samps = rx_stream->recv(bankptr_ch0 + usrp_bufsize*n, usrp_bufsize, md, timeout, false);
      
      // Throw an exception on error.
      if (md.error_code == uhd::rx_metadata_t::ERROR_CODE_OVERFLOW) {
        std::cerr << std::endl << "OVERFLOW DETECTED" << std::endl;
      } else if (md.error_code != uhd::rx_metadata_t::ERROR_CODE_NONE) {
        std::cout << boost::format("\nReceived %u samples.") % num_acc_samps;
        std::cout << std::endl;
        throw std::runtime_error(md.strerror());
      }
      
      // Increment number of received samples.
      num_acc_samps += num_rx_samps;

    }

    // Unlock buffer.
    pthread_spin_unlock(&lock[bufselect]);  
  }

  pthread_join(proc_thread, NULL);
  
  pthread_spin_destroy(&lock[0]);
  pthread_spin_destroy(&lock[1]);
  zmq_sender_close();
  std::cout << "Done." << std::endl;

  return 0;
}
