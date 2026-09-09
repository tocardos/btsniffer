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

#include <string>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <pthread.h>
#include <csignal>
#include <stdio.h>
#include <unordered_map>
#include "lapnode.hpp"

lap_node::lap_node()
{
	lap = 0;
	memset(uaps, 0, sizeof(uaps));
	state = LAP_STATE_NEW;
	ts = 0;
	processed_packets = 0;
	brute_force_failures = 0;
	brute_force_noinit = 0;
	brute_force_noinfo = 0;
	packet_too_close_confirmed = 0;
	packet_too_close_notconfirmed = 0;
	broken_uap = 0;
	packet_inthepast = 0;
	two_uap_stage = false;
    uap_locked=false;
    locked_uap=0;
	last_clk=0; 
    clk_idx=-1;
    pdu_ok=0;
    pdu_fail=0;
	last_rssi = 0.0f;
    last_snr = 0.0f;
    avg_rssi = 0.0f;
    avg_snr = 0.0f;
	// NEW: slot-parity role tracking + per-role signal metrics
    slot_counter = 0;
    slot_ref_set = false;
	// NEW: clk-parity role tracking + per-role signal metrics
    master_clk_parity = 0;       // spec default: CLK1=0 => master, until an anchor says otherwise
    parity_confirmed = false;
    last_role = ROLE_UNKNOWN;

    last_rssi_master = last_rssi_slave = 0.0;
    avg_rssi_master  = avg_rssi_slave  = 0.0;
    last_snr_master  = last_snr_slave  = 0.0;
    avg_snr_master   = avg_snr_slave   = 0.0;
    last_cfo_master  = last_cfo_slave  = 0.0;
    avg_cfo_master   = avg_cfo_slave   = 0.0;
    has_master_sample = has_slave_sample = false;
    cfo_diff_hz = 0.0;
}

lap_node::lap_node(uint32_t _lap): lap_node()
{
	lap = _lap;
}

void lap_node::set_status(lap_status_t _state)
{
	state = _state;
}

lap_status_t lap_node::get_status(void)
{
	return state;
}

void lap_node::set_uap_data(int index, bool _valid, uint32_t _uap,
                            uint32_t _clk1, uint32_t _clk2, int _clk_index)
{
	if(index < 0 || index > 32) {
		std::cerr << "ERROR: Invalid UAP index " << index
		          << ", aborting..." << std::endl;
		exit(1);
	}
	uaps[index].uap_valid = _valid;
	uaps[index].uap = _uap;
	uaps[index].clk1 = _clk1;
	uaps[index].clk2 = _clk2;
	uaps[index].clk_index = _clk_index;
}

void lap_node::get_uap_data(int index, bool *_valid, uint32_t *_uap,
                            uint32_t *_clk1, uint32_t *_clk2, int *_clk_index)
{
	if(index < 0 || index > 32) {
		std::cerr << "ERROR: Invalid UAP index " << index
		          << ", aborting..." << std::endl;
		exit(1);
	}
	*_valid = uaps[index].uap_valid;
	*_uap = uaps[index].uap;
	*_clk1 = uaps[index].clk1;
	*_clk2 = uaps[index].clk2;
	*_clk_index = uaps[index].clk_index;
}

// vde added
bool lap_node::get_uap_data_by_uap( uint32_t uap,uint32_t *_clk1, uint32_t *_clk2, int *_clk_index)
{
	for (int index=0; index<32; index++) {
		if (uaps[index].uap == uap) {
			*_clk1 = uaps[index].clk1;
			*_clk2 = uaps[index].clk2;
			*_clk_index = uaps[index].clk_index;
			return true;
		}
	}
	return false;
	
}

void lap_node::set_ts(long long _ts)
{
	prevts = ts;
	ts = _ts;
}

long long lap_node::get_ts(void)
{
	return ts;
}

void lap_node::increase_processed_packets (void)
{
	processed_packets ++;
}

int lap_node::get_processed_packets (void)
{
	return processed_packets;
}

void lap_node::bf_failed (void)
{
  brute_force_failures ++;
}

int lap_node::get_bf_failures (void)
{
	return brute_force_failures;
}

void lap_node::bf_cannot_init (void)
{
	brute_force_noinit ++;
}

int lap_node::get_bf_noinit (void)
{
	return brute_force_noinit;
}

void lap_node::bf_noinfo (void)
{
	brute_force_noinfo ++;
}

int lap_node::get_bf_noinfo (void)
{
	return brute_force_noinfo;
}

void lap_node::new_packet_too_close (bool confirmed)
{
	if (confirmed) packet_too_close_confirmed ++;
	else packet_too_close_notconfirmed ++;
}

void lap_node::get_packet_too_close (int *confirmed, int *notconfirmed)
{
	*confirmed = packet_too_close_confirmed;
	*notconfirmed = packet_too_close_notconfirmed;
}

void lap_node::count_broken_uap (void)
{
	broken_uap ++;
}

int lap_node::get_broken_uap (void)
{
	return broken_uap;
}

void lap_node::count_packet_inthepast (void)
{
	packet_inthepast ++;
}

int lap_node::get_packet_inthepast (void)
{
	return packet_inthepast;
}

// NEW: BR/EDR slot duration in microseconds. Master transmits on even slots,
// slave on odd slots. We don't get this from the header-dewhitened `clk`
// (that's CLK[6:1], identical for a master packet and the slave's reply in
// the same slot pair) -- instead we track the cumulative number of 625us
// slots elapsed since the first packet seen for this LAP, using inter-packet
// timing, and use that running count's parity.
static const double SLOT_DURATION_US = 625.0;

bt_role_t lap_node::role_from_clk(uint32_t clk)
{
	last_role = ((clk & 1u) == (uint32_t) master_clk_parity) ? ROLE_MASTER : ROLE_SLAVE;
	return last_role;
}

void lap_node::confirm_master_parity(uint32_t clk)
{
	master_clk_parity = (int) (clk & 1u);
	parity_confirmed = true;
}

bt_role_t lap_node::update_role(long long ts_now_us)
{
	if (!slot_ref_set) {
		slot_ref_set = true;
		slot_counter = 0;
		// No ground truth on the very first packet for this LAP -- assume
		// it opens a fresh slot pair (master). If fingerprinting output
		// looks systematically swapped, flip this initial assumption.
		last_role = ROLE_MASTER;
		return last_role;
	}

	long long delta_us = ts_now_us - ts;
	if (delta_us <= 0) {
		// Non-monotonic timestamp (out-of-order/duplicate) -- can't infer
		// a slot count, leave role as unknown for this packet without
		// disturbing slot_counter.
		last_role = ROLE_UNKNOWN;
		return last_role;
	}

	long long slots = (long long) ((delta_us / SLOT_DURATION_US) + 0.5); // round
	if (slots <= 0) slots = 1;

	slot_counter += (uint64_t) slots;
	last_role = ((slot_counter % 2) == 0) ? ROLE_MASTER : ROLE_SLAVE;
	return last_role;
}


void lap_node::update_role_from_header(uint8_t packet_type, uint8_t lt_addr, uint32_t clk) {
    // Bluetooth Core Spec Packet Types:
    // 0x00 = NULL, 0x01 = POLL, 0x02 = FHS
    // Broadcast packets (LT_ADDR == 0) can ONLY come from the Master.
    
    bool is_master_packet = false;
    bool is_anchor_event = false;

    if (lt_addr == 0) { 
        is_master_packet = true; // Broadcast
        is_anchor_event = true;
    } else if (packet_type == 0x01) { 
        is_master_packet = true; // POLL packet is strictly Master -> Slave
        is_anchor_event = true;
    } else if (packet_type == 0x02) { 
        is_master_packet = true; // FHS packet is sent by Master during paging/inquiry
        is_anchor_event = true;
    }

    if (is_anchor_event) {
        // Confirm or calibrate master_clk_parity based on deterministic frame
        uint8_t pkt_parity = (uint8_t)(clk & 1u);
        
        if (is_master_packet) {
            master_clk_parity = pkt_parity;
        } else {
            master_clk_parity = 1 - pkt_parity; // If it were a slave-only packet
        }
        parity_confirmed = true;
    }
}

void lap_node::update_signal_metrics(bt_role_t role, double rssi_dbm,
                                      double snr_db, double cfo_hz)
{
	// Role-agnostic aggregate, kept for backward compatibility.
	last_rssi = (float) rssi_dbm;
	last_snr  = (float) snr_db;
	avg_rssi  = (avg_rssi * 0.8f) + ((float) rssi_dbm * 0.2f);
	avg_snr   = (avg_snr  * 0.8f) + ((float) snr_db  * 0.2f);

	if (role == ROLE_MASTER) {
		last_rssi_master = rssi_dbm;
		last_snr_master  = snr_db;
		last_cfo_master  = cfo_hz;
		avg_rssi_master  = has_master_sample ? (avg_rssi_master * 0.8 + rssi_dbm * 0.2) : rssi_dbm;
		avg_snr_master   = has_master_sample ? (avg_snr_master  * 0.8 + snr_db  * 0.2) : snr_db;
		avg_cfo_master   = has_master_sample ? (avg_cfo_master  * 0.8 + cfo_hz  * 0.2) : cfo_hz;
		has_master_sample = true;
	} else if (role == ROLE_SLAVE) {
		last_rssi_slave = rssi_dbm;
		last_snr_slave  = snr_db;
		last_cfo_slave  = cfo_hz;
		avg_rssi_slave  = has_slave_sample ? (avg_rssi_slave * 0.8 + rssi_dbm * 0.2) : rssi_dbm;
		avg_snr_slave   = has_slave_sample ? (avg_snr_slave  * 0.8 + snr_db  * 0.2) : snr_db;
		avg_cfo_slave   = has_slave_sample ? (avg_cfo_slave  * 0.8 + cfo_hz  * 0.2) : cfo_hz;
		has_slave_sample = true;
	}
	// role == ROLE_UNKNOWN: aggregate already updated above, nothing more to do.

	if (has_master_sample && has_slave_sample)
		cfo_diff_hz = avg_cfo_master - avg_cfo_slave;
}

void lap_node::dump (void)
{
	fprintf(stdout, "lap = %u\n", lap);
	fprintf(stdout, "brute_forced = %d\n", (int) brute_forced);
	fprintf(stdout, "ts = %llX\n", (long long) ts);
	fprintf(stdout, "lap = %06X\n", lap);
	fprintf(stdout, "%d %d %d %d %d %d %d %d\n",
	        processed_packets,
	        brute_force_failures,
	        brute_force_noinit,
	        brute_force_noinfo,
	        packet_too_close_confirmed,
	        packet_too_close_notconfirmed,
	        broken_uap,
	        packet_inthepast);
	fprintf(stdout, "role = %s\n",
	        last_role == ROLE_MASTER ? "master" :
	        last_role == ROLE_SLAVE  ? "slave"  : "unknown");
	fprintf(stdout, "rssi master/slave = %.1f / %.1f dBm (avg %.1f / %.1f)\n",
	        last_rssi_master, last_rssi_slave, avg_rssi_master, avg_rssi_slave);
	fprintf(stdout, "snr  master/slave = %.1f / %.1f dB (avg %.1f / %.1f)\n",
	        last_snr_master, last_snr_slave, avg_snr_master, avg_snr_slave);
	fprintf(stdout, "cfo  master/slave = %.1f / %.1f Hz (avg %.1f / %.1f), diff = %.1f Hz\n",
	        last_cfo_master, last_cfo_slave, avg_cfo_master, avg_cfo_slave, cfo_diff_hz);
}