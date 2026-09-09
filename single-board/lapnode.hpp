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

#ifndef BTDECODER_H
#define BTDECODER_H

#include <stdint.h>
#include <vector>
#include <functional>
#include <unordered_map>
#include <sys/time.h>

typedef enum {
    LAP_STATE_NEW = 0,
    LAP_STATE_BRUTE_FORCING = 1,
    LAP_STATE_RESOLVED = 2,
	LAP_STATE_LOCKED = 3,
	LAP_STATE_UAP_LOCKED = 4,
} lap_status_t;

// NEW: which side of the piconet a given packet came from. Derived from
// cumulative 625us-slot parity (see lap_node::update_role in lapnode.cpp),
// NOT from the parity of the 6-bit `clk` extracted via header dewhitening --
// that value is CLK[6:1] and is identical for a master packet and the
// slave's reply within the same slot pair, so it can't distinguish them.
typedef enum {
	ROLE_UNKNOWN = 0,
	ROLE_MASTER  = 1,
	ROLE_SLAVE   = 2
} bt_role_t;

typedef struct {
	bool     uap_valid;
	uint32_t uap;
	uint32_t clk1;
	uint32_t clk2;
	int      clk_index;
} uap_slot_t;


class lap_node
{
public:
	lap_node();
	lap_node(uint32_t lap);
	// bool is_bruteforce_done(void);
	// void set_bruteforce_done(void);
	void set_tstart(long long t) {tstart = t;}
	long long get_tstart() {return tstart;}
	void dump(void);
    lap_status_t get_status(void);
	void set_status(lap_status_t);
	void set_uap_data(int index, bool valid, uint32_t uap, uint32_t clk1, uint32_t clk2, int clk_index);
	void get_uap_data(int index, bool *valid, uint32_t *uap, uint32_t *clk1, uint32_t *clk2, int *clk_index);
	bool get_uap_data_by_uap( uint32_t uap, uint32_t *clk1, uint32_t *clk2, int *clk_index);
	void set_ts(long long);
	long long get_ts();
	void increase_processed_packets (void);
	int get_processed_packets (void);
	void bf_failed (void);
	int get_bf_failures (void);
	void bf_cannot_init (void);
	int get_bf_noinit (void);
	void bf_noinfo (void);
	int get_bf_noinfo (void);
	void new_packet_too_close (bool);
	void get_packet_too_close (int *confirmed, int *notcofirmed);
	void count_broken_uap (void);
	int get_broken_uap (void);
	void count_packet_inthepast (void);
	int get_packet_inthepast (void);
	// === VDE to keep UAPs ===
    bool two_uap_stage = false;
    bool uap_locked    = false;
	int set_clk_index = 0;
    uint8_t uap_pair[2] = {0, 0};
    uint8_t locked_uap  = 0;
	uint32_t last_clk=0;      // Last known CLK value
    int clk_idx=-1;            // Which CLK index (0 or 1) was valid
	// Optional: stats
    uint32_t pdu_ok = 0;
    uint32_t pdu_fail = 0;
	// signal metric
    double last_rssi = 0.0f;
    double last_snr = 0.0f;
    double avg_rssi = 0.0f;
    double avg_snr = 0.0f;
	// NEW: role determination from slot-pair parity. Call once per confirmed
	// packet, BEFORE the caller's own set_ts() updates `ts` for this packet
	// (update_role reads the *previous* ts to compute the elapsed slot
	// count). Also stores the result in last_role.
	bt_role_t update_role(long long ts_now_us);
	void update_role_from_header(uint8_t packet_type, uint8_t lt_addr, uint32_t clk);
	// NEW: role determination from clk parity. Unlike an earlier version of
	// this, this does NOT track cumulative timing -- it reads the role
	// directly off whichever `clk` value the state machine already resolved
	// for this specific packet (btsniffer.cpp already recomputes CLK[1:6]
	// from scratch, via HEC-validated brute force or HEC-validated
	// prediction, for every single packet -- there's no need to track
	// anything across packets, and doing so was the bug: any dropped
	// packet desynced a running counter and silently flipped every
	// subsequent role assignment until the next desync corrected it back).
	//
	// btsniffer.cpp's own clk-prediction arithmetic (slots_elapsed =
	// round(time_diff/625.0); predicted_clk = last_clk + slots_elapsed)
	// proves their `clk` increments by 1 per 625us slot, not per 1.25ms
	// slot-pair -- so its LSB *is* CLK1, the exact bit BR/EDR uses to
	// alternate master (CLK1=0) and slave (CLK1=1) transmission slots.
	bt_role_t role_from_clk(uint32_t clk);

	// Which clk-parity value corresponds to "master" for this LAP. Defaults
	// to 0 (CLK1=0 => master, per spec) but gets corrected on the fly the
	// first time we see a packet that can only have come from the master
	// (broadcast LT_ADDR==0, or a POLL packet) -- see confirm_master_parity.
	// This is the "check the header for packet type" cross-check.
	void confirm_master_parity(uint32_t clk);

	
	                           
	// NEW: record RSSI/SNR/CFO for a packet of a given role. Updates both
	// the role-specific last/average fields and the pre-existing
	// role-agnostic last_rssi/last_snr/avg_rssi/avg_snr fields (kept for
	// backward compatibility with anything that already reads those).
	void update_signal_metrics(bt_role_t role, double rssi_dbm,
	                            double snr_db, double cfo_hz);
	
	// --- NEW: slot-parity role tracking ------------------------------------
	// --- NEW: clk-parity role determination --------------------------------
	int master_clk_parity;   // 0 or 1: clk&1 value that means "master" for this LAP
	bool parity_confirmed;   // whether an anchor packet (broadcast/POLL) set this,
	                          // vs. it still being the untested spec-default (0)
	
	uint64_t slot_counter;    // cumulative 625us slots since first packet
	bool     slot_ref_set;    // whether slot_counter has a valid reference
	bt_role_t last_role;      // role of the most recently processed packet

	// --- NEW: per-role RSSI / SNR / CFO (last value + EMA average) --------
	double last_rssi_master, last_rssi_slave;
	double avg_rssi_master,  avg_rssi_slave;
	double last_snr_master,  last_snr_slave;
	double avg_snr_master,   avg_snr_slave;
	double last_cfo_master,  last_cfo_slave;   // Hz
	double avg_cfo_master,   avg_cfo_slave;    // Hz
	bool   has_master_sample, has_slave_sample;

	// Fingerprint signal: difference between master and slave average CFO.
	// Only meaningful once has_master_sample && has_slave_sample are both
	// true; 0.0 until then.
	double cfo_diff_hz;
	// ========================
private:
	lap_status_t state;
	uint32_t lap;
	struct uap_data {
		bool uap_valid;
		uint32_t uap;
		uint32_t clk1;
		uint32_t clk2;
		int clk_index;
	};
	struct uap_data uaps[32];
	long long ts;
	long long prevts;
	bool brute_forced;
	int processed_packets;
	int brute_force_failures;
	int brute_force_noinit;
	int brute_force_noinfo;
	int packet_too_close_confirmed;
	int packet_too_close_notconfirmed;
	int broken_uap;
	int packet_inthepast;
	long long tstart;
};

#endif
