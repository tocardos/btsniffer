import numpy as np
import matplotlib.pyplot as plt
from scipy.signal import firwin, freqz

# Filter Parameters
fs = 40e6          # 40 MS/s
cutoff = 0.51e6    # 500 kHz cutoff (1 MHz total RF passband)
numtaps = 121       # 95 taps
beta = 4.7         # Kaiser beta ~ 65 dB ultimate stopband attenuation

# 1. Design the FIR Filter
taps = firwin(
    numtaps,
    cutoff=cutoff,
    fs=fs,
    window=('kaiser', beta)
    #window=('hamming')
)

# 2. Print C++ Array Output
print(f"// Generated {numtaps} taps (Kaiser beta = {beta})")
print("const double filter_taps[] = {")
print(",\n".join(f"    {c:.18e}" for c in taps))
print("};")

# 3. Calculate Frequency Response using freqz
w, h = freqz(taps, worN=8192, fs=fs)

# Convert magnitude to dB
magnitude_db = 20 * np.log10(np.abs(h) + 1e-12) # Avoid log(0)
phase_deg = np.unwrap(np.angle(h)) * (180.0 / np.pi)

# 4. Plot Frequency and Phase Response
plt.figure(figsize=(10, 6))

# Magnitude Plot
plt.subplot(2, 1, 1)
plt.plot(w / 1e6, magnitude_db, 'b', linewidth=1.5)
plt.axvline(cutoff / 1e6, color='r', linestyle='--', label=f'Cutoff ({cutoff/1e6:.2f} MHz)')
plt.axvline(1.0, color='g', linestyle=':', label='Adjacent Channel (+1.0 MHz)')
plt.title(f'FIR Low-Pass Filter Frequency Response ({numtaps} Taps, Kaiser $\\beta$={beta})')
plt.ylabel('Magnitude [dB]')
plt.xlabel('Frequency [MHz]')
plt.ylim([-90, 5])
plt.xlim([0, 3.0]) # Focus on passband and first adjacent channels
plt.grid(True, which='both', linestyle='--', alpha=0.6)
plt.legend(loc='upper right')

# Phase Plot
plt.subplot(2, 1, 2)
plt.plot(w / 1e6, phase_deg, 'g', linewidth=1.5)
plt.ylabel('Phase [Degrees]')
plt.xlabel('Frequency [MHz]')
plt.xlim([0, 3.0])
plt.grid(True, which='both', linestyle='--', alpha=0.6)

plt.tight_layout()
plt.show()