from scipy.signal import firwin

#fs = 24e6 
cutoff = 0.45e6
#numtaps = 49 # for 24e6
fs = 40e6
numtaps = 81  # for 40e6
#numtaps = 65  # for 32e6
taps = firwin(
    numtaps,
    cutoff=cutoff,
    fs=fs,
    window=('kaiser', 7.5)
)

#print(taps)
print(",\n".join(f"{c:.18e}" for c in taps))