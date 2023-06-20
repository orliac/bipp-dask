import sys
import os
import time
import argparse
import numpy as np
from bipp import measurement_set
import multiprocessing
import shutil

def parse_args(args):
    p = argparse.ArgumentParser()
    p.add_argument("--ms")
    p.add_argument("--telescope", help="Observing instrument in use",
                   choices=['SKALOW', 'LOFAR'], required=True)
    return p.parse_args(args)


def instrument(args):

    # Instrument
    if args.ms == None:
        raise Exception("args.ms not set.")
    if not os.path.exists(args.ms):
        raise Exception(f"Could not open {args.ms}")

    if args.telescope == 'LOFAR':
        ms = measurement_set.LofarMeasurementSet(args.ms, args.nsta, station_only=True)
    elif args.telescope == 'SKALOW':
        ms = measurement_set.SKALowMeasurementSet(args.ms)
    else:
        raise Exception(f"Unknown telescope {args.telescope}!")

    return ms


if __name__ == "__main__":

    args = parse_args(sys.argv[1:])
    print(args)

    t_tot_s = time.time()

    ts = time.time()
    ms = instrument(args)
    t_instrument = time.time() - ts

    ts = time.time()
    n_epochs = len(ms.time)
    t_ms_time = time.time() - ts

    t_bipp_s = time.time()
    for t, f, S in ms.visibilities(channel_id=[0], time_id=slice(0,0,1), column="DATA"):
        print(t)
    t_bipp = time.time() - t_bipp_s

    print(f"-T- BIPP t_instrument {t_instrument:.3f}, t_ms_time {t_ms_time:.3f},  t_bipp {t_bipp:.3f}, bipp_tot = {t_instrument + t_ms_time + t_bipp:.3f}")

    
