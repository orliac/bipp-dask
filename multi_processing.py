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
    p.add_argument("--n_proc", help="Number of processors", type=int, default=1)

    return p.parse_args(args)

def visibilities(x):
    args, beg, end, sli, ms_time = x

    print(f"-D- @visib@ {multiprocessing.current_process()} => from {beg} to {end}")

    #EO: very slow, check why
    if 1 == 0:
        # Local copy of MS for each process
        ts = time.time()
        worker_ms = os.path.join('/tmp', f"{multiprocessing.current_process().name}-MS")
        if os.path.exists(worker_ms):
            shutil.rmtree(worker_ms)
        print(f"-D- copying {args.ms} -> {worker_ms}")
        shutil.copytree(args.ms, worker_ms)
        t_copy_ms = time.time() - ts
        args.ms = worker_ms
        print(f"-T- t_copy_ms = {t_copy_ms:.3f}")

    ms = instrument(args)
    ms._time = ms_time

    vis = []
    ii = 0
    for t, f, S in ms.visibilities(channel_id=[0], time_id=slice(beg, end, 1), column="DATA"):
        print(f"{ii} -> {t}")
        vis.append([t,f,S])
        ii += 1
    return vis


def instrument(args):

    # Instrument
    if args.ms == None:
        raise Exception("args.ms not set.")
    if not os.path.exists(args.ms):
        raise Exception(f"Could not open {args.ms}")

    #print("-D- @instr@", multiprocessing.current_process())
    
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
    print(f"-T- t_instrument {t_instrument:.3f}")

    ts = time.time()
    n_epochs = len(ms.time)
    t_ms_time = time.time() - ts
    #print(f"-T- t_n_epochs {t_ms_time:.3f} eq to time_id.indices!")
    
    ts = time.time()
    lst_ = np.array_split(range(n_epochs), args.n_proc)
    lst = []
    for l in lst_:
        lst.append([args, l[0], l[-1]+1, 1, ms.time]) #ms.time already populated
    t_lst = time.time() - ts


    # BIPP multi-processing solution
    t_bipp_mp_s = time.time()
    all_vis = []
    with multiprocessing.get_context("spawn").Pool(args.n_proc) as pool:
        for vis in pool.map(visibilities, lst):
            all_vis.append(vis)
    t_bipp_mp = time.time() - t_bipp_mp_s

    t_tot = time.time() - t_tot_s
    print(f"-T- t_instrument {t_instrument:.3f}, t_ms_time {t_ms_time:.3f}, t_lst {t_lst:.3f}, t_bipp_mp {t_bipp_mp:.3f}, tot {t_tot:.3f}")

    #print(all_vis)
    print("-D- len(all_vis) =", len(all_vis))
    
