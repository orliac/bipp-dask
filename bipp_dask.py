import sys
import os
import time
import dask
import numpy as np
import pandas as pd
import argparse
from dask.distributed import LocalCluster, Client
from daskms import xds_from_ms, xds_from_table
from bipp import measurement_set
import bipp.statistics as vis

#import dask
#dask.config.set(scheduler='threads')

#import dask.multiprocessing
#dask.config.set(scheduler='processes')  # overwrite default with multiprocessing scheduler

def print_ms_data(x):
    print(x.DATA.data)
    print(x.FLAG.data)

def visibilities(x, channel_id, beam_id, all_vis):
    print(x.TIME)
    beam_id_0 = x.ANTENNA1.data #sub_table.getcol("ANTENNA1")  # (N_entry,)
    #print(x.ANTENNA2.data)
    beam_id_1 = x.ANTENNA2.data #sub_table.getcol("ANTENNA2")  # (N_entry,)
    data_flag = x.FLAG.data     #sub_table.getcol("FLAG")      # (N_entry, N_channel, 4)
    data = x.DATA.data          #sub_table.getcol(column)      # (N_entry, N_channel, 4)

    # We only want XX and YY correlations
    data = np.average(data[:, :, [0, 3]], axis=2)[:, channel_id]
    data_flag = np.any(data_flag[:, :, [0, 3]], axis=2)[:, channel_id]
    
    # Set broken visibilities to 0
    data[data_flag] = 0
            
    # DataFrame description of visibility data.
    # Each column represents a different channel.
    S_full_idx = pd.MultiIndex.from_arrays((beam_id_0, beam_id_1), names=("B_0", "B_1"))
    #print(data)
    #print(channel_id)
    #print(S_full_idx)
    S_full = pd.DataFrame(data=data, columns=channel_id, index=S_full_idx)

    # Drop rows of `S_full` corresponding to unwanted beams.
    #as arg# beam_id = np.unique(self.instrument._layout.index.get_level_values("STATION_ID"))
    N_beam = len(beam_id)
    #print("-D-", N_beam)
    #print("-D-", beam_id)
    i, j = np.triu_indices(N_beam, k=0)
    wanted_index = pd.MultiIndex.from_arrays((beam_id[i], beam_id[j]), names=("B_0", "B_1"))
    #print("-D- wanted_index =\n", wanted_index)
    index_to_drop = S_full_idx.difference(wanted_index)
    S_trunc = S_full.drop(index=index_to_drop)
    
    # Depending on the dataset, some (ANTENNA1, ANTENNA2) pairs that have correlation=0 are
    # omitted in the table.
    # This is problematic as the previous DataFrame construction could be potentially
    # missing entire antenna ranges.
    # To fix this issue, we augment the dataframe to always make sure `S_trunc` matches the
    # desired shape.
    index_diff = wanted_index.difference(S_trunc.index)
    N_diff = len(index_diff)
    #print("-D- N_diff =", N_diff)
    
    S_fill_in = pd.DataFrame(
        data=np.zeros((N_diff, len(channel_id)), dtype=data.dtype),
        columns=channel_id,
        index=index_diff,
    )

    S = pd.concat([S_trunc, S_fill_in], axis=0, ignore_index=False).sort_index(
        level=["B_0", "B_1"]
    )

    # Break S into columns and stream out
    vis_ = []
    beam_idx = pd.Index(beam_id, name="BEAM_ID")
    for ch_id in channel_id:
        v = measurement_set._series2array(S[ch_id].rename("S", inplace=True))
        visibility = vis.VisibilityMatrix(v, beam_idx)
        vis_.append([t, freq[ch_id], visibility])

    all_vis.append(vis)


def check_args(args):
    print("-I- command line arguments =", args)
    parser = argparse.ArgumentParser(args)
    parser.add_argument("--ms_file", help="Path to MS file to process", required=True)
    parser.add_argument("--telescope", help="Observing instrument in use",
                        choices=['SKALOW', 'LOFAR'], required=True)
    parser.add_argument("--nsta", help="Number of stations in simulation",  #EO: should defaults to None == all stations
                        required=False, type=int)

    args = parser.parse_args()

    # 0 station means all stations
    if args.nsta == 0: args.nsta = None

    return args


if __name__ == '__main__':

    args = check_args(sys.argv)
    #sys.exit(0)

    client = Client(n_workers=20, threads_per_worker=1)  # start distributed scheduler locally.
    #cluster = LocalCluster()  # Launches a scheduler and workers locally
    #client = Client(cluster)


    #MS="/work/ska/orliac/RADIOBLOCKS/EOS_21cm-gf_202MHz_4h1d_1000.MS"   # 17 GB
    #MS="/home/orliac/SKA/epfl-radio-astro/bipp-bench/gauss4_t201806301100_SBL180.MS"
    #if not os.path.isdir(MS):
    #    raise Exception(f"MS dataset {MS} not found.")
    #    print(f"-I- MS: {MS}")

    # Instrument
    if args.ms_file == None:
        raise Exception("args.ms_file not set.")
    if not os.path.exists(args.ms_file):
        raise Exception(f"Could not open {args.ms_file}")
    if args.telescope == 'LOFAR':
        ms = measurement_set.LofarMeasurementSet(args.ms_file, args.nsta, station_only=True)
    elif args.telescope == 'SKALOW':
        ms = measurement_set.SKALowMeasurementSet(args.ms_file)
    else:
        raise Exception(f"Unknown telescope {args.telescope}!")

    beam_id = np.unique(ms.instrument._layout.index.get_level_values("STATION_ID"))
    #print(beam_id)

    print("-I- start working now...")
    ts = time.time()
    columns=["TIME", "ANTENNA1", "ANTENNA2", "FLAG", "DATA"]
    #datasets = xds_from_table(MS, chunks={'row': 100000}, columns=columns, group_cols=["TIME"])
    datasets = xds_from_table(args.ms_file, chunks={'row': 100000}, columns=columns, group_cols=["TIME"])
    te = time.time()
    print(f"-I- dask-ms::xds_from_table took {te - ts:.3f} sec.")

    #print("datasets:\n", datasets)
    print(" ====================================================== ")
    print("-I- dataset[0] =\n", datasets[0]);
    print(datasets[0].ANTENNA1.data)
    #print(dask.compute(datasets[0].ANTENNA1.data))
    print(datasets[0].ANTENNA2.data)
    #print(dask.compute(datasets[0].ANTENNA2.data))
    print(" -------------------------  ...  ---------------------- ")
    print("-I- dataset[-1] =\n", datasets[-1]);
    print(datasets[-1].ANTENNA1.data)
    print(datasets[-1].ANTENNA2.data)
    print(" ====================================================== ")

    #dask.compute([dask.delayed(print_ms_data)(x) for x in datasets])

    ts = time.time()
    channel_ids = [0]
    all_vis = []
    dask.compute([dask.delayed(visibilities)(x, channel_ids, beam_id, all_vis) for x in datasets])
    #dask.compute([dask.delayed(visibilities)(x, channel_ids, beam_id) for x in (datasets[0], datasets[1])])
    te = time.time()
    print(f"-I- dask.compute(visibilities) took {te - ts:.3f} sec.")

    print("-D- all_vis =\n", all_vis)

    sys.exit(0)
    for ds in datasets:
        print(dask.compute(ds.DATA.data, ds.FLAG.data))

    
    #ds = datasets[0]

    #print("-I- Sleeping 3 minutes...")
    #time.sleep(180)
