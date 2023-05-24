import sys
import os
import time
import dask
import numpy as np
import pandas as pd
import argparse
from astropy.time import Time
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

def visibilities(x, **kwargs):
    #t_vis = Time(x.TIME / 86400.0, format='mjd')
    #t = t_vis.mjd
    t = x.TIME / 86400.0
    channel_ids = kwargs["channel_ids"]
    beam_ids    = kwargs["beam_ids"]
    freqs       = kwargs["freqs"]
    #print(f"-D- t = {t}, channel_ids = {channel_ids}, beam_ids = {beam_ids}, freqs = {freqs}")

    beam_id_0 = x.ANTENNA1.data #sub_table.getcol("ANTENNA1")  # (N_entry,)
    #print("-D- beam_id_0 =", beam_id_0, beam_id_0.dtype)
    beam_id_1 = x.ANTENNA2.data #sub_table.getcol("ANTENNA2")  # (N_entry,)
    data_flag = x.FLAG.data     #sub_table.getcol("FLAG")      # (N_entry, N_channel, 4)
    data = x.DATA.data          #sub_table.getcol(column)      # (N_entry, N_channel, 4)

    # We only want XX and YY correlations
    data = np.average(data[:, :, [0, 3]], axis=2)[:, channel_ids]
    data_flag = np.any(data_flag[:, :, [0, 3]], axis=2)[:, channel_ids]
    
    # Set broken visibilities to 0
    data[data_flag] = 0
    
    # DataFrame description of visibility data.
    # Each column represents a different channel.
    if 1 == 0:
        S_full_idx = pd.MultiIndex.from_arrays((beam_id_0.compute(), beam_id_1.compute()), names=("B_0", "B_1"))
        S_full = pd.DataFrame(data=data.compute(), columns=channel_ids, index=S_full_idx)
    else:
        S_full_idx = pd.MultiIndex.from_arrays((beam_id_0, beam_id_1), names=("B_0", "B_1"))
        S_full = pd.DataFrame(data=data, columns=channel_ids, index=S_full_idx)
  
    # Drop rows of `S_full` corresponding to unwanted beams.
    #as arg# beam_id = np.unique(self.instrument._layout.index.get_level_values("STATION_ID"))
    N_beam = len(beam_ids)
    #print("-D-", N_beam)
    #print("-D-", beam_ids)
    i, j = np.triu_indices(N_beam, k=0)
    wanted_index = pd.MultiIndex.from_arrays((beam_ids[i], beam_ids[j]), names=("B_0", "B_1"))
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
        data=np.zeros((N_diff, len(channel_ids)), dtype=data.dtype),
        columns=channel_ids,
        index=index_diff,
    )

    S = pd.concat([S_trunc, S_fill_in], axis=0, ignore_index=False).sort_index(
        level=["B_0", "B_1"]
    )

    # Break S into columns and stream out
    vis_ = []
    beam_idx = pd.Index(beam_ids, name="BEAM_ID")
    for ch_id in channel_ids:
        v = measurement_set._series2array(S[ch_id].rename("S", inplace=True))
        visibility = vis.VisibilityMatrix(v, beam_idx)
        vis_.append([t, freqs[ch_id], visibility])

    return vis_



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

    channel_ids = [0]


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

    beam_ids = np.unique(ms.instrument._layout.index.get_level_values("STATION_ID"))
    #print("-D- beam_id =\n", beam_ids)

    freqs = np.array(ms.channels["FREQUENCY"].value)
    #print("-D- freqs =", freqs)

    n_epochs = len(ms.time)
    n_epochs = 60 #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    print("-I- n_epochs =", n_epochs)



    # Run reference solution for first few epochs
    # ===========================================
    ts = time.time()
    if 1 == 0:
        stop = n_epochs
        i = 0
        ts_ = time.time()
        for t, f, S in ms.visibilities(channel_id=channel_ids, time_id=slice(0, stop, 1), column="DATA"):
            print(f"  ... {t} took {time.time()- ts_:.3f}")
            if i < 2:
                print(f"-D- Ref S[{i}] = {S}\n")
            i += 1
            ts_ = time.time()
    te = time.time()
    print(f"-I- reference implemenation took {te - ts:.3f} sec to process {n_epochs} epochs.")
    #print(ms.time)
   

    # Run Dask solution on all epochs
    # ===============================
    print(f"-I- Will preprocess visibilities for {n_epochs} epochs with Dask.")
    ts = time.time()

    ts_ = time.time()
    columns=["TIME", "ANTENNA1", "ANTENNA2", "FLAG", "DATA"]
    datasets = xds_from_table(args.ms_file, chunks={'row': 140000}, columns=columns,
                              group_cols=["TIME"], index_cols=["TIME"])
    #group_cols=["TIME"], index_cols=["TIME", "ANTENNA1", "ANTENNA2"])
    te = time.time()
    print(f"-I- dask-ms::xds_from_table took {te - ts_:.3f} sec.")

    #print("datasets:\n", datasets)
    print(" ====================================================== ")
    print("-I- dataset[0] =\n", datasets[0]);
    #print(datasets[0].ANTENNA1.data)
    #print(dask.compute(datasets[0].ANTENNA1.data))
    #print(datasets[0].ANTENNA2.data)
    #print(dask.compute(datasets[0].ANTENNA2.data))
    #print(" -------------------------  ...  ---------------------- ")
    #print("-I- dataset[-1] =\n", datasets[-1]);
    #print(datasets[-1].ANTENNA1.data)
    #print(datasets[-1].ANTENNA2.data)
    print(" ====================================================== ")

    ts_ = time.time()
    #client = Client(n_workers=20, threads_per_worker=1)
    client = Client(n_workers=10, threads_per_worker=1, memory_limit='18GB',
                    local_directory="/tmp/dask-eo",
                    timeout="30s")
    print(f"-D- Setting up Dask cluster took {time.time() - ts_:.3f} sec.")
    print(client)

    
    ts_ = time.time()

    """
    X = []
    for i in range(n_epochs):
        X.append(datasets[i])
    futures = client.map(visibilities, X, channel_ids=channel_ids, beam_ids = beam_ids, freqs = freqs)
    res = client.gather(futures)
    """

    futures = [dask.delayed(visibilities)(x, channel_ids=channel_ids, beam_ids = beam_ids, freqs = freqs) for x in datasets]
    res = dask.compute(futures)
    
    te = time.time()
    print(f"-I- dask.compute(visibilities) took {te - ts_:.3f} sec to process {n_epochs} epochs.")
    print(f"-I- Overall Dask computation took {te - ts:.3f} sec (setup + processing)")

    print(len(res))
    res = res[0]
    print(res)

    print("-I- Sleeping a bit...")
    time.sleep(60)

    #EO: To check visually, limit the processing to the first 2-3 epochs
    #print("-W- Order very likely wrong, check that. Just to get an impression.")
    #print("-D- res =\n", res[0], "\n", res[1])
