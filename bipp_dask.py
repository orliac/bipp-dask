import os
os.environ["OMP_NUM_THREADS"]        = "1"
os.environ["OPENBLAS_NUM_THREADS"]   = "1"
os.environ["MKL_NUM_THREADS"]        = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"]    = "1"
import sys
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


def visibilities(x, **kwargs):
    t = x.TIME / 86400.0
    channel_ids = kwargs["channel_ids"]
    beam_ids    = kwargs["beam_ids"]
    freqs       = kwargs["freqs"]
    #print(f"-D- t = {t}, channel_ids = {channel_ids}, beam_ids = {beam_ids}, freqs = {freqs}")

    beam_id_0 = x.ANTENNA1.data
    beam_id_1 = x.ANTENNA2.data
    data_flag = x.FLAG.data
    data = x.DATA.data

    # We only want XX and YY correlations
    data = np.average(data[:, :, [0, 3]], axis=2)[:, channel_ids]
    data_flag = np.any(data_flag[:, :, [0, 3]], axis=2)[:, channel_ids]
    
    # Set broken visibilities to 0
    data[data_flag] = 0
    
    S_full_idx = pd.MultiIndex.from_arrays((beam_id_0, beam_id_1), names=("B_0", "B_1"))
    S_full = pd.DataFrame(data=data, columns=channel_ids, index=S_full_idx)
  
    # Drop rows of `S_full` corresponding to unwanted beams.
    #as arg# beam_id = np.unique(self.instrument._layout.index.get_level_values("STATION_ID"))
    N_beam = len(beam_ids)
    i, j = np.triu_indices(N_beam, k=0)
    wanted_index = pd.MultiIndex.from_arrays((beam_ids[i], beam_ids[j]), names=("B_0", "B_1"))
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
    parser.add_argument("--bipp_vis", help="Run BIPP visibilities function", required=False,
                        action='store_true')
    parser.add_argument("--n_epochs", help="Number of time steps to process", required=True, type=int)

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
    if args.n_epochs > 0:
        n_epochs = args.n_epochs
    print("-I- n_epochs =", n_epochs)


    # Run reference solution for first few epochs
    # ===========================================
    if args.bipp_vis:
        ts = time.time()
        stop = n_epochs
        i = 0
        ts_ = time.time()
        for t, f, S in ms.visibilities(channel_id=channel_ids, time_id=slice(0, stop, 1), column="DATA"):
            if i < 3 or i >= n_epochs - 3:
                print(f" ... {t.mjd:.7f} took {time.time() - ts_:.3f}")
            i += 1
            ts_ = time.time()
        te = time.time()
        print(f"-I- reference implementation took {te - ts:.3f} sec to process {n_epochs} epochs.")
        #print(ms.time)
   

    # Run Dask solution on all epochs
    # ===============================
    print(f"-I- Will preprocess visibilities for {n_epochs} epochs with Dask.")
    ts = time.time()

    ts_ = time.time()
    columns=["TIME", "ANTENNA1", "ANTENNA2", "FLAG", "DATA"]
    datasets = xds_from_table(args.ms_file, chunks={'row': 140000}, columns=columns,
                              group_cols=["TIME"])
    #                          group_cols=["TIME"], index_cols=["TIME"])
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
    
    cluster = LocalCluster(n_workers=5, threads_per_worker=1, memory_limit='9GB',
                           local_directory="/tmp/dask-eo",
                           timeout="30s",
                           processes = True)
    """
    client = Client(n_workers=5, threads_per_worker=1, memory_limit='9GB',
                    local_directory="/tmp/dask-eo",
                    timeout="30s",
                    processes = True)#, diagnostics_port=None)
    """
    print(f"-D- Setting up Dask cluster took {time.time() - ts_:.3f} sec.")

    client = Client(cluster)
    print(client)

    
    ts_ = time.time()




    sys.exit(0)


    """
    X = []
    for i in range(n_epochs):
        X.append(datasets[i])
    futures = client.map(visibilities, X, channel_ids=channel_ids, beam_ids = beam_ids, freqs = freqs)
    res = client.gather(futures)
    """

    futures = [dask.delayed(visibilities)(x, channel_ids=channel_ids, beam_ids = beam_ids, freqs = freqs) for x in datasets[0:n_epochs]]
    res = dask.compute(futures)
    
    te = time.time()
    print(f"-I- dask.compute(visibilities) took {te - ts_:.3f} sec to process {n_epochs} epochs.")
    print(f"-I- Overall Dask computation took {te - ts:.3f} sec (setup + processing)")

    res = res[0]
    #for r in res:
        #print(r)
        #print(f" .B mjd = {r[0][0]:.6f}")
    for i in list(range(0, 3)) + list(range(n_epochs - 3, n_epochs)):
        print(i)
        print(res[i][0][2].data[0,0:10])
        #print(f" ... {t} took {time.time() - ts_:.3f} S[0,:] =", res[0][0][i][0:10])

    
    #print("-I- Sleeping a bit...")
    #time.sleep(180)

    #EO: To check visually, limit the processing to the first 2-3 epochs
    #print("-W- Order very likely wrong, check that. Just to get an impression.")
    #print("-D- res =\n", res[0], "\n", res[1])
