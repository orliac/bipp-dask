import sys
import os
import time
import argparse
import dask
import dask.array as da
import numpy as np
import pandas as pd
from dask.distributed import LocalCluster, Client
from daskms import xds_from_ms
from bipp import measurement_set
from bipp import statistics as vis
import xarray


def parse_args(args):
    p = argparse.ArgumentParser()
    p.add_argument("--ms")
    p.add_argument("--nsta", type=int, default=None)
    p.add_argument("--desired-rows", type=int, default=10000)
    p.add_argument("--dask_workers", type=int, default=1)
    p.add_argument("--dask_tasks_per_worker", type=int, default=1)
    p.add_argument("--telescope", help="Observing instrument in use",
                   choices=['SKALOW', 'LOFAR'], required=True)

    return p.parse_args(args)


class Application:

    def __init__(self, args, free_mb, ms):
        self.args    = args
        self.free_mb = free_mb
        self.ms      = ms


    def dataset_row_chunks(self, datasets):

        unique_values = [da.unique(ds.TIME.data, return_counts=True) for ds in datasets]
        #print("unique_values =\n", dask.compute(unique_values))

        monotonic = [da.all(ds.TIME.data) for ds in datasets]

        # Compute unique properties as well as monotonicity
        # for each dataset's TIME column at once.
        unique_values, monotonic = dask.compute(unique_values, monotonic)

        if not np.all(monotonic):
            raise ValueError("TIME column is not monotonic")

        row_chunks = []

        # Total number of epochs
        n_epochs = 0
        for i, j in unique_values:
            n_epochs += len(i)
        epoch_ranges = np.array_split(np.arange(n_epochs), self.dask_tasks, axis=0)
    
        for unique_time, counts in unique_values:
            uc = list(zip(unique_time, counts))
            dataset_rows = []
            for r in epoch_ranges:
                range_size = 0
                for i in range(r[0], r[0]+len(r)):
                    range_size += uc[i][1]
                #print("range_size =", range_size)
                #row_chunks.append(range_size)
                dataset_rows.append(range_size)

            row_chunks.append(tuple(dataset_rows))
            
        return row_chunks


    def run(self):

        beam_ids = np.unique(self.ms.instrument._layout.index.get_level_values("STATION_ID"))
        n_beams = len(beam_ids)

        freqs = np.array(self.ms.channels["FREQUENCY"].value)

        t_tot_s = time.time()

        ts = time.time()
        datasets = xds_from_ms(self.args.ms, columns=["TIME"])
        t_xds_from_ms_1 = time.time() - ts

        # Cannot use instrument._layout info, as it can be disconnected from what
        # is actually contained in the MS
        n_rows = datasets[0].dims['row']
        dask_tasks = self.args.dask_workers * self.args.dask_tasks_per_worker
        self.dask_tasks = dask_tasks
        #print("-I- n rows =", n_rows)
        #print("-I- self.args.dask_workers          =", self.args.dask_workers)
        #print("-I- self.args.dask_tasks_per_worker =", self.args.dask_tasks_per_worker)
        #print("-I- self.dask_tasks                 =", self.dask_tasks)
        
        row_chunks = self.dataset_row_chunks(datasets)
        print("-I- row_chuncks =", row_chunks)
        
        n_chunks = len(row_chunks[0])
        if n_chunks < dask_tasks:
            raise Warning(f"Number of chunks < dask_tasks")
        elif n_chunks % dask_tasks != 0:
            raise Exception(f"number of tasks ({n_chunks}) not a multiple of the number of Dask tasks ({dask_tasks})")

        assert(np.sum(row_chunks) == n_rows)

        # Now open each dataset with the derived row chunking schema
        ts = time.time()
        datasets = xds_from_ms(self.args.ms, chunks=[{"row": rc for rc in list(row_chunks)}])
        #print("-I- datasets =\n", datasets)
        t_xds_from_ms_2 = time.time() - ts

        ts = time.time()
        cluster = LocalCluster(n_workers=self.args.dask_workers,
                               threads_per_worker=1,
                               memory_limit=str(int(self.free_mb * 0.8 / self.args.dask_workers))+'MB',
                               local_directory="/tmp/dask-eo",
                               timeout="30s",
                               processes = True)
        #print(cluster)
        client = Client(cluster)
        #print(client)
        t_dask_cluster = time.time() - ts

        operations = []

        ts = time.time()
        for ds in datasets:

            # We only want XX and YY correlations
            red_data = np.average(ds.DATA.data[:,0,[0,3]], axis=1)
            #print("red_data.shape =", red_data.shape)
            red_flag = np.any(ds.FLAG.data[:,0,[0,3]], axis=1)
            #print("red_flag.shape =", red_flag.shape)
            op = da.map_blocks(_map3, ds.TIME.data,
                               ds.ANTENNA1.data,
                               ds.ANTENNA2.data,
                               red_data,
                               red_flag,
                               beam_id=beam_ids,
                               new_axis=([1,2]),
                               meta=np.array((), dtype=np.complex64))
            operations.append(op)
        t_dask_map_blocks = time.time() - ts

        ts = time.time()
        res = dask.compute(operations)
        #print(res)
        t_dask_compute = time.time() - ts

        t_tot = time.time() - t_tot_s

        print(f"-R- {self.args.dask_workers} {self.args.dask_tasks_per_worker} {n_chunks}: xds_from_ms_1 {t_xds_from_ms_1:.3f} sec, xds_from_ms_2 = {t_xds_from_ms_2:.3f} sec, dask_cluster = {t_dask_cluster:.3f} sec, dask_map_blocks = {t_dask_map_blocks:.3f}, dask_compute = {t_dask_compute:.3f} sec, total = {t_tot:.3f} sec.")



def _map3(time, ant1, ant2, data, flag, beam_id):
    
    utime, inv = np.unique(time, return_inverse=True)
    
    n_times = len(utime)
    n_beams = len(beam_id)

    result = np.empty((n_times, n_beams, n_beams), dtype=np.complex64)

    for t_i, t_t in enumerate(utime):
        
        mask = inv == t_i

        # ant{1,2}, flag and data values for the unique timestep
        ut_a1 = ant1[mask]
        ut_a2 = ant2[mask]
        ut_flag = flag[mask]
        ut_data = data[mask]

        # Set broken visibilities to 0
        ut_data[ut_flag] = 0
        #print(type(ut_a1), ut_a1.shape)
        S_full_idx = pd.MultiIndex.from_arrays((ut_a1, ut_a2), names=("B_0", "B_1"))
        S_full = pd.DataFrame(data=ut_data, index=S_full_idx)

        # Drop rows of `S_full` corresponding to unwanted beams.
        #beam_id = np.unique(self.instrument._layout.index.get_level_values("STATION_ID"))
        N_beam = len(beam_id)
        i, j = np.triu_indices(N_beam, k=0)
        wanted_index = pd.MultiIndex.from_arrays((beam_id[i], beam_id[j]), names=("B_0", "B_1"))
        index_to_drop = S_full_idx.difference(wanted_index)
        S_trunc = S_full.drop(index=index_to_drop)

        index_diff = wanted_index.difference(S_trunc.index)
        N_diff = len(index_diff)

        ### len(channel_id) is 1
        S_fill_in = pd.DataFrame(
            data=np.zeros(N_diff, dtype=data.dtype),
            index=index_diff
        )
        S = pd.concat([S_trunc, S_fill_in], axis=0, ignore_index=False).sort_index(
            level=["B_0", "B_1"]
        )

        # Break S into columns and stream out
        beam_idx = pd.Index(beam_id, name="BEAM_ID")
        v = measurement_set._series2array(S[0].rename("S", inplace=True))
        visibility = vis.VisibilityMatrix(v, beam_idx)
        result[t_i,:,:] = visibility.data

    #print(result.shape)
    return result


if __name__ == "__main__":

    args = parse_args(sys.argv[1:])
    print(args)


    # Getting all memory using os.popen()
    total_memory, used_memory, free_memory = map(
        int, os.popen('free -t -m').readlines()[-1].split()[1:])
    print("-D- total, used, free memory =", total_memory, used_memory, free_memory)
    free_mb = int(os.environ.get('SLURM_MEM_PER_NODE', free_memory))
    
    # Instrument
    if args.ms == None:
        raise Exception("args.ms_file not set.")
    if not os.path.exists(args.ms):
        raise Exception(f"Could not open {args.ms_file}")
    if args.telescope == 'LOFAR':
        ms = measurement_set.LofarMeasurementSet(args.ms, N_station=args.nsta, station_only=True)
    elif args.telescope == 'SKALOW':
        ms = measurement_set.SKALowMeasurementSet(args.ms)
    else:
        raise Exception(f"Unknown telescope {args.telescope}!")

    Application(args, free_mb, ms).run()
