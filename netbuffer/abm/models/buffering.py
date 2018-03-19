
import logging
import os

import orca
import pandas as pd
import numpy as np
import pandana as pdna

from netbuffer.core import buffer
from activitysim.core import tracing
from activitysim.core import config


logger = logging.getLogger(__name__)


@orca.injectable()
def buffer_parcels_spec(configs_dir):
    f = os.path.join(configs_dir, 'buffering.csv')
    return buffer.read_buffer_spec(f)


@orca.injectable()
def buffer_parcels_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'buffer_parcels.yaml')


@orca.step()
def buffer_parcels(settings, buffer_parcels_spec,
                   buffer_parcels_settings,
                   parcel_data, data_dir, trace_parcels):

    """
    Performs network buffering (using Pandana libary http://udst.github.io/pandana/)
    for each point in parcel file using expressions from buffer_parcels_spec.

    The actual results depend on the expressions in buffer_parcels_spec, but this is initially
    intended to replicate PSRC's Soundcast (Daysim) parcel accessibilities.

    Using a point file representing the centroid of a land use boundary (parcel), quanitifes
    the amount of some variable within a specified distance or the distance to a variable. For
    example, the total number of jobs within a half mile of each parcel or the distance to the
    nearest bus stop from each parcel.

    """

    logger.info("Running buffer_parcels")

    constants = config.get_model_constants(buffer_parcels_settings)

    #trace_parcels = settings['trace_parcels']
    # create pandana network
    # see pandana documentation to download and store a pandana network specific to the study area.
    network_fname = os.path.join(data_dir, settings["buffer_network"])
    network = pdna.Network.from_hdf5(network_fname)
    network.init_pois(num_categories=constants["num_categories"], 
                      max_dist=constants["max_dist"], max_pois=constants["max_pois"])

    parcel_data_df = parcel_data.to_frame()
    parcel_data_df.reset_index(level=0, inplace=True)

    # attach the node_id of the nearest network node to each parcel
    parcel_data_df['node_id'] = network.get_node_ids(parcel_data_df['xcoord_p'].values,
                                                     parcel_data_df['ycoord_p'].values)
    #parcel_data_df.set_index(constants['parcel_index'], inplace=True)
    
    # trace parcels are specified by parcelid
    if trace_parcels:
        trace_parcel_rows = parcel_data_df.parcelid.isin(trace_parcels)
    else:
        trace_parcel_rows = None

    # TO-FIX-reading the following df directly for now since easier table reading is being added.
    # this table provides the location of transit stops and types of transit they serve
    # used to find distance from parcels to nearest bus, light rail, commuter rail etc.
    # also- might be better to store in the master h5 file.
    poi_fname = os.path.join(data_dir, settings["transit_stops"])
    poi_df = pd.read_csv(poi_fname, index_col=False)
    poi_df['node_id'] = network.get_node_ids(poi_df['x'].values, poi_df['y'].values)

    # intersections:
    # combine from and to columns
    all_nodes = pd.DataFrame(network.edges_df['from'].append(network.edges_df.to),
                             columns=['node_id'])

    # get the frequency of each node, which is the number of intersecting ways
    intersections_df = pd.DataFrame(all_nodes.node_id.value_counts())
    intersections_df = intersections_df.rename(columns={'node_id': 'edge_count'})
    intersections_df.reset_index(0, inplace=True)
    intersections_df = intersections_df.rename(columns={'index': 'node_id'})

    # add a column for each way count
    intersections_df['nodes1'] = np.where(intersections_df['edge_count'] == 1, 1, 0)
    intersections_df['nodes3'] = np.where(intersections_df['edge_count'] == 3, 1, 0)
    intersections_df['nodes4'] = np.where(intersections_df['edge_count'] > 3, 1, 0)

    locals_d = {
        'network': network,
        'parcels_df': parcel_data_df,
        'intersections_df': intersections_df,
        'poi_df': poi_df
    }

    if constants is not None:
        locals_d.update(constants)

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(buffer_parcels_spec, 'parcels_df', locals_d, trace_rows=trace_parcel_rows)

    results = results.fillna(0)

    # since parcels_data_df is local in buffer.py, it can come back with extra columns:
    del_cols = list(set(parcel_data_df.columns) - set(parcel_data.to_frame().columns))
    parcel_data_df = parcel_data_df.drop(del_cols, axis=1)


    #results.reset_index(level = 0, inplace = True)

    for column in results.columns:
        orca.add_column("parcel_data", column, results[column])

    if trace_parcels:

        if trace_results is None:
            logger.warn("trace_parcels not found ini parcels = %s" % (trace_parcels))
        else:
            # 
            df = parcel_data_df.loc[parcel_data_df[trace_parcel_rows].index]
            df = df.merge(trace_results, how = 'left', left_index = True, right_index = True)
            
            tracing.trace_df(df,
                             label='buffered_parcels',
                             index_label='None',
                             slicer='NONE',
                             warn_if_empty=True)

            if trace_assigned_locals:
                tracing.write_locals(df, file_name="accessibility_locals")

        

   
