
import logging
import os

import pandas as pd
import numpy as np
import pandana as pdna

from netbuffer.core import buffer
from activitysim.core import tracing
from activitysim.core import config
from activitysim.core import inject
from activitysim.core import pipeline

logger = logging.getLogger(__name__)


@inject.injectable()
def buffer_parcels_spec(configs_dir, buffer_parcels_settings):
    f = os.path.join(
        configs_dir, buffer_parcels_settings['buffer_parcels_spec'])
    return buffer.read_buffer_spec(f)


@inject.injectable()
def buffer_parcels_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'buffer_parcels.yaml')


@inject.step()
def buffer_parcels(settings, buffer_parcels_spec,
                   buffer_parcels_settings,
                   parcel_data, data_dir, output_dir, trace_parcels):

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

    # create pandana network
    network_fname = os.path.join(data_dir, buffer_parcels_settings["buffer_network"])
    network = pdna.Network.from_hdf5(network_fname)
    network.init_pois(num_categories=constants["num_categories"],
                      max_dist=constants["max_dist"], max_pois=constants["max_pois"])

    parcel_data_df = parcel_data.to_frame()
    parcel_data_df.reset_index(level=0, inplace=True)

    # attach the node_id of the nearest network node to each parcel
    parcel_data_df[constants['nodes-id']] = \
        network.get_node_ids(parcel_data_df[constants['parcels-x']].values,
                             parcel_data_df[constants['parcels-y']].values)
    parcel_data_df.set_index(constants['parcel_index'], inplace=True, drop=False)
    inject.add_table('parcel_data', parcel_data_df)

    # trace parcels are specified by parcelid
    if 'trace_parcels' in buffer_parcels_settings:
        trace_parcels = buffer_parcels_settings['trace_parcels']
        trace_parcel_rows = parcel_data_df[constants['parcel_index']].isin(trace_parcels)
    else:
        trace_parcel_rows = None

    # read pois table
    poi_fname = os.path.join(data_dir, buffer_parcels_settings["pois"])
    poi_df = pd.read_csv(poi_fname, index_col=False)
    poi_df[constants['nodes-id']] = network.get_node_ids(poi_df[constants["pois-x"]].values,
                                                         poi_df[constants["pois-y"]].values)

    # intersections:
    # combine from and to columns
    all_nodes = pd.DataFrame(network.edges_df[constants["links-a"]].append(
                             network.edges_df[constants["links-b"]]),
                             columns=[constants['nodes-id']])

    # get the frequency of each node, which is the number of intersecting ways
    intersections_df = pd.DataFrame(all_nodes[constants['nodes-id']].value_counts())
    intersections_df = intersections_df.rename(columns={constants['nodes-id']: 'edge_count'})
    intersections_df.reset_index(0, inplace=True)
    intersections_df = intersections_df.rename(columns={'index': constants['nodes-id']})

    # add a column for each way count
    intersections_df['nodes1'] = np.where(intersections_df['edge_count'] == 1, 1, 0)
    intersections_df['nodes2'] = np.where(intersections_df['edge_count'] == 2, 1, 0)
    intersections_df['nodes3'] = np.where(intersections_df['edge_count'] == 3, 1, 0)
    intersections_df['nodes4p'] = np.where(intersections_df['edge_count'] > 3, 1, 0)

    locals_d = {
        'network': network,
        'node_id': constants['nodes-id'],
        'parcels_df': parcel_data_df,
        'intersections_df': intersections_df,
        'poi_df': poi_df,
        'poi_x': constants['pois-x'],
        'poi_y': constants['pois-y'],
        'parcel_index': constants['parcel_index']
    }

    if constants is not None:
        locals_d.update(constants)

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(buffer_parcels_spec, 'parcels_df',
                                  locals_d, trace_rows=trace_parcel_rows)
    results = results.fillna(0)

    # since parcels_data_df is local in buffer.py, it can come back with extra columns:
    del_cols = list(set(parcel_data_df.columns) - set(parcel_data.to_frame().columns))
    parcel_data_df = parcel_data_df.drop(del_cols, axis=1)

    for column in results.columns:
        inject.add_column("parcel_data", column, results[column])

    if trace_parcels:

        if trace_results is None:
            logger.warn("trace_parcels not found in parcels = %s" % (trace_parcels))
        else:
            df = parcel_data_df.loc[parcel_data_df[trace_parcel_rows].index]
            df = df.merge(trace_results, how='left', left_index=True, right_index=True)

            tracing.trace_df(df,
                             label='buffered_parcels',
                             index_label='None',
                             slicer='NONE',
                             warn_if_empty=True)

            if trace_assigned_locals:
                tracing.write_locals(df, file_name="netbuffer_locals")

    # write result
    buffered_parcels_fname = os.path.join(output_dir, buffer_parcels_settings["parcel_output_file"])
    pipeline.get_table("parcel_data").to_csv(buffered_parcels_fname, index=False)


@inject.injectable()
def create_network_settings(configs_dir):
    return config.read_model_settings(configs_dir, 'create_network.yaml')


@inject.step()
def create_network(settings, create_network_settings, data_dir):

    """
    Build a Pandana network from CSV files
    """

    # create pandana network
    logger.info("Running create_network")
    nodes = pd.read_csv(os.path.join(data_dir, create_network_settings["nodes"]))
    links = pd.read_csv(os.path.join(data_dir, create_network_settings["links"]))

    nodes.index = nodes[create_network_settings["nodes-id"]]

    network = pdna.Network(nodes[create_network_settings["nodes-x"]],
                           nodes[create_network_settings["nodes-y"]],
                           links[create_network_settings["links-a"]],
                           links[create_network_settings["links-b"]],
                           links[[create_network_settings["links-impedance"]]],
                           twoway=create_network_settings["twoway"])

    network.save_hdf5(os.path.join(data_dir, create_network_settings["output"]))
