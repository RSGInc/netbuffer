
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
def buffer_zones_spec(buffer_zones_settings):
    spec_path = config.config_file_path(buffer_zones_settings['buffer_zones_spec'])
    return buffer.read_buffer_spec(spec_path)


@inject.injectable()
def buffer_zones_settings():
    return config.read_model_settings('buffer_zones.yaml')


@inject.step()
def buffer_zones(settings, buffer_zones_spec,
                 buffer_zones_settings,
                 zone_data, trace_zones, network):

    """
    Performs network buffering (using Pandana libary http://udst.github.io/pandana/)
    for each point in zone file using expressions from buffer_zones_spec.

    The actual results depend on the expressions in buffer_zones_spec, but this is initially
    intended to replicate PSRC's Soundcast (Daysim) zone accessibilities.

    Using a point file representing the centroid of a land use boundary (zone), quanitifes
    the amount of some variable within a specified distance or the distance to a variable. For
    example, the total number of jobs within a half mile of each zone or the distance to the
    nearest bus stop from each zone.

    The following configs should be set in buffer_zones.yaml:

    - buffer_zones_spec: expressions file
    - pois: Point of Interest file

    The CONSTANTS hash is made availabe to the expressions parser.

    - CONSTANTS:
      - max_pois: integer, maxitems used in nearest pois calculation
      - pois-x: longitude column in pois file
      - pois-y: latitude column in pois file

    """

    logger.info('Running buffer_zones')

    constants = config.get_model_constants(buffer_zones_settings)

    zones_df = zones_with_network_nodes(zone_data, network, settings)
    poi_df = read_pois_table(buffer_zones_settings, network, constants)
    intersections_df = get_intersections(network)

    locals_d = {
        'network': network,
        'node_id': 'net_node_id',
        'zones_df': zones_df,
        'intersections_df': intersections_df,
        'poi_df': poi_df,
        'poi_x': constants['pois-x'],
        'poi_y': constants['pois-y'],
        'max_dist': settings['max_dist'],
        'max_pois': constants['max_pois']
    }

    if constants is not None:
        locals_d.update(constants)

    if 'trace_zones' in buffer_zones_settings:
        trace_zones = buffer_zones_settings['trace_zones']
        trace_zone_rows = zones_df.index.isin(trace_zones)
    else:
        trace_zone_rows = None

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(buffer_zones_spec, 'zones_df',
                                  locals_d, trace_rows=trace_zone_rows)
    results.fillna(0, inplace=True)
    add_results_to_zones(results, zones_df, zone_data)

    if trace_zones:
        write_trace_data(trace_results, trace_zones, zones_df,
                         trace_assigned_locals, trace_zone_rows)


def zones_with_network_nodes(zone_data, network, settings):
    zones_df = zone_data.to_frame()

    # attach the node_id of the nearest network node to each zone
    if 'net_node_id' not in zones_df.columns:
        zones_df['net_node_id'] = \
            network.get_node_ids(zones_df[settings['zones_lon']],
                                 zones_df[settings['zones_lat']])
        inject.add_column('zone_data', 'net_node_id', zones_df['net_node_id'])

    return zones_df


def read_pois_table(buffer_zones_settings, network, constants):
    poi_fname = config.data_file_path(buffer_zones_settings['pois'])
    poi_df = pd.read_csv(poi_fname, index_col=False)
    poi_df['net_node_id'] = network.get_node_ids(poi_df[constants['pois-x']].values,
                                                 poi_df[constants['pois-y']].values)

    return poi_df


def get_intersections(network):
    # intersections:
    # combine from and to columns
    all_nodes = pd.DataFrame(network.edges_df['from'].append(
                             network.edges_df['to']),
                             columns=['net_node_id'])

    # get the frequency of each node, which is the number of intersecting ways
    intersections_df = pd.DataFrame(all_nodes['net_node_id'].value_counts())
    intersections_df = intersections_df.rename(columns={'net_node_id': 'edge_count'})
    intersections_df.reset_index(0, inplace=True)
    intersections_df = intersections_df.rename(columns={'index': 'net_node_id'})

    # add a column for each way count
    intersections_df['nodes1'] = np.where(intersections_df['edge_count'] == 1, 1, 0)
    intersections_df['nodes2'] = np.where(intersections_df['edge_count'] == 2, 1, 0)
    intersections_df['nodes3'] = np.where(intersections_df['edge_count'] == 3, 1, 0)
    intersections_df['nodes4p'] = np.where(intersections_df['edge_count'] > 3, 1, 0)

    return intersections_df


def write_trace_data(trace_results, trace_zones, zones,
                     trace_assigned_locals, trace_zone_rows):
    if trace_results is None:
        logger.warn('trace_zones not found in zones = %s' % (trace_zones))
        return

    df = zones.loc[zones[trace_zone_rows].index]
    df = df.merge(trace_results, how='left', left_index=True, right_index=True)

    tracing.trace_df(df,
                     label='buffered_zones',
                     index_label='None',
                     slicer='NONE',
                     warn_if_empty=True)

    if trace_assigned_locals:
        tracing.write_locals(df, file_name='netbuffer_locals')


def add_results_to_zones(results, zones, zone_data_table):
    # since zones_data_df is local in buffer.py, it can come back with extra columns:
    del_cols = list(set(zones.columns) - set(zone_data_table.to_frame().columns))
    zones.drop(del_cols, axis=1, inplace=True)

    for column in results.columns:
        inject.add_column('zone_data', column, results[column])
