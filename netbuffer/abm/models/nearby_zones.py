import logging
import os

import pandana as pdna
import pandas as pd
import numpy as np
import pyproj

from netbuffer.core import buffer
from activitysim.core import tracing
from activitysim.core import config
from activitysim.core import inject
from activitysim.core import pipeline

logger = logging.getLogger(__name__)


@inject.step()
def nearby_zones(zone_data, network, settings):
    """
    Calculates distances to nearby zones for each zone
    in the zone_data table. The 'max_dist' setting determines
    which zones are included in the calculation.

    Saves a 'nearby_zones' table to the pipeline with
    from/to/total_dist columns.
    """
    logger.debug('Running nearby_zones')

    # get nearest network node and distance
    zones = get_nearest_network_nodes(zone_data, network, settings)

    # for each network node, count zones within buffer
    logger.debug('counting zones within buffer')
    network.set(zones['net_node_id'], name='zone')
    zones_for_each_net_node = network.aggregate(settings['max_dist'], type='count', name='zone')
    nearby_zone_count = list(zones_for_each_net_node.loc[zones['net_node_id']].astype(int))

    # for each network node, get the zones within the buffer and their distances
    logger.debug('getting nearest pois')
    max_num_pois = int(max(nearby_zone_count))
    network.set_pois(category='zone',
                     maxdist=settings['max_dist']+1,
                     maxitems=max_num_pois+1,
                     x_col=zones[settings['zones_lon']],
                     y_col=zones[settings['zones_lat']])

    all_net_nodes = network.nearest_pois(settings['max_dist'], 'zone',
                                         num_pois=max_num_pois,
                                         include_poi_ids=True)

    # keep only zone snap nodes
    nodes_to_keep = zones['net_node_id'].value_counts().index
    near_zones = all_net_nodes.loc[all_net_nodes.index.isin(nodes_to_keep)]
    zone_pairs = build_zone_pairs_df(near_zones, max_num_pois, zones, settings)

    pipeline.replace_table('nearby_zones', zone_pairs)


def get_nearest_network_nodes(zone_data, network, settings):
    """
    Updates zone_data table with network data.

    Adds the nearest net_node_id for each zone and calculates the
    distance from the zone centroid to the network node.
    """
    logger.debug('saving network info to zones_df')
    zones = zone_data.to_frame()
    zones['net_node_id'] = network.get_node_ids(zones[settings['zones_lon']],
                                                zones[settings['zones_lat']])
    zones['net_node_x'] = list(network.nodes_df.loc[zones['net_node_id']].x)
    zones['net_node_y'] = list(network.nodes_df.loc[zones['net_node_id']].y)

    # Use pyproj's latlong projection
    geod = pyproj.Geod(ellps='WGS84')
    lon1 = np.asarray(zones[settings['zones_lon']])
    lat1 = np.asarray(zones[settings['zones_lat']])
    lon2 = np.asarray(zones['net_node_x'])
    lat2 = np.asarray(zones['net_node_y'])
    _, _, net_node_dist = geod.inv(lon1, lat1, lon2, lat2)

    units = settings.get('distance_units', 'meters')
    assert units in ['meters', 'miles'], "'distance_units' setting must be 'meters' or 'miles'"

    zones['net_node_dist'] = net_node_dist / 1609.34 if units == 'miles' else net_node_dist

    inject.add_table('zone_data', zones, replace=True)

    return zones


def build_zone_pairs_df(near_zones, max_num_pois, zones, settings):
    """
    Pandana's `nearest_pois` returns a DataFrame with
        index: node_id
        column[i]: distance from index node to ith closest poi (zone)
        column[i*2]: zone id for ith closest poi
    """
    logger.debug('building zone pairs distance table')
    dists_cols = near_zones.columns[:max_num_pois]
    zones_cols = near_zones.columns[max_num_pois:max_num_pois*2]
    dists_df = near_zones[dists_cols].copy()
    zones_df = near_zones[zones_cols].copy()

    zone_pairs = pd.DataFrame()
    zone_pairs['a_node_id'] = np.repeat(near_zones.index, max_num_pois)
    zone_pairs['b_zone_id'] = zones_df.values.reshape(len(near_zones.index) * max_num_pois)
    zone_pairs['node_to_node_dist'] = dists_df.values.reshape(len(near_zones.index) * max_num_pois)

    # drop rows with dist==max_dist and with no to_zone
    zone_pairs = zone_pairs[zone_pairs.node_to_node_dist != settings['max_dist']].dropna()
    zone_pairs['b_zone_id'] = zone_pairs['b_zone_id'].astype('int')

    # add zone/node ids
    zone_pairs['a_zone_id'] = zone_pairs['a_node_id'].map(dict(zip(zones.net_node_id, zones.index)))
    zone_pairs['b_node_id'] = zone_pairs['b_zone_id'].map(zones.net_node_id)

    # add connector distances
    zone_pairs['a_connector_dist'] = zone_pairs['a_zone_id'].map(zones.net_node_dist)
    zone_pairs['b_connector_dist'] = zone_pairs['b_zone_id'].map(zones.net_node_dist)

    # calculate total zone-to-zone distance and reduce set
    zone_pairs['total_dist'] = \
        zone_pairs['a_connector_dist'] + \
        zone_pairs['node_to_node_dist'] + \
        zone_pairs['b_connector_dist']

    zone_pairs = zone_pairs.loc[zone_pairs['total_dist'] < settings['max_dist']]

    return zone_pairs
