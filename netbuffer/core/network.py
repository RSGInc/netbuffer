from activitysim.core import config
from activitysim.core import inject

import logging
import sys
import os
import pandas as pd
import pandana as pdna
from pandana.loaders import osm

logger = logging.getLogger(__name__)


@inject.injectable(cache=True)
def network(zone_data, settings):
    """
    Injected Pandana Network object containing network
    node and edge data.

    User can specify three 'network' options in settings.yaml:

        - network: read
            uses an existing saved network HDF5 file specified by
            and additional 'saved_network' setting
        - network: build
            creates a new network using a set of node and link files
            specified in an additional 'network_settings_file'.
        - network: download
            downloads a complete network from Open Street Maps using
            the 'max_dist' setting and the zone latitudes/longitudes
            found in the zone_data table.
    """

    if settings['network'] == 'read':
        network = read_network_file(settings)

    elif settings['network'] == 'download':
        network = get_osm_network(zone_data, settings)

    elif settings['network'] == 'build':
        network = build_network(settings)

    else:
        raise "Invalid 'network' setting %s" % settings['network']

    network.precompute(settings.get('max_dist')+1)

    return network


def read_network_file(settings):
    """
    Read network from saved HDF5 file
    """

    network_fname = settings['saved_network']
    if not network_fname:
        logger.error("Please specify 'saved_network' file in settings")
        return

    network_fpath = config.data_file_path(network_fname, mandatory=False) or \
        config.output_file_path(network_fname)

    if not os.path.exists(network_fpath):
        logger.error('No network file %s found' % network_fname)
        return

    logger.info('Reading network from %s' % network_fpath)
    network = pdna.Network.from_hdf5(network_fpath)

    return network


def get_osm_network(zone_data, settings):
    """
    Retrieve Pandana network from Open Street Maps
    """

    logger.info('getting osm network')
    zones_df = zone_data.to_frame()

    miles = settings.get('distance_units') == 'miles'
    # distance to degrees: 111 km = 69 miles = 1 degree of long (y), 3mi = 0.043
    conversion = 69 if miles else 111 * 1000
    buffer = settings.get('max_dist') / conversion
    xmin = min(zones_df[settings['zones_lon']]) - buffer
    xmax = max(zones_df[settings['zones_lon']]) + buffer
    ymin = min(zones_df[settings['zones_lat']]) - buffer
    ymax = max(zones_df[settings['zones_lat']]) + buffer
    logger.debug('bounding box: %s, %s, %s, %s' % (str(ymin), str(xmin), str(ymax), str(xmax)))

    # default type=walk, which excludes freeways
    nodes, edges = osm.network_from_bbox(lat_min=ymin,
                                         lng_min=xmin,
                                         lat_max=ymax,
                                         lng_max=xmax,
                                         two_way=True,
                                         network_type='walk')

    if miles:
        logger.info('converting network distance units to miles...')
        edges[['distance']] = edges[['distance']] / 1609.34

    network = pdna.Network(nodes['x'],
                           nodes['y'],
                           edges['from'],
                           edges['to'],
                           edges[['distance']])

    print(edges.head())
    print(edges[['distance']])
    network.save_hdf5(config.output_file_path('pandana_network.h5'))

    return network


def build_network(settings):
    """
    Build a Pandana network from CSV files
    """

    logger.info('building pandana network')
    network_settings_file = settings['network_settings_file']
    if not network_settings_file:
        logger.error("Please specify 'network_settings_file' in settings")
        return

    network_settings = config.read_model_settings(network_settings_file)
    logger.debug('using settings %s' % network_settings)

    nodes = pd.read_csv(config.data_file_path(network_settings['nodes']))
    links = pd.read_csv(config.data_file_path(network_settings['links']))

    nodes.index = nodes[network_settings['nodes-id']]

    network = pdna.Network(nodes[network_settings['nodes-x']],
                           nodes[network_settings['nodes-y']],
                           links[network_settings['links-a']],
                           links[network_settings['links-b']],
                           links[[network_settings['links-impedance']]],
                           twoway=network_settings['twoway'])

    network.save_hdf5(config.output_file_path('pandana_network.h5'))

    return network
