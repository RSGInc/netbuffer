import logging

import pandas as pd
import numpy as np

from activitysim.core import config
from activitysim.core import inject
from activitysim.core import pipeline

logger = logging.getLogger(__name__)

SEP = {
    'space': ' ',
    'tab': '\t',
    'comma': ','
}


@inject.step()
def write_daysim_files():
    """
    Pipeline step that writes formatted output files.

    Specify in daysim_files.yaml to output pipeline tables:

    nearby_zones:
      - outfile: filename
      - delimiter: 'comma', 'space', or 'tab'
      - header: bool, whether to include column headers
      - cols: list of columns from nearby zones to include.

    Nearby zones column options are 'from', 'to', 'distance',
    'net_node_dist', 'net_node_dist_to_zone', 'total_dist'.

    buffered_zones:
      - outfile: filename
      - delimiter: ascii code
      - header: bool
      - cols: list of columns to include from buffered zones.

    Buffered zone column names must match the input zones table.
    """
    daysim_settings = config.read_model_settings('daysim_files.yaml')

    nearby_zones_settings = daysim_settings.get('nearby_zones')
    buffer_zones_settings = daysim_settings.get('buffered_zones')
    network_file_settings = daysim_settings.get('network_files')

    if nearby_zones_settings:
        write_pipeline_table(nearby_zones_settings, 'nearby_zones')

    if buffer_zones_settings:
        write_pipeline_table(buffer_zones_settings, 'zone_data')

    if network_file_settings:
        write_network_files(network_file_settings)


def write_pipeline_table(file_settings, pipeline_table):
    """
    Writes output files according to user settings.

    Parameters
    ----------
    file_settings : dict
        cols : pipeline_table columns to include in output
        delimiter : str, either 'comma', 'space', or 'tab'
        col_types : dict, col/type mapping (python or numpy dtypes)
        outfile : output file name
        header : bool, whether to include header row in output
    """
    df = pipeline.get_table(pipeline_table)
    drop_index = df.index.name is None
    df.reset_index(drop=drop_index, inplace=True)
    expected_cols = file_settings.get('cols', [])

    for col in list(expected_cols):
        if col not in df:
            logger.warn('%s table is missing %s column' % (pipeline_table, col))
            expected_cols.remove(col)

    col_types = file_settings.get('col_types')
    df_to_write = df[expected_cols].astype(col_types)

    outfile = file_settings.get('outfile')
    header = file_settings.get('header', True)
    delimiter = file_settings.get('delimiter', 'comma')

    df_to_write.to_csv(config.output_file_path(outfile),
                       sep=SEP[delimiter],
                       header=header,
                       index=False)


def write_network_files(network_file_settings):
    ztn_settings = network_file_settings.get('zone_to_node')
    nd_settings = network_file_settings.get('node_distances')
    ni_settings = network_file_settings.get('node_indices')

    nearby_zones_df = pipeline.get_table('nearby_zones')
    zone_nodes = pipeline.get_table('zone_data')['net_node_id']  # zone id to OSM node mapping

    # The OSM node ids can get very big. Provide a way to remap
    # them to smaller values in the output files.
    if network_file_settings.get('remap_osm_ids', False):
        unique_nodes = zone_nodes.unique()
        remap = dict(zip(unique_nodes, np.arange(1, len(unique_nodes) + 1)))
        zone_nodes = zone_nodes.map(remap)
        nearby_zones_df['a_node_id'] = nearby_zones_df['a_node_id'].map(remap)
        nearby_zones_df['b_node_id'] = nearby_zones_df['b_node_id'].map(remap)

    if ztn_settings:
        zone_nodes.to_csv(config.output_file_path(ztn_settings.get('outfile')),
                          sep=SEP[ztn_settings.get('delimiter', 'space')],
                          header=False,
                          index=True)

    if nd_settings:
        nodes = pd.DataFrame()

        nodes['onode'] = nearby_zones_df.a_node_id
        nodes['dnode'] = nearby_zones_df.b_node_id

        units = config.setting('distance_units', default='miles')
        conversion = 5280 if units == 'miles' else 3.28084

        nodes['feet'] = nearby_zones_df.node_to_node_dist * conversion
        nodes = nodes.drop_duplicates(ignore_index=True).sort_values(by=['onode', 'dnode'])
        nodes = nodes[nodes['onode'] != nodes['dnode']]

        nodes.astype('int64').to_csv(config.output_file_path(nd_settings.get('outfile')),
                                     sep=SEP[nd_settings.get('delimiter', 'space')],
                                     header=True,
                                     index=False)

        if not ni_settings:
            return

        nodes['seq'] = np.arange(1, nodes.shape[0] + 1)
        index_df = pd.DataFrame(index=nodes['onode'].unique())
        index_df['firstrec'] = nodes.groupby('onode').first()['seq']
        index_df['lastrec'] = nodes.groupby('onode').last()['seq']
        index_df.index.name = 'node_Id'
        index_df.fillna(0).astype('int64').to_csv(
            config.output_file_path(ni_settings.get('outfile')),
            sep=SEP[ni_settings.get('delimiter', 'space')],
            header=True,
            index=True)
