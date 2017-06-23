# ActivitySim
# See full license in LICENSE.txt.

import logging
import os

import orca
import pandas as pd
import numpy as np
import pandana as pdna

from activitysim.core import buffer
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
def buffer_parcels(settings, buffer_parcels_spec, buffer_parcels_settings, parcel_data, data_dir):

    """
    Performs buffering for each point in parcel file using expressions from buffer_parcels_spec

    The actual results depend on the expressions in buffer_parcels_spec, but this is initially
    intended to replicate PSRC's Soundcast (Daysim) parcel accessibilities. 

    Using a point file representing a land use boundary (parcel), quanitifes the amount or distance of
    a varaible associated with the zone. For example, the total number of jobs within a half mile 
    for each parcel for the distance to the nearest bus stop from each parcel.  
  
    """

    logger.info("Running buffer_parcels")

    constants = config.get_model_constants(buffer_parcels_settings)
    
    pdna.network.reserve_num_graphs(2)
    net_fname = os.path.join(data_dir, settings["buffer_network"])
    network = pdna.Network.from_hdf5(net_fname)
    network.init_pois(num_categories=constants["num_categories"], max_dist=constants["max_dist"], max_pois=constants["max_pois"])
    
    parcel_data_df = parcel_data.to_frame()
    parcel_data_df.reset_index(level = 0, inplace = True)

    # attach the node_id of the nearest network node to each parcel
    parcel_data_df['node_id'] = network.get_node_ids(parcel_data_df['xcoord_p'].values, parcel_data_df['ycoord_p'].values)
    parcel_data_df.set_index(constants['parcel_index'], inplace = True)
    
    # TO-FIX-reading the following df directly for now since easier table reading is being added.
    # this table provides the location of transit stops and types of transit they serve
    # used to find distance from parcels to nearest bus, light rail, commuter rail etc.
    # also- might be better to store in the master h5 file.  
    poi_fname = os.path.join(data_dir, settings["transit_stops"]) 
    poi_df = pd.read_csv(poi_fname, index_col = False)
 
    # intersections:
    # combine from and to columns
    all_nodes = pd.DataFrame(network.edges_df['from'].append(network.edges_df.to), columns = ['node_id'])

    # get the frequency of each node, which is the number of intersecting ways
    intersections_df = pd.DataFrame(all_nodes.node_id.value_counts())
    intersections_df = intersections_df.rename(columns = {'node_id' : 'edge_count'})
    intersections_df.reset_index(0, inplace = True)
    intersections_df = intersections_df.rename(columns = {'index' : 'node_id'})

    # add a column for each way count
    intersections_df['nodes1'] = np.where(intersections_df['edge_count']==1, 1, 0)
    intersections_df['nodes3'] = np.where(intersections_df['edge_count']==3, 1, 0)
    intersections_df['nodes4'] = np.where(intersections_df['edge_count']>3, 1, 0)

    locals_d = {
        'network': network
    }

    # perhaps these could be stored in locals_d
    df_dic = {
        'parcels_df' : parcel_data_df,
        'intersections_df' : intersections_df,
        'poi_df': poi_df
        }

    
    if constants is not None:
        locals_d.update(constants)

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(buffer_parcels_spec, 'parcels_df', df_dic, locals_d, trace_rows=None)
    

    tracing.trace_df(results,
                             label='buffered_parcels',
                             index_label='None',
                             slicer='NONE',
                             transpose = False)
        

