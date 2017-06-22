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
    Compute accessibility for each zone in land use file using expressions from accessibility_spec

    The actual results depend on the expressions in accessibility_spec, but this is initially
    intended to permit implementation of the mtc accessibility calculation as implemented by
    Accessibility.job

    Compute measures of accessibility used by the automobile ownership model.
    The accessibility measure first multiplies an employment variable by a mode-specific decay
    function.  The product reflects the difficulty of accessing the activities the farther
    (in terms of round-trip travel time) the jobs are from the location in question. The products
    to each destination zone are next summed over each origin zone, and the logarithm of the
    product mutes large differences.  The decay function on the walk accessibility measure is
    steeper than automobile or transit.  The minimum accessibility is zero.
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
 
    

    #parcel_data_df['test'] = res.loc[parcel_data_df.node_id].values

    locals_d = {
        'network': network
    }
    
    if constants is not None:
        locals_d.update(constants)

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(buffer_parcels_spec, parcel_data_df, poi_df, locals_d, trace_rows=None)
    

    tracing.trace_df(results,
                             label='buffered_parcels',
                             index_label='None',
                             slicer='NONE',
                             transpose = False)
        

#@orca.injectable()
#def nearest_poi_spec(configs_dir):
#    f = os.path.join(configs_dir, 'nearest_poi.csv')
#    return assign.read_assignment_spec(f)


#@orca.step()
#def distance_to_nearest_poi(settings, nearest_poi_spec, buffer_parcels_settings, parcel_data, data_dir):

#    """
#    Compute accessibility for each zone in land use file using expressions from accessibility_spec

#    The actual results depend on the expressions in accessibility_spec, but this is initially
#    intended to permit implementation of the mtc accessibility calculation as implemented by
#    Accessibility.job

#    Compute measures of accessibility used by the automobile ownership model.
#    The accessibility measure first multiplies an employment variable by a mode-specific decay
#    function.  The product reflects the difficulty of accessing the activities the farther
#    (in terms of round-trip travel time) the jobs are from the location in question. The products
#    to each destination zone are next summed over each origin zone, and the logarithm of the
#    product mutes large differences.  The decay function on the walk accessibility measure is
#    steeper than automobile or transit.  The minimum accessibility is zero.
#    """

#    logger.info("Running buffer_parcels")

#    constants = config.get_model_constants(buffer_parcels_settings)

#    net_fname = os.path.join(data_dir, settings["buffer_network"])
#    network = pdna.Network.from_hdf5(net_fname)
    

#    network.init_pois(num_categories=1, max_dist=2000, max_pois=5)

#    # TO-FIX-reading the following df directly for now since easier table reading is being added.
#    # this table provides the location of transit stops and types of transit they serve
#    # used to find distance from parcels to nearest bus, light rail, commuter rail etc.
#    # also- might be better to store in the master h5 file.  
#    poi_fname = net_fname = os.path.join(data_dir, settings["transit_stops"]) 

#    parcel_data_df = parcel_data.to_frame()
#    parcel_data_df.reset_index(level = 0, inplace = True)
 
#    # attach the node_id of the nearest network node to each parcel
#    parcel_data_df['node_id'] = network.get_node_ids(parcel_data_df['xcoord_p'].values, parcel_data_df['ycoord_p'].values)

#    #network.set_pois("bus", poi_df['x'], poi_df['y'])
#    #res = network.nearest_pois(2000, "bus", num_pois=1, max_distance=99999)

    

#    locals_d = {
#        'network': network,
#        'poi_df': pd.read_csv(poi_fname, index_col = False)
#    }
    
#    if constants is not None:
#        locals_d.update(constants)

#    results, trace_results, trace_assigned_locals \
#        = assign.assign_variables(nearest_poi_spec, network.nodes_df, locals_d, trace_rows=None)
    
#    parcel_data_df['test'] = results.loc[parcel_data_df.node_id].values

#    #tracing.trace_df(results,
#    #                         label='buffered_parcels',
#    #                         index_label='None',
#    #                         slicer='NONE',
#    #                         transpose = False)