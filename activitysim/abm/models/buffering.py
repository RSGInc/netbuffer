# ActivitySim
# See full license in LICENSE.txt.

import logging
import os

import orca
import pandas as pd
import numpy as np
import pandana as pdna

from activitysim.core import assign
from activitysim.core import tracing
from activitysim.core import config


logger = logging.getLogger(__name__)

@orca.injectable()
def buffer_parcels_spec(configs_dir):
    f = os.path.join(configs_dir, 'buffering.csv')
    return assign.read_assignment_spec(f)


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

    fname = os.path.join(data_dir, settings["buffer_network"])
    network = pdna.Network.from_hdf5(fname)
    
    parcel_data_df = parcel_data.to_frame()
    parcel_data_df.reset_index(level = 0, inplace = True)
 
    # attach the node_id of the nearest network node to each parcel
    parcel_data_df['node_id'] = network.get_node_ids(parcel_data_df['xcoord_p'].values, parcel_data_df['ycoord_p'].values)

    locals_d = {
        'network': network,
        'factor' : 3,
        'sum' : 'sum',
        'exponential' : 'exponential',
    }
    
    if constants is not None:
        locals_d.update(constants)

    l = {}
    for e in zip(buffer_parcels_spec.target, buffer_parcels_spec.expression):
        target, expression = e
        locals_d['target'] = target 
        my_exp = 'network.aggregate(%s)'%(expression)
        network.set(parcel_data_df['node_id'], variable=parcel_data_df[target], name=target)
        x = eval(my_exp, globals(), locals_d)
        l[target] = x 

    buffered_parcels = pd.DataFrame(l)
    tracing.trace_df(buffered_parcels,
                             label='buffered_parcels',
                             index_label='None',
                             slicer='NONE',
                             transpose = False)
        
