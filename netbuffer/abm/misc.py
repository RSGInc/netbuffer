# ActivitySim
# See full license in LICENSE.txt.

import os
import warnings
import logging

import numpy as np
import orca
import pandas as pd
import yaml

from activitysim.core import pipeline

warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)


@orca.injectable(cache=True)
def trace_parcels(settings):

    parcels = settings.get('trace_parcels', None)

    if parcels and not (isinstance(parcels, list) and all(isinstance(x, int) for x in parcels)):
        logger.warn("setting trace_parcels is wrong type, should be a list of type integer, but was %s" % parcels)
        parcels = None

    return parcels
