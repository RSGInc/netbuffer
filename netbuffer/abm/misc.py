

import os
import warnings
import logging

import numpy as np
from activitysim.core import inject
import pandas as pd
import yaml

from activitysim.core import pipeline

warnings.filterwarnings('ignore', category=pd.io.pytables.PerformanceWarning)
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)


@inject.injectable(cache=True)
def store(data_dir, settings):
    if 'store' not in settings:
        logger.error("store file name not specified in settings")
        raise RuntimeError("store file name not specified in settings")
    fname = os.path.join(data_dir, settings["store"])
    if not os.path.exists(fname):
        logger.error("store file not found: %s" % fname)
        raise RuntimeError("store file not found: %s" % fname)

    file = pd.HDFStore(fname, mode='r')
    pipeline.close_on_exit(file, fname)

    return file


@inject.injectable(cache=True)
def trace_parcels(settings):

    parcels = settings.get('trace_parcels', None)

    if parcels and not (isinstance(parcels, list) and all(isinstance(x, int) for x in parcels)):
        logger.warn("setting trace_parcels is wrong type, should be a list of integers, but was %s"
                    % parcels)
        parcels = None

    return parcels
