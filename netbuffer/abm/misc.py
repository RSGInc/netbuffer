

import os
import warnings
import logging

import numpy as np
from activitysim.core import inject
import pandas as pd
import yaml

from activitysim.core import pipeline
from activitysim.core import config

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
def trace_zones(settings):

    zones = settings.get('trace_zones', None)

    if zones and not (isinstance(zones, list) and all(isinstance(x, int) for x in zones)):
        logger.warn("setting trace_zones is wrong type, should be a list of integers, but was %s"
                    % zones)
        zones = None

    return zones
