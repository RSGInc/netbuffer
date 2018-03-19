
import orca
from activitysim.core import tracing
import pandas as pd
import numpy as np
import os

from activitysim.core.tracing import print_elapsed_time

from activitysim.core import pipeline
import extensions


# comment out the line below to default base seed to 0 random seed
# so that run results are reproducible
# pipeline.set_rn_generator_base_seed(seed=None)


tracing.config_logger()

t0 = print_elapsed_time()

_MODELS = [
    'buffer_parcels'
]


# If you provide a resume_after argument to pipeline.run
# the pipeline manager will attempt to load checkpointed tables from the checkpoint store
# and resume pipeline processing on the next submodel step after the specified checkpoint
resume_after = None
# resume_after = 'mandatory_scheduling'

pipeline.run(models=_MODELS, resume_after=resume_after)


print "\n#### run completed"


# tables will no longer be available after pipeline is closed
pipeline.close()


t0 = print_elapsed_time("all models", t0)
