

import logging
from activitysim.core import inject

logger = logging.getLogger(__name__)


@inject.table()
def parcel_data(store):

    df = store["parcel_data"]
    logger.info("loaded parcels %s" % (df.shape,))

    # replace table function with dataframe
    inject.add_table('parcel_data', df)

    return df
