# ActivitySim
# See full license in LICENSE.txt.

import logging
import orca


logger = logging.getLogger(__name__)


@orca.table()
def parcel_data(store):

    df = store["land_use/parcel_data"]


    logger.info("loaded parcels %s" % (df.shape,))

    # replace table function with dataframe
    orca.add_table('parcel_data', df)

    return df

