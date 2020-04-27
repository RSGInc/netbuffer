import logging
from activitysim.core import inject
from activitysim.core.input import read_input_table

logger = logging.getLogger(__name__)


@inject.table()
def zone_data():
    """
    Pipeline table containing zone info. Specify with 'input_table_list'
    in settings.yaml. Must contain columns for at least zone id, latitude,
    and longitude.

    """
    df = read_input_table('zone_data')

    logger.info('loaded zone data %s' % (df.shape,))

    # replace table function with dataframe
    inject.add_table('zone_data', df)

    return df
