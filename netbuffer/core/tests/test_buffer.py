
import os.path
import numpy.testing as npt
import numpy as np
import pandas as pd
import pandas.util.testing as pdt
import pandana as pdna
import pytest


from .. import buffer
from activitysim.core import tracing


@pytest.fixture(scope='module')
def data_dir():
    return os.path.join(os.path.dirname(__file__), 'data')


@pytest.fixture(scope='module')
def configs_dir():
    return os.path.join(os.path.dirname(__file__), 'configs')


@pytest.fixture(scope='module')
def spec_name(data_dir):
    return os.path.join(data_dir, 'buffer_spec.csv')


@pytest.fixture(scope='module')
def net_name(data_dir):
    return os.path.join(data_dir, 'test_net.h5')


@pytest.fixture(scope='module')
def zone_name(data_dir):
    return os.path.join(data_dir, 'test_zones.csv')


@pytest.fixture(scope='module')
def data_name(data_dir):
    return os.path.join(data_dir, 'data.csv')


@pytest.fixture(scope='module')
def data(data_name):
    return pd.read_csv(data_name)


def test_read_model_spec(spec_name):

    spec = buffer.read_buffer_spec(spec_name)

    # assert len(spec) == 8

    assert list(spec.columns) == ['description', 'target', 'variable', 'target_df', 'expression']


def test_buffer_variables(capsys, spec_name, net_name, zone_name):

    spec = buffer.read_buffer_spec(spec_name)

    network = pdna.Network.from_hdf5(net_name)
    zone_data_df = pd.read_csv(zone_name, index_col='zoneid')
    assert len(zone_data_df) == 3

    zone_data_df['node_id'] = network.get_node_ids(zone_data_df['xcoord_p'],
                                                   zone_data_df['ycoord_p'])

    print(zone_data_df.index)
    # zone 1219983 should be assigned to network node 82030 becuase it is the nearest node:
    assert int(zone_data_df.loc[735313].node_id) == 84076
    print(zone_data_df.loc[735313].node_id)

    locals_d = {
        'network': network,
        'zones_df': zone_data_df,
        'node_id': 'node_id'
    }
    # zone_data_df.set_index(constants['zone_index'], inplace=True)
    # zone_data_df.reset_index(level=0, inplace=True)

    # locals_d = {'CONSTANT': 7, '_shadow': 99}

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(spec, 'zones_df', locals_d, trace_rows=None)

    print(results.columns)

    assert list(results.columns) == ['target1', 'target2', 'target3']
    assert list(results.target1) == [382, 382, 382]
    assert list(results.target2) == [1, 1, 1]
    assert list(results.target3) == [383, 383, 383]
    assert trace_results is None
    assert trace_assigned_locals is None

    trace_zone_rows = zone_data_df.index.isin([735313])

    results, trace_results, trace_assigned_locals \
        = buffer.buffer_variables(spec, 'zones_df', locals_d, trace_rows=trace_zone_rows)

    # should get same results as before
    assert list(results.target3) == [383, 383, 383]

    # should assign trace_results for second row in data
    print(trace_results)

    # assert trace_results is not None
    # assert '_temp' in trace_results.columns
    # assert list(trace_results['_scalar']) == [42]

    # shadow should have been assigned
    # assert list(trace_results['_shadow']) == [1]
    # assert list(trace_results['_temp']) == [9]
    # assert list(trace_results['target3']) == [530]

    # print "trace_assigned_locals", trace_assigned_locals
    # assert trace_assigned_locals['_DF_COL_NAME'] == 'thing2'

    # shouldn't have been changed even though it was a target
    # assert locals_d['_shadow'] == 99

    out, err = capsys.readouterr()
