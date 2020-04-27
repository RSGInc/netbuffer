

import logging
import os

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


def undupe_column_names(df, template="{} ({})"):
    """
    rename df column names so there are no duplicates (in place)

    e.g. if there are two columns named "dog", the second column will be reformatted to "dog (2)"

    Parameters
    ----------
    df : pandas.DataFrame
        dataframe whose column names should be de-duplicated
    template : template taking two arguments (old_name, int) to use to rename columns

    Returns
    -------
    df : pandas.DataFrame
        dataframe that was renamed in place, for convenience in chaining
    """

    new_names = []
    seen = set()
    for name in df.columns:
        n = 1
        new_name = name
        while new_name in seen:
            n += 1
            new_name = template.format(name, n)
        new_names.append(new_name)
        seen.add(new_name)
    df.columns = new_names
    return df


def read_buffer_spec(fname,
                     description_name="Description",
                     target_name="Target",
                     variable_name="Variable",
                     target_df_name="TargetDF",
                     expression_name="Expression"
                     ):

    """
    Read a CSV model specification into a Pandas DataFrame or Series.

    The CSV is expected to have columns for component descriptions
    targets, and expressions,

    The CSV is required to have a header with column names. For example:

        Description,Target,Variable, TargetDF, Expression

    Parameters
    ----------
    fname : str
        Name of a CSV spec file.
    description_name : str, optional
        Name of the column in `fname` that contains the component description.
    target_name : str, optional
        Name of the column in `fname` that contains the component target.
    variable_name : str, optional
        Name of the column in `fname` that contains the variable target.
    target_df_name : str, optional
        Name of the column in `fname` that contains the target dataframe.
    expression_name : str, optional
        Name of the column in `fname` that contains the component expression.


    Returns
    -------
    spec : pandas.DataFrame
        dataframe with three columns: ['description' 'variable' 'target_df' 'target' 'expression']
    """

    cfg = pd.read_csv(fname, comment='#')

    # drop null expressions
    # cfg = cfg.dropna(subset=[expression_name])

    cfg.rename(columns={target_name: 'target',
                        expression_name: 'expression',
                        description_name: 'description',
                        variable_name: 'variable',
                        target_df_name: 'target_df'},
               inplace=True)

    # backfill description
    if 'description' not in cfg.columns:
        cfg.description = ''

    cfg.target = cfg.target.str.strip()
    cfg.expression = cfg.expression.str.strip()

    return cfg


class NumpyLogger(object):
    def __init__(self, logger):
        self.logger = logger
        self.target = ''
        self.expression = ''

    def write(self, msg):
        self.logger.error("numpy warning: %s" % (msg.rstrip()))
        self.logger.error("expression: %s = %s" % (str(self.target), str(self.expression)))


def buffer_variables(buffer_expressions,
                     zone_df_name, locals_dict,
                     df_alias=None, trace_rows=None):
    """
    Perform network accessibility calculations (using Pandana libary
    http://udst.github.io/pandana/) on point based data (e.g. zone
    centroids) using a set of expressions from a spec in the context of
    a given data table.

    Expressions are evaluated using Python's eval function.
    Python expressions have access to variables in locals_d.
    They also have access to previously assigned
    targets as the assigned target name.

    zone_df_name is the name of the data frame in locals_dict, which represents
    point data, to which all buffering and network distance measurements are
    applied. In order to do this, each row (point) in zone_df must be associated with it's
    nearest node in the Pandana network. This is achieved using the Pandana method
    network.get_node_ids. The buffering operations are performed on each node in the
    network, thus allowing the results to be joined to the zone df via node_id. Only
    the results that share the same nodes in the zone_df_name data frame
    are returned.

    For example, in order to find the distance of each zone to the nearest
    bus stop, we need a data frame representing bus stop locations and their
    nearest network node. Pandana then finds the distance from every node in
    the network to the nearest node that represents a bus stop. Next, only
    the distances for nodes that are associated with the zone dataframe are
    kept and the results are indexed to the zone dataframe.

    lowercase variables starting with underscore are temp variables (e.g. _local_var)
    and not returned except in trace_restults

    uppercase variables starting with underscore are temp variables (e.g. _LOCAL_SCALAR)
    and not returned except in trace_assigned_locals
    This is useful for defining general purpose local constants in expression file

    Users should take care that expressions should result in
    a Pandas Series (scalars will be automatically promoted to series.)

    Parameters
    ----------
    buffer_expressions : pandas.DataFrame of target assignment expressions
        target: target column names
        variable: target variable to be buffered
        target_df: datafram that contains the variable to be buffered.
        expression: pandana, pandas or python expression to evaluate
    zone_df_name : the name of the df in df_dict to which all results
        will be indexed.
    df_dict : dictionary of pandas.DataFrames. This must include the df
        referenced by zone_df_name. A poi_df can be used to find distances
        to other points like bus stops. All poi's should be stored in this
        one df. Other dfs can be used for aggregate buffering such as
        intersections_df.
    locals_dict : Dict
        This is a dictionary of local variables that will be the environment
        for an evaluation of "python" expression.
    trace_rows: series or array of bools to use as mask to select target rows to trace

    Returns
    -------
    variables : pandas.DataFrame
        Will have the index of `df` and columns named by target and containing
        the result of evaluating expression
    trace_df : pandas.DataFrame or None
        a dataframe containing the eval result values for each assignment expression
    """

    np_logger = NumpyLogger(logger)

    def is_local(target):
        return target.startswith('_') and target.isupper()

    def is_temp(target):
        return target.startswith('_')

    def to_series(x, target=None):
        if x is None or np.isscalar(x):
            if target:
                logger.warn("WARNING: assign_variables promoting scalar %s to series" % target)
            x = pd.Series([x] * len(locals_dict[zone_df_name].index),
                          index=locals_dict[zone_df_name].index)
        if not isinstance(x, pd.Series):
            x = pd.Series(x)
        x.name = target

        return x

    trace_assigned_locals = trace_results = None
    if trace_rows is not None:
        # convert to numpy array so we can slice ndarrays as well as series
        trace_rows = np.asanyarray(trace_rows)
        if trace_rows.any():
            trace_results = []
            trace_assigned_locals = {}

    # avoid touching caller's passed-in locals_d parameter (they may be looping)
    locals_dict = locals_dict.copy() if locals_dict is not None else {}
    local_keys = list(locals_dict.keys())

    le = []
    traceable = True
    # need to be able to identify which variables causes an error, which keeps
    # this from being expressed more parsimoniously
    for e in zip(buffer_expressions.target, buffer_expressions.variable,
                 buffer_expressions.target_df, buffer_expressions.expression):
        target, var, target_df, expression = e

        if target in local_keys:
            logger.warn("buffer_variables target obscures local_d name '%s'" % str(target))

        if is_local(target):

            x = eval(expression, globals(), locals_dict)
            locals_dict[target] = x
            if trace_assigned_locals is not None:
                trace_assigned_locals[target] = x
            continue

        try:

            # FIXME - log any numpy warnings/errors but don't raise
            np_logger.target = str(target)
            np_logger.expression = str(expression)
            saved_handler = np.seterrcall(np_logger)
            save_err = np.seterr(all='log')

            network = locals_dict['network']
            logger.debug("solving expression: %s" % expression)

            # aggregate query
            if 'aggregate' in expression:
                network.set(locals_dict[target_df][locals_dict['node_id']],
                            variable=locals_dict[target_df][var], name=var)
                values = to_series(eval(expression, globals(), locals_dict), target=target)
                # index results to the zone_df:
                locals_dict[zone_df_name][target] = \
                    values.loc[locals_dict[zone_df_name][locals_dict['node_id']]].values
                values = locals_dict[zone_df_name][target]
                traceable = True

            # nearest poi
            elif 'nearest_pois' in expression:
                # records we want to run nearest poi on should have a value of 1.
                # Ex- Could have a table of transit stops,
                # where each column is a type of transit stop, e.g. light rail, and a
                # value of 1 in the light rail column
                # means that that stop is a light rail stop.
                temp_df = locals_dict[target_df][(locals_dict[target_df][var] == 1)]
                network.set_pois(category=var,
                                 maxdist=locals_dict['max_dist'],
                                 maxitems=locals_dict['max_pois'],
                                 x_col=temp_df[locals_dict['poi_x']],
                                 y_col=temp_df[locals_dict['poi_y']])
                # poi queries return a df, no need to put through to_series function.
                values = eval(expression, globals(), locals_dict)
                # index results to the zone_df:
                locals_dict[zone_df_name][target] = \
                    values.loc[locals_dict[zone_df_name][locals_dict['node_id']]].values
                values = locals_dict[zone_df_name][target]
                # if assignment is to a df that is not the zone df, then cannot trace results
                if target_df != zone_df_name:
                    traceable = False

            # panda df assignment:
            else:
                values = to_series(eval(expression, globals(), locals_dict), target=target)
                # the target_df might need this column for a subsequent buffer operation
                # delete if exists:
                if target in locals_dict[target_df].columns:
                    locals_dict[target_df].drop(target, 1, inplace=True)
                locals_dict[target_df] = locals_dict[target_df].merge(pd.DataFrame(
                    values), how='left', left_index=True, right_index=True)
                # if assignment is to a df that is not the zone df, then cannot trace results
                if target_df != zone_df_name:
                    traceable = False

            np.seterr(**save_err)
            np.seterrcall(saved_handler)

        except Exception as err:
            logger.error("assign_variables error: %s: %s" % (type(err).__name__, str(err)))

            logger.error("assign_variables expression: %s = %s"
                         % (str(target), str(expression)))

            # values = to_series(None, target=target)
            raise err

        le.append((target, values))

        if trace_results is not None:
            # some calcs are not included in the final df so may not have the
            # zones that being traced. These should have a value of 'None' in
            # spec under the 'variable' column.
            if traceable:
                trace_results.append((target, values[trace_rows]))

        # update locals to allows us to ref previously assigned targets
        locals_dict[target] = values

    # build a dataframe of eval results for non-temp targets
    # since we allow targets to be recycled, we want to only keep the last usage
    # we scan through targets in reverse order and add them to the front of the list
    # the first time we see them so they end up in execution order
    variables = []
    seen = set()
    for statement in reversed(le):
        # statement is a tuple (<target_name>, <eval results in pandas.Series>)
        target_name = statement[0]
        if not is_temp(target_name) and target_name not in seen:
            variables.insert(0, statement)
            seen.add(target_name)

    # DataFrame from list of tuples [<target_name>, <eval results>), ...]
    variables = pd.DataFrame.from_dict(dict(variables))
    if trace_results is not None:
        trace_results = pd.DataFrame.from_dict(dict(trace_results))
        trace_results.index = locals_dict[zone_df_name][trace_rows].index
        trace_results = undupe_column_names(trace_results)

        # add df columns to trace_results
        # trace_results = pd.concat([locals_dict[zone_df_name], trace_results], axis=1)
    return variables, trace_results, trace_assigned_locals
