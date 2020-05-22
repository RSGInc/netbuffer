
.. _example :

Example
=======

This page describes the example model design and how to setup and run the Nashville
regional example.  The Nashville example does three things:

  * nearby_zones - calculates nearby zone-to-zone network distances
  * buffer_zones - calculates network accessibility (buffer) variables 
  * write_daysim_files - writes DaySim formatted output files

.. index:: tutorial
.. index:: example

Example Model Design
--------------------

Netbuffer contains two network models and one output model:

  * nearby_zones
  * buffer_zones
  * write_daysim_files

The main input for the network models is a CSV file containing zone data. This file is called
``zones_sample.csv`` in the Nashville example.

**nearby_zones** calculates the nearby zones for each zone in the input file within a given network distance. 
It does this by finding the nearest network node for each zone, using Pandana's ``nearest_pois`` algorithm to 
find the nearest network nodes.

.. automodule:: netbuffer.abm.models.nearby_zones
   :members:

**buffer_zones** takes two additional input files -- a POI (Point of Interest) file and a Python
expressions file, and performs custom operations on the zone data to calculate network buffer / accessibility 
attributes for each input zone.

.. automodule:: netbuffer.abm.models.buffer_zones
   :members:

**write_daysim_files** then outputs the final tables according to the formats required for input to DaySim.

.. automodule:: netbuffer.abm.models.write_daysim_files
  :members:

Each network model is independent and can be run separately. Configuration is described in the
following section.


Setup
-----

The following describes the example model setup.


Folder and File Setup
~~~~~~~~~~~~~~~~~~~~~

The example has the following root folder/file setup:

  * configs - settings, expressions files, etc.
  * data - input data such as the zones CSV, the POI CSV, and saved network H5
  * output - outputs folder
  * run_netbuffer.py - main script to run the model


Configuration
~~~~~~~~~~~~~

The ``configs`` folder contains settings, expressions files, and other files required for specifying
model utilities and form.  The first place to start in the ``configs`` folder is ``settings.yaml``, which
is the main settings file for the model run.  This file includes:

* ``models`` - list of model steps to run
* ``input_table_list`` - input file name and index column for the initial zone table
* ``network`` - instruction for sourcing the Pandana network ('read', 'build', or 'download')
* ``max_dist`` - maximum network search distance (in meters) for calculating nearby zones and POIs
* ``zones_lon``, ``zones_lat`` - columns to use for latitude/longitude in zones input file

The ``buffer_zones.yaml`` file provides instructions to the buffer_zones step.

* ``buffer_zones_spec`` - filename for the user-defined network expressions
* ``pois`` - Point of Interest file name
* ``CONSTANTS`` - variables that are made available to the Python interpreter when evaluating the
  expressions from ``buffer_zones_spec``. The following constants are required:

  * ``max_pois`` - number of POIs to look for around each zone. E.x. ``max_pois: 1`` will find only the closest bus stop in a given zone
  * ``pois-x`` - the longitude column in the POI file
  * ``pois-y`` - the latitude column in the POI file

`Read more <https://activitysim.github.io/activitysim/core.html#utility-expressions>`__ on
expressions files in the ActivitySim framework.

Network
~~~~~~~

Netbuffer uses a `Pandana network <http://udst.github.io/pandana/index.html>`__ for the majority of
the heavy lifting. This network must be loaded at the program's start and can be sourced in three
different ways via the ``network`` flag in ``settings.yaml``:

* ``download`` - calculates a geographic area from the input zones table and
  download a network from the Open Street Map API. Netbuffer will then save a file in the outputs
  folder named `pandana_network.h5` which can be used for subsequent model runs.
* ``read`` - loads a network from a saved H5 file. Be sure to use a network that
  geographically matches the input zones/POIs. This option requires an additional ``saved_network``
  flag specifying the input file name.
* ``build`` - creates a network from a set of nodes and links files. Requires an additional
  ``network_settings_file`` configuration setting. See ``example_psrc`` for more details.

.. note::

  Netbuffer uses Pandana's default distance unit *meters*. All distances in the input tables,
  expressions, and configs are assumed to be in meters.

.. automodule:: netbuffer.core.network
   :members:
