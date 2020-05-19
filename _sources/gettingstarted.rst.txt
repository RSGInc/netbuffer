
Getting Started
===============

Netbuffer includes the :ref:`example` model to help get you started.

Installation
------------

* Install `Anaconda 64bit Python 3 <https://www.anaconda.com/distribution/>`__, which includes a number of required Python packages.
* Create and activate an Anaconda environment (i.e. a Python install just for this project).

::

  conda create -n netbuffer3.7 python=3.7
  activate netbuffer3.7

* Install `Pandana 0.4 <http://udst.github.io/pandana/installation.html>`__. Pandana performs Netbuffer's core network operations
  and can be installed either via conda or pip. Using conda is recommended; see the Pandana documentation for more details.

::

  conda install pandana --channel conda-forge

* Get and install the netbuffer package from `GitHub <https://github.com/RSGInc/netbuffer>`_

::

  git clone https://github.com/RSGInc/netbuffer.git
  cd netbuffer
  pip install .


Running the Model
-----------------

* Change to the example folder and then run the run_netbuffer.py program

::

  cd example_nashville
  python run_netbuffer.py

* Check the outputs folder for results
