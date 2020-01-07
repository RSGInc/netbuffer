NetBuffer
=========

NetBuffer is a tool to perform network based queries and aggregations
on land use data.  NetBuffer is implemented in the
[ActivitySim](https://github.com/activitysim) framework.

Here are some simple steps to install the package and run the example:

1. Download [Anaconda](https://www.anaconda.com/distribution/)
2. Create and activate an Anaconda environment (i.e. a Python install just for this project).
  * `conda create -n netbuffertest python=3.7`
  * `activate netbuffertest`
3. Install [Pandana](http://udst.github.io/pandana/installation.html):
  `conda install pandana --channel conda-forge`
4. Install Netbuffer:
  * `git clone https://github.com/RSGInc/netbuffer.git`
  * (optional) Run `git checkout nashville` to test the new Nashville-specific changes
  * `cd netbuffer`
  * `pip install .`
5. Revert geopandas 
  `conda install geopandas=0.6.3 -c conda-forge`
6. Run the example:
  * `cd example_nashville`
  * `python run_netbuffer.py`
```
