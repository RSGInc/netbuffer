
rem # Microsoft Visual C++ 2008 SP1 Redistributable Package (x64) 
rem # https://www.microsoft.com/en-us/download/details.aspx?id=2092
rem # is required for running Pandana on Windows

conda create -q -n pdn python=2.7
activate pdn

conda install -c conda-forge osmnet brewer2mpl
conda install -c udst pandana

pip install orca
pip install activitysim

python setup.py develop