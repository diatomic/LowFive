import sys

memory = False
if '-m' in sys.argv:
    memory = True
    sys.argv.remove('-m')
file = False
if '-f' in sys.argv:
    file = True
    sys.argv.remove('-f')
if not file and not memory:
    print("Neither -m, nor -f specified. Forcing -f")
    file = True

import lowfive
vol = lowfive.create_MetadataVOL()
if file:
    vol.set_passthru("*","*")
if memory:
    vol.set_memory("*","*")
# vol.set_keep(True)

import h5py
import numpy as np

f = h5py.File('foo.h5', 'w')

f.attrs['def'] = np.ones((5,), 'f')

grp = f.create_group('group1')
grp.attrs['abc'] = np.ones((10,), 'f')


f.create_dataset("data", data=np.ones((4, 3, 2), 'f'))
f["data2"] = np.ones((8, 6, 4), 'f')

# there is something wrong with how destructors are invoked when Python
# terminates, so for now need to delete f and grp explicitly
del grp
del f
