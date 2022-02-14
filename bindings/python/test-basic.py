import lowfive      # lowfive needs to be imported before h5py
# vol = lowfive.VOLBase()
vol = lowfive.MetadataVOL()
vol.set_passthru("*","*")

import h5py
import numpy as np

f = h5py.File('foo.h5', 'w')
f.create_dataset("data", data=np.ones((4, 3, 2), 'f'))
# f["data"] = np.ones((4, 3, 2), 'f')

# there is something wrong with how destructors are invoked when Python
# terminates, so for now need to delete f explicitly
del f
