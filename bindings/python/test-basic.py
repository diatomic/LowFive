import lowfive      # lowfive needs to be imported before h5py
vol = lowfive.VOLBase()
# vol = lowfive.MetadataVOL()

import h5py
import numpy as np

f = h5py.File('foo.h5', 'w')
f['data'] = np.ones((4, 3, 2), 'f')

