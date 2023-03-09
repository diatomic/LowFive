import lowfive      # lowfive needs to be imported before h5py
import h5py
import numpy as np

vol = lowfive.create_MetadataVOL()
vol.set_memory("*","*")

with h5py.File('foo.h5', 'w') as f:
    f['data'] = np.ones((4, 3, 2), 'f')

    f['data'].dims[0].label = 'z'
    f['data'].dims[2].label = 'x'

    f['x1'] = [1, 2]
    f['x2'] = [1, 1.1]
    f['y1'] = [0, 1, 2]
    f['z1'] = [0, 1, 4, 9]

    f['x1'].make_scale()
    f['x2'].make_scale('x2 name')
    f['y1'].make_scale('y1 name')
    f['z1'].make_scale('z1 name')

    f['data'].dims[0].attach_scale(f['z1'])
    f['data'].dims[1].attach_scale(f['y1'])
    f['data'].dims[2].attach_scale(f['x1'])
    f['data'].dims[2].attach_scale(f['x2'])
