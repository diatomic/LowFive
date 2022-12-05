# Copyright 2013-2020 Lawrence Livermore National Security, LLC and other
# Spack Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: (Apache-2.0 OR MIT)

from spack import *


class Lowfive(CMakePackage):
    """HDF5 VOL Plugin for In Situ Data Transport."""

    homepage = "https://github.com/diatomic/LowFive.git"
    url      = "https://github.com/diatomic/LowFive.git"
    git      = "https://github.com/diatomic/LowFive.git"

    version('master', branch='master')
    version('tom-group-get', branch='tom-group-get')    # TODO: remove when no longer needed

    depends_on('mpi')
    depends_on('hdf5+mpi+hl@1.12.1', type='link')
    depends_on('zlib', type='link')

    def cmake_args(self):
        args = ['-DCMAKE_C_COMPILER=%s' % self.spec['mpi'].mpicc,
                '-DCMAKE_CXX_COMPILER=%s' % self.spec['mpi'].mpicxx,
                '-DBUILD_SHARED_LIBS=false',
                self.define_from_variant('lowfive_install_examples', 'examples')]
        return args
