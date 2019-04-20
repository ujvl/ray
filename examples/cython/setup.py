from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from distutils.extension import Extension

import os
from setuptools import setup
from Cython.Build import cythonize

from Cython.Distutils import build_ext

import numpy

pkg_dir = "cython_examples"
modules = ["cython_simple.pyx"]
install_requires = ["cython", "numpy"]
include_dirs = [numpy.get_include()]

# TODO: Need scipy to run BrainIAK example, but don't want to add additional
# dependencies
#try:
#    import scipy  # noqa
#    modules.append("cython_blas.pyx")
#    install_requires.append("scipy")
#except ImportError as e:  # noqa
#    pass

modules = [os.path.join(pkg_dir, module) for module in modules]

setup(
        name=pkg_dir,
        version="0.0.1",
        description="Cython examples for Ray",
        packages=[pkg_dir],
        ext_modules=[
            Extension(pkg_dir,
                sources=modules,
                language='c++',
                extra_compile_args=['-std=c++11'])],
        install_requires=install_requires,
        include_dirs=include_dirs,
        cmdclass = {'build_ext': build_ext}
        )
