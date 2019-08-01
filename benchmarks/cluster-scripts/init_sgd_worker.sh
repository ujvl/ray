#!/bin/bash

source activate tensorflow_p36

cp ~/ray/python/setup_skip_build.py ~/ray/python/setup.py
cd ~/ray/python && pip install -e . --verbose
git checkout -- ~/ray/python/setup.py

pip install -U horovod
git clone https://github.com/tensorflow/benchmarks ~/tf-benchmarks
cd ~/tf-benchmarks
git checkout cnn_tf_v1.12_compatible
