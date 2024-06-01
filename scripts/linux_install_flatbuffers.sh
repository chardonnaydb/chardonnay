#!/bin/bash

set -euo pipefail

cd /tmp
git clone https://github.com/google/flatbuffers.git
cd flatbuffers
git checkout v24.3.25
cmake -G "Unix Makefiles"
make -j$(nproc) #compile
sudo make install #install
sudo ldconfig #Configuring a dynamic link library
flatc --version #Check if FlatBuffers is installed successfully
