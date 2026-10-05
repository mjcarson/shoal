#!/bin/sh
# Spike X14's E6: Ceph's own CRUSH on X2's shapes, offline, with crushtool from the image v20.2.0
# was released as. Needs no cluster: run from the repository root on any host with docker.
#
#   sh shoal-spike/results/x14-crush.sh      # writes shoal-spike/results/x14-e6-crush.md
set -u
IMAGE=${IMAGE:-quay.io/ceph/ceph@sha256:1228c3d05e45fbc068a8c33614e4409b6dac688bcc77369b06009b5830fa8d86}
sudo docker run --rm -v "$(pwd)/shoal-spike/results:/x14" --entrypoint python3 "$IMAGE" \
    /x14/x14-crush.py > shoal-spike/results/x14-e6-crush.md
