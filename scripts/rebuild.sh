#!/usr/bin/env bash

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"
cd $work_dir/gui
docker build -t tcep-gui .
docker tag tcep-gui mluthra/tcep-gui
docker push mluthra/tcep-gui
cd $work_dir/scripts/
bash publish_tcep.sh publish
