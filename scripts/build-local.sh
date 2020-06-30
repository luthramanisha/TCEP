#!/usr/bin/env bash
# Author: Manisha Luthra
# Modified by: Sebastian Hennig
# Description: Builds and creates the docker image of TCEP

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/../"

cd $work_dir
sbt clean
sbt one-jar

if [ $? -ne 0 ]
then
    printf "\nBuild failed\n"
    exit 1
fi

rm -rf $work_dir/dockerbuild

mkdir $work_dir/dockerbuild
printf "\nBuilding image\n"
cp -R $work_dir/data $work_dir/dockerbuild/data
cp $work_dir/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar $work_dir/dockerbuild
cp $work_dir/Dockerfile $work_dir/dockerbuild
cp $work_dir/docker-entrypoint.sh $work_dir/dockerbuild
docker build -t tcep $work_dir/dockerbuild

exit 0
