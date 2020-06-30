#!/usr/bin/env bash
# Author: Manisha Luthra
# Modified by: Sebastian Hennig
# Modified by: Niels Danger
# Description: Builds and creates the docker image of TCEP

work_dir="$(cd "$(dirname "$0")" ; pwd -P)/.."
source "${work_dir}/docker-swarm.cfg"
remote_build='false'
sbt_remote='false'

build_local() {
  cd $work_dir
  sbt assembly || exit 1

  if [[ $registry_user == "localhost:5000" ]]; then
    echo "creating local docker registry service"
    [[ $(docker service ls -q -f name=registry) ]] || docker service create --name registry --publish 5000:5000 registry:2
  else
    echo "dockerhub login"
    docker login
  fi
  mkdir $work_dir/dockerbuild
  printf "\nBuilding image\n"
  cp $work_dir/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar $work_dir/dockerbuild
  cp $work_dir/Dockerfile $work_dir/dockerbuild
  cp $work_dir/docker-entrypoint.sh $work_dir/dockerbuild
  cp -r $work_dir/mobility_traces $work_dir/dockerbuild/
  docker build -t $registry_user/$tcep_image $work_dir/dockerbuild && \
  printf "\nBuilding GUI image\n" && \
  cd $work_dir/gui && \
  docker build -t $registry_user/$gui_image . && \
  printf "\nPushing images to registry\n" && \
  docker push $registry_user/$tcep_image && \
  docker push $registry_user/$gui_image
}


# builds onejar and docker image remotely on the manager -> use when developing on non-linux OS, since docker container must be built with linux
build_remote() {
    ssh $user@$manager "rm -rd ~/tcep"
    ssh $user@$manager "mkdir -p ~/tcep/dockerbuild && mkdir -p ~/tcep/src && mkdir -p ~/tcep/gui && mkdir -p ~/tcep/project && mkdir -p ~/cplex"
    rm src.zip && rm gui.zip
    scp $work_dir/Dockerfile $user@$manager:~/tcep/dockerbuild/
    scp $work_dir/docker-entrypoint.sh $user@$manager:~/tcep/dockerbuild/

    printf "\nLogin required to push images for localhost\n"
    ssh $user@$manager "docker login -u $registry_user -p $PW"

    #zip -r -o src.zip $work_dir/src
    zip a -r src.zip $work_dir/src > /dev/null # using 7zip on windows
    scp src.zip $user@$manager:~/tcep && ssh $user@$manager "unzip -o ~/tcep/src.zip -d ~/tcep" 2> /dev/null
    scp $work_dir/build.sbt $user@$manager:~/tcep/build.sbt
    scp $work_dir/project/plugins.sbt $user@$manager:~/tcep/project/
    scp -r $work_dir/mobility_traces $user@manager:~/tcep

    zip a -r gui.zip $work_dir/gui > /dev/null
    scp gui.zip $user@$manager:~/tcep && ssh $user@$manager "unzip -o ~/tcep/gui.zip -d ~/tcep" 2> /dev/null

    if [[ $sbt_remote == 'true' ]]; then
        ssh -T $user@$manager <<-'ENDSSH'

    if ! [ -x "$(command -v sbt)" ]; then
        printf "\n sbt not installed on manager $manager , installing now..."
        echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
        sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
        sudo apt-get update && sudo apt-get install sbt
    fi

    if ! [ -x "$(command -v java)" ]; then
        printf "\n jre not installed on manager $manager, installing now..."
        sudo apt-get update && sudo apt-get install openjdk-8-jdk
    fi
ENDSSH

        printf "\n Building fatjar with assembly on remote... \n"
        ssh $user@$manager "cd ~/tcep && sbt assembly"
        ssh $user@$manager "cp ~/tcep/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar ~/tcep/dockerbuild"

    else # sbt build locally
        rm -f $work_dir/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar
        printf "\n building fatjar with assembly locally..."
        cd $work_dir && sbt assembly || exit 1
        scp $work_dir/target/scala-2.12/tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar $user@$manager:~/tcep/dockerbuild/
    fi

    printf "\n Building Docker image of application and GUI \n"
    ssh $user@$manager "cp -r ~/tcep/mobilityTraces ~/tcep/dockerbuild/"
    ssh $user@$manager "cd ~/tcep/dockerbuild && docker build -t tcep ~/tcep/dockerbuild && docker tag tcep $registry_user/$tcep_image" || exit 1
    ssh $user@$manager "cd ~/tcep/gui && docker build -t tcep-gui . && docker tag tcep-gui $registry_user/$gui_image" || exit 1
    printf "\nPushing images to registry\n"
    ssh $user@$manager "docker push $registry_user/$tcep_image && docker push $registry_user/$gui_image"

}


if [[ $remote_build == 'false' ]]; then
    build_local $1
else
    build_remote $1
fi
