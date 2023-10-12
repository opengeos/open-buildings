#!/usr/bin/env bash

#################################################################
#                                                               #
#   Simple Bash script to simplify development in a container.  #
#   Builds Dockerfile.dev if it does not already exists.        #
#   The CWD is mounted in the container.                        #
#                                                               #
#   Use --rebuild flag to force rebuild of the image even if    #
#   (a potentially older version of) the image already exists.  #
#   This is necessary if e.g. the dependencies are updated.     #
#                                                               #
#################################################################


rebuild=$([[ $1 == '--rebuild' ]] && echo true || echo false)

if [[ $rebuild == true ]] 
then
    docker build --build-arg UID=$UID --build-arg USER=$USER -t ob-dev -f Dockerfile.dev .
else
    docker inspect --type=image ob-dev &> /dev/null || {
        echo "Image doesn't exist locally, building ...";
        docker build --build-arg UID=$UID --build-arg USER=$USER -t ob-dev -f Dockerfile.dev .
    }
fi

docker run -it --workdir /workspace -v $PWD:/workspace -v $HOME/.gitconfig:$HOME/.gitconfig ob-dev bash