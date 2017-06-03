Spark Web page analyzer [![Build Status](https://travis-ci.org/GRpro/spark-popular-words-web.svg?branch=master)]
-----------------------------

This is a Spark job which receives a bunch of URIs of web pages and calculates top 10 popular words on these pages.

*Java 1.8*, *Docker*, *Maven* have to be installed on your machine.
The project has been developed and tested on MacOS Sierra 10.12.5.

### Run

1. Put in `data/input.txt` URLs of web pages that you wanna analyze (URL per line)
2. Go to project root directory and execute `run.sh`. This will pull the image from [Docker Hub repo](https://hub.docker.com/r/grpro/spark-popular-words-web/)
   and start the container with mounted `data` directory as a volume.

After the job finishes see results within `data/output` directory

### Build

In order to compile application and create docker image locally execute:

`mvn clean install -Pdocker`

See `docker images` output to find new image

Use `run.sh` to run it.





