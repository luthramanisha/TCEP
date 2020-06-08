# TCEP GUI

## Setup

The project provides a Dockerfile to quickly build and run a version of the GUI and GUI server. To build the docker container run the following command:

```
docker build -t tcep-gui . 
```

After the image is build successfully, you can run the GUI with the following command:

```
docker run -p 3000:3000 -d tcep-gui
```

You can access the GUI by navigating with the browser to http://localhost:3000
