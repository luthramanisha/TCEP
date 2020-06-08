## Troubleshooting

### Setting up GENI

* Upload your ssh keys atleast some minutes ago before accessing the cluster. It takes a while for GENI to upload the SSH keys - GENI test - "Update SSH keys" because of which you might get "Permission denied (public key)" error. Only after the ssh keys are uploaded you can access the cluster. 

### Deploying TCEP

* Currently, in case the services are not initially placed successfully, TCEP will not be able to start the execution. Use the following command to see if the services are placed
`docker stack ps tcep`. If some service is not placed, make sure this node is correctly setup. Otherwise, remove it from `docker-swarm.cfg` and regenerate the docker swarm configuration `docker-swarm.yml`.

* Sometimes GENI resets the nodes. Thus, everytime you are deploying TCEP, make sure you run the setup. 
This could lead (reset of GENI) to the following message: 
```
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!
Someone could be eavesdropping on you right now (man-in-the-middle attack)!
It is also possible that a host key has just been changed.
The fingerprint for the RSA key sent by the remote host is
SHA256:oKDbOG9AlL5wor4V9cu09pZVmj8y3hX6tqqKVsZ0waY.
Please contact your system administrator.
Add correct host key in /home/manisha/.ssh/known_hosts to get rid of this message.
Offending RSA key in /home/manisha/.ssh/known_hosts:44
  remove with:
  ssh-keygen -f "/home/manisha/.ssh/known_hosts" -R 72.36.65.79
RSA host key for 72.36.65.79 has changed and you have requested strict checking.
Host key verification failed.
```

To resolve this, please *remove* the respective line, example line 44 from `.ssh/known_hosts`, to enable the connection. 

* The service deployment might fail with the error "Rejected 2 seconds ago "No such image: mluthra/tcep:l…"". This might be caused due to failure in pulling the image to the resource. Make sure that either the repository is public, or you login to docker using your credentials before pulling the image.

* Make sure that the producers and the simulation services are up to confirm that TCEP is running fine. The service might not get started for some reason you see `docker stack ps tcep` "Starting x..sec/min ago" in the current state of the service. To troubleshoot this,
    -  either please change the cluster which is not running to the one that is running in the `docker-stack.yml` file and deploy system again by `publish_tcep.sh publish`
    - restart the nodes causing troubles by `sudo shutdown -r now` 	 
Note: you do not have to restart the swarm (or take it down). Just re-deploying the services would work. 

* If you do not see any transition modes in the GUI, then please check your docker-stack yml file for the following:
    - you need to expose port 25001 (in the simulator service) so make sure you have - 25001:25001 in 'ports'
    - make sure you started the simulation with TEST_GUI. To set it please pass '7' as an argument in the simulator service 


* If you see the following issue when running the setup script "sudo: no tty present and no askpass program specified" it usually means that the user you are using on the instance is not allowed to issue sudo commands without entering the password everytime.
If it is ok for you to let your user run sudo commands without entering the password everytime you can do the following steps when connected to the instance
    - Issue command `sudo visudo`
    - The /etc/sudoers file will open
    - Add the following line to the bottom of the file: `username ALL=(ALL) NOPASSWD:ALL` where username is the username you are using when connected to the instance
This will allow sudo commands to be executed without entering the password for this user everytime.

* If you see the issue "Got permission denied while trying to connect to the Docker daemon socket at unix:///var/run/docker.sock: Get http://%2Fvar%2Frun%2Fdocker.sock/v1.35/info: dial unix /var/run/docker.sock: connect: permission denied" it usually means that the current user
you are using is not permitted to access the docker daemon. You can fix this by adding the current user to the group again by issuing the following command on the respective cluster node `sudo usermod -a -G docker $USER`