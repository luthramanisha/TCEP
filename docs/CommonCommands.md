# Docker containers
#### RAM usage of the containers
```docker stats $(docker ps --format={{.Names}})```


#### Disk usage of the containers
```docker ps -s```

#### Connect to a container
```docker exec -it <container-name> bash```

#### Build docker-compose file
```docker-compose build```

# Docker Services
#### Inspect a service
```docker service inspect --pretty utwlmmf2aqnl```

#### List all services
```docker service ls```

#### Create a service on a particular node
```docker service create <image> --constraint 'node.id == <node-id>' --replicas 0 --name <name>```

#### Remove a service
```docker service rm <service-id>```

#### Get logs of a service
```docker service logs <service-id>```
```docker service logs <service-id> -f #Continous logs```

#### Inpect why service crashed last time
```docker service ps <service-name>```
```docker service ps <service-name> --no-trunc```

#### Deploy a docker swarm
```docker stack deploy -c <file>.yml <name>```

### Scale a service in a swarm
```docker service scale tcep_worker=<instances>```