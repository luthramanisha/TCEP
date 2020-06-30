# Approach#1 #
	
- Update `build.sbt` 
	- Add `akka-remote` dependency
- Create `application.conf` file
- Update `tcep\src\main\scala\tcep\simulation\Main.scala`

	    val actorSystem: ActorSystem = ActorSystem()
	to 
    	
    	val config=ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
    	.withFallback(ConfigFactory.load())
    	
    	val actorSystem: ActorSystem = ActorSystem("ClusterSystem",config)
 
# Approach#2 #

- Create `PublisherApp` (using `object` keyword)
- Create `actorsystem` inside that object. 
	- We don't need to create 4 publishers as we have in `tcep\src\main\scala\tcep\simulation\Main.scala`.
- Upon `MemberUp` event, we will add publisher inside the **lookup** table
- Create `Graph` object (using object keyword)
- Create `actorsystem` inside that object class. And pass that actor object to `GraphFactory`

# Approach#3 #

- Create `lookup` table of `actorsystem` with varying configuration & respective actor listener class
- `GraphFactory` will use get the required `actorsystem` from the `lookup`.