package tcep.simulation.tcep

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{entity, _}
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

class TCEPSocket(actorSystem: ActorSystem) {

  implicit val system: ActorSystem = actorSystem
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  private def config: Config = ConfigFactory.load()
  val log = LoggerFactory.getLogger(getClass)

  def startServer(startRequest: (String, String, List[String], String, String) => Unit, transitionRequest: List[String] => Unit, manualTransitionRequest: String => Unit, stopRequest: () => Unit, statusRequest: () => Map[String, String]): Unit = {

    val queries: List[String] = List("Stream", "Filter", "Conjunction", "Disjunction", "Join", "SelfJoin", "AccidentDetection")
    val algorithmNames = config.getConfig("benchmark.general").getStringList("algorithms")
    var algorithms = new ListBuffer[JSONObject]()

    algorithmNames.forEach(algo => {
      val algoConf = config.getConfig("benchmark").getConfig(algo)
      val optimizations: List[String] = algoConf.getStringList("optimizationCriteria").toList

      algorithms += JSONObject(Map(
        "algorithm" -> algo,
        "optimizationCriteria" -> JSONArray(optimizations)
      ))
    })

    val modes: List[String] = List("MFGS", "SMS")
    val mapeks: List[String] = ConfigFactory.load().getStringList("constants.mapek.availableTypes").toList

    val algorithmsResponse = JSONObject(Map(
      "algorithms" -> JSONArray(algorithms.toList),
      "modes" -> JSONArray(modes),
      "queries" -> JSONArray(queries),
      "mapekTypes" -> JSONArray(mapeks)
    )).toString()

    val route =
      cors() {
        path("algorithms") {
          get {
            log.info(s"received Algorithms request from GUI")
            complete(HttpEntity(ContentTypes.`application/json`, algorithmsResponse))
          }
        }
      } ~
      cors() {
        path("status") {
          get {
            log.info("received status request from GUI")
            val response = JSONObject(statusRequest()).toString()
            complete(HttpEntity(ContentTypes.`application/json`, response))
          }
        }
      } ~
      cors() {
        path("start") {
          post {
            entity(as[String]) { entity =>
              val result = JSON.parseFull(entity)
              result match {
                case Some(body: Map[String, Any]) => {
                  val criteria: List[String] = body.getOrElse("criteria", List("BDP")).asInstanceOf[List[String]]
                  val query: String = body.getOrElse("query", "Join").asInstanceOf[String]
                  val algorithmName: String = body.getOrElse("algorithmStr", "Relaxation").asInstanceOf[String]
                  val mapek: String = body.getOrElse("mapek", "requirementBased").asInstanceOf[String]
                  log.info("Algorithm name: ", algorithmName)
                  log.info(s"received Start simulation request from GUI with data \n $body")
                  startRequest(body.getOrElse("mode", "MFGS").asInstanceOf[String], algorithmName, criteria, query, mapek)
                  complete(HttpEntity("success"))
                }

                case None => complete(HttpEntity("Parsing failed"))
                case other => complete(HttpEntity("Unknown data structure: " + other))
              }
            }
          }
        }
      } ~
        cors() {
          path("stop") {
            post {
              entity(as[String]) { entity =>
                log.info("received Stop request from GUI")
                stopRequest()
                complete(HttpEntity("success"))
              }
            }
          }
        } ~
        cors() {
          path("autoTransition") {
            post {
              entity(as[String]) { entity =>
                // TODO: enable/disable auto transition
                complete(HttpEntity("success"))
              }
            }
          }
        } ~
      cors() {
        path("manualTransition") {
          post {
            entity(as[String]) { entity =>
              val result = JSON.parseFull(entity)
              result match {
                case Some(body: Map[String, String]) => {
                  log.info(s"received Manual Transition request from GUI with data \n $body")
                  manualTransitionRequest(body.getOrElse("algorithm", "Relaxation"))
                  complete(HttpEntity("success"))
                }

                case None => complete(HttpEntity("Parsing failed"))
                case other => complete(HttpEntity("Unknown data structure: " + other))
              }
            }
          }
        }
      } ~
      cors() {
        path("transition") {
          post {
            entity(as[String]) { entity =>
              val result = JSON.parseFull(entity)
              result match {
                case Some(criteria: List[Any]) =>
                  log.info(s"received Transition request from GUI with data \n $criteria")
                  transitionRequest(criteria.map(_.toString))
                  complete(HttpEntity("success"))
                case None => complete(HttpEntity("Parsing failed"))
                case other => complete(HttpEntity("Unknown data structure: " + other))
              }

            }
          }
        }
      }
    val port = 25001
    val server = Http().bindAndHandle(route, "0.0.0.0", port)
    log.info(s"Awaiting GUI requests at port $port")
  }
}