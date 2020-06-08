package tcep.simulation.adaptive.cep

import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import javax.management.{Attribute, ObjectName}
import org.slf4j.LoggerFactory

import scala.language.postfixOps
import scala.sys.process._
/**
  * Created by Raheel on 05/07/2017.
  */
object SystemLoad {
  val log = LoggerFactory.getLogger(getClass)

  @volatile
  var runningOperators: AtomicInteger = new AtomicInteger(0)
  val maxOperators = 100d

  def newOperatorAdded(): Unit = {
    runningOperators.incrementAndGet()
  }

  def operatorRemoved(): Unit = {
    runningOperators.decrementAndGet()
  }

  def isSystemOverloaded(currentLoad: Double, maxLoad: Double): Boolean = {
    currentLoad > maxLoad
  }

  def getSystemLoad: Double = {
    getCpuUsageByJavaManagementFactory
  }

  // this takes about ~7-10ms!
  private def getCpuUsageByUnixCommand: Double = {
    //TODO: make this generic, currently this only works for alpine linux
    val loadavg = "cat /proc/loadavg".!!;
    loadavg.split(" ")(0).toDouble
  }

  private def getCpuUsageByJavaManagementFactory: Double = {
    val bean = ManagementFactory.getOperatingSystemMXBean
    val mbs = ManagementFactory.getPlatformMBeanServer
    val name = ObjectName.getInstance("java.lang:type=OperatingSystem")
    val list = mbs.getAttributes(name, Array("SystemCpuLoad"))
    if (!list.isEmpty) {
      list.get(0).asInstanceOf[Attribute].getValue.asInstanceOf[Double]
    } else {
      0
    }
  }
}
