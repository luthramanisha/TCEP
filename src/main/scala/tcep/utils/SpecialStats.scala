package tcep.utils

import java.io.{BufferedOutputStream, File, FileOutputStream, PrintStream}
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors, TimeUnit}

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.collection.immutable.{HashMap, SortedMap}
import scala.collection.mutable
import scala.util.Random

/**
  * Saves special stats in stats.log file
  * Created on 17/01/2018.
  */
object SpecialStats {
  val logFilePath = System.getProperty("logFilePath")
  val logger = LoggerFactory.getLogger(getClass)
  val debugID = Random.nextInt()
  val debug = new ConcurrentLinkedQueue[String]()
  val log = new TrieMap[String, ConcurrentLinkedQueue[String]]() // outer map: tag (separate files) -> inner queue (timestamp -> message)
  val startTime = System.currentTimeMillis()
  private val refreshInterval = 10

  Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() => publish(), 10, refreshInterval, TimeUnit.SECONDS)

  def publish(): Unit = {
    try {
      log.keys.foreach(k => {
        val stats = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(logFilePath, s"stats-$k-$debugID.csv"), true)))
        val sb = new mutable.StringBuilder()
        log(k).forEach(l => sb.append(l))
        stats.append(sb.toString())
        stats.flush()
        stats.close()
        log.remove(k)
      })
      log.clear()

      val debugFile = new PrintStream(new BufferedOutputStream(new FileOutputStream(new File(logFilePath, s"debug-$debugID.csv"), true)))
      val sb = new mutable.StringBuilder()
      debug.forEach(l => sb.append(l + "\n"))
      debugFile.append(sb.toString)
      debugFile.flush()
      debugFile.close()
      debug.clear()
    }
    catch {
      case e: Throwable => logger.error("error in SpecialStats logger: ", e)
    }
  }

  /**
    * create a separate logfile with name tag
    * @param caller caller of the log entry
    * @param tag filename
    * @param msg
    */
  def log(caller: String, tag: String, msg: String, timestamp: String = getTimestamp): Unit = {
    try {
      val tagQueue = log.getOrElse(tag, new ConcurrentLinkedQueue[String])
      tagQueue.add(s"$timestamp;$caller;$msg\n")
      if(!log.contains(tag)) log.update(tag, tagQueue)
    }
    catch {
      case e: Throwable => logger.error("error in SpecialStats logger: ", e)
    }
  }

  def debug(caller: String, msg: String): Unit = debug.add(s"$getTimestamp;$caller;$msg\n")

  def getTimestamp: String = {
    val now = System.currentTimeMillis()
    val passed = now - startTime
    val df: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSS")
    s"${df.format(now)};${df.format(passed)}"
  }
}
