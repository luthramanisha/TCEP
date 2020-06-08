name := "tcep"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-cluster" % "2.6.0",
  "com.typesafe.akka" %% "akka-cluster-tools" % "2.6.0",
  "com.typesafe.akka" % "akka-cluster-metrics_2.12" % "2.6.0",
  "com.typesafe.akka" %% "akka-http"   % "10.1.5",
  "io.netty" % "netty" % "3.10.6.Final", // for using classic akka remoting instead of artery
  "ch.megard" %% "akka-http-cors" % "0.3.1",
  "com.typesafe.akka" %% "akka-slf4j" % "2.6.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.espertech"     %  "esper"        % "5.5.0",
  "com.twitter" % "chill-akka_2.12" % "0.9.2",
  "org.scala-lang" % "scala-reflect" % "2.12.1",
  "org.scalaj" %% "scalaj-http" % "2.4.1",
  "commons-lang" % "commons-lang" % "2.6",
  "com.google.guava" % "guava" % "19.0",
  "com.typesafe.akka" %% "akka-testkit" % "2.6.0"  % "test",
  "com.typesafe.akka" %% "akka-multi-node-testkit" % "2.6.0" % "test",
  "org.scalatest"     %% "scalatest"    % "3.0.1"   % "test",
  "org.mockito"       % "mockito-core"  % "2.23.0"  % "test"
)

import com.typesafe.sbt.SbtMultiJvm.multiJvmSettings

lazy val root = (project in file("."))
  .enablePlugins(MultiJvmPlugin) // use the plugin
  .configs(MultiJvm) // load the multi-jvm configuration
  .settings(multiJvmSettings: _*) // apply the default settings
  .settings(
    parallelExecution in Test := false // do not run test cases in parallel
  )

//mainClass in(Compile, run) := Some("tcep.simulation.tcep.SimulationRunner")
mainClass in assembly := Some("tcep.simulation.tcep.SimulationRunner")
assemblyJarName in assembly := "tcep_2.12-0.0.1-SNAPSHOT-one-jar.jar"
test in assembly := {} // skip tests on build


 