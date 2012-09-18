organization := "com.sparcedge"

name := "turbine-blade"

version := "0.1"

scalaVersion := "2.9.1"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

seq(ScctPlugin.scctSettings: _*)

libraryDependencies ++= Seq (
	"com.typesafe.akka" % "akka-actor" % "2.0.1",
	"com.typesafe.akka" % "akka-remote" % "2.0.1",
	"com.typesafe.akka" % "akka-slf4j" % "2.0.1",
	"org.slf4j" % "slf4j-nop" % "1.6.4",
	"com.mongodb.casbah" % "casbah_2.9.0-1" % "2.1.5.0",
	"net.liftweb" %% "lift-json" % "2.4-M4",
	"org.scalatest" %% "scalatest" % "1.6.1" % "test",
	"org.mockito" % "mockito-all" % "1.9.0" % "test"
)

//crossTarget := new File("/Users/bgilbert/projects/sparc/turbine-blade-disk/build")
