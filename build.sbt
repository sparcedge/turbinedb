organization := "com.sparcedge"

name := "turbine-db"

version := "0.1"

scalaVersion := "2.10.0"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "spray repo" at "http://repo.spray.io"

seq(com.github.retronym.SbtOneJar.oneJarSettings: _*)

libraryDependencies ++= Seq (
	"com.typesafe.akka" %% "akka-actor" % "2.1.0",
	"com.typesafe.akka" %% "akka-remote" % "2.1.0",
	"com.typesafe.akka" %% "akka-slf4j" % "2.1.0",
	"org.json4s" %% "json4s-jackson" % "3.1.0",
	"org.slf4j" % "slf4j-nop" % "1.6.4",
	"io.spray" % "spray-can" % "1.1-M7",
	"io.spray" % "spray-routing" % "1.1-M7",
	"joda-time" % "joda-time" % "2.1",
	"org.joda" % "joda-convert" % "1.2",
	"org.scalatest" %% "scalatest" % "1.9.1" % "test"
)

crossTarget := new File("/Users/bgilbert/projects/personal/turbine-db/build")
