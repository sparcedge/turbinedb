import sbt._
import Keys._
import com.github.retronym.SbtOneJar

object BuildSettings {
	val buildOrganization = "com.sparcedge"
	val buildVersion      = "0.1"
	val buildScalaVersion = "2.10.0"

	val buildSettings = Defaults.defaultSettings ++ Seq (
		organization := buildOrganization,
		version      := buildVersion,
		scalaVersion := buildScalaVersion
	)
}

object Resolvers {
	val typesafeRepo = "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
	val sprayRepo = "Spray Repo" at "http://repo.spray.io"
	val playJsonSnapRepo = "Mandubian repository snapshots" at "https://github.com/mandubian/mandubian-mvn/raw/master/snapshots/"
	val playJsonRelRepo = "Mandubian repository releases" at "https://github.com/mandubian/mandubian-mvn/raw/master/releases/"
}

object Dependencies {
	val akkaVersion = "2.1.1"
	val sprayVersion = "1.1-M7"

	val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
	val akkaRemote = "com.typesafe.akka" %% "akka-remote" % akkaVersion
	val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
	val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
	val akkaCluster = "com.typesafe.akka" %% "akka-cluster-experimental" % akkaVersion

	val sprayCan = "io.spray" % "spray-can" % sprayVersion
	val sprayRouting = "io.spray" % "spray-routing" % sprayVersion

	val playJson = "play" %% "play-json" % "2.2-SNAPSHOT"
	val jodaTime = "joda-time" % "joda-time" % "2.1"
	val jodaConvert = "org.joda" % "joda-convert" % "1.2"
	val slf4j = "org.slf4j" % "slf4j-nop" % "1.6.4"
	val journalio = "com.github.sbtourist" % "journalio" % "1.3"

	val scalatest	= "org.scalatest" %% "scalatest" % "1.9.1" % "test"

	val akkaDependencies = Seq(akkaActor, akkaRemote, akkaSlf4j, akkaTestkit, akkaCluster)
	val sprayDependencies = Seq(sprayCan, sprayRouting)
	val miscDependencies = Seq(jodaTime, jodaConvert, slf4j, scalatest, journalio, playJson)
	val allDependencies = akkaDependencies ++ sprayDependencies ++ miscDependencies
}

object TurbineDB extends Build {
	import Resolvers._
	import BuildSettings._

	lazy val turbineDB = Project (
		id = "turbine-db",
		base = file("."),
		settings = buildSettings ++ SbtOneJar.oneJarSettings ++ Seq (
			resolvers ++= Seq(typesafeRepo, sprayRepo, playJsonSnapRepo),
			libraryDependencies ++= Dependencies.allDependencies,
			crossTarget := new File("/Users/bgilbert/projects/personal/turbine-db/build")
		)
	)
}