import sbt._
import Keys._
import com.github.retronym.SbtOneJar

object BuildSettings {
	val buildOrganization = "com.sparcedge"
	val buildVersion      = "0.1"
	val buildScalaVersion = "2.10.3"

	val buildSettings = Defaults.defaultSettings ++ Seq (
		organization := buildOrganization,
		version      := buildVersion,
		scalaVersion := buildScalaVersion
	)
}

object Resolvers {
	val typesafeRepo = "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
	val sprayRepo = "Spray Repo" at "http://repo.spray.io"
}

object Dependencies {
	val akkaVersion = "2.3.0"
	val sprayVersion = "1.3.1"

	val akkaActor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
	val akkaRemote = "com.typesafe.akka" %% "akka-remote" % akkaVersion
	val akkaSlf4j = "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
	val akkaTestkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
	val akkaCluster = "com.typesafe.akka" %% "akka-cluster" % akkaVersion

	val sprayCan = "io.spray" % "spray-can" % sprayVersion
	val sprayRouting = "io.spray" % "spray-routing" % sprayVersion

	val playJson = "com.typesafe.play" %% "play-json" % "2.2.2"
	val jodaTime = "joda-time" % "joda-time" % "2.1"
	val jodaConvert = "org.joda" % "joda-convert" % "1.2"
	val slf4j = "org.slf4j" % "slf4j-nop" % "1.6.4"
	val journalio = "com.github.sbtourist" % "journalio" % "1.4.2"

	val scalatest = "org.scalatest" %% "scalatest" % "2.1.0" % "test"
	val googleCaliper = "com.google.caliper" % "caliper" % "0.5-rc1"
	val googleInstrumenter = "com.google.code.java-allocation-instrumenter" % "java-allocation-instrumenter" % "2.0"

	val akkaDependencies = Seq(akkaActor, akkaRemote, akkaSlf4j, akkaTestkit, akkaCluster)
	val sprayDependencies = Seq(sprayCan, sprayRouting)
	val miscDependencies = Seq(jodaTime, jodaConvert, slf4j, journalio, playJson)
	val testDependencies = Seq(scalatest)
	val allDependencies = akkaDependencies ++ sprayDependencies ++ miscDependencies ++ testDependencies

	val benchmarkDependencies = Seq(googleCaliper, googleInstrumenter)
}

object TurbineDB extends Build {
	import Resolvers._
	import BuildSettings._
	import Defaults._

	lazy val turbineDB = 
		Project ("turbine-db", file("."))
			.settings ( buildSettings : _* )
			.settings ( SbtOneJar.oneJarSettings : _* )
			.settings ( resolvers ++= Seq(typesafeRepo, sprayRepo) )
			.settings ( libraryDependencies ++= Dependencies.allDependencies )
			.settings ( scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature") )

	lazy val turbineBenchmark = 
		Project ("benchmark", file("benchmark"))
			.settings ( buildSettings : _* )
			.settings ( resolvers ++= Seq(typesafeRepo, sprayRepo) )
			.settings ( libraryDependencies ++= Dependencies.allDependencies )
			.settings ( libraryDependencies ++= Dependencies.benchmarkDependencies )
			.settings ( fork in run := true )
			.settings ( javaOptions in run <++= (fullClasspath in Runtime) map { cp => Seq("-cp", sbt.Build.data(cp).mkString(":")) } )
			.dependsOn ( turbineDB )

}