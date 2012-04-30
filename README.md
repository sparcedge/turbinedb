Turbine Blade
=============

Scala code base for the turbine blade, which is a standalone binary that caches and handles queries submitted via standard in.

Prerequisites
------------

* [SBT](https://github.com/harrah/xsbt) -- [install instructions](https://github.com/harrah/xsbt/wiki/Getting-Started-Setup)
* [Scala IDE (Eclipse)](http://www.scala-ide.org/) -- [install](http://download.scala-ide.org/)

Getting Started
------------

Download Project References
	
	$ sbt update

Generate Eclipse Project

	$ sbt eclipse

Generate IntelliJ Idea Project

	$ sbt idea

Compile

	$ sbt compile
	
Test
	
	$ sbt test

Package (Create Jar)

	$ sbt one-jar