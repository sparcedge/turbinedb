
resolvers += Classpaths.typesafeResolver

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.5.0")

resolvers += "retronym-releases" at "http://retronym.github.com/repo/releases"

resolvers += "retronym-snapshots" at "http://retronym.github.com/repo/snapshots"

addSbtPlugin("com.github.retronym" % "sbt-onejar" % "0.6")

resolvers += "gilbertw1-scct-plugin" at "https://raw.github.com/gilbertw1/sbt-scct-repo/master/repo"

addSbtPlugin("ch.craven" %% "scct-plugin" % "0.2.1")

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.0.0")