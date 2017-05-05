
// available for Scala 2.11.8, 2.12.0
libraryDependencies += "co.fs2" %% "fs2-core" % "0.9.5"

// optional I/O library
libraryDependencies += "co.fs2" %% "fs2-io" % "0.9.5"

scalaVersion := "2.12.2"

scalacOptions ++= Seq(
  "-deprecation",
  "-encoding", "UTF-8", // yes, this is 2 args
  "-feature",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  // "-Ywarn-dead-code", // N.B. doesn't work well with the ??? hole
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard",
  "-Ywarn-unused",
  "-Ywarn-unused-import",
  "-Xfuture",
  "-Ypartial-unification"
)
