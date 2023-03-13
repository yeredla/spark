name := "sayari_sanctions"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8"
)

assemblyMergeStrategy in assembly := {
  case (m: String) if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case (m: String) if m.toLowerCase.endsWith("META-INF") => MergeStrategy.discard
  case PathList("git.properties") => MergeStrategy.first
  case x => MergeStrategy.defaultMergeStrategy(x)
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case _ => MergeStrategy.discard

}
