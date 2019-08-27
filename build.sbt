lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "edu.usma",
      scalaVersion := "2.11.12",
      version      := "0.0.1-SNAPSHOT"
    )),
    name := "cc",
libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided",
      "org.apache.spark" % "spark-streaming_2.11" % "2.3.1",
      "org.jwat" % "jwat-warc" % "1.1.1",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.11.618",
      "com.amazonaws" % "aws-java-sdk-emr" % "1.11.618"
    )
  )
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

