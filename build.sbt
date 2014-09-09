import AssemblyKeys._

assemblySettings

name := "fulfillment"

version := "1.0-SNAPSHOT"

scalaVersion := "2.10.2"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
  "Internal Snapshot Repository" at "http://oak.dev.balihoo.local:8080/archiva/repository/snapshots/",
  "Internal Repository" at "http://oak.dev.balihoo.local:8080/archiva/repository/internal/",
  "Keyczar at Google Code" at "http://keyczar.googlecode.com/svn/trunk/java/maven/"
)


libraryDependencies ++= Seq(
  //aws
  "com.amazonaws" % "aws-java-sdk" % "1.6.12",
  "org.scala-lang" % "scala-actors" % "2.10.2",
  //jansi is included with ivy2, but deduplicate fails so exclude it from jline explicitly
  ("org.scala-lang" % "jline" % scalaVersion.value).exclude("org.fusesource.jansi", "jansi"),
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  //joda
  "org.joda" % "joda-convert" % "1.5",
  "joda-time" % "joda-time" % "2.3",
  //json. Using play's implementation because its already known to the team
  "com.typesafe.play" % "play-json_2.10" % "2.2.0",
  "org.specs2" %% "specs2" % "2.3.12" % "test",
  "commons-io" % "commons-io" % "2.4",
  "commons-configuration" % "commons-configuration" % "1.10",
  "commons-beanutils" % "commons-beanutils" % "1.9.2",
  ("com.balihoo.commons" % "commons-client" % "latest.snapshot").exclude("com.google.guava", "guava"),
  "com.google.api-ads" % "ads-lib" % "1.29.0",
  "com.google.api-ads" % "adwords-axis" % "1.29.0",
  "javax.activation" % "activation" % "1.1",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106",
  "org.keyczar" % "keyczar" % "0.66",
  "com.stackmob" %% "newman" % "1.3.5"
)

scalacOptions in Test ++= Seq("-Yrangepos")

// Read here for optional dependencies:
// http://etorreborre.github.io/specs2/guide/org.specs2.guide.Runners.html#Dependencies
resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

///Use this to set a specific main class and run that when running the jar with:
///  java -jar target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar
// exportJars := true
// mainClass in (Compile, run) := Some("com.balihoo.fulfillment.main")
// mainClass in (Compile, packageBin) := Some("com.balihoo.fulfillment.main")

///Or, leave that all out and run the desired main like this
// java -cp target/scala-2.10/fulfillment-assembly-1.0-SNAPSHOT.jar com.balihoo.fulfillment.workers.sendemailworker -p <propfile> -d <propdir>

resourceGenerators in Compile <+= (resourceManaged, baseDirectory) map
  { (managedBase, base) =>
    val webappBase = base / "src" / "main" / "webapp"
    for {
      (from, to) <- webappBase ** "*" x rebase(webappBase, managedBase /
        "main" / "webapp")
    } yield {
      Sync.copy(from, to)
      to
    }
  }

//exclude some sources to allow testing compilation
// of refactoring changes one file at a time
excludeFilter in unmanagedSources :=
/*
//adapters
  "FTPAdapter.scala" ||
  "SQSAdapter.scala" ||
  "SESAdapter.scala" ||
  "AWSAdapter.scala" ||
  "SWFAdapter.scala" ||
  "DynamoAdapter.scala" ||
  "AdWordsAdapter.scala" ||
//utilities
  "Splogger.scala" ||
  "Getch.scala" ||
//config
  "PropertiesLoader.scala" ||
  "FTPUploadConfig.scala" ||
  "FulfillmentSection.scala" ||
  "CategorizedSections.scala" ||
  "FulfillmentCoordinator.scala" ||
  "SWFIdentifier.scala" ||
//dashboard
  "RestServlet.scala" ||
  "Dashboard.scala" ||
  "CommandComponent.scala" ||
//workers
  "FulfillmentWorker.scala" ||
  "AdWordsAccountCreator.scala" ||
  "AdWordsAccountLookup.scala" ||
  "AdWordsAdGroupProcessor.scala" ||
  "AdWordsCampaignProcessor.scala" ||
  "AdWordsUserInterests.scala" ||
  "AdWordsImageAdProcessor.scala" ||
  "AdWordsTextAdProcessor.scala" ||
  "GeoNamesTimeZoneRetriever.scala" ||
  "Chaos.scala" ||
  "EmailAddressVerifier.scala" ||
  "EmailSender.scala" ||
  "EmailVerifiedAddressLister.scala" ||
  "FtpUploadValidator.scala" ||
  "FtpUploader.scala" ||
  "Noop.scala" ||
  "ZipCodeDemographics.scala" ||
  "ParticipantData.scala" ||
*/
  "EmailWorkers.scala" ||
  ""
