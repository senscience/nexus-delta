addSbtPlugin("org.typelevel" % "sbt-tpolecat" % "0.5.7")

addSbtPlugin("org.scalameta"    % "sbt-scalafmt"              % "2.6.2")
addSbtPlugin("ch.epfl.scala"    % "sbt-scalafix"              % "0.14.7")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"             % "2.4.4")
addSbtPlugin("com.github.cb372" % "sbt-explicit-dependencies" % "0.3.1")
addSbtPlugin("com.timushev.sbt" % "sbt-updates"               % "0.7.0")

addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")
addSbtPlugin("com.eed3si9n"   % "sbt-assembly"        % "2.3.1")
addSbtPlugin("com.eed3si9n"   % "sbt-buildinfo"       % "0.13.1")

addSbtPlugin("com.github.sbt"        % "sbt-site-paradox"           % "1.7.0")
addSbtPlugin("com.lightbend.paradox" % "sbt-paradox"                % "0.10.7")
addSbtPlugin("com.github.sbt"        % "sbt-paradox-material-theme" % "0.7.0")

addSbtPlugin("com.github.sbt" % "sbt-dynver"     % "5.1.1")
addSbtPlugin("com.github.sbt" % "sbt-ci-release" % "1.12.0")

addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.8")

addSbtPlugin("io.gatling" % "gatling-sbt" % "4.19.0")

addDependencyTreePlugin
