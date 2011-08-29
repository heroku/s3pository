web: sh target/bin/s3rver
updater: sh target/bin/s3updater maven-metadata SNAPSHOT
fullupdater: sh target/bin/s3updater
stress: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Stress -Dexec.classpathScope=test -Dexec.args='http://s3pository.heroku.com:80/ 4 640'
blitz: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Blitz -Dexec.classpathScope=test
load: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Stress -Dexec.classpathScope=test -Dexec.args='http://s3pository.heroku.com:80/ 4 64000'
disable: sh target/bin/disableJava && sh target/bin/disableScala && sh target/bin/disablePlay
enable: sh target/bin/enableJava && sh target/bin/enableScala && sh target/bin/enablePlay
updateSettings: sh target/bin/updateJavaSettings && sh target/bin/updatePlaySettings
