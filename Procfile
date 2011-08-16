web: sh target/bin/s3rver
updater: sh target/bin/s3updater maven-metadata SNAPSHOT
fullupdater: sh target/bin/s3updater
stress: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Stress -Dexec.classpathScope=test -Dexec.args='http://s3pository.herokuapp.com:80/ 4 640'
blitz: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Blitz -Dexec.classpathScope=test
load: mvn exec:java -Dexec.mainClass=com.heroku.maven.s3pository.Stress -Dexec.classpathScope=test -Dexec.args='http://s3pository.herokuapp.com:80/ 4 64000'
disable: sh target/bin/disable
enable: sh target/bin/enable
updateSettings: sh target/bin/updateSettings
