web: target/start com.heroku.maven.s3pository.S3rver
updater: target/start com.heroku.maven.s3pository.S3Updater maven-metadata SNAPSHOT
fullupdater: target/start com.heroku.maven.s3pository.S3Updater
stress: target/start com.heroku.maven.s3pository.Stress http://s3pository.heroku.com:80/ 4 640
disable target/start com.heroku.maven.s3pository.JavaOff &&  target/start target/start com.heroku.maven.s3pository.ScalaOff target/start && target/start com.heroku.maven.s3pository.PlayOff
enable: target/start com.heroku.maven.s3pository.JavaOn &&  target/start target/start com.heroku.maven.s3pository.ScalaOn target/start && target/start com.heroku.maven.s3pository.PlayOn
updateSettings: target/start com.heroku.maven.s3pository.JavaUpdate &&  target/start com.heroku.maven.s3pository.PlayUpdate
