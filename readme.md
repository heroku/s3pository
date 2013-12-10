#s3pository - Proxy Server with S3 caching

This server is used to speed up builds of Java, Scala, Play!, and Node.js on Heroku.  It acts as a caching proxy server to several popular repositories.

See the [Java](https://github.com/heroku/heroku-buildpack-java), [Scala](https://github.com/heroku/heroku-buildpack-scala) and [Play!](https://github.com/heroku/heroku-buildpack-play) buildpacks for the locations of the corresponding build tool settings used,
which are downloaded from S3 during the execution of the buildpack's bin/compile script.

##License

    Copyright 2001-2013 Scott Clasen <scott.clasen@gmail.com>

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

