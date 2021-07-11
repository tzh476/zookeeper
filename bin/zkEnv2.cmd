@echo off
REM Licensed to the Apache Software Foundation (ASF) under one or more
REM contributor license agreements.  See the NOTICE file distributed with
REM this work for additional information regarding copyright ownership.
REM The ASF licenses this file to You under the Apache License, Version 2.0
REM (the "License"); you may not use this file except in compliance with
REM the License.  You may obtain a copy of the License at
REM
REM     http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing, software
REM distributed under the License is distributed on an "AS IS" BASIS,
REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM See the License for the specific language governing permissions and
REM limitations under the License.

set ZOOCFGDIR=%~dp0%..\conf
set ZOO_LOG_DIR=%~dp0%..\logs
set ZOO_LOG4J_PROP=INFO,CONSOLE

REM for sanity sake assume Java 1.6
REM see: http://java.sun.com/javase/6/docs/technotes/tools/windows/java.html

REM add the zoocfg dir to classpath
set CLASSPATH=%ZOOCFGDIR%

REM make it work in the release
SET CLASSPATH=%~dp0..\*;%~dp0..\lib\*;%CLASSPATH%

REM make it work for developers
SET CLASSPATH=%~dp0..\zookeeper-server\target\classes;%~dp0..\zookeeper-server\target\classes\lib\*;C:\Program Files\Java\jdk1.8.0_281\jre\lib\charsets.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\deploy.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\access-bridge-64.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\cldrdata.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\dnsns.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\jaccess.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\jfxrt.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\localedata.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\nashorn.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunec.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunjce_provider.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunmscapi.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\sunpkcs11.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\ext\zipfs.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\javaws.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jce.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jfr.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jfxswt.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\jsse.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\management-agent.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\plugin.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\resources.jar;C:\Program Files\Java\jdk1.8.0_281\jre\lib\rt.jar;C:\Program Files\Java\jdk1.8.0_281\lib\sa-jdi.jar;C:\Users\ThinkPad\IdeaProjects\zookeeper\zookeeper-server\target\classes;C:\Users\ThinkPad\IdeaProjects\zookeeper\zookeeper-jute\target\classes;C:\Users\ThinkPad\.m2\repository\commons-cli\commons-cli\1.2\commons-cli-1.2.jar;C:\Users\ThinkPad\.m2\repository\org\apache\yetus\audience-annotations\0.5.0\audience-annotations-0.5.0.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-handler\4.1.48.Final\netty-handler-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-common\4.1.48.Final\netty-common-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-resolver\4.1.48.Final\netty-resolver-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-buffer\4.1.48.Final\netty-buffer-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-transport\4.1.48.Final\netty-transport-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-codec\4.1.48.Final\netty-codec-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-transport-native-epoll\4.1.48.Final\netty-transport-native-epoll-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\io\netty\netty-transport-native-unix-common\4.1.48.Final\netty-transport-native-unix-common-4.1.48.Final.jar;C:\Users\ThinkPad\.m2\repository\org\slf4j\slf4j-api\1.7.25\slf4j-api-1.7.25.jar;C:\Users\ThinkPad\.m2\repository\org\slf4j\slf4j-log4j12\1.7.25\slf4j-log4j12-1.7.25.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-server\9.4.24.v20191120\jetty-server-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\javax\servlet\javax.servlet-api\3.1.0\javax.servlet-api-3.1.0.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-http\9.4.24.v20191120\jetty-http-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-util\9.4.24.v20191120\jetty-util-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-io\9.4.24.v20191120\jetty-io-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-servlet\9.4.24.v20191120\jetty-servlet-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\org\eclipse\jetty\jetty-security\9.4.24.v20191120\jetty-security-9.4.24.v20191120.jar;C:\Users\ThinkPad\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.10.3\jackson-databind-2.10.3.jar;C:\Users\ThinkPad\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.10.3\jackson-annotations-2.10.3.jar;C:\Users\ThinkPad\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.10.3\jackson-core-2.10.3.jar;C:\Users\ThinkPad\.m2\repository\com\googlecode\json-simple\json-simple\1.1.1\json-simple-1.1.1.jar;C:\Users\ThinkPad\.m2\repository\log4j\log4j\1.2.17\log4j-1.2.17.jar;%CLASSPATH%

set ZOOCFG=%ZOOCFGDIR%\zoo.cfg

@REM setup java environment variables

if not defined JAVA_HOME (
  echo Error: JAVA_HOME is not set.
  goto :eof
)

set JAVA_HOME=%JAVA_HOME:"=%

if not exist "%JAVA_HOME%"\bin\java.exe (
  echo Error: JAVA_HOME is incorrectly set.
  goto :eof
)

REM strip off trailing \ from JAVA_HOME or java does not start
if "%JAVA_HOME:~-1%" EQU "\" set "JAVA_HOME=%JAVA_HOME:~0,-1%"
 
set JAVA="%JAVA_HOME%"\bin\java

