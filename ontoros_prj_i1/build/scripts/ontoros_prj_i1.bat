@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  ontoros_prj_i1 startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Add default JVM options here. You can also use JAVA_OPTS and ONTOROS_PRJ_I_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windowz variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\ontoros_prj_i1-0.1.0.jar;%APP_HOME%\lib\sample_prj-0.1.0.jar;%APP_HOME%\lib\pellet-rules.jar;%APP_HOME%\lib\jcl-over-slf4j-1.6.4.jar;%APP_HOME%\lib\httpclient-4.2.3.jar;%APP_HOME%\lib\aterm-java-1.6.jar;%APP_HOME%\lib\commons-codec-1.6.jar;%APP_HOME%\lib\jena-tdb-0.10.0.jar;%APP_HOME%\lib\slf4j-api-1.6.4.jar;%APP_HOME%\lib\xml-apis-1.4.01.jar;%APP_HOME%\lib\httpcore-4.2.2.jar;%APP_HOME%\lib\jena-core-2.10.0.jar;%APP_HOME%\lib\pellet-core.jar;%APP_HOME%\lib\owlapi-distribution-3.4.3-bin.jar;%APP_HOME%\lib\pellet-el.jar;%APP_HOME%\lib\pellet-owlapiv3.jar;%APP_HOME%\lib\jena-arq-2.10.0.jar;%APP_HOME%\lib\slf4j-log4j12-1.6.4.jar;%APP_HOME%\lib\xercesImpl-2.10.0.jar;%APP_HOME%\lib\log4j-1.2.16.jar;%APP_HOME%\lib\jgrapht-jdk1.5.jar;%APP_HOME%\lib\pellet-cli.jar;%APP_HOME%\lib\pellet-jena.jar;%APP_HOME%\lib\jena-iri-0.9.5.jar;%APP_HOME%\lib\pellet-datatypes.jar;%APP_HOME%\lib\pellet-query.jar;%APP_HOME%\lib\rosjava-0.2.1.jar;%APP_HOME%\lib\ontology_msgs-0.0.0.jar;%APP_HOME%\lib\apache_xmlrpc_common-0.2.1.jar;%APP_HOME%\lib\apache_xmlrpc_server-0.2.1.jar;%APP_HOME%\lib\apache_xmlrpc_client-0.2.1.jar;%APP_HOME%\lib\message_generation-0.2.1.jar;%APP_HOME%\lib\rosjava_test_msgs-0.2.1.jar;%APP_HOME%\lib\rosgraph_msgs-1.11.2.jar;%APP_HOME%\lib\geometry_msgs-1.11.9.jar;%APP_HOME%\lib\nav_msgs-1.11.9.jar;%APP_HOME%\lib\tf2_msgs-0.5.13.jar;%APP_HOME%\lib\dnsjava-2.1.1.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.logging-1.1.1.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.net-2.0.0.jar;%APP_HOME%\lib\guava-12.0.jar;%APP_HOME%\lib\roscpp-1.11.20.jar;%APP_HOME%\lib\std_msgs-0.5.10.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.httpclient-3.1.0.jar;%APP_HOME%\lib\ws-commons-util-1.0.1.jar;%APP_HOME%\lib\netty-3.5.2.Final.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.codec-1.3.0.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.io-1.4.0.jar;%APP_HOME%\lib\commons-pool-1.6.jar;%APP_HOME%\lib\com.springsource.org.apache.commons.lang-2.4.0.jar;%APP_HOME%\lib\gradle_plugins-0.2.1.jar;%APP_HOME%\lib\actionlib_msgs-1.11.9.jar;%APP_HOME%\lib\junit-3.8.2.jar;%APP_HOME%\lib\jsr305-1.3.9.jar;%APP_HOME%\lib\xml-apis-1.0.b2.jar

@rem Execute ontoros_prj_i1
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %ONTOROS_PRJ_I_OPTS%  -classpath "%CLASSPATH%" org.ros.RosRun %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable ONTOROS_PRJ_I_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%ONTOROS_PRJ_I_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
