@ECHO OFF
mvn compile && mvn package && pscp -l hadoop01 -pw 7Gtxz6kwn~Qd %1 hadoop01@deerstalker.cs.brandeis.edu:./miketest 
