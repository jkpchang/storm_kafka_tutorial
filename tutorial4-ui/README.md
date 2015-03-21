Hortonworks-Tutorials
=====================
 
####Steps to restore database 
	1. Create database named `trafficvisualization`.
	2. Execute db.sql file to restore mysql DB.
	3. Open file “<cloned_directory>/ src/main/webapp/WEB-INF/applicationContextLocal.xml”  and replace “url”, “username” and “password” property with running mysql details.
	4. Open file “<cloned_directory>/kmlfeeder/src/main/resources/applicationContext.xml and repeat the process mentioned in step3.
####Steps to run traffic visualization web application
	1. Clone the project to directory.
	2. Make sure jdk7 and maven 3 is installed on system
	3. Go to the clone directory and execute command from command line mvn tomcat:run
	4. Wait for message of successful start of app.
####To Run Kml feeder 
	1. Go to command prompt(new window)
	2. Go to the directory kmlfeeder directory in cloned directory    
    3. execute command "mvn compile" to compile the project
	3. Execute command “mvn exec:java -Dexec.mainClass="com.thirdeye.Main"”
	4. Open browser and put http://localhost:8080/trafficvisualization/



