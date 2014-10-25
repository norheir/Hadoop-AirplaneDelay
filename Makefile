hadoop:
	rm -rf output
	hadoop com.sun.tools.javac.Main *.java
	jar cf Driver.jar *.class

run:
	rm -rf output
	hadoop jar Driver.jar Driver input output planedata.csv

clean:
	rm -rf output
	rm *.class
	rm *.jar
