.PHONY: clean build install test test-coverage

build:
	sbt one-jar

clean:
	sbt clean

install:
	./recipes/install.sh
	#sbt one-jar

uninstall:
	./recipes/uninstall.sh

test:
	sbt test
	
test-coverage:
	sbt clean
	sbt coverage:compile
	sbt coverage:test
