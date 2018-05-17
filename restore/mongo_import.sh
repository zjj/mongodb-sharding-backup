#!/bin/bash

DB=XXXXXXXXXXXXX

mongo_import () {
	bson=$1
	collection=${bson%.bson.gz}
	gunzip -c $bson > ${collection}.bson &&
	bsondump ${collection}.bson > ${collection}.json &&
	echo mongoimport --db $DB -c $collection  ${collection}.json &&
	mongoimport --db $DB -c $collection  ${collection}.json &&
	rm -f ${collection}.bson &&
	rm -f ${collection}.json
}

mongo_import $1
