BSONS = $(wildcard *.bson.gz)
OBJS = $(foreach BSON, $(BSONS), $(BSON:.bson.gz=.bson.gz.done))

all: $(OBJS)

$(OBJS): %.bson.gz.done: %.bson.gz
	./mongo_import.sh $<	
	touch $@
