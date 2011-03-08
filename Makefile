ERL = erl +K true +A -shared -pa ebin -boot start_sasl

all:
	(cd src;$(MAKE))

run: all
	${ERL} -s emc start

clean:
	rm ebin/*