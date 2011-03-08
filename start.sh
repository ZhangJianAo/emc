#!/bin/sh

erl -detached -sname emc +K true +A -shared -pa $PWD/ebin -boot start_sasl -config emc -s emc start