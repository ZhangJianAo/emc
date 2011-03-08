EBIN_DIR := ../ebin
ERL_SOURCES := $(wildcard *.erl)
EBIN_FILES := $(ERL_SOURCES:%.erl=$(EBIN_DIR)/%.beam) $(EBIN_DIR)/emc.app

all: $(EBIN_FILES)


../ebin/%.app: %.app
	cp $< $@

$(EBIN_DIR)/%.beam: %.erl
	erlc -smp +native -W -o $(EBIN_DIR) $<
