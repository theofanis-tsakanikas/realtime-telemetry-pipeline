.PHONY: start stop build restart logs ps test lint clean

VENV_BIN = .venv/bin
JAVA_HOME ?= /opt/homebrew/opt/openjdk@17
export JAVA_HOME
export PATH := $(JAVA_HOME)/bin:$(PATH)

start:
	./run.sh up

stop:
	./run.sh down

build:
	./run.sh build

restart:
	./run.sh restart

logs:
	./run.sh logs

ps:
	./run.sh ps

test:
	$(VENV_BIN)/pytest tests/ -v --tb=short

lint:
	$(VENV_BIN)/ruff check scripts/ tests/

clean:
	./run.sh down
	find data/checkpoints -mindepth 1 -type f -not -name '.gitkeep' -delete 2>/dev/null || true
	find data/logs -mindepth 1 -type f -not -name '.gitkeep' -delete 2>/dev/null || true
