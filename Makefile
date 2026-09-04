.PHONY: install test run-simulator

install:
	python -m pip install -r requirements.txt

test:
	python -m pytest tests

run-simulator:
	python -m src.simulator.machine_simulator
