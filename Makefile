
.PHONY: test
test:
	pytest -v -W ignore::DeprecationWarning:invoke.loaderpython --durations=0 --runslow


.PHONY: test-fast
test-fast:
	pytest -v -W ignore::DeprecationWarning:invoke.loaderpython --durations=0