all:
	docker build .

venv:
	@- python3.6 -m venv venv

reqs: venv
	@- ./venv/bin/pip install -U pip setuptools wheel
	@- ./venv/bin/pip install -Ur requirements-frozen.txt

nuke_reqs:
	@- ./venv/bin/pip freeze | xargs ./venv/bin/pip uninstall -y