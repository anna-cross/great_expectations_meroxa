SHELL=/bin/bash -o pipefail

install: requirements.txt
	pip install -r requirements.txt
