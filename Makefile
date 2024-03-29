.PHONY: build
build: ## build python sdist package.
	python setup.py sdist

.PHONY: install
install:  ## Install to local storage
	@pip install .

.PHONY: test
test:  ## Unit test
	pytest -p "no:warnings" .

.PHONY: pep8
pep8:  ## pep8
	@black $(CURDIR) --line-length 99

# Absolutely awesome: http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := build
