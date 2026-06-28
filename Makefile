.PHONY: help
help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make <target>\033[36m\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

.PHONY: install		## Install production dependencies.
install:
	uv sync --frozen --no-dev

.PHONY: install-dev ## Install all dependencies.
install-dev:
	uv sync --frozen

.PHONY: outdated	## Check for outdated dependencies.
outdated:
	uv tree --depth=1 --outdated

.PHONY: upgrade		## Upgrade dependencies.
upgrade:
	uv lock --upgrade

.PHONY: unit	## Run unit tests.
unit: 		 ## Run tests.
	uv run --frozen pytest

.PHONY: lint
lint: 		 ## Run linter.
	uv run --frozen ruff check
	uv run --frozen ruff format --check
	uv run --frozen ty check

.PHONY: format
format: 	 ## Format code.
	uv run --frozen ruff format

.PHONY: test
test: lint unit  ## Run all tests.

.PHONY: coverage
coverage:
	uv run --frozen pytest --cov --cov-report=xml

.PHONY: cfn-lint
cfn-lint:  ## Run CloudFormation linter.
	uv run cfn-lint aws/*.yml template.yml

.PHONY: requirements
requirements:  ## Generate requirements.txt from Pipfile.lock.
	uv export --frozen --no-dev --no-editable -o requirements.txt

.PHONY: install-pip-ci	## Install dependencies for pip CI.
install-pip-ci: requirements
	uv pip install \
		--no-installer-metadata \
		--no-compile-bytecode \
		--python-platform x86_64-manylinux2014 \
		--python 3.14 \
		--target build \
		-r requirements.txt

.PHONY: build-lambda-package
build-lambda-package: install-pip-ci  ## Build lambda package.
	cp -r app build/app
	cd build && zip -r ../lambda.zip .
