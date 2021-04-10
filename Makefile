.PHONY: test help clean
.DEFAULT_GOAL := help

# Global Variables
CURRENT_PWD:=$(shell pwd)
VENV_DIR:=.env
AWS_PROFILE:=elf

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

install: ## Install CDK and bootstrap
	npm -g install aws-cdk
	cdk bootstrap

init: ## Initialize a empty project using python language
	cdk init app --language python

cdk_ls: ## List the stacks
	/bin/bash -c "cd $(CURRENT_PWD) && . .env/bin/activate && cdk ls"


pre_build: ## Run build
	npm run build

build: ## Synthesize the template
	cdk synth

post_build: ## Show differences
	cdk diff

deploy: ## Deploy ALL stack
	cdk ls | xargs cdk deploy

destroy: ## Delete Stack without confirmation
	cdk ls | xargs cdk destroy -f

deps: deps_python ## Install dependancies

deps_python:
	.env/bin/activate
	pip3 install -r requirements.txt


clean: ## Remove All virtualenvs
	@rm -rf ${PWD}/${VENV_DIR} build dist *.egg-info .eggs .pytest_cache .coverage
	@find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf
