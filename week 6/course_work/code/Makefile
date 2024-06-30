LOCAL_TAG:=$(shell date +"%Y-%m-%d-%H-%M")
LOCAL_IMAGE_NAME:=stream-model-duration:${LOCAL_TAG}

test:
	pytest tests/



quality_check:
	isort .
	black .
	pylint --recursive=y .

build: quality_check  test
	echo ${LOCAL_IMAGE_NAME}
	docker build -t ${LOCAL_IMAGE_NAME} .

test_integration:build
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME} ./integration-test/run.sh

publish: build test_integration
	LOCAL_IMAGE_NAME=${LOCAL_IMAGE_NAME}  ./scripts/publish.sh