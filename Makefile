ifdef app_name
export container_name:=${app_name}
endif

build:
	docker build ${extra_build_opts} --tag ${app_name} .

run-without-build:
	docker run -it --init --rm --name ${container_name} ${extra_run_opts} ${app_name} ${command} ${extra_args}

.PHONY: run
run: build run-without-build
