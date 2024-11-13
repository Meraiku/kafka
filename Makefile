docker:
	@docker compose up -d --build

stop:
	@docker compose stop

build_prod:
	@go build -o ./.bin/producer ./cmd/producer

build_cons:
	@go build -o ./.bin/consumer ./cmd/consumer

producer:build_prod
	@./.bin/producer

consumer:build_cons
	@./.bin/consumer
