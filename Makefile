install:
	make down
	rm -rf ./target
	docker compose up -d 
	sleep 5
	make migrate
	make build
	make seed
	make run

up:
	docker compose up -d

down:
	docker compose down -v

migrate:
	docker compose exec app sqlx migrate run --source src/common/migrations

run:
	docker compose exec app cargo run --release

seed:
	# docker compose exec app cargo run -- --release seed

build:
	docker compose exec app cargo build --release