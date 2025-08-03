install:
	make down
	rm -rf ./target
	make up 
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
	docker compose exec app ./target/release/w_collider

seed:
	docker compose exec app ./target/release/w_collider seeder

build:
	docker compose exec app cargo build --release
