.PHONY: setup jars up down ps logs test dbt psql backfill clean

setup: jars
	@test -f .env || (cp .env.example .env && echo "Created .env - add your GITHUB_TOKEN")
	docker compose build

jars:
	bash streaming/download_jars.sh

up:
	docker compose up -d

down:
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=50

test:
	uv run ruff check .
	uv run pytest -q

dbt:
	cd dbt && uv run dbt build --profiles-dir .

psql:
	docker exec -it de-postgres psql -U de_user -d analytics

backfill:
	docker compose exec airflow airflow backfill create --dag-id gharchive_hourly --from-date $(FROM) --to-date $(TO)

clean:
	docker compose down -v
