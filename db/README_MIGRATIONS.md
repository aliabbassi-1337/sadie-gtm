# Database Migrations with migra

**No Alembic, no SQLAlchemy. Just pure SQL migrations.**

migra connects to your **local Docker database** using psycopg2-binary to compare schemas and generate SQL. Your application code uses asyncpg.

## How it works

1. Keep your schema in `db/schema.sql`
2. Use `migra` to compare current DB vs target schema
3. Generate SQL migration
4. Run it with psql

## Initial Setup

```bash
# 1. Start local DB
docker compose up -d

# 2. Apply current schema
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/schema.sql
```

## When you change the schema

```bash
# 1. Edit db/schema.sql with your changes

# 2. Create a fresh DB with new schema (temporary)
docker run -d --name temp-db \
  -e POSTGRES_PASSWORD=temp \
  -e POSTGRES_DB=temp \
  postgis/postgis:17-3.5-alpine

# Wait a few seconds for DB to start
sleep 5

# Load new schema into temp DB
docker exec -i temp-db psql -U postgres -d temp < db/schema.sql

# 3. Generate migration SQL by comparing DBs
uv run migra \
  postgresql://sadie:sadie_dev_password@localhost:5432/sadie_gtm \
  postgresql://postgres:temp@localhost:PORT/temp \
  --unsafe > db/migrations/$(date +%Y%m%d_%H%M%S)_migration.sql

# 4. Review the generated SQL
cat db/migrations/*_migration.sql

# 5. Apply migration to your local DB
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/migrations/*_migration.sql

# 6. Clean up temp DB
docker rm -f temp-db
```

## Simpler workflow (manual migrations)

Just write SQL migrations manually:

```bash
# 1. Create migration file
cat > db/migrations/$(date +%Y%m%d_%H%M%S)_add_new_table.sql << 'EOF'
SET search_path TO sadie_gtm;

CREATE TABLE new_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);
EOF

# 2. Apply it
docker exec -i sadie-gtm-local-db psql -U sadie -d sadie_gtm < db/migrations/*_add_new_table.sql
```

## Running migrations in production

```bash
# Apply all migrations in order
for f in db/migrations/*.sql; do
    psql $DATABASE_URL < "$f"
done
```

## Migration file naming

Use timestamp prefix: `YYYYMMDD_HHMMSS_description.sql`

Example: `20260113_143022_add_hotels_table.sql`
