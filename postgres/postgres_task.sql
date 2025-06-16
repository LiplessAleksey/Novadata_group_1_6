CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

CREATE OR REPLACE FUNCTION log_user_update()
RETURNS TRIGGER AS $$
DECLARE
    diffs hstore;
    key TEXT;
    v_changed_by TEXT := session_user;
BEGIN
    diffs := hstore(NEW) - hstore(OLD);

    FOREACH key IN ARRAY akeys(diffs)
    LOOP
        INSERT INTO users_audit(user_id, changed_at, changed_by, field_changed, old_value, new_value)
        VALUES (
            OLD.id,
            CURRENT_TIMESTAMP,
            v_changed_by,
            key,
            hstore(OLD)->key,
            hstore(NEW)->key
        );
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_log_user_update
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION log_user_update();

CREATE OR REPLACE FUNCTION export_today_audit_log()
RETURNS void AS $$
DECLARE
    file_path TEXT;
BEGIN
    file_path := '/tmp/users_audit_export_' || to_char(current_date, 'YYYY-MM-DD') || '.csv';

    EXECUTE format(
        'COPY (
				SELECT user_id, field_changed, old_value, new_value, changed_by, changed_at
				FROM users_audit
				WHERE changed_at >= CURRENT_DATE
			) TO %L WITH CSV HEADER',
        file_path
    );
END;
$$ LANGUAGE plpgsql;

SELECT cron.schedule(
  'daily_audit_export',
  '40 20 * * *',
  $$SELECT export_today_audit_log();$$
);

-- SELECT * FROM cron.job;

INSERT INTO users (id, name, email, role, updated_at)
VALUES
(1, 'Aleksey', 'abcd@gmail.com', 'RUN', CURRENT_TIMESTAMP),
(2, 'Petr', 'efgh@gmail.com', 'RUN', CURRENT_TIMESTAMP);

SELECT * FROM users u;
SELECT * FROM users_audit ua;
UPDATE users
SET role = 'DE!'
WHERE name = 'Aleksey';

-- SELECT export_today_audit_log();