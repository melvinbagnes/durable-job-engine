namespace JobEngine.Storage;

internal static class Sql
{
	public const string Ddl = """
		CREATE TABLE IF NOT EXISTS jobs (
			id uuid PRIMARY KEY,
			queue text NOT NULL DEFAULT 'default',
			type text NOT NULL,
			payload jsonb NOT NULL DEFAULT '{}'::jsonb,
			state text NOT NULL,
			priority integer NOT NULL DEFAULT 0,
			run_at timestamptz NOT NULL,
			attempts integer NOT NULL DEFAULT 0,
			max_attempts integer NOT NULL,
			lease_token uuid NULL,
			lease_owner text NULL,
			lease_expires_at timestamptz NULL,
			last_error text NULL,
			idempotency_key text NULL,
			created_at timestamptz NOT NULL,
			updated_at timestamptz NOT NULL,
			completed_at timestamptz NULL
		);

		CREATE UNIQUE INDEX IF NOT EXISTS ux_jobs_idempotency
			ON jobs (idempotency_key) WHERE idempotency_key IS NOT NULL;

		CREATE INDEX IF NOT EXISTS ix_jobs_claim
			ON jobs (queue, priority DESC, run_at) WHERE state = 'queued';

		CREATE INDEX IF NOT EXISTS ix_jobs_lease
			ON jobs (lease_expires_at) WHERE state = 'running';

		CREATE TABLE IF NOT EXISTS job_dependencies (
			job_id uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
			depends_on_id uuid NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
			PRIMARY KEY (job_id, depends_on_id)
		);

		CREATE INDEX IF NOT EXISTS ix_job_dependencies_parent
			ON job_dependencies (depends_on_id);

		CREATE TABLE IF NOT EXISTS schedules (
			name text PRIMARY KEY,
			cron text NOT NULL,
			queue text NOT NULL DEFAULT 'default',
			job_type text NOT NULL,
			payload jsonb NOT NULL DEFAULT '{}'::jsonb,
			max_attempts integer NOT NULL,
			enabled boolean NOT NULL DEFAULT true,
			next_run_at timestamptz NOT NULL,
			last_run_at timestamptz NULL,
			time_zone text NULL
		);

		ALTER TABLE schedules ADD COLUMN IF NOT EXISTS time_zone text NULL;
		""";

	public const string InsertJob = """
		INSERT INTO jobs (id, queue, type, payload, state, priority, run_at, attempts, max_attempts, idempotency_key, created_at, updated_at)
		VALUES (@id, @queue, @type, @payload, @state, @priority, @run_at, 0, @max_attempts, @key, @now, @now)
		ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING id;
		""";

	public const string SelectIdByKey = "SELECT id FROM jobs WHERE idempotency_key = @key;";

	public const string InsertDependency = """
		INSERT INTO job_dependencies (job_id, depends_on_id) VALUES (@job_id, @depends_on_id);
		""";

	// Claims ready jobs, plus running jobs whose lease expired (the worker crashed or stalled) and
	// that still have attempts left. SKIP LOCKED lets concurrent workers claim disjoint rows.
	public const string Claim = """
		WITH candidates AS (
			SELECT id
			FROM jobs
			WHERE queue = ANY(@queues)
			  AND (
					(state = 'queued' AND run_at <= @now)
				 OR (state = 'running' AND lease_expires_at <= @now AND attempts < max_attempts)
			  )
			ORDER BY priority DESC, run_at ASC, created_at ASC
			LIMIT @max
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET state = 'running',
			attempts = j.attempts + 1,
			lease_token = gen_random_uuid(),
			lease_owner = @owner,
			lease_expires_at = @lease_until,
			last_error = CASE WHEN j.state = 'running' THEN 'Lease expired (worker crashed or stalled); retrying.' ELSE j.last_error END,
			updated_at = @now
		FROM candidates c
		WHERE j.id = c.id
		RETURNING j.id, j.queue, j.type, j.payload::text, j.attempts, j.max_attempts, j.lease_token;
		""";

	public const string ExtendLease = """
		UPDATE jobs
		SET lease_expires_at = @lease_until, updated_at = @now
		WHERE id = @id AND lease_token = @token AND state = 'running';
		""";

	public const string Complete = """
		UPDATE jobs
		SET state = 'succeeded',
			completed_at = @now,
			updated_at = @now,
			lease_token = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL
		WHERE id = @id AND lease_token = @token AND state = 'running';
		""";

	// A waiting job becomes queued once every one of its dependencies has succeeded.
	public const string PromoteDependents = """
		UPDATE jobs c
		SET state = 'queued', updated_at = @now
		WHERE c.state = 'waiting'
		  AND c.id IN (SELECT job_id FROM job_dependencies WHERE depends_on_id = @id)
		  AND NOT EXISTS (
				SELECT 1
				FROM job_dependencies d
				JOIN jobs p ON p.id = d.depends_on_id
				WHERE d.job_id = c.id AND p.state <> 'succeeded'
		  );
		""";

	// Two parents finishing at the same moment would each see the other as "not yet succeeded" and
	// neither would promote the child. Locking the waiting children first (in id order, so no deadlocks)
	// serialises the check: the second transaction's next statement sees the first one's commit.
	public const string LockDependents = """
		SELECT c.id
		FROM jobs c
		WHERE c.state = 'waiting'
		  AND c.id IN (SELECT job_id FROM job_dependencies WHERE depends_on_id = @id)
		ORDER BY c.id
		FOR UPDATE OF c;
		""";

	public const string Release = """
		UPDATE jobs
		SET state = 'queued',
			attempts = GREATEST(attempts - 1, 0),
			run_at = @now,
			lease_token = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL,
			updated_at = @now
		WHERE id = @id AND lease_token = @token AND state = 'running';
		""";

	public const string Purge = """
		DELETE FROM jobs j
		WHERE j.state IN ('succeeded', 'dead', 'canceled')
		  AND j.completed_at < @cutoff
		  AND NOT EXISTS (
				SELECT 1
				FROM job_dependencies d
				JOIN jobs c ON c.id = d.job_id
				WHERE d.depends_on_id = j.id AND c.state IN ('waiting', 'queued', 'running')
		  );
		""";

	public const string Fail = """
		UPDATE jobs
		SET state = CASE WHEN @permanent OR attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
			run_at = CASE WHEN @permanent OR attempts >= max_attempts THEN run_at ELSE @next_run END,
			completed_at = CASE WHEN @permanent OR attempts >= max_attempts THEN @now ELSE NULL END,
			last_error = @error,
			lease_token = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL,
			updated_at = @now
		WHERE id = @id AND lease_token = @token AND state = 'running'
		RETURNING state, run_at;
		""";

	public const string CancelDownstream = """
		WITH RECURSIVE downstream(job_id) AS (
			SELECT job_id FROM job_dependencies WHERE depends_on_id = @id
			UNION
			SELECT d.job_id FROM job_dependencies d JOIN downstream s ON d.depends_on_id = s.job_id
		)
		UPDATE jobs
		SET state = 'canceled', last_error = @reason, completed_at = @now, updated_at = @now
		WHERE id IN (SELECT job_id FROM downstream) AND state = 'waiting';
		""";

	public const string ReviveDownstream = """
		WITH RECURSIVE downstream(job_id) AS (
			SELECT job_id FROM job_dependencies WHERE depends_on_id = @id
			UNION
			SELECT d.job_id FROM job_dependencies d JOIN downstream s ON d.depends_on_id = s.job_id
		)
		UPDATE jobs
		SET state = 'waiting', last_error = NULL, completed_at = NULL, updated_at = @now
		WHERE id IN (SELECT job_id FROM downstream) AND state = 'canceled';
		""";

	public const string RequeueDead = """
		UPDATE jobs
		SET state = 'queued', attempts = 0, run_at = @now, last_error = NULL, completed_at = NULL, updated_at = @now
		WHERE id = @id AND state = 'dead';
		""";

	public const string ReapExhausted = """
		UPDATE jobs
		SET state = 'dead',
			last_error = 'Lease expired on the final attempt (worker crashed or stalled).',
			completed_at = @now,
			lease_token = NULL,
			lease_owner = NULL,
			lease_expires_at = NULL,
			updated_at = @now
		WHERE state = 'running' AND lease_expires_at <= @now AND attempts >= max_attempts
		RETURNING id;
		""";

	public const string SelectJob = """
		SELECT id, queue, type, payload::text, state, priority, run_at, attempts, max_attempts,
			   last_error, idempotency_key, created_at, completed_at
		FROM jobs
		WHERE id = @id;
		""";

	public const string Stats = "SELECT state, count(*) FROM jobs GROUP BY state;";

	public const string UpsertSchedule = """
		INSERT INTO schedules (name, cron, queue, job_type, payload, max_attempts, enabled, next_run_at, time_zone)
		VALUES (@name, @cron, @queue, @job_type, @payload, @max_attempts, true, @next_run_at, @time_zone)
		ON CONFLICT (name) DO UPDATE
		SET cron = EXCLUDED.cron,
			time_zone = EXCLUDED.time_zone,
			queue = EXCLUDED.queue,
			job_type = EXCLUDED.job_type,
			payload = EXCLUDED.payload,
			max_attempts = EXCLUDED.max_attempts,
			enabled = true,
			next_run_at = CASE
				WHEN schedules.cron = EXCLUDED.cron AND schedules.time_zone IS NOT DISTINCT FROM EXCLUDED.time_zone
				THEN schedules.next_run_at
				ELSE EXCLUDED.next_run_at
			END;
		""";

	public const string SelectDueSchedules = """
		SELECT name, cron, queue, job_type, payload::text, max_attempts, next_run_at, time_zone
		FROM schedules
		WHERE enabled AND next_run_at <= @now
		ORDER BY next_run_at
		LIMIT @max
		FOR UPDATE SKIP LOCKED;
		""";

	public const string AdvanceSchedule = """
		UPDATE schedules
		SET next_run_at = COALESCE(@next, next_run_at),
			enabled = (@next IS NOT NULL),
			last_run_at = @now
		WHERE name = @name;
		""";
}
