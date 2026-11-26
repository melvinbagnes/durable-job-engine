using System.Globalization;
using JobEngine.Jobs;
using JobEngine.Scheduling;
using JobEngine.Workflows;
using Npgsql;
using NpgsqlTypes;

namespace JobEngine.Storage;

/// <summary>
/// All persistence for the engine. Every state transition is a single SQL statement (or a short
/// transaction) so that correctness comes from the database, not from process memory.
/// </summary>
public sealed class JobStore : IAsyncDisposable
{
	private const long MigrationLockKey = 7_272_001;
	private const int MaxErrorLength = 4000;

	private readonly NpgsqlDataSource _dataSource;
	private readonly JobEngineOptions _options;
	private readonly TimeProvider _time;
	private readonly bool _ownsDataSource;

	public JobStore(NpgsqlDataSource dataSource, JobEngineOptions options, TimeProvider? time = null)
		: this(dataSource, options, time, ownsDataSource: false)
	{
	}

	private JobStore(NpgsqlDataSource dataSource, JobEngineOptions options, TimeProvider? time, bool ownsDataSource)
	{
		_dataSource = dataSource;
		_options = options;
		_time = time ?? TimeProvider.System;
		_ownsDataSource = ownsDataSource;
	}

	public static JobStore Create(JobEngineOptions options, TimeProvider? time = null)
	{
		ArgumentNullException.ThrowIfNull(options);
		ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString);

		return new JobStore(NpgsqlDataSource.Create(options.ConnectionString), options, time, ownsDataSource: true);
	}

	public async ValueTask DisposeAsync()
	{
		if (_ownsDataSource)
		{
			await _dataSource.DisposeAsync();
		}
	}

	private DateTime Now() => _time.GetUtcNow().UtcDateTime;

	// ---------------------------------------------------------------- schema

	public async Task MigrateAsync(CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		// Several processes may start at once; the advisory lock makes the DDL run one at a time.
		await using (var lockCommand = new NpgsqlCommand("SELECT pg_advisory_xact_lock(@key);", connection, transaction))
		{
			lockCommand.Parameters.AddWithValue("key", MigrationLockKey);
			await lockCommand.ExecuteNonQueryAsync(ct);
		}

		await using (var ddl = new NpgsqlCommand(Sql.Ddl, connection, transaction))
		{
			await ddl.ExecuteNonQueryAsync(ct);
		}

		await transaction.CommitAsync(ct);
	}

	// ---------------------------------------------------------------- enqueue

	public async Task<EnqueueResult> EnqueueAsync(EnqueueRequest request, CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);

		return await EnqueueCoreAsync(connection, null, request, JobState.Queued, ct);
	}

	/// <summary>
	/// Transactional outbox: enqueue inside the caller's own transaction, so the job exists if and only if
	/// the business change commits. The jobs table is the outbox; workers are the relay.
	/// </summary>
	public Task<EnqueueResult> EnqueueAsync(NpgsqlTransaction transaction, EnqueueRequest request, CancellationToken ct = default)
	{
		ArgumentNullException.ThrowIfNull(transaction);

		return EnqueueCoreAsync(transaction.Connection!, transaction, request, JobState.Queued, ct);
	}

	public async Task<WorkflowEnqueueResult> EnqueueWorkflowAsync(
		IReadOnlyList<WorkflowStep> steps,
		string? idempotencyKey = null,
		CancellationToken ct = default)
	{
		var ordered = WorkflowGraph.Validate(steps);

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		var ids = new Dictionary<string, Guid>(StringComparer.Ordinal);

		foreach (var step in ordered)
		{
			var request = new EnqueueRequest
			{
				Type = step.Type,
				Queue = step.Queue,
				PayloadJson = step.PayloadJson,
				Priority = step.Priority,
				MaxAttempts = step.MaxAttempts,
				IdempotencyKey = StepKey(idempotencyKey, step.Key)
			};

			var state = step.DependsOn.Count == 0 ? JobState.Queued : JobState.Waiting;
			var result = await EnqueueCoreAsync(connection, transaction, request, state, ct);

			if (!result.Created)
			{
				if (ids.Count > 0)
				{
					throw new InvalidOperationException(
						$"Idempotency key '{request.IdempotencyKey}' already belongs to a job outside this workflow.");
				}

				// The first step already exists, so this exact workflow was enqueued before.
				await transaction.RollbackAsync(ct);

				return await LookupWorkflowAsync(connection, ordered, idempotencyKey, ct);
			}

			ids[step.Key] = result.JobId;
		}

		foreach (var step in ordered)
		{
			foreach (var dependency in step.DependsOn.Distinct(StringComparer.Ordinal))
			{
				await using var insert = new NpgsqlCommand(Sql.InsertDependency, connection, transaction)
					.With("job_id", NpgsqlDbType.Uuid, ids[step.Key])
					.With("depends_on_id", NpgsqlDbType.Uuid, ids[dependency]);

				await insert.ExecuteNonQueryAsync(ct);
			}
		}

		await transaction.CommitAsync(ct);

		return new WorkflowEnqueueResult(ids, Created: true);
	}

	private static string? StepKey(string? workflowKey, string stepKey) =>
		workflowKey is null ? null : $"{workflowKey}:{stepKey}";

	private static async Task<WorkflowEnqueueResult> LookupWorkflowAsync(
		NpgsqlConnection connection,
		IReadOnlyList<WorkflowStep> steps,
		string? idempotencyKey,
		CancellationToken ct)
	{
		var ids = new Dictionary<string, Guid>(StringComparer.Ordinal);

		foreach (var step in steps)
		{
			await using var lookup = new NpgsqlCommand(Sql.SelectIdByKey, connection)
				.With("key", NpgsqlDbType.Text, StepKey(idempotencyKey, step.Key));

			if (await lookup.ExecuteScalarAsync(ct) is not Guid id)
			{
				throw new InvalidOperationException(
					$"Workflow '{idempotencyKey}' exists but step '{step.Key}' is missing (a different workflow shape was enqueued under the same key).");
			}

			ids[step.Key] = id;
		}

		return new WorkflowEnqueueResult(ids, Created: false);
	}

	private async Task<EnqueueResult> EnqueueCoreAsync(
		NpgsqlConnection connection,
		NpgsqlTransaction? transaction,
		EnqueueRequest request,
		JobState initialState,
		CancellationToken ct)
	{
		Validate(request);

		var now = Now();
		var id = Guid.CreateVersion7();

		await using (var insert = new NpgsqlCommand(Sql.InsertJob, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("queue", NpgsqlDbType.Text, request.Queue)
			.With("type", NpgsqlDbType.Text, request.Type)
			.With("payload", NpgsqlDbType.Jsonb, request.PayloadJson)
			.With("state", NpgsqlDbType.Text, initialState.ToDb())
			.With("priority", NpgsqlDbType.Integer, request.Priority)
			.With("run_at", NpgsqlDbType.TimestampTz, now + request.Delay)
			.With("max_attempts", NpgsqlDbType.Integer, request.MaxAttempts ?? _options.DefaultMaxAttempts)
			.With("key", NpgsqlDbType.Text, request.IdempotencyKey)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			if (await insert.ExecuteScalarAsync(ct) is Guid createdId)
			{
				return new EnqueueResult(createdId, Created: true);
			}
		}

		// Nothing was inserted, so the idempotency key already exists: hand back the original job.
		await using var lookup = new NpgsqlCommand(Sql.SelectIdByKey, connection, transaction)
			.With("key", NpgsqlDbType.Text, request.IdempotencyKey);

		if (await lookup.ExecuteScalarAsync(ct) is Guid existingId)
		{
			return new EnqueueResult(existingId, Created: false);
		}

		throw new InvalidOperationException("The job was not inserted and no job with that idempotency key exists.");
	}

	private static void Validate(EnqueueRequest request)
	{
		ArgumentNullException.ThrowIfNull(request);
		ArgumentException.ThrowIfNullOrWhiteSpace(request.Type);
		ArgumentException.ThrowIfNullOrWhiteSpace(request.Queue);
		ArgumentOutOfRangeException.ThrowIfLessThan(request.Delay, TimeSpan.Zero);

		if (request.MaxAttempts is < 1)
		{
			throw new ArgumentOutOfRangeException(nameof(request), "MaxAttempts must be at least 1.");
		}
	}

	// ---------------------------------------------------------------- claim / lease

	/// <summary>
	/// Atomically leases up to <paramref name="maxJobs"/> jobs. Concurrent callers receive disjoint sets
	/// because rows are taken with FOR UPDATE SKIP LOCKED. Jobs whose lease expired are claimed again.
	/// </summary>
	public async Task<IReadOnlyList<ClaimedJob>> ClaimAsync(
		string owner,
		IReadOnlyCollection<string> queues,
		int maxJobs,
		TimeSpan leaseDuration,
		CancellationToken ct = default)
	{
		if (maxJobs < 1 || queues.Count == 0)
		{
			return [];
		}

		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.Claim, connection)
			.With("queues", NpgsqlDbType.Array | NpgsqlDbType.Text, queues.ToArray())
			.With("now", NpgsqlDbType.TimestampTz, now)
			.With("max", NpgsqlDbType.Integer, maxJobs)
			.With("owner", NpgsqlDbType.Text, owner)
			.With("lease_until", NpgsqlDbType.TimestampTz, now + leaseDuration);

		var claimed = new List<ClaimedJob>();

		await using var reader = await command.ExecuteReaderAsync(ct);

		while (await reader.ReadAsync(ct))
		{
			claimed.Add(new ClaimedJob(
				reader.GetGuid(0),
				reader.GetString(1),
				reader.GetString(2),
				reader.GetString(3),
				reader.GetInt32(4),
				reader.GetInt32(5),
				reader.GetGuid(6)));
		}

		return claimed;
	}

	/// <summary>Heartbeat. Returns false when the lease was lost (expired and reclaimed, or the job finished).</summary>
	public async Task<bool> ExtendLeaseAsync(Guid id, Guid leaseToken, TimeSpan leaseDuration, CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.ExtendLease, connection)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("token", NpgsqlDbType.Uuid, leaseToken)
			.With("now", NpgsqlDbType.TimestampTz, now)
			.With("lease_until", NpgsqlDbType.TimestampTz, now + leaseDuration);

		return await command.ExecuteNonQueryAsync(ct) == 1;
	}

	/// <summary>Marks the job succeeded and releases dependents. Fenced by the lease token.</summary>
	public async Task<bool> CompleteAsync(Guid id, Guid leaseToken, CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		int updated;

		await using (var complete = new NpgsqlCommand(Sql.Complete, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("token", NpgsqlDbType.Uuid, leaseToken)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			updated = await complete.ExecuteNonQueryAsync(ct);
		}

		if (updated == 0)
		{
			await transaction.RollbackAsync(ct);

			return false;
		}

		await using (var lockDependents = new NpgsqlCommand(Sql.LockDependents, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id))
		{
			await lockDependents.ExecuteNonQueryAsync(ct);
		}

		await using (var promote = new NpgsqlCommand(Sql.PromoteDependents, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			await promote.ExecuteNonQueryAsync(ct);
		}

		await transaction.CommitAsync(ct);

		return true;
	}

	/// <summary>
	/// Records a failed attempt. The job is requeued after <paramref name="retryDelay"/> unless attempts are
	/// exhausted or <paramref name="permanent"/> is set, in which case it goes to the dead-letter state and
	/// its downstream workflow steps are canceled. Returns null when the lease was already lost.
	/// </summary>
	public async Task<FailureOutcome?> FailAsync(
		Guid id,
		Guid leaseToken,
		string error,
		bool permanent,
		TimeSpan retryDelay,
		CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		FailureOutcome? outcome = null;

		await using (var fail = new NpgsqlCommand(Sql.Fail, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("token", NpgsqlDbType.Uuid, leaseToken)
			.With("permanent", NpgsqlDbType.Boolean, permanent)
			.With("next_run", NpgsqlDbType.TimestampTz, now + retryDelay)
			.With("error", NpgsqlDbType.Text, Truncate(error))
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			await using var reader = await fail.ExecuteReaderAsync(ct);

			if (await reader.ReadAsync(ct))
			{
				outcome = new FailureOutcome(JobStateExtensions.FromDb(reader.GetString(0)), reader.GetFieldValue<DateTime>(1));
			}
		}

		if (outcome is null)
		{
			await transaction.RollbackAsync(ct);

			return null;
		}

		if (outcome.State == JobState.Dead)
		{
			await CancelDownstreamAsync(connection, transaction, id, now, ct);
		}

		await transaction.CommitAsync(ct);

		return outcome;
	}

	/// <summary>Gives a job back without spending an attempt (used on graceful shutdown).</summary>
	public async Task<bool> ReleaseAsync(Guid id, Guid leaseToken, CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.Release, connection)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("token", NpgsqlDbType.Uuid, leaseToken)
			.With("now", NpgsqlDbType.TimestampTz, Now());

		return await command.ExecuteNonQueryAsync(ct) == 1;
	}

	/// <summary>Dead-letter replay: resets attempts and revives the canceled downstream steps.</summary>
	public async Task<bool> RequeueDeadAsync(Guid id, CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		int updated;

		await using (var requeue = new NpgsqlCommand(Sql.RequeueDead, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			updated = await requeue.ExecuteNonQueryAsync(ct);
		}

		if (updated == 0)
		{
			await transaction.RollbackAsync(ct);

			return false;
		}

		await using (var revive = new NpgsqlCommand(Sql.ReviveDownstream, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			await revive.ExecuteNonQueryAsync(ct);
		}

		await transaction.CommitAsync(ct);

		return true;
	}

	/// <summary>
	/// A job whose lease expired on its final attempt can never be claimed again; this moves it to dead so
	/// it does not sit in "running" forever, and cancels its downstream steps.
	/// </summary>
	public async Task<int> ReapExhaustedAsync(CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		var reaped = new List<Guid>();

		await using (var reap = new NpgsqlCommand(Sql.ReapExhausted, connection, transaction)
			.With("now", NpgsqlDbType.TimestampTz, now))
		{
			await using var reader = await reap.ExecuteReaderAsync(ct);

			while (await reader.ReadAsync(ct))
			{
				reaped.Add(reader.GetGuid(0));
			}
		}

		foreach (var id in reaped)
		{
			await CancelDownstreamAsync(connection, transaction, id, now, ct);
		}

		await transaction.CommitAsync(ct);

		return reaped.Count;
	}

	/// <summary>Deletes finished jobs older than <paramref name="olderThan"/> that nothing unfinished depends on.</summary>
	public async Task<int> PurgeAsync(TimeSpan olderThan, CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.Purge, connection)
			.With("cutoff", NpgsqlDbType.TimestampTz, Now() - olderThan);

		return await command.ExecuteNonQueryAsync(ct);
	}

	private static async Task CancelDownstreamAsync(
		NpgsqlConnection connection,
		NpgsqlTransaction transaction,
		Guid id,
		DateTime now,
		CancellationToken ct)
	{
		await using var cancel = new NpgsqlCommand(Sql.CancelDownstream, connection, transaction)
			.With("id", NpgsqlDbType.Uuid, id)
			.With("reason", NpgsqlDbType.Text, $"Canceled because upstream job {id} failed permanently.")
			.With("now", NpgsqlDbType.TimestampTz, now);

		await cancel.ExecuteNonQueryAsync(ct);
	}

	private static string Truncate(string text) =>
		text.Length <= MaxErrorLength ? text : text[..MaxErrorLength];

	// ---------------------------------------------------------------- queries

	public async Task<JobRecord?> GetAsync(Guid id, CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.SelectJob, connection)
			.With("id", NpgsqlDbType.Uuid, id);

		await using var reader = await command.ExecuteReaderAsync(ct);

		if (!await reader.ReadAsync(ct))
		{
			return null;
		}

		return new JobRecord(
			reader.GetGuid(0),
			reader.GetString(1),
			reader.GetString(2),
			reader.GetString(3),
			JobStateExtensions.FromDb(reader.GetString(4)),
			reader.GetInt32(5),
			reader.GetFieldValue<DateTime>(6),
			reader.GetInt32(7),
			reader.GetInt32(8),
			reader.IsDBNull(9) ? null : reader.GetString(9),
			reader.IsDBNull(10) ? null : reader.GetString(10),
			reader.GetFieldValue<DateTime>(11),
			reader.IsDBNull(12) ? null : reader.GetFieldValue<DateTime>(12));
	}

	public async Task<JobStats> GetStatsAsync(CancellationToken ct = default)
	{
		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.Stats, connection);
		await using var reader = await command.ExecuteReaderAsync(ct);

		var counts = new Dictionary<JobState, long>();

		while (await reader.ReadAsync(ct))
		{
			counts[JobStateExtensions.FromDb(reader.GetString(0))] = reader.GetInt64(1);
		}

		return new JobStats(counts);
	}

	// ---------------------------------------------------------------- recurring schedules

	public async Task UpsertScheduleAsync(RecurringJobDefinition definition, CancellationToken ct = default)
	{
		ArgumentNullException.ThrowIfNull(definition);
		ArgumentException.ThrowIfNullOrWhiteSpace(definition.Name);
		ArgumentException.ThrowIfNullOrWhiteSpace(definition.Type);

		var cron = CronExpression.Parse(definition.Cron);
		var now = Now();
		var next = cron.GetNextOccurrence(now)
			?? throw new InvalidOperationException($"Cron expression '{definition.Cron}' never fires.");

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var command = new NpgsqlCommand(Sql.UpsertSchedule, connection)
			.With("name", NpgsqlDbType.Text, definition.Name)
			.With("cron", NpgsqlDbType.Text, cron.Expression)
			.With("queue", NpgsqlDbType.Text, definition.Queue)
			.With("job_type", NpgsqlDbType.Text, definition.Type)
			.With("payload", NpgsqlDbType.Jsonb, definition.PayloadJson)
			.With("max_attempts", NpgsqlDbType.Integer, definition.MaxAttempts ?? _options.DefaultMaxAttempts)
			.With("next_run_at", NpgsqlDbType.TimestampTz, next);

		await command.ExecuteNonQueryAsync(ct);
	}

	/// <summary>
	/// Enqueues one job for every schedule that is due and moves it to its next slot. Safe to call from many
	/// processes at once: due rows are taken with SKIP LOCKED and each slot's job carries a unique
	/// idempotency key, so a slot can never produce two jobs. Missed slots (downtime) fire once, not N times.
	/// </summary>
	public async Task<int> FireDueSchedulesAsync(int maxSchedules = 50, CancellationToken ct = default)
	{
		var now = Now();

		await using var connection = await _dataSource.OpenConnectionAsync(ct);
		await using var transaction = await connection.BeginTransactionAsync(ct);

		var due = new List<DueSchedule>();

		await using (var lookup = new NpgsqlCommand(Sql.SelectDueSchedules, connection, transaction)
			.With("now", NpgsqlDbType.TimestampTz, now)
			.With("max", NpgsqlDbType.Integer, maxSchedules))
		{
			await using var reader = await lookup.ExecuteReaderAsync(ct);

			while (await reader.ReadAsync(ct))
			{
				due.Add(new DueSchedule(
					reader.GetString(0),
					reader.GetString(1),
					reader.GetString(2),
					reader.GetString(3),
					reader.GetString(4),
					reader.GetInt32(5),
					reader.GetFieldValue<DateTime>(6)));
			}
		}

		foreach (var schedule in due)
		{
			var slot = schedule.NextRunAt.ToString("yyyyMMddHHmm", CultureInfo.InvariantCulture);

			await EnqueueCoreAsync(
				connection,
				transaction,
				new EnqueueRequest
				{
					Type = schedule.JobType,
					Queue = schedule.Queue,
					PayloadJson = schedule.PayloadJson,
					MaxAttempts = schedule.MaxAttempts,
					IdempotencyKey = $"schedule:{schedule.Name}:{slot}"
				},
				JobState.Queued,
				ct);

			var next = CronExpression.Parse(schedule.Cron).GetNextOccurrence(now);

			await using var advance = new NpgsqlCommand(Sql.AdvanceSchedule, connection, transaction)
				.With("name", NpgsqlDbType.Text, schedule.Name)
				.With("next", NpgsqlDbType.TimestampTz, next)
				.With("now", NpgsqlDbType.TimestampTz, now);

			await advance.ExecuteNonQueryAsync(ct);
		}

		await transaction.CommitAsync(ct);

		return due.Count;
	}

	private sealed record DueSchedule(
		string Name,
		string Cron,
		string Queue,
		string JobType,
		string PayloadJson,
		int MaxAttempts,
		DateTime NextRunAt);
}

internal static class NpgsqlCommandExtensions
{
	public static NpgsqlCommand With(this NpgsqlCommand command, string name, NpgsqlDbType type, object? value)
	{
		command.Parameters.Add(new NpgsqlParameter(name, type) { Value = value ?? DBNull.Value });

		return command;
	}
}
