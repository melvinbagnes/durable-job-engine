namespace JobEngine.Jobs;

public enum JobState
{
	/// <summary>Waiting for one or more dependencies to succeed.</summary>
	Waiting,

	/// <summary>Ready to be claimed once <c>run_at</c> has passed.</summary>
	Queued,

	/// <summary>Claimed by a worker that holds a lease on it.</summary>
	Running,

	Succeeded,

	/// <summary>Attempts exhausted or permanently failed (dead-letter).</summary>
	Dead,

	/// <summary>Never ran because an upstream dependency died.</summary>
	Canceled
}

public static class JobStateExtensions
{
	public static string ToDb(this JobState state) => state.ToString().ToLowerInvariant();

	public static JobState FromDb(string value) => Enum.Parse<JobState>(value, ignoreCase: true);
}

public sealed class EnqueueRequest
{
	public required string Type { get; init; }

	public string Queue { get; init; } = "default";

	public string PayloadJson { get; init; } = "{}";

	/// <summary>Higher runs first.</summary>
	public int Priority { get; init; }

	public TimeSpan Delay { get; init; }

	public int? MaxAttempts { get; init; }

	/// <summary>When set, enqueueing the same key again returns the original job instead of creating a duplicate.</summary>
	public string? IdempotencyKey { get; init; }
}

public sealed record EnqueueResult(Guid JobId, bool Created);

public sealed record JobRecord(
	Guid Id,
	string Queue,
	string Type,
	string PayloadJson,
	JobState State,
	int Priority,
	DateTime RunAt,
	int Attempts,
	int MaxAttempts,
	string? LastError,
	string? IdempotencyKey,
	DateTime CreatedAt,
	DateTime? CompletedAt);

/// <summary>A job leased to a worker. <see cref="LeaseToken"/> fences every later write.</summary>
public sealed record ClaimedJob(
	Guid Id,
	string Queue,
	string Type,
	string PayloadJson,
	int Attempts,
	int MaxAttempts,
	Guid LeaseToken);

public sealed record FailureOutcome(JobState State, DateTime RunAt);

public sealed record JobStats(IReadOnlyDictionary<JobState, long> ByState)
{
	public long Count(JobState state) => ByState.TryGetValue(state, out var count) ? count : 0;
}
