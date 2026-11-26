using JobEngine.Scheduling;

namespace JobEngine;

/// <param name="MaxConcurrency">Most jobs of this queue running at once across all processes.</param>
/// <param name="MaxPerSecond">Sustained start rate across all processes (token bucket).</param>
/// <param name="Burst">Bucket size; defaults to MaxPerSecond rounded up (at least 1).</param>
public sealed record QueueLimit(int? MaxConcurrency = null, double? MaxPerSecond = null, int? Burst = null);

public sealed class JobEngineOptions
{
	public string ConnectionString { get; set; } = "";

	/// <summary>Recorded as lease_owner on claimed jobs; useful when debugging a stuck job.</summary>
	public string WorkerId { get; set; } = $"{Environment.MachineName}:{Environment.ProcessId}";

	public IReadOnlyList<string> Queues { get; set; } = ["default"];

	/// <summary>Maximum number of jobs this process runs at the same time.</summary>
	public int Concurrency { get; set; } = 4;

	/// <summary>
	/// Idle workers are woken by PostgreSQL LISTEN/NOTIFY the moment a job becomes runnable. Polling stays as a
	/// backstop: it picks up delayed jobs and retries when they come due, and covers a dropped listener connection.
	/// </summary>
	public bool UseListenNotify { get; set; } = true;

	/// <summary>Channel name. Channels are database-wide, so give each environment sharing a database its own.</summary>
	public string NotificationChannel { get; set; } = "job_engine";

	/// <summary>How long an idle worker waits for a notification before polling anyway.</summary>
	public TimeSpan PollInterval { get; set; } = TimeSpan.FromSeconds(1);

	/// <summary>
	/// How long a claimed job stays reserved without a heartbeat. Heartbeats run every third of this,
	/// so a crashed worker's jobs are picked up again after at most this long.
	/// </summary>
	public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30);

	public TimeSpan SchedulerInterval { get; set; } = TimeSpan.FromSeconds(5);

	public int DefaultMaxAttempts { get; set; } = 5;

	public RetryPolicy RetryPolicy { get; set; } = new(TimeSpan.FromSeconds(5), TimeSpan.FromMinutes(30));

	/// <summary>When set, finished jobs older than this are deleted (their idempotency keys are released with them).</summary>
	public TimeSpan? CompletedRetention { get; set; }

	/// <summary>
	/// Applied to jobs that do not set their own <see cref="Jobs.EnqueueRequest.Timeout"/>. Null means no timeout.
	/// On timeout the handler's CancellationToken is canceled and the attempt counts as failed (it is retried
	/// under the usual backoff). A handler that ignores its token cannot be killed: after
	/// <see cref="TimeoutGrace"/> it is abandoned (it keeps running in the background) so the slot is freed.
	/// </summary>
	public TimeSpan? DefaultJobTimeout { get; set; }

	public TimeSpan TimeoutGrace { get; set; } = TimeSpan.FromSeconds(5);

	/// <summary>
	/// When set, succeeded jobs older than this move from <c>jobs</c> to <c>job_archive</c>, which keeps the hot
	/// table (and its indexes) small. Their idempotency keys are released with them. Dead and canceled jobs
	/// stay in <c>jobs</c> so they can be inspected and replayed.
	/// </summary>
	public TimeSpan? ArchiveAfter { get; set; }

	/// <summary>When set, archived jobs older than this are deleted for good.</summary>
	public TimeSpan? ArchiveRetention { get; set; }

	/// <summary>
	/// Cluster-wide limits per queue, shared by every worker process. A queue that is not listed is unlimited.
	/// Limited queues are claimed under a per-queue advisory lock, which serialises their claims.
	/// </summary>
	public Dictionary<string, QueueLimit> QueueLimits { get; } = new();

	public bool AutoMigrate { get; set; } = true;

	public bool EnableWorker { get; set; } = true;

	public bool EnableScheduler { get; set; } = true;
}
