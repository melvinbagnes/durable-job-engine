using JobEngine.Scheduling;

namespace JobEngine;

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

	public bool AutoMigrate { get; set; } = true;

	public bool EnableWorker { get; set; } = true;

	public bool EnableScheduler { get; set; } = true;
}
