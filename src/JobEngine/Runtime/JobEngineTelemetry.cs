using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace JobEngine.Runtime;

/// <summary>
/// Built-in instrumentation using only System.Diagnostics, so it needs no extra package. Subscribe with
/// OpenTelemetry via <c>AddMeter(JobEngineTelemetry.Name)</c> and <c>AddSource(JobEngineTelemetry.Name)</c>.
/// </summary>
public static class JobEngineTelemetry
{
	public const string Name = "JobEngine";

	public const string EnqueuedMetric = "jobengine.jobs.enqueued";
	public const string ProcessedMetric = "jobengine.jobs.processed";
	public const string DurationMetric = "jobengine.job.duration";

	public static readonly ActivitySource ActivitySource = new(Name);

	private static readonly Meter Meter = new(Name);

	/// <summary>Jobs created. Tags: queue, type.</summary>
	internal static readonly Counter<long> Enqueued = Meter.CreateCounter<long>(EnqueuedMetric, description: "Jobs created.");

	/// <summary>
	/// Handler executions that ended. Tags: queue, type, outcome = succeeded | retried | dead | timed_out |
	/// released | lease_lost.
	/// </summary>
	internal static readonly Counter<long> Processed = Meter.CreateCounter<long>(ProcessedMetric, description: "Handler executions by outcome.");

	/// <summary>Handler wall time. Tags: queue, type, outcome.</summary>
	internal static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(DurationMetric, unit: "ms", description: "Handler execution time.");
}
