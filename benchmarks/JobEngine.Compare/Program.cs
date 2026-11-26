using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using Hangfire;
using Hangfire.PostgreSql;
using Npgsql;

// The same two workloads as JobEngine.Benchmarks, run against Hangfire + Hangfire.PostgreSql:
// 1) enqueue N no-op jobs from 8 parallel producers, 2) drain N no-op jobs with 16 workers.
// Same machine, same PostgreSQL, one process. Hangfire's default fetch uses polling, so its poll interval
// is set to one second, matching the engine's default PollInterval.

var admin = Environment.GetEnvironmentVariable("JOBENGINE_TEST_CONNECTION")
	?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";
var jobs = int.Parse(Environment.GetEnvironmentVariable("BENCH_JOBS") ?? "20000", CultureInfo.InvariantCulture);
var schema = "hf_" + Guid.NewGuid().ToString("N")[..12];
var connectionString = new NpgsqlConnectionStringBuilder(admin) { MaxPoolSize = 100 }.ConnectionString;

try
{
	GlobalConfiguration.Configuration.UsePostgreSqlStorage(
		c => c.UseNpgsqlConnection(connectionString),
		new PostgreSqlStorageOptions
		{
			SchemaName = schema,
			QueuePollInterval = TimeSpan.FromSeconds(1),
			PrepareSchemaIfNecessary = true
		});

	var client = new BackgroundJobClient(JobStorage.Current);
	var next = -1;

	var enqueueClock = Stopwatch.StartNew();

	await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
	{
		while (Interlocked.Increment(ref next) < jobs)
		{
			client.Enqueue<NoOp>(job => job.Run());
		}
	})));

	var enqueueRate = jobs / enqueueClock.Elapsed.TotalSeconds;

	var drainClock = Stopwatch.StartNew();

	using (var server = new BackgroundJobServer(new BackgroundJobServerOptions { WorkerCount = 16, ServerName = "compare" }))
	{
		while (NoOp.Executed < jobs && drainClock.Elapsed < TimeSpan.FromMinutes(10))
		{
			await Task.Delay(50);
		}
	}

	var drainRate = NoOp.Executed / drainClock.Elapsed.TotalSeconds;

	string version;

	await using (var connection = new NpgsqlConnection(connectionString))
	{
		await connection.OpenAsync();
		version = (string)(await new NpgsqlCommand("SHOW server_version;", connection).ExecuteScalarAsync())!;
	}

	var report = new StringBuilder();
	report.AppendLine("## Comparison: Hangfire + Hangfire.PostgreSql");
	report.AppendLine();
	report.AppendLine(
		$"Same machine and PostgreSQL {version} as the benchmark above, {Environment.ProcessorCount} logical cores, " +
		$"{RuntimeInformation.FrameworkDescription}. {jobs:N0} no-op jobs, 16 workers, poll interval 1 s. " +
		"Hangfire's executed counter is incremented inside the job, so its final state write is not in the timing; " +
		"that favours Hangfire slightly.");
	report.AppendLine();
	report.AppendLine("| Measurement | Hangfire |");
	report.AppendLine("|---|---|");
	report.AppendLine($"| Enqueue, 8 parallel producers | {enqueueRate:N0} jobs/s |");
	report.AppendLine($"| Drain, 16 workers, no-op job | {drainRate:N0} jobs/s ({NoOp.Executed:N0} of {jobs:N0} executed) |");

	Console.WriteLine(report.ToString());

	var summary = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");

	if (!string.IsNullOrEmpty(summary))
	{
		await File.AppendAllTextAsync(summary, report.ToString());
	}

	return NoOp.Executed >= jobs ? 0 : 1;
}
finally
{
	await using var connection = new NpgsqlConnection(connectionString);
	await connection.OpenAsync();
	await new NpgsqlCommand($"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;", connection).ExecuteNonQueryAsync();
}

public sealed class NoOp
{
	private static long _executed;

	public static long Executed => Interlocked.Read(ref _executed);

	public void Run() => Interlocked.Increment(ref _executed);
}
