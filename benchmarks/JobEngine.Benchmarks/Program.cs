using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using JobEngine;
using JobEngine.Jobs;
using JobEngine.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

// Measures the engine against a real PostgreSQL. Everything runs on one machine, with the database
// and the workers sharing CPU, so treat the numbers as a floor, not as a capacity claim.

var adminConnection = Environment.GetEnvironmentVariable("JOBENGINE_TEST_CONNECTION")
	?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";

var jobsToDrain = int.Parse(Environment.GetEnvironmentVariable("BENCH_JOBS") ?? "20000", CultureInfo.InvariantCulture);
var schema = "bench_" + Guid.NewGuid().ToString("N");

await using (var connection = new NpgsqlConnection(adminConnection))
{
	await connection.OpenAsync();
	await Execute(connection, $"CREATE SCHEMA \"{schema}\";");
}

var benchConnection = new NpgsqlConnectionStringBuilder(adminConnection) { SearchPath = schema, MaxPoolSize = 100 }.ConnectionString;
var report = new StringBuilder();

try
{
	string serverVersion;

	await using (var connection = new NpgsqlConnection(benchConnection))
	{
		await connection.OpenAsync();
		serverVersion = (string)(await new NpgsqlCommand("SHOW server_version;", connection).ExecuteScalarAsync())!;
	}

	report.AppendLine("## Benchmark results");
	report.AppendLine();
	report.AppendLine(
		$"PostgreSQL {serverVersion} on the same machine as the workers, {Environment.ProcessorCount} logical cores, " +
		$"{RuntimeInformation.OSDescription}, {RuntimeInformation.FrameworkDescription}. {jobsToDrain:N0} jobs per throughput run.");
	report.AppendLine();

	var enqueue = await MeasureEnqueue(benchConnection, jobsToDrain);
	var drain1 = await MeasureDrain(benchConnection, jobsToDrain, workers: 1, concurrency: 16);
	var drain4 = await MeasureDrain(benchConnection, jobsToDrain, workers: 4, concurrency: 16);

	report.AppendLine("| Measurement | Result |");
	report.AppendLine("|---|---|");
	report.AppendLine($"| Enqueue, 8 parallel producers | {enqueue:N0} jobs/s |");
	report.AppendLine($"| Drain, 1 worker, concurrency 16, no-op handler | {drain1:N0} jobs/s |");
	report.AppendLine($"| Drain, 4 workers x concurrency 16, no-op handler | {drain4:N0} jobs/s |");
	report.AppendLine();

	var notify = await MeasureWakeLatency(benchConnection, useListenNotify: true, samples: 100);
	var polling = await MeasureWakeLatency(benchConnection, useListenNotify: false, samples: 30);

	report.AppendLine("Idle-to-running latency (job enqueued while the worker is idle; poll interval 1 s):");
	report.AppendLine();
	report.AppendLine("| Mode | p50 | p95 | max |");
	report.AppendLine("|---|---|---|---|");
	report.AppendLine($"| LISTEN/NOTIFY | {notify.P50:N0} ms | {notify.P95:N0} ms | {notify.Max:N0} ms |");
	report.AppendLine($"| Polling only | {polling.P50:N0} ms | {polling.P95:N0} ms | {polling.Max:N0} ms |");
}
finally
{
	await using var connection = new NpgsqlConnection(adminConnection);
	await connection.OpenAsync();
	await Execute(connection, $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;");
}

Console.WriteLine(report.ToString());

var summary = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");

if (!string.IsNullOrEmpty(summary))
{
	await File.AppendAllTextAsync(summary, report.ToString());
}

await File.WriteAllTextAsync("benchmark-results.md", report.ToString());

return 0;

static async Task Execute(NpgsqlConnection connection, string sql)
{
	await using var command = new NpgsqlCommand(sql, connection);
	await command.ExecuteNonQueryAsync();
}

static JobEngineOptions NewOptions(string connectionString, string workerId, int concurrency, bool listenNotify, TimeSpan poll) => new()
{
	ConnectionString = connectionString,
	WorkerId = workerId,
	Concurrency = concurrency,
	UseListenNotify = listenNotify,
	NotificationChannel = "bench_" + Guid.NewGuid().ToString("N")[..16],
	PollInterval = poll,
	AutoMigrate = false,
	EnableScheduler = false
};

static async Task Reset(string connectionString)
{
	await using var connection = new NpgsqlConnection(connectionString);
	await connection.OpenAsync();
	await Execute(connection, "TRUNCATE jobs CASCADE;");
}

static async Task<double> MeasureEnqueue(string connectionString, int count)
{
	var options = NewOptions(connectionString, "producer", 1, true, TimeSpan.FromSeconds(1));
	await using var store = JobStore.Create(options);
	await store.MigrateAsync();
	await Reset(connectionString);

	var next = -1;
	var stopwatch = Stopwatch.StartNew();

	await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
	{
		while (Interlocked.Increment(ref next) < count)
		{
			await store.EnqueueAsync(new EnqueueRequest { Type = "noop" });
		}
	})));

	stopwatch.Stop();
	await Reset(connectionString);

	return count / stopwatch.Elapsed.TotalSeconds;
}

static async Task<double> MeasureDrain(string connectionString, int count, int workers, int concurrency)
{
	var producerOptions = NewOptions(connectionString, "producer", 1, true, TimeSpan.FromSeconds(1));
	await using var store = JobStore.Create(producerOptions);
	await store.MigrateAsync();
	await Reset(connectionString);

	var done = 0;
	var engines = new List<(ServiceProvider Provider, List<IHostedService> Services)>();

	for (var i = 0; i < workers; i++)
	{
		var workerIndex = i;
		var services = new ServiceCollection();
		services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
		services
			.AddJobEngine(o =>
			{
				var template = NewOptions(connectionString, $"worker-{workerIndex}", concurrency, true, TimeSpan.FromMilliseconds(250));
				o.ConnectionString = template.ConnectionString;
				o.WorkerId = template.WorkerId;
				o.Concurrency = template.Concurrency;
				o.PollInterval = template.PollInterval;
				o.NotificationChannel = producerOptions.NotificationChannel;
				o.AutoMigrate = false;
				o.EnableScheduler = false;
			})
			.AddHandler("noop", (_, _) =>
			{
				Interlocked.Increment(ref done);

				return Task.CompletedTask;
			});

		var provider = services.BuildServiceProvider();
		engines.Add((provider, provider.GetServices<IHostedService>().ToList()));
	}

	// Pre-load the queue so the run measures claiming and completing, not producing.
	var next = -1;

	await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
	{
		while (Interlocked.Increment(ref next) < count)
		{
			await store.EnqueueAsync(new EnqueueRequest { Type = "noop" });
		}
	})));

	var stopwatch = Stopwatch.StartNew();

	foreach (var (_, services) in engines)
	{
		foreach (var service in services)
		{
			await service.StartAsync(CancellationToken.None);
		}
	}

	while (Volatile.Read(ref done) < count)
	{
		await Task.Delay(10);
	}

	stopwatch.Stop();

	foreach (var (provider, services) in engines)
	{
		foreach (var service in Enumerable.Reverse(services))
		{
			await service.StopAsync(CancellationToken.None);
		}

		await provider.DisposeAsync();
	}

	await Reset(connectionString);

	return count / stopwatch.Elapsed.TotalSeconds;
}

static async Task<(double P50, double P95, double Max)> MeasureWakeLatency(string connectionString, bool useListenNotify, int samples)
{
	var options = NewOptions(connectionString, "latency", 1, useListenNotify, TimeSpan.FromSeconds(1));
	await using var store = JobStore.Create(options);
	await store.MigrateAsync();
	await Reset(connectionString);

	TaskCompletionSource? ran = null;
	var services = new ServiceCollection();
	services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Warning));
	services
		.AddJobEngine(o =>
		{
			o.ConnectionString = options.ConnectionString;
			o.WorkerId = options.WorkerId;
			o.Concurrency = 1;
			o.UseListenNotify = useListenNotify;
			o.NotificationChannel = options.NotificationChannel;
			o.PollInterval = TimeSpan.FromSeconds(1);
			o.AutoMigrate = false;
			o.EnableScheduler = false;
		})
		.AddHandler("ping", (_, _) =>
		{
			Volatile.Read(ref ran)?.TrySetResult();

			return Task.CompletedTask;
		});

	await using var provider = services.BuildServiceProvider();
	var hosted = provider.GetServices<IHostedService>().ToList();

	foreach (var service in hosted)
	{
		await service.StartAsync(CancellationToken.None);
	}

	await Task.Delay(1500); // let the worker settle into its idle wait

	var latencies = new List<double>();
	var random = new Random(42);

	for (var i = 0; i < samples; i++)
	{
		await Task.Delay(random.Next(50, 400)); // vary the phase relative to the poll timer

		var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		Volatile.Write(ref ran, signal);

		var started = Stopwatch.GetTimestamp();
		await store.EnqueueAsync(new EnqueueRequest { Type = "ping" });
		await signal.Task.WaitAsync(TimeSpan.FromSeconds(10));

		latencies.Add(Stopwatch.GetElapsedTime(started).TotalMilliseconds);
	}

	foreach (var service in Enumerable.Reverse(hosted))
	{
		await service.StopAsync(CancellationToken.None);
	}

	latencies.Sort();
	await Reset(connectionString);

	return (latencies[latencies.Count / 2], latencies[(int)Math.Ceiling(latencies.Count * 0.95) - 1], latencies[^1]);
}
