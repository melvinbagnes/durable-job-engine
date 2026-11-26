using System.Diagnostics;
using System.Globalization;
using System.Text;
using JobEngine;
using JobEngine.Jobs;
using JobEngine.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

// Chaos soak test. Real worker PROCESSES drain a large queue while the orchestrator SIGKILLs one every couple
// of seconds (no graceful shutdown, no lease release). Afterwards it checks that nothing was lost or left stuck,
// and it reports every repeated execution (the engine is at-least-once) together with the likely reason.

const int WorkerConcurrency = 12;

var role = Environment.GetEnvironmentVariable("SOAK_ROLE");

if (role == "worker")
{
	return await RunWorker(Environment.GetEnvironmentVariable("SOAK_WORKER_ID")!);
}

return await RunOrchestrator();

static string ConnectionFor(string admin, string schema, int pool) =>
	new NpgsqlConnectionStringBuilder(admin) { SearchPath = schema, MaxPoolSize = pool }.ConnectionString;

static async Task<int> RunWorker(string workerId)
{
	var schema = Environment.GetEnvironmentVariable("SOAK_SCHEMA")!;
	var admin = Environment.GetEnvironmentVariable("JOBENGINE_TEST_CONNECTION")
		?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";

	await using var dataSource = NpgsqlDataSource.Create(ConnectionFor(admin, schema, 16));

	var services = new ServiceCollection();
	services.AddLogging(logging => logging.SetMinimumLevel(LogLevel.Error));
	services
		.AddJobEngine(o =>
		{
			o.ConnectionString = ConnectionFor(admin, schema, 24);
			o.WorkerId = workerId;
			o.Concurrency = WorkerConcurrency;
			o.LeaseDuration = TimeSpan.FromSeconds(double.Parse(Environment.GetEnvironmentVariable("SOAK_LEASE_SECONDS") ?? "3", CultureInfo.InvariantCulture));
			o.PollInterval = TimeSpan.FromMilliseconds(200);
			o.SchedulerInterval = TimeSpan.FromSeconds(1);
			o.RetryPolicy = new JobEngine.Scheduling.RetryPolicy(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1), 2.0, 0.2);
			o.AutoMigrate = false;
			o.NotificationChannel = "soak_" + schema[^12..];
		})
		.AddHandler("soak", async (context, ct) =>
		{
			var failThisRun = context.Attempt == 1 && Random.Shared.NextDouble() < 0.03;

			await using (var insert = dataSource.CreateCommand("INSERT INTO soak_runs (job_id, attempt, worker, failed) VALUES ($1, $2, $3, $4);"))
			{
				insert.Parameters.AddWithValue(context.JobId);
				insert.Parameters.AddWithValue(context.Attempt);
				insert.Parameters.AddWithValue(workerId);
				insert.Parameters.AddWithValue(failThisRun);
				await insert.ExecuteNonQueryAsync(ct);
			}

			await Task.Delay(Random.Shared.Next(1, 8), ct);

			if (failThisRun)
			{
				throw new InvalidOperationException("injected failure");
			}
		});

	await using var provider = services.BuildServiceProvider();

	foreach (var service in provider.GetServices<IHostedService>())
	{
		await service.StartAsync(CancellationToken.None);
	}

	await Task.Delay(Timeout.Infinite);

	return 0;
}

static async Task<int> RunOrchestrator()
{
	var admin = Environment.GetEnvironmentVariable("JOBENGINE_TEST_CONNECTION")
		?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";
	var jobCount = int.Parse(Environment.GetEnvironmentVariable("SOAK_JOBS") ?? "30000", CultureInfo.InvariantCulture);
	var workerCount = int.Parse(Environment.GetEnvironmentVariable("SOAK_WORKERS") ?? "4", CultureInfo.InvariantCulture);
	var killEvery = TimeSpan.FromSeconds(double.Parse(Environment.GetEnvironmentVariable("SOAK_KILL_SECONDS") ?? "2.5", CultureInfo.InvariantCulture));
	var deadline = TimeSpan.FromMinutes(8);
	var leaseSeconds = Environment.GetEnvironmentVariable("SOAK_LEASE_SECONDS") ?? "3";
	var schema = "soak_" + Guid.NewGuid().ToString("N");

	await using (var connection = new NpgsqlConnection(admin))
	{
		await connection.OpenAsync();
		await Exec(connection, $"CREATE SCHEMA \"{schema}\";");
	}

	var connectionString = ConnectionFor(admin, schema, 20);
	var options = new JobEngineOptions { ConnectionString = connectionString };
	await using var store = JobStore.Create(options);
	await store.MigrateAsync();

	await using var dataSource = NpgsqlDataSource.Create(connectionString);

	await using (var create = dataSource.CreateCommand(
		"CREATE TABLE soak_runs (job_id uuid NOT NULL, attempt int NOT NULL, worker text NOT NULL, failed boolean NOT NULL, at timestamptz NOT NULL DEFAULT clock_timestamp());"))
	{
		await create.ExecuteNonQueryAsync();
	}

	Console.WriteLine($"Enqueueing {jobCount:N0} jobs...");
	var next = -1;

	await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(async () =>
	{
		while (Interlocked.Increment(ref next) < jobCount)
		{
			await store.EnqueueAsync(new EnqueueRequest { Type = "soak", MaxAttempts = 50 });
		}
	})));

	var host = Environment.ProcessPath!;
	var assembly = System.Reflection.Assembly.GetEntryAssembly()!.Location;
	var alive = new List<Process>();
	var killedAt = new Dictionary<string, DateTimeOffset>();
	var workerIds = new Dictionary<Process, string>();
	var spawned = 0;

	Process Spawn()
	{
		var info = new ProcessStartInfo(host) { UseShellExecute = false };

		// Started through the dotnet muxer the dll is an argument; started through the apphost it is not.
		if (string.Equals(Path.GetFileNameWithoutExtension(host), "dotnet", StringComparison.OrdinalIgnoreCase))
		{
			info.ArgumentList.Add(assembly);
		}

		info.Environment["SOAK_ROLE"] = "worker";
		var id = $"worker-{++spawned}";
		info.Environment["SOAK_WORKER_ID"] = id;
		info.Environment["SOAK_SCHEMA"] = schema;
		info.Environment["JOBENGINE_TEST_CONNECTION"] = admin;

		var process = Process.Start(info)!;
		workerIds[process] = id;

		return process;
	}

	for (var i = 0; i < workerCount; i++)
	{
		alive.Add(Spawn());
	}

	var clock = Stopwatch.StartNew();
	var random = new Random(7);
	var kills = 0;
	var lastKill = TimeSpan.Zero;
	var lastReport = TimeSpan.Zero;
	long succeeded = 0;

	while (clock.Elapsed < deadline)
	{
		await Task.Delay(500);

		succeeded = await Scalar<long>(dataSource, "SELECT count(*) FROM jobs WHERE state = 'succeeded'");

		if (succeeded >= jobCount)
		{
			break;
		}

		if (clock.Elapsed - lastKill >= killEvery)
		{
			lastKill = clock.Elapsed;
			var victim = alive[random.Next(alive.Count)];
			killedAt[workerIds[victim]] = DateTimeOffset.UtcNow;
			victim.Kill(entireProcessTree: true);
			victim.WaitForExit();
			alive.Remove(victim);
			alive.Add(Spawn());
			kills++;
		}

		if (clock.Elapsed - lastReport >= TimeSpan.FromSeconds(10))
		{
			lastReport = clock.Elapsed;
			Console.WriteLine($"  {clock.Elapsed.TotalSeconds,5:N0}s  succeeded {succeeded:N0}/{jobCount:N0}  kills {kills}");
		}
	}

	var elapsed = clock.Elapsed;

	foreach (var process in alive)
	{
		process.Kill(entireProcessTree: true);
		process.WaitForExit();
	}

	var states = new Dictionary<string, long>();

	await using (var command = dataSource.CreateCommand("SELECT state, count(*) FROM jobs GROUP BY state;"))
	await using (var reader = await command.ExecuteReaderAsync())
	{
		while (await reader.ReadAsync())
		{
			states[reader.GetString(0)] = reader.GetInt64(1);
		}
	}

	var runs = await Scalar<long>(dataSource, "SELECT count(*) FROM soak_runs");
	var distinctRan = await Scalar<long>(dataSource, "SELECT count(DISTINCT job_id) FROM soak_runs");
	var injected = await Scalar<long>(dataSource, "SELECT count(*) FROM soak_runs WHERE failed");
	var missingCompletion = await Scalar<long>(dataSource, "SELECT count(*) FROM jobs WHERE state = 'succeeded' AND completed_at IS NULL");
	var multiAttempt = await Scalar<long>(dataSource, "SELECT count(*) FROM jobs WHERE attempts > 1");

	// Classify every handler execution that followed an earlier execution of the same job. The engine is
	// at-least-once, so repeats are allowed; what matters is why they happened.
	var afterKill = 0;
	var afterOtherStall = 0;
	var afterInjectedFailure = 0;
	var gaps = new[] { 0, 0, 0, 0 }; // < 1 s, 1-3 s, 3-10 s, >= 10 s between two runs of a job that did not fail
	var samples = new List<string>();

	await using (var command = dataSource.CreateCommand(
		"SELECT job_id, worker, at, failed FROM soak_runs WHERE job_id IN (SELECT job_id FROM soak_runs GROUP BY job_id HAVING count(*) > 1) ORDER BY job_id, at;"))
	await using (var reader = await command.ExecuteReaderAsync())
	{
		Guid currentJob = Guid.Empty;
		string previousWorker = "";
		DateTimeOffset previousAt = default;
		bool previousFailed = false;

		while (await reader.ReadAsync())
		{
			var job = reader.GetGuid(0);
			var worker = reader.GetString(1);
			var at = reader.GetFieldValue<DateTimeOffset>(2);
			var failed = reader.GetBoolean(3);

			if (job == currentJob)
			{
				var gap = (at - previousAt).TotalSeconds;

				if (previousFailed)
				{
					afterInjectedFailure++;
				}
				else
				{
					gaps[gap < 1 ? 0 : gap < 3 ? 1 : gap < 10 ? 2 : 3]++;

					if (killedAt.TryGetValue(previousWorker, out var killTime) && (killTime - previousAt).TotalSeconds is > -1 and < 1)
					{
						afterKill++;
					}
					else
					{
						afterOtherStall++;

						if (samples.Count < 8)
						{
							samples.Add($"{job}: {previousWorker} at {previousAt:HH:mm:ss.fff}, then {worker} at {at:HH:mm:ss.fff} (gap {gap:N2} s, first worker killed: {(killedAt.TryGetValue(previousWorker, out var kt) ? kt.ToString("HH:mm:ss.fff") : "never")})");
						}
					}
				}
			}

			currentJob = job;
			previousWorker = worker;
			previousAt = at;
			previousFailed = failed;
		}
	}

	var repeats = runs - distinctRan;

	states.TryGetValue("succeeded", out var finalSucceeded);
	var notFinished = states.Where(pair => pair.Key != "succeeded").Sum(pair => pair.Value);

	var problems = new List<string>();

	if (finalSucceeded != jobCount) problems.Add($"{finalSucceeded:N0} of {jobCount:N0} jobs succeeded.");
	if (notFinished != 0) problems.Add($"{notFinished:N0} job(s) are not succeeded: {string.Join(", ", states.Where(p => p.Key != "succeeded").Select(p => $"{p.Key}={p.Value}"))}.");
	if (distinctRan != jobCount) problems.Add($"{jobCount - distinctRan:N0} job(s) never ran.");
	if (missingCompletion != 0) problems.Add($"{missingCompletion:N0} succeeded job(s) have no completion time.");
	if (elapsed >= deadline) problems.Add("The run hit its deadline before the queue drained.");

	var report = new StringBuilder();
	report.AppendLine("## Chaos soak test");
	report.AppendLine();
	report.AppendLine(
		$"{jobCount:N0} jobs drained by {workerCount} worker processes ({WorkerConcurrency} concurrent jobs each, {leaseSeconds} s leases), " +
		$"with one process killed by SIGKILL every {killEvery.TotalSeconds:N1} s and replaced. {(problems.Count == 0 ? "**Passed.**" : "**FAILED.**")}");
	report.AppendLine();
	report.AppendLine("| Check | Result |");
	report.AppendLine("|---|---|");
	report.AppendLine($"| Jobs succeeded | {finalSucceeded:N0} of {jobCount:N0} |");
	report.AppendLine($"| Jobs lost, stuck, dead or canceled | {notFinished:N0} |");
	report.AppendLine($"| Jobs that never ran | {jobCount - distinctRan:N0} |");
	report.AppendLine($"| Worker processes killed | {kills} |");
	report.AppendLine($"| Handler executions in total | {runs:N0} |");
	report.AppendLine($"| Retries from injected failures (3% of first attempts) | {injected:N0} |");
	report.AppendLine($"| Repeat executions in total (at-least-once) | {repeats:N0} ({repeats * 100.0 / jobCount:N1}% of jobs) |");
	report.AppendLine($"| ...after the previous worker was killed within 1 s | {afterKill:N0} (a kill strands at most {WorkerConcurrency} jobs; {kills} kills = at most {(long)kills * WorkerConcurrency:N0}) |");
	report.AppendLine($"| ...with no kill within 1 s of the earlier run | {afterOtherStall:N0} |");
	report.AppendLine($"| ...retries after an injected failure | {afterInjectedFailure:N0} |");
	report.AppendLine($"| Gap between repeats of a job that had not failed: <1 s / 1-3 s / 3-10 s / 10 s+ | {gaps[0]:N0} / {gaps[1]:N0} / {gaps[2]:N0} / {gaps[3]:N0} |");
	report.AppendLine($"| Jobs that needed more than one attempt | {multiAttempt:N0} |");
	report.AppendLine($"| Wall time / throughput under chaos | {elapsed.TotalSeconds:N0} s / {jobCount / elapsed.TotalSeconds:N0} jobs/s |");

	foreach (var problem in problems)
	{
		report.AppendLine();
		report.AppendLine($"- Problem: {problem}");
	}

	if (samples.Count > 0)
	{
		report.AppendLine();
		report.AppendLine("Examples of repeats with no kill within 1 s:");
		report.AppendLine();

		foreach (var sample in samples)
		{
			report.AppendLine($"- {sample}");
		}
	}

	Console.WriteLine(report.ToString());

	var summary = Environment.GetEnvironmentVariable("GITHUB_STEP_SUMMARY");

	if (!string.IsNullOrEmpty(summary))
	{
		await File.AppendAllTextAsync(summary, report.ToString());
	}

	await using (var connection = new NpgsqlConnection(admin))
	{
		await connection.OpenAsync();
		await Exec(connection, $"DROP SCHEMA IF EXISTS \"{schema}\" CASCADE;");
	}

	return problems.Count == 0 ? 0 : 1;
}

static async Task Exec(NpgsqlConnection connection, string sql)
{
	await using var command = new NpgsqlCommand(sql, connection);
	await command.ExecuteNonQueryAsync();
}

static async Task<T> Scalar<T>(NpgsqlDataSource dataSource, string sql)
{
	await using var command = dataSource.CreateCommand(sql);

	return (T)(await command.ExecuteScalarAsync())!;
}
