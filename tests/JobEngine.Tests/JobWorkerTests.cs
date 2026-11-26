using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using JobEngine.Jobs;
using JobEngine.Runtime;
using JobEngine.Scheduling;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace JobEngine.Tests;

/// <summary>End-to-end tests: real worker, real clock, real Postgres.</summary>
public class JobWorkerTests : PostgresTestBase
{
	private int _longRunningCalls;

	/// <summary>No TimeProvider: the store and the engine under test both use the database clock.</summary>
	protected override TimeProvider? CreateTimeProvider() => null;

	private sealed class RunningEngine(ServiceProvider provider) : IAsyncDisposable
	{
		private readonly List<IHostedService> _services = provider.GetServices<IHostedService>().ToList();
		private bool _stopped;

		public async Task StartAsync()
		{
			foreach (var service in _services)
			{
				await service.StartAsync(CancellationToken.None);
			}
		}

		public async Task StopAsync()
		{
			if (_stopped)
			{
				return;
			}

			_stopped = true;

			foreach (var service in Enumerable.Reverse(_services))
			{
				await service.StopAsync(CancellationToken.None);
			}
		}

		public async ValueTask DisposeAsync()
		{
			await StopAsync();
			await provider.DisposeAsync();
		}
	}

	private RunningEngine Build(Action<JobEngineBuilder> configure, Action<JobEngineOptions>? tweak = null)
	{
		var services = new ServiceCollection();

		var builder = services.AddJobEngine(options =>
		{
			options.ConnectionString = Options.ConnectionString;
			options.WorkerId = "test-worker";
			options.NotificationChannel = Options.NotificationChannel;
			options.PollInterval = TimeSpan.FromMilliseconds(50);
			options.LeaseDuration = TimeSpan.FromSeconds(5);
			options.SchedulerInterval = TimeSpan.FromMilliseconds(100);
			options.DefaultMaxAttempts = 3;
			options.RetryPolicy = new RetryPolicy(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200), 2.0, 0);
			tweak?.Invoke(options);
		});

		configure(builder);

		return new RunningEngine(services.BuildServiceProvider());
	}

	private Task WaitForStateAsync(Guid id, JobState state) =>
		WaitUntilAsync(async () => (await GetJobAsync(id)).State == state);

	[Fact]
	public async Task Worker_ProcessesEveryJobExactlyOnce()
	{
		var seen = new ConcurrentBag<Guid>();

		await using var engine = Build(
			builder => builder.AddHandler("work", async (context, ct) =>
			{
				await Task.Delay(10, ct);
				seen.Add(context.JobId);
			}),
			options => options.Concurrency = 6);

		for (var i = 0; i < 30; i++)
		{
			await Store.EnqueueAsync(Job("work"));
		}

		await engine.StartAsync();
		await WaitUntilAsync(async () => (await Store.GetStatsAsync()).Count(JobState.Succeeded) == 30);
		await engine.StopAsync();

		Assert.Equal(30, seen.Count);
		Assert.Equal(30, seen.Distinct().Count());
	}

	[Fact]
	public async Task Worker_RetriesAFlakyJobUntilItSucceeds()
	{
		await using var engine = Build(builder => builder.AddHandler("flaky", (context, _) =>
		{
			if (context.Attempt < 3)
			{
				throw new InvalidOperationException("boom");
			}

			return Task.CompletedTask;
		}));

		var id = (await Store.EnqueueAsync(Job("flaky"))).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Succeeded);

		var job = await GetJobAsync(id);

		Assert.Equal(3, job.Attempts);
		Assert.Contains("boom", job.LastError);
	}

	[Fact]
	public async Task Worker_DeadLettersAJobThatKeepsFailing()
	{
		await using var engine = Build(builder => builder.AddHandler("always-fails", (_, _) =>
			throw new InvalidOperationException("still broken")));

		var id = (await Store.EnqueueAsync(Job("always-fails"))).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Dead);

		var job = await GetJobAsync(id);

		Assert.Equal(3, job.Attempts);
		Assert.Contains("still broken", job.LastError);
	}

	[Fact]
	public async Task Worker_DoesNotRetryAPermanentFailure()
	{
		await using var engine = Build(builder => builder.AddHandler("bad-input", (_, _) =>
			throw new PermanentFailureException("payload is invalid")));

		var id = (await Store.EnqueueAsync(Job("bad-input"))).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Dead);

		var job = await GetJobAsync(id);

		Assert.Equal(1, job.Attempts);
		Assert.Contains("payload is invalid", job.LastError);
	}

	[Fact]
	public async Task Worker_DeadLettersJobsWithoutAHandler()
	{
		await using var engine = Build(builder => builder.AddHandler("known", (_, _) => Task.CompletedTask));

		var id = (await Store.EnqueueAsync(Job("nobody-handles-this"))).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Dead);

		Assert.Contains("No handler registered", (await GetJobAsync(id)).LastError);
	}

	[Fact]
	public async Task Worker_Heartbeat_KeepsALongJobFromBeingRunTwice()
	{
		await using var engine = Build(
			builder => builder.AddHandler("long", async (_, ct) =>
			{
				Interlocked.Increment(ref _longRunningCalls);
				await Task.Delay(TimeSpan.FromSeconds(4), ct);
			}),
			options => options.LeaseDuration = TimeSpan.FromMilliseconds(1500));

		var id = (await Store.EnqueueAsync(Job("long"))).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Succeeded);

		Assert.Equal(1, Volatile.Read(ref _longRunningCalls));
		Assert.Equal(1, (await GetJobAsync(id)).Attempts);
	}

	[Fact]
	public async Task Worker_PicksUpAJobAbandonedByACrashedWorker()
	{
		var id = (await Store.EnqueueAsync(Job("work"))).JobId;

		// A worker claims the job and dies without finishing it; its lease is short.
		var abandoned = await Store.ClaimAsync("crashed-worker", ["default"], 1, TimeSpan.FromMilliseconds(300));

		Assert.Single(abandoned);

		await using var engine = Build(builder => builder.AddHandler("work", (_, _) => Task.CompletedTask));

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Succeeded);

		Assert.Equal(2, (await GetJobAsync(id)).Attempts);
	}

	[Fact]
	public async Task Worker_ShutdownReleasesRunningJobsWithoutSpendingAnAttempt()
	{
		var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		await using var engine = Build(builder => builder.AddHandler("forever", async (_, ct) =>
		{
			started.TrySetResult();
			await Task.Delay(Timeout.Infinite, ct);
		}));

		var id = (await Store.EnqueueAsync(Job("forever"))).JobId;

		await engine.StartAsync();
		await started.Task.WaitAsync(TimeSpan.FromSeconds(15));
		await engine.StopAsync();

		var job = await GetJobAsync(id);

		Assert.Equal(JobState.Queued, job.State);
		Assert.Equal(0, job.Attempts);
	}

	[Fact]
	public async Task Worker_CancelsAHandlerWhoseLeaseWasTakenAway()
	{
		var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
		var canceled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

		await using var engine = Build(
			builder => builder.AddHandler("zombie", async (_, ct) =>
			{
				started.TrySetResult();

				try
				{
					await Task.Delay(Timeout.Infinite, ct);
				}
				catch (OperationCanceledException)
				{
					canceled.TrySetResult();

					throw;
				}
			}),
			options => options.LeaseDuration = TimeSpan.FromMilliseconds(900));

		await Store.EnqueueAsync(Job("zombie"));
		await engine.StartAsync();
		await started.Task.WaitAsync(TimeSpan.FromSeconds(15));

		// Simulate another worker having reclaimed the job: the lease token no longer matches.
		await using (var steal = DataSource.CreateCommand("UPDATE jobs SET lease_token = gen_random_uuid();"))
		{
			await steal.ExecuteNonQueryAsync();
		}

		await canceled.Task.WaitAsync(TimeSpan.FromSeconds(15));
	}

	[Fact]
	public async Task Worker_RunsAWorkflowInDependencyOrder()
	{
		var order = new ConcurrentQueue<string>();

		await using var engine = Build(builder =>
		{
			foreach (var type in new[] { "a", "b", "c", "d" })
			{
				builder.AddHandler(type, (context, _) =>
				{
					order.Enqueue(context.Type);

					return Task.CompletedTask;
				});
			}
		});

		var result = await Store.EnqueueWorkflowAsync(
		[
			Step("a").Build(),
			Step("b").After("a").Build(),
			Step("c").After("a").Build(),
			Step("d").After("b", "c").Build()
		]);

		await engine.StartAsync();
		await WaitForStateAsync(result.JobIds["d"], JobState.Succeeded);

		var ran = order.ToArray();

		Assert.Equal(4, ran.Length);
		Assert.Equal("a", ran[0]);
		Assert.Equal("d", ran[3]);
	}

	[Fact]
	public async Task Worker_FailedWorkflowStep_CancelsWhatDependsOnIt()
	{
		await using var engine = Build(builder =>
		{
			builder.AddHandler("a", (_, _) => throw new PermanentFailureException("nope"));
			builder.AddHandler("b", (_, _) => Task.CompletedTask);
		});

		var result = await Store.EnqueueWorkflowAsync([Step("a").Build(), Step("b").After("a").Build()]);

		await engine.StartAsync();
		await WaitForStateAsync(result.JobIds["a"], JobState.Dead);

		Assert.Equal(JobState.Canceled, (await GetJobAsync(result.JobIds["b"])).State);
	}

	[Fact]
	public async Task Startup_RegistersRecurringJobs()
	{
		await using var engine = Build(builder => builder
			.AddHandler("nightly", (_, _) => Task.CompletedTask)
			.AddRecurring(new RecurringJobDefinition("nightly-report", "0 3 * * *", "nightly")));

		await engine.StartAsync();

		Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM schedules WHERE name = 'nightly-report'"));
	}

	[Fact]
	public void Registration_FailsFast_OnBadCronOrDuplicateHandlers()
	{
		Assert.Throws<FormatException>(() => Build(builder =>
			builder.AddRecurring(new RecurringJobDefinition("broken", "not a cron", "t"))));

		Assert.Throws<InvalidOperationException>(() => Build(builder => builder
			.AddHandler("dup", (_, _) => Task.CompletedTask)
			.AddHandler("dup", (_, _) => Task.CompletedTask)));
	}

	// ------------------------------------------------------------ LISTEN/NOTIFY

	[Fact]
	public async Task Notify_WakesAnIdleWorker_WithoutWaitingForThePollInterval()
	{
		await using var engine = Build(
			builder => builder.AddHandler("ping", (_, _) => Task.CompletedTask),
			options => options.PollInterval = TimeSpan.FromSeconds(60));

		await engine.StartAsync();
		await Task.Delay(500); // the first (empty) claim is done and the worker is waiting

		var id = (await Store.EnqueueAsync(Job("ping"))).JobId;

		await WaitUntilAsync(async () => (await GetJobAsync(id)).State == JobState.Succeeded, TimeSpan.FromSeconds(10));
	}

	[Fact]
	public async Task WithoutNotify_AnIdleWorkerOnlyNoticesTheJobAtTheNextPoll()
	{
		await using var engine = Build(
			builder => builder.AddHandler("ping", (_, _) => Task.CompletedTask),
			options =>
			{
				options.UseListenNotify = false;
				options.PollInterval = TimeSpan.FromSeconds(60);
			});

		await engine.StartAsync();
		await Task.Delay(500);

		var id = (await Store.EnqueueAsync(Job("ping"))).JobId;

		await Task.Delay(1500);

		Assert.Equal(JobState.Queued, (await GetJobAsync(id)).State);
	}

	[Fact]
	public async Task Notify_WakesTheWorkerWhenADependencyCompletes()
	{
		await using var engine = Build(
			builder => builder
				.AddHandler("a", (_, _) => Task.CompletedTask)
				.AddHandler("b", (_, _) => Task.CompletedTask),
			options => options.PollInterval = TimeSpan.FromSeconds(60));

		await engine.StartAsync();
		await Task.Delay(500);

		var result = await Store.EnqueueWorkflowAsync([Step("a").Build(), Step("b").After("a").Build()]);

		await WaitUntilAsync(
			async () => (await GetJobAsync(result.JobIds["b"])).State == JobState.Succeeded,
			TimeSpan.FromSeconds(10));
	}

	[Fact]
	public async Task Notify_FromACallersTransaction_WakesTheWorkerOnlyAfterCommit()
	{
		await using var engine = Build(
			builder => builder.AddHandler("ping", (_, _) => Task.CompletedTask),
			options => options.PollInterval = TimeSpan.FromSeconds(60));

		await engine.StartAsync();
		await Task.Delay(500);

		Guid id;

		await using (var connection = await DataSource.OpenConnectionAsync())
		await using (var transaction = await connection.BeginTransactionAsync())
		{
			id = (await Store.EnqueueAsync(transaction, Job("ping"))).JobId;

			await Task.Delay(500);
			await transaction.CommitAsync();
		}

		await WaitUntilAsync(async () => (await GetJobAsync(id)).State == JobState.Succeeded, TimeSpan.FromSeconds(10));
	}

	[Fact]
	public async Task DelayedJob_RunsOnceDue_EvenWithNoNotificationForIt()
	{
		await using var engine = Build(
			builder => builder.AddHandler("later", (_, _) => Task.CompletedTask),
			options => options.PollInterval = TimeSpan.FromMilliseconds(100));

		await engine.StartAsync();

		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "later", Delay = TimeSpan.FromSeconds(1) })).JobId;

		await WaitUntilAsync(async () => (await GetJobAsync(id)).State == JobState.Succeeded, TimeSpan.FromSeconds(10));
	}

	// ------------------------------------------------------------ per-job timeout

	[Fact]
	public async Task Timeout_CancelsACooperativeHandler_ThenRetries()
	{
		await using var engine = Build(builder => builder.AddHandler("slow-then-fast", async (context, ct) =>
		{
			if (context.Attempt == 1)
			{
				await Task.Delay(Timeout.Infinite, ct);
			}
		}));

		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "slow-then-fast", Timeout = TimeSpan.FromMilliseconds(300) })).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Succeeded);

		var job = await GetJobAsync(id);

		Assert.Equal(2, job.Attempts);
		Assert.Contains("timeout", job.LastError, StringComparison.OrdinalIgnoreCase);
	}

	[Fact]
	public async Task Timeout_AbandonsAHandlerThatIgnoresItsToken_AndFreesTheSlot()
	{
		await using var engine = Build(
			builder => builder.AddHandler("deaf", async (_, _) => await Task.Delay(TimeSpan.FromSeconds(5))),
			options =>
			{
				options.Concurrency = 1;
				options.TimeoutGrace = TimeSpan.FromMilliseconds(200);
			});

		var stuck = (await Store.EnqueueAsync(new EnqueueRequest { Type = "deaf", Timeout = TimeSpan.FromMilliseconds(200), MaxAttempts = 1 })).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(stuck, JobState.Dead);

		Assert.Contains("timeout", (await GetJobAsync(stuck)).LastError, StringComparison.OrdinalIgnoreCase);
	}

	[Fact]
	public async Task DefaultJobTimeout_AppliesToJobsThatSetNone()
	{
		await using var engine = Build(
			builder => builder.AddHandler("hang", async (_, ct) => await Task.Delay(Timeout.Infinite, ct)),
			options => options.DefaultJobTimeout = TimeSpan.FromMilliseconds(300));

		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "hang", MaxAttempts = 1 })).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(id, JobState.Dead);
	}

	// ------------------------------------------------------------ cluster-wide queue limit

	[Fact]
	public async Task QueueLimit_MaxConcurrency_HoldsAcrossAllSlots()
	{
		var running = 0;
		var peak = 0;

		await using var engine = Build(
			builder => builder.AddHandler("serial", async (_, ct) =>
			{
				var now = Interlocked.Increment(ref running);
				InterlockedMax(ref peak, now);
				await Task.Delay(40, ct);
				Interlocked.Decrement(ref running);
			}),
			options =>
			{
				options.Queues = ["serial"];
				options.Concurrency = 4;
				options.QueueLimits["serial"] = new QueueLimit(MaxConcurrency: 1);
			});

		for (var i = 0; i < 6; i++)
		{
			await Store.EnqueueAsync(Job("serial", "serial"));
		}

		await engine.StartAsync();
		await WaitUntilAsync(async () => (await Store.GetStatsAsync()).Count(JobState.Succeeded) == 6);

		Assert.Equal(1, peak);
	}

	private static void InterlockedMax(ref int target, int value)
	{
		int current;

		do
		{
			current = Volatile.Read(ref target);
		}
		while (value > current && Interlocked.CompareExchange(ref target, value, current) != current);
	}

	// ------------------------------------------------------------ metrics and tracing

	private sealed record Measurement(string Instrument, double Value, Dictionary<string, object?> Tags);

	private static Dictionary<string, object?> ToDictionary(ReadOnlySpan<KeyValuePair<string, object?>> tags)
	{
		var result = new Dictionary<string, object?>();

		foreach (var tag in tags)
		{
			result[tag.Key] = tag.Value;
		}

		return result;
	}

	[Fact]
	public async Task Telemetry_RecordsEnqueueAndOutcomeMetrics_AndOneSpanPerExecution()
	{
		var measurements = new ConcurrentBag<Measurement>();
		var spans = new ConcurrentBag<Activity>();

		using var meterListener = new MeterListener();
		meterListener.InstrumentPublished = (instrument, listener) =>
		{
			if (instrument.Meter.Name == JobEngineTelemetry.Name)
			{
				listener.EnableMeasurementEvents(instrument);
			}
		};
		meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
			measurements.Add(new Measurement(instrument.Name, value, ToDictionary(tags))));
		meterListener.SetMeasurementEventCallback<double>((instrument, value, tags, _) =>
			measurements.Add(new Measurement(instrument.Name, value, ToDictionary(tags))));
		meterListener.Start();

		using var activityListener = new ActivityListener
		{
			ShouldListenTo = source => source.Name == JobEngineTelemetry.Name,
			Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
			ActivityStopped = activity => spans.Add(activity)
		};
		ActivitySource.AddActivityListener(activityListener);

		await using var engine = Build(
			builder => builder
				.AddHandler("good", (_, _) => Task.CompletedTask)
				.AddHandler("bad", (_, _) => throw new InvalidOperationException("nope")),
			options => options.Queues = ["telemetry"]);

		var good = (await Store.EnqueueAsync(Job("good", "telemetry"))).JobId;
		var bad = (await Store.EnqueueAsync(new EnqueueRequest { Type = "bad", Queue = "telemetry", MaxAttempts = 1 })).JobId;

		await engine.StartAsync();
		await WaitForStateAsync(good, JobState.Succeeded);
		await WaitForStateAsync(bad, JobState.Dead);
		await engine.StopAsync();

		var ours = measurements.Where(m => Equals(m.Tags.GetValueOrDefault("queue"), "telemetry")).ToList();

		Assert.Equal(2, ours.Where(m => m.Instrument == JobEngineTelemetry.EnqueuedMetric).Sum(m => m.Value));
		Assert.Equal(1, ours.Where(m => m.Instrument == JobEngineTelemetry.ProcessedMetric && Equals(m.Tags["outcome"], "succeeded")).Sum(m => m.Value));
		Assert.Equal(1, ours.Where(m => m.Instrument == JobEngineTelemetry.ProcessedMetric && Equals(m.Tags["outcome"], "dead")).Sum(m => m.Value));
		Assert.Equal(2, ours.Count(m => m.Instrument == JobEngineTelemetry.DurationMetric));

		var goodSpan = Assert.Single(spans, span => Equals(span.GetTagItem("jobengine.job.id")?.ToString(), good.ToString()));
		var badSpan = Assert.Single(spans, span => Equals(span.GetTagItem("jobengine.job.id")?.ToString(), bad.ToString()));

		Assert.Equal(ActivityStatusCode.Unset, goodSpan.Status);
		Assert.Equal(ActivityStatusCode.Error, badSpan.Status);
		Assert.Equal("succeeded", goodSpan.GetTagItem("jobengine.job.outcome"));
		Assert.Equal("dead", badSpan.GetTagItem("jobengine.job.outcome"));
	}
}
