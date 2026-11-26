using System.Collections.Concurrent;
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

	protected override TimeProvider CreateTimeProvider() => TimeProvider.System;

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
}
