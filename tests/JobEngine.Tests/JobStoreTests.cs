using System.Collections.Concurrent;
using JobEngine.Jobs;
using JobEngine.Scheduling;
using JobEngine.Storage;
using JobEngine.Workflows;
using Xunit;

namespace JobEngine.Tests;

public class JobStoreTests : PostgresTestBase
{
	private static readonly string[] DefaultQueue = ["default"];

	private Task<IReadOnlyList<ClaimedJob>> ClaimAsync(int max = 10, string queue = "default") =>
		Store.ClaimAsync("test-worker", [queue], max, TimeSpan.FromSeconds(30));

	// ------------------------------------------------------------ basics

	[Fact]
	public async Task Enqueue_ThenClaim_LeasesTheJobExactlyOnce()
	{
		var result = await Store.EnqueueAsync(new EnqueueRequest { Type = "email.send", PayloadJson = """{"to":"a@b.c"}""" });

		Assert.True(result.Created);

		var claimed = Assert.Single(await ClaimAsync());

		Assert.Equal(result.JobId, claimed.Id);
		Assert.Equal("email.send", claimed.Type);
		Assert.Equal(1, claimed.Attempts);
		Assert.Equal(3, claimed.MaxAttempts);
		Assert.Contains("a@b.c", claimed.PayloadJson);
		Assert.Equal(JobState.Running, (await GetJobAsync(result.JobId)).State);
		Assert.Empty(await ClaimAsync());
	}

	[Fact]
	public async Task DelayedJob_IsNotClaimableUntilItsTime()
	{
		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "later", Delay = TimeSpan.FromMinutes(5) })).JobId;

		Assert.Empty(await ClaimAsync());

		Clock.Advance(TimeSpan.FromMinutes(5));

		Assert.Equal(id, Assert.Single(await ClaimAsync()).Id);
	}

	[Fact]
	public async Task Claim_PrefersHigherPriority_ThenOldestFirst()
	{
		await Store.EnqueueAsync(new EnqueueRequest { Type = "low-old" });
		Clock.Advance(TimeSpan.FromSeconds(1));
		await Store.EnqueueAsync(new EnqueueRequest { Type = "high", Priority = 10 });
		Clock.Advance(TimeSpan.FromSeconds(1));
		await Store.EnqueueAsync(new EnqueueRequest { Type = "low-new" });
		Clock.Advance(TimeSpan.FromSeconds(1));

		var order = new List<string>
		{
			Assert.Single(await ClaimAsync(max: 1)).Type,
			Assert.Single(await ClaimAsync(max: 1)).Type,
			Assert.Single(await ClaimAsync(max: 1)).Type
		};

		Assert.Equal(new[] { "high", "low-old", "low-new" }, order);
	}

	[Fact]
	public async Task Claim_OnlyReturnsJobsFromTheRequestedQueues()
	{
		var emails = (await Store.EnqueueAsync(Job("a", queue: "emails"))).JobId;
		await Store.EnqueueAsync(Job("b", queue: "reports"));

		Assert.Equal(emails, Assert.Single(await ClaimAsync(queue: "emails")).Id);
		Assert.Empty(await ClaimAsync(queue: "emails"));
	}

	[Fact]
	public async Task Enqueue_RejectsInvalidRequests()
	{
		await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
			() => Store.EnqueueAsync(new EnqueueRequest { Type = "x", MaxAttempts = 0 }));
		await Assert.ThrowsAsync<ArgumentException>(
			() => Store.EnqueueAsync(new EnqueueRequest { Type = "" }));
	}

	[Fact]
	public async Task Stats_CountJobsByState()
	{
		for (var i = 0; i < 3; i++)
		{
			await Store.EnqueueAsync(Job());
		}

		await ClaimAsync(max: 1);

		var stats = await Store.GetStatsAsync();

		Assert.Equal(2, stats.Count(JobState.Queued));
		Assert.Equal(1, stats.Count(JobState.Running));
		Assert.Equal(0, stats.Count(JobState.Dead));
	}

	// ------------------------------------------------------------ concurrency

	[Fact]
	public async Task ConcurrentWorkers_NeverClaimTheSameJob()
	{
		const int jobCount = 200;

		for (var i = 0; i < jobCount; i++)
		{
			await Store.EnqueueAsync(Job());
		}

		var claimedIds = new ConcurrentBag<Guid>();

		var workers = Enumerable.Range(0, 8).Select(worker => Task.Run(async () =>
		{
			while (true)
			{
				var batch = await Store.ClaimAsync($"w{worker}", DefaultQueue, 7, TimeSpan.FromMinutes(1));

				if (batch.Count == 0)
				{
					return;
				}

				foreach (var job in batch)
				{
					claimedIds.Add(job.Id);
				}
			}
		}));

		await Task.WhenAll(workers);

		Assert.Equal(jobCount, claimedIds.Count);
		Assert.Equal(jobCount, claimedIds.Distinct().Count());
		Assert.Equal(jobCount, (await Store.GetStatsAsync()).Count(JobState.Running));
	}

	// ------------------------------------------------------------ idempotency

	[Fact]
	public async Task SameIdempotencyKey_ReturnsTheOriginalJob()
	{
		var first = await Store.EnqueueAsync(new EnqueueRequest { Type = "charge", PayloadJson = """{"n":1}""", IdempotencyKey = "order-42" });
		var second = await Store.EnqueueAsync(new EnqueueRequest { Type = "charge", PayloadJson = """{"n":2}""", IdempotencyKey = "order-42" });

		Assert.True(first.Created);
		Assert.False(second.Created);
		Assert.Equal(first.JobId, second.JobId);
		Assert.DoesNotContain("2", (await GetJobAsync(first.JobId)).PayloadJson);
		Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
	}

	[Fact]
	public async Task ConcurrentEnqueuesWithTheSameKey_CreateExactlyOneJob()
	{
		var results = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ =>
			Store.EnqueueAsync(new EnqueueRequest { Type = "once", IdempotencyKey = "same-key" })));

		Assert.Single(results, r => r.Created);
		Assert.Single(results.Select(r => r.JobId).Distinct());
		Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
	}

	// ------------------------------------------------------------ outbox

	[Fact]
	public async Task EnqueueInsideACallersTransaction_RollsBackWithIt()
	{
		Guid rolledBack;
		Guid committed;

		await using (var connection = await DataSource.OpenConnectionAsync())
		{
			await using (var transaction = await connection.BeginTransactionAsync())
			{
				rolledBack = (await Store.EnqueueAsync(transaction, Job("rolled-back"))).JobId;
				await transaction.RollbackAsync();
			}

			await using (var transaction = await connection.BeginTransactionAsync())
			{
				committed = (await Store.EnqueueAsync(transaction, Job("committed"))).JobId;
				await transaction.CommitAsync();
			}
		}

		Assert.Null(await Store.GetAsync(rolledBack));
		Assert.Equal(JobState.Queued, (await GetJobAsync(committed)).State);
		Assert.Equal(committed, Assert.Single(await ClaimAsync()).Id);
	}

	// ------------------------------------------------------------ retries, backoff, dead letter

	[Fact]
	public async Task FailedJob_IsRetriedWithBackoff_ThenDeadLettered()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;

		var first = Assert.Single(await ClaimAsync());
		var outcome = await Store.FailAsync(id, first.LeaseToken, "boom 1", permanent: false, TimeSpan.FromSeconds(10));

		Assert.NotNull(outcome);
		Assert.Equal(JobState.Queued, outcome.State);
		Assert.Equal(Clock.GetUtcNow().UtcDateTime.AddSeconds(10), outcome.RunAt);
		Assert.Empty(await ClaimAsync());

		Clock.Advance(TimeSpan.FromSeconds(9));
		Assert.Empty(await ClaimAsync());

		Clock.Advance(TimeSpan.FromSeconds(1));
		var second = Assert.Single(await ClaimAsync());
		Assert.Equal(2, second.Attempts);

		await Store.FailAsync(id, second.LeaseToken, "boom 2", permanent: false, TimeSpan.FromSeconds(20));
		Clock.Advance(TimeSpan.FromSeconds(20));

		var third = Assert.Single(await ClaimAsync());
		Assert.Equal(3, third.Attempts);

		var last = await Store.FailAsync(id, third.LeaseToken, "boom 3", permanent: false, TimeSpan.FromSeconds(40));
		var job = await GetJobAsync(id);

		Assert.Equal(JobState.Dead, last!.State);
		Assert.Equal(JobState.Dead, job.State);
		Assert.Equal("boom 3", job.LastError);
		Assert.NotNull(job.CompletedAt);

		Clock.Advance(TimeSpan.FromDays(1));
		Assert.Empty(await ClaimAsync());
	}

	[Fact]
	public async Task PermanentFailure_SkipsRemainingRetries()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var claimed = Assert.Single(await ClaimAsync());

		var outcome = await Store.FailAsync(id, claimed.LeaseToken, "bad input", permanent: true, TimeSpan.Zero);

		Assert.Equal(JobState.Dead, outcome!.State);
		Assert.Equal(1, (await GetJobAsync(id)).Attempts);
	}

	[Fact]
	public async Task DeadJob_CanBeRequeuedWithAFreshAttemptBudget()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var claimed = Assert.Single(await ClaimAsync());
		await Store.FailAsync(id, claimed.LeaseToken, "bad input", permanent: true, TimeSpan.Zero);

		Assert.True(await Store.RequeueDeadAsync(id));
		Assert.False(await Store.RequeueDeadAsync(id));

		var job = await GetJobAsync(id);

		Assert.Equal(JobState.Queued, job.State);
		Assert.Equal(0, job.Attempts);
		Assert.Null(job.LastError);
		Assert.Equal(id, Assert.Single(await ClaimAsync()).Id);
	}

	// ------------------------------------------------------------ leases and fencing

	[Fact]
	public async Task ExpiredLease_IsReclaimed_AndTheStaleHolderIsFencedOut()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var stale = Assert.Single(await ClaimAsync());

		Clock.Advance(TimeSpan.FromSeconds(31));

		var fresh = Assert.Single(await ClaimAsync());

		Assert.Equal(id, fresh.Id);
		Assert.Equal(2, fresh.Attempts);
		Assert.NotEqual(stale.LeaseToken, fresh.LeaseToken);
		Assert.Contains("Lease expired", (await GetJobAsync(id)).LastError);

		Assert.False(await Store.ExtendLeaseAsync(id, stale.LeaseToken, TimeSpan.FromSeconds(30)));
		Assert.False(await Store.CompleteAsync(id, stale.LeaseToken));
		Assert.Null(await Store.FailAsync(id, stale.LeaseToken, "late", permanent: false, TimeSpan.Zero));
		Assert.Equal(JobState.Running, (await GetJobAsync(id)).State);

		Assert.True(await Store.CompleteAsync(id, fresh.LeaseToken));
		Assert.Equal(JobState.Succeeded, (await GetJobAsync(id)).State);
	}

	[Fact]
	public async Task Heartbeat_KeepsALongRunningJobFromBeingReclaimed()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var claimed = Assert.Single(await ClaimAsync());

		Clock.Advance(TimeSpan.FromSeconds(25));
		Assert.True(await Store.ExtendLeaseAsync(id, claimed.LeaseToken, TimeSpan.FromSeconds(30)));

		Clock.Advance(TimeSpan.FromSeconds(20));

		Assert.Empty(await ClaimAsync());
		Assert.True(await Store.CompleteAsync(id, claimed.LeaseToken));
	}

	[Fact]
	public async Task LeaseExpiringOnTheFinalAttempt_IsDeadLetteredByTheReaper()
	{
		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "crashy", MaxAttempts = 1 })).JobId;
		Assert.Single(await ClaimAsync());

		Clock.Advance(TimeSpan.FromSeconds(31));

		Assert.Empty(await ClaimAsync());
		Assert.Equal(1, await Store.ReapExhaustedAsync());
		Assert.Equal(0, await Store.ReapExhaustedAsync());

		var job = await GetJobAsync(id);

		Assert.Equal(JobState.Dead, job.State);
		Assert.Contains("final attempt", job.LastError);
	}

	[Fact]
	public async Task Release_GivesTheJobBackWithoutSpendingAnAttempt()
	{
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var claimed = Assert.Single(await ClaimAsync());

		Assert.True(await Store.ReleaseAsync(id, claimed.LeaseToken));

		var job = await GetJobAsync(id);

		Assert.Equal(JobState.Queued, job.State);
		Assert.Equal(0, job.Attempts);
		Assert.Equal(1, Assert.Single(await ClaimAsync()).Attempts);
	}

	// ------------------------------------------------------------ workflows

	[Fact]
	public async Task Workflow_Diamond_RunsStepsInDependencyOrder()
	{
		var result = await Store.EnqueueWorkflowAsync(
		[
			Step("a").Build(),
			Step("b").After("a").Build(),
			Step("c").After("a").Build(),
			Step("d").After("b", "c").Build()
		]);

		var ids = result.JobIds;

		Assert.True(result.Created);
		Assert.Equal(JobState.Queued, (await GetJobAsync(ids["a"])).State);

		foreach (var key in new[] { "b", "c", "d" })
		{
			Assert.Equal(JobState.Waiting, (await GetJobAsync(ids[key])).State);
		}

		var a = Assert.Single(await ClaimAsync());
		Assert.Equal(ids["a"], a.Id);
		Assert.True(await Store.CompleteAsync(a.Id, a.LeaseToken));

		Assert.Equal(JobState.Queued, (await GetJobAsync(ids["b"])).State);
		Assert.Equal(JobState.Queued, (await GetJobAsync(ids["c"])).State);
		Assert.Equal(JobState.Waiting, (await GetJobAsync(ids["d"])).State);

		var middle = await ClaimAsync();
		Assert.Equal(2, middle.Count);

		var b = middle.Single(j => j.Id == ids["b"]);
		var c = middle.Single(j => j.Id == ids["c"]);

		Assert.True(await Store.CompleteAsync(b.Id, b.LeaseToken));
		Assert.Equal(JobState.Waiting, (await GetJobAsync(ids["d"])).State);

		Assert.True(await Store.CompleteAsync(c.Id, c.LeaseToken));
		Assert.Equal(JobState.Queued, (await GetJobAsync(ids["d"])).State);
	}

	[Fact]
	public async Task Workflow_TwoParentsFinishingTogether_AlwaysReleaseTheChild()
	{
		// Regression guard for the classic race: both parents see the other as "still running" and nobody promotes the child.
		for (var round = 0; round < 25; round++)
		{
			var queue = $"race-{round}";
			var result = await Store.EnqueueWorkflowAsync(
			[
				Step("a").OnQueue(queue).Build(),
				Step("b").OnQueue(queue).Build(),
				Step("c").After("a", "b").OnQueue(queue).Build()
			]);

			var parents = await ClaimAsync(queue: queue);
			Assert.Equal(2, parents.Count);

			var completions = await Task.WhenAll(parents.Select(p => Store.CompleteAsync(p.Id, p.LeaseToken)));

			Assert.All(completions, completed => Assert.True(completed));
			Assert.Equal(JobState.Queued, (await GetJobAsync(result.JobIds["c"])).State);
		}
	}

	[Fact]
	public async Task Workflow_PermanentFailure_CancelsDownstreamSteps_AndReplayRevivesThem()
	{
		var result = await Store.EnqueueWorkflowAsync(
		[
			Step("a").Build(),
			Step("b").After("a").Build(),
			Step("c").After("b").Build()
		]);

		var ids = result.JobIds;
		var a = Assert.Single(await ClaimAsync());

		await Store.FailAsync(a.Id, a.LeaseToken, "boom", permanent: true, TimeSpan.Zero);

		var b = await GetJobAsync(ids["b"]);
		var c = await GetJobAsync(ids["c"]);

		Assert.Equal(JobState.Canceled, b.State);
		Assert.Equal(JobState.Canceled, c.State);
		Assert.Contains("upstream", b.LastError);
		Assert.Empty(await ClaimAsync());

		Assert.True(await Store.RequeueDeadAsync(a.Id));

		Assert.Equal(JobState.Waiting, (await GetJobAsync(ids["b"])).State);
		Assert.Equal(JobState.Waiting, (await GetJobAsync(ids["c"])).State);

		var replay = Assert.Single(await ClaimAsync());
		await Store.CompleteAsync(replay.Id, replay.LeaseToken);

		Assert.Equal(JobState.Queued, (await GetJobAsync(ids["b"])).State);
		Assert.Equal(JobState.Waiting, (await GetJobAsync(ids["c"])).State);
	}

	[Fact]
	public async Task Workflow_WithTheSameIdempotencyKey_IsOnlyCreatedOnce()
	{
		WorkflowStep[] Steps() => [Step("a").Build(), Step("b").After("a").Build()];

		var first = await Store.EnqueueWorkflowAsync(Steps(), idempotencyKey: "nightly-2026-01-01");
		var second = await Store.EnqueueWorkflowAsync(Steps(), idempotencyKey: "nightly-2026-01-01");

		Assert.True(first.Created);
		Assert.False(second.Created);
		Assert.Equal(first.JobIds["a"], second.JobIds["a"]);
		Assert.Equal(first.JobIds["b"], second.JobIds["b"]);
		Assert.Equal(2L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
		Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM job_dependencies"));
	}

	[Fact]
	public async Task Workflow_WithACycle_IsRejectedBeforeAnythingIsWritten()
	{
		await Assert.ThrowsAsync<WorkflowValidationException>(() => Store.EnqueueWorkflowAsync(
		[
			Step("a").After("b").Build(),
			Step("b").After("a").Build()
		]));

		Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
	}

	// ------------------------------------------------------------ recurring schedules

	[Fact]
	public async Task Schedule_FiresOncePerSlot_EvenWhenManyProcessesTick()
	{
		Clock.Advance(TimeSpan.FromSeconds(30)); // 12:00:30
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("minutely", "* * * * *", "tick"));

		Assert.Equal(0, await Store.FireDueSchedulesAsync());

		Clock.Advance(TimeSpan.FromSeconds(35)); // 12:01:05
		Assert.Equal(1, await Store.FireDueSchedulesAsync());
		Assert.Equal(0, await Store.FireDueSchedulesAsync());

		Clock.Advance(TimeSpan.FromSeconds(55)); // 12:02:00
		var fired = await Task.WhenAll(Store.FireDueSchedulesAsync(), Store.FireDueSchedulesAsync());

		Assert.Equal(1, fired.Sum());
		Assert.Equal(2, (await Store.GetStatsAsync()).Count(JobState.Queued));
		Assert.Equal(
			new DateTime(2026, 1, 1, 12, 3, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'minutely'"));
	}

	[Fact]
	public async Task Schedule_MissedSlotsFireOnce_NotOncePerMissedSlot()
	{
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("minutely", "* * * * *", "tick"));

		Clock.Advance(TimeSpan.FromHours(3)); // the scheduler was down

		Assert.Equal(1, await Store.FireDueSchedulesAsync());
		Assert.Equal(0, await Store.FireDueSchedulesAsync());
		Assert.Equal(1, (await Store.GetStatsAsync()).Count(JobState.Queued));
	}

	[Fact]
	public async Task Schedule_Redeploy_KeepsTheNextSlot_UnlessTheCronChanged()
	{
		Clock.Advance(TimeSpan.FromSeconds(30)); // 12:00:30
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("report", "*/10 * * * *", "report"));

		Clock.Advance(TimeSpan.FromMinutes(11)); // 12:11:30, slot 12:10 still pending
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("report", "*/10 * * * *", "report"));

		Assert.Equal(
			new DateTime(2026, 1, 1, 12, 10, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'report'"));

		await Store.UpsertScheduleAsync(new RecurringJobDefinition("report", "*/5 * * * *", "report"));

		Assert.Equal(
			new DateTime(2026, 1, 1, 12, 15, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'report'"));
	}

	[Fact]
	public async Task Schedule_RejectsInvalidCron()
	{
		await Assert.ThrowsAsync<FormatException>(
			() => Store.UpsertScheduleAsync(new RecurringJobDefinition("bad", "not a cron", "tick")));
	}

	// ------------------------------------------------------------ retention

	[Fact]
	public async Task Purge_RemovesOldFinishedJobs_AndReleasesTheirIdempotencyKeys()
	{
		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "done", IdempotencyKey = "k" })).JobId;
		var claimed = Assert.Single(await ClaimAsync());
		await Store.CompleteAsync(id, claimed.LeaseToken);

		Assert.Equal(0, await Store.PurgeAsync(TimeSpan.FromHours(1)));

		Clock.Advance(TimeSpan.FromHours(2));

		Assert.Equal(1, await Store.PurgeAsync(TimeSpan.FromHours(1)));
		Assert.Null(await Store.GetAsync(id));
		Assert.True((await Store.EnqueueAsync(new EnqueueRequest { Type = "done", IdempotencyKey = "k" })).Created);
	}

	[Fact]
	public async Task Purge_KeepsFinishedJobsThatUnfinishedStepsStillDependOn()
	{
		var result = await Store.EnqueueWorkflowAsync([Step("a").Build(), Step("b").After("a").Build()]);
		var a = Assert.Single(await ClaimAsync());
		await Store.CompleteAsync(a.Id, a.LeaseToken);

		Clock.Advance(TimeSpan.FromHours(2));

		// b is queued (not finished) and depends on a, so a must stay.
		Assert.Equal(0, await Store.PurgeAsync(TimeSpan.FromHours(1)));
		Assert.NotNull(await Store.GetAsync(result.JobIds["a"]));
	}

	[Fact]
	public async Task Schedule_WithATimeZone_FiresAtLocalWallClockTime()
	{
		// Clock: 2026-01-01 12:00 UTC = 20:00 in Manila, so the next 09:00 Manila is 2026-01-02 01:00 UTC.
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("morning", "0 9 * * *", "digest", TimeZone: "Asia/Manila"));

		Assert.Equal(
			new DateTime(2026, 1, 2, 1, 0, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'morning'"));

		Clock.Advance(TimeSpan.FromHours(13).Add(TimeSpan.FromMinutes(5))); // 2026-01-02 01:05 UTC

		Assert.Equal(1, await Store.FireDueSchedulesAsync());
		Assert.Equal(
			new DateTime(2026, 1, 3, 1, 0, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'morning'"));
	}

	[Fact]
	public async Task Schedule_ChangingOnlyTheTimeZone_RecomputesTheNextSlot()
	{
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("morning", "0 9 * * *", "digest"));

		Assert.Equal(
			new DateTime(2026, 1, 2, 9, 0, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'morning'"));

		await Store.UpsertScheduleAsync(new RecurringJobDefinition("morning", "0 9 * * *", "digest", TimeZone: "Asia/Manila"));

		Assert.Equal(
			new DateTime(2026, 1, 2, 1, 0, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'morning'"));
		Assert.Equal("Asia/Manila", await ScalarAsync<string>("SELECT time_zone FROM schedules WHERE name = 'morning'"));
	}

	[Fact]
	public async Task Schedule_Redeploy_WithTheSameTimeZone_KeepsTheNextSlot()
	{
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("morning", "0 9 * * *", "digest", TimeZone: "Asia/Manila"));
		Clock.Advance(TimeSpan.FromHours(20)); // 08:00 UTC on Jan 2: the 01:00 slot is overdue but has not fired
		await Store.UpsertScheduleAsync(new RecurringJobDefinition("morning", "0 9 * * *", "digest", TimeZone: "Asia/Manila"));

		Assert.Equal(
			new DateTime(2026, 1, 2, 1, 0, 0, DateTimeKind.Utc),
			await ScalarAsync<DateTime>("SELECT next_run_at FROM schedules WHERE name = 'morning'"));
	}

	[Fact]
	public async Task Schedule_RejectsAnUnknownTimeZone()
	{
		await Assert.ThrowsAsync<TimeZoneNotFoundException>(() => Store.UpsertScheduleAsync(
			new RecurringJobDefinition("bad", "0 9 * * *", "digest", TimeZone: "Mars/Olympus_Mons")));
	}

	// ------------------------------------------------------------ notifications

	[Fact]
	public void Options_RejectAnUnsafeNotificationChannelName()
	{
		var options = new JobEngineOptions
		{
			ConnectionString = Options.ConnectionString,
			NotificationChannel = "x\"; DROP TABLE jobs; --"
		};

		Assert.Throws<ArgumentException>(() => JobStore.Create(options));
	}

	// ------------------------------------------------------------ archive

	[Fact]
	public async Task Archive_MovesOldSucceededJobs_ButKeepsThemReadable()
	{
		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "done", IdempotencyKey = "once" })).JobId;
		var claimed = Assert.Single(await ClaimAsync());
		await Store.CompleteAsync(claimed.Id, claimed.LeaseToken);

		Clock.Advance(TimeSpan.FromMinutes(30));
		Assert.Equal(0, await Store.ArchiveAsync(TimeSpan.FromHours(1)));

		Clock.Advance(TimeSpan.FromHours(1));
		Assert.Equal(1, await Store.ArchiveAsync(TimeSpan.FromHours(1)));

		Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
		Assert.Equal(1L, await ScalarAsync<long>("SELECT count(*) FROM job_archive"));
		Assert.Equal(JobState.Succeeded, (await GetJobAsync(id)).State);

		// The key was released with the hot row, as documented.
		Assert.True((await Store.EnqueueAsync(new EnqueueRequest { Type = "done", IdempotencyKey = "once" })).Created);
	}

	[Fact]
	public async Task Archive_LeavesDeadAndUnfinishedJobsAlone()
	{
		await Store.EnqueueAsync(new EnqueueRequest { Type = "dead", MaxAttempts = 1 });
		var claimed = Assert.Single(await ClaimAsync());
		await Store.FailAsync(claimed.Id, claimed.LeaseToken, "boom", permanent: true, retryDelay: TimeSpan.Zero);
		await Store.EnqueueAsync(Job("waiting-in-line"));

		Clock.Advance(TimeSpan.FromDays(2));

		Assert.Equal(0, await Store.ArchiveAsync(TimeSpan.FromHours(1)));
		Assert.Equal(2L, await ScalarAsync<long>("SELECT count(*) FROM jobs"));
	}

	[Fact]
	public async Task Archive_RespectsTheBatchSize_AndPurgeArchiveDeletesOldRows()
	{
		for (var i = 0; i < 5; i++)
		{
			await Store.EnqueueAsync(Job("done"));
		}

		foreach (var job in await ClaimAsync())
		{
			await Store.CompleteAsync(job.Id, job.LeaseToken);
		}

		Clock.Advance(TimeSpan.FromHours(2));

		Assert.Equal(3, await Store.ArchiveAsync(TimeSpan.FromHours(1), batchSize: 3));
		Assert.Equal(2, await Store.ArchiveAsync(TimeSpan.FromHours(1), batchSize: 3));
		Assert.Equal(0, await Store.ArchiveAsync(TimeSpan.FromHours(1), batchSize: 3));

		Clock.Advance(TimeSpan.FromDays(10));

		Assert.Equal(5, await Store.PurgeArchiveAsync(TimeSpan.FromDays(7)));
		Assert.Equal(0L, await ScalarAsync<long>("SELECT count(*) FROM job_archive"));
	}

	// ------------------------------------------------------------ per-queue limits

	private Task<IReadOnlyList<ClaimedJob>> ClaimLimitedAsync(QueueLimit limit, int max = 10) =>
		Store.ClaimLimitedAsync("test-worker", "lim", limit, max, TimeSpan.FromSeconds(30));

	private async Task EnqueueManyAsync(int count, string queue = "lim")
	{
		for (var i = 0; i < count; i++)
		{
			await Store.EnqueueAsync(Job("limited", queue));
		}
	}

	[Fact]
	public async Task QueueLimit_MaxConcurrency_CapsRunningJobs_AndFreesRoomWhenOneFinishes()
	{
		await EnqueueManyAsync(5);
		var limit = new QueueLimit(MaxConcurrency: 2);

		var first = await ClaimLimitedAsync(limit);

		Assert.Equal(2, first.Count);
		Assert.Empty(await ClaimLimitedAsync(limit));

		await Store.CompleteAsync(first[0].Id, first[0].LeaseToken);

		Assert.Single(await ClaimLimitedAsync(limit));
		Assert.Empty(await ClaimLimitedAsync(limit));
	}

	[Fact]
	public async Task QueueLimit_MaxConcurrency_DoesNotCountExpiredLeases()
	{
		await EnqueueManyAsync(4);
		var limit = new QueueLimit(MaxConcurrency: 2);

		Assert.Equal(2, (await ClaimLimitedAsync(limit)).Count);

		Clock.Advance(TimeSpan.FromSeconds(31)); // both leases lapsed: those workers are presumed dead

		Assert.Equal(2, (await ClaimLimitedAsync(limit)).Count);
	}

	[Fact]
	public async Task QueueLimit_MaxConcurrency_HoldsWhenManyWorkersClaimAtOnce()
	{
		await EnqueueManyAsync(30);
		var limit = new QueueLimit(MaxConcurrency: 3);

		var results = await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => ClaimLimitedAsync(limit, max: 5)));

		Assert.Equal(3, results.Sum(claimed => claimed.Count));
		Assert.Equal(3L, await ScalarAsync<long>("SELECT count(*) FROM jobs WHERE state = 'running'"));
	}

	[Fact]
	public async Task QueueLimit_MaxPerSecond_IsATokenBucketSharedByEveryCaller()
	{
		await EnqueueManyAsync(20);
		var limit = new QueueLimit(MaxPerSecond: 2);

		Assert.Equal(2, (await ClaimLimitedAsync(limit)).Count); // the initial burst
		Assert.Empty(await ClaimLimitedAsync(limit));

		Clock.Advance(TimeSpan.FromSeconds(1));
		Assert.Equal(2, (await ClaimLimitedAsync(limit)).Count);

		Clock.Advance(TimeSpan.FromMilliseconds(250)); // half a token
		Assert.Empty(await ClaimLimitedAsync(limit));

		Clock.Advance(TimeSpan.FromMilliseconds(250)); // a whole token has accumulated since the last claim
		Assert.Single(await ClaimLimitedAsync(limit));

		Clock.Advance(TimeSpan.FromMinutes(10)); // the bucket never holds more than its burst
		Assert.Equal(2, (await ClaimLimitedAsync(limit)).Count);
	}

	[Fact]
	public async Task QueueLimit_UnlimitedQueuesAreUnaffected()
	{
		await EnqueueManyAsync(5, queue: "free");

		Assert.Equal(5, (await Store.ClaimAsync("w", ["free"], 10, TimeSpan.FromSeconds(30))).Count);
	}
}
