using JobEngine.Jobs;
using Xunit;

namespace JobEngine.Tests;

/// <summary>A store built without a TimeProvider takes every timestamp from the database server.</summary>
public class DatabaseClockTests : PostgresTestBase
{
	protected override TimeProvider? CreateTimeProvider() => null;

	[Fact]
	public async Task Enqueue_StampsTheJobWithTheDatabaseClock()
	{
		var before = await ScalarAsync<DateTime>("SELECT clock_timestamp()");
		var id = (await Store.EnqueueAsync(Job())).JobId;
		var after = await ScalarAsync<DateTime>("SELECT clock_timestamp()");

		var createdAt = await ScalarAsync<DateTime>($"SELECT created_at FROM jobs WHERE id = '{id}'");

		Assert.InRange(createdAt, before, after);
	}

	[Fact]
	public async Task Claim_SetsTheLeaseRelativeToTheDatabaseClock()
	{
		await Store.EnqueueAsync(Job());

		var claimed = Assert.Single(await Store.ClaimAsync("w1", ["default"], 1, TimeSpan.FromSeconds(30)));

		var remaining = await ScalarAsync<double>(
			$"SELECT EXTRACT(EPOCH FROM (lease_expires_at - clock_timestamp()))::double precision FROM jobs WHERE id = '{claimed.Id}'");

		Assert.InRange(remaining, 28, 30.5);
	}

	[Fact]
	public async Task Delay_IsMeasuredFromTheDatabaseClock()
	{
		var id = (await Store.EnqueueAsync(new EnqueueRequest { Type = "later", Delay = TimeSpan.FromHours(1) })).JobId;

		var wait = await ScalarAsync<double>(
			$"SELECT EXTRACT(EPOCH FROM (run_at - clock_timestamp()))::double precision FROM jobs WHERE id = '{id}'");

		Assert.InRange(wait, 3590, 3600.5);
		Assert.Empty(await Store.ClaimAsync("w1", ["default"], 5, TimeSpan.FromSeconds(30)));
	}
}
