using JobEngine.Jobs;
using JobEngine.Scheduling;
using JobEngine.Storage;
using JobEngine.Workflows;
using Npgsql;
using Xunit;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace JobEngine.Tests;

public sealed class FakeClock(DateTimeOffset start) : TimeProvider
{
	private DateTimeOffset _now = start;

	public override DateTimeOffset GetUtcNow() => _now;

	public void Advance(TimeSpan by) => _now += by;
}

/// <summary>
/// Gives every test its own Postgres schema (via search_path), so tests are isolated and can run against
/// one shared server. Needs a reachable Postgres: JOBENGINE_TEST_CONNECTION, or localhost:5432 postgres/postgres.
/// </summary>
public abstract class PostgresTestBase : IAsyncLifetime
{
	private static readonly string AdminConnectionString =
		Environment.GetEnvironmentVariable("JOBENGINE_TEST_CONNECTION")
		?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=postgres";

	private readonly string _schema = "t_" + Guid.NewGuid().ToString("N");

	protected FakeClock Clock { get; } = new(new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));

	protected JobEngineOptions Options { get; private set; } = new();

	protected JobStore Store { get; private set; } = null!;

	protected NpgsqlDataSource DataSource { get; private set; } = null!;

	/// <summary>Store tests use a fake clock so leases and backoff can be fast-forwarded.</summary>
	protected virtual TimeProvider CreateTimeProvider() => Clock;

	public virtual async Task InitializeAsync()
	{
		await using (var admin = new NpgsqlConnection(AdminConnectionString))
		{
			await admin.OpenAsync();
			await using var create = new NpgsqlCommand($"CREATE SCHEMA \"{_schema}\";", admin);
			await create.ExecuteNonQueryAsync();
		}

		var connectionString = new NpgsqlConnectionStringBuilder(AdminConnectionString)
		{
			SearchPath = _schema,
			MaxPoolSize = 60
		}.ConnectionString;

		Options = new JobEngineOptions
		{
			ConnectionString = connectionString,
			DefaultMaxAttempts = 3
		};

		DataSource = NpgsqlDataSource.Create(connectionString);
		Store = JobStore.Create(Options, CreateTimeProvider());

		await Store.MigrateAsync();
	}

	public virtual async Task DisposeAsync()
	{
		await Store.DisposeAsync();
		await DataSource.DisposeAsync();

		await using var admin = new NpgsqlConnection(AdminConnectionString);
		await admin.OpenAsync();
		await using var drop = new NpgsqlCommand($"DROP SCHEMA IF EXISTS \"{_schema}\" CASCADE;", admin);
		await drop.ExecuteNonQueryAsync();
	}

	protected async Task<JobRecord> GetJobAsync(Guid id) =>
		await Store.GetAsync(id) ?? throw new InvalidOperationException($"Job {id} does not exist.");

	protected async Task<T> ScalarAsync<T>(string sql)
	{
		await using var command = DataSource.CreateCommand(sql);

		return (T)(await command.ExecuteScalarAsync())!;
	}

	protected static async Task WaitUntilAsync(Func<Task<bool>> condition, TimeSpan? timeout = null)
	{
		var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(30));

		while (DateTime.UtcNow < deadline)
		{
			if (await condition())
			{
				return;
			}

			await Task.Delay(50);
		}

		throw new TimeoutException("The condition was not met in time.");
	}

	protected static EnqueueRequest Job(string type = "test", string queue = "default") =>
		new() { Type = type, Queue = queue };

	protected static WorkflowStepBuilder Step(string key) => new(key);
}

public sealed class WorkflowStepBuilder(string key)
{
	private readonly List<string> _dependsOn = [];
	private string _queue = "default";

	public WorkflowStepBuilder After(params string[] keys)
	{
		_dependsOn.AddRange(keys);

		return this;
	}

	public WorkflowStepBuilder OnQueue(string queue)
	{
		_queue = queue;

		return this;
	}

	public WorkflowStep Build() => new()
	{
		Key = key,
		Type = key,
		Queue = _queue,
		DependsOn = _dependsOn
	};
}
