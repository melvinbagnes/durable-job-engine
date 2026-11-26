using System.Text.Json;
using JobEngine.Scheduling;

namespace JobEngine.Runtime;

public interface IJobHandler
{
	Task HandleAsync(JobContext context, CancellationToken cancellationToken);
}

/// <summary>Throw to skip the remaining retries and send the job straight to the dead-letter state.</summary>
public sealed class PermanentFailureException(string message, Exception? innerException = null)
	: Exception(message, innerException);

public sealed record JobContext(
	Guid JobId,
	string Type,
	string Queue,
	string PayloadJson,
	int Attempt,
	int MaxAttempts,
	IServiceProvider Services)
{
	public T? GetPayload<T>() => JsonSerializer.Deserialize<T>(PayloadJson, JsonSerializerOptions.Web);
}

internal sealed class DelegateJobHandler(Func<JobContext, CancellationToken, Task> handler) : IJobHandler
{
	public Task HandleAsync(JobContext context, CancellationToken cancellationToken) => handler(context, cancellationToken);
}

public sealed class JobHandlerRegistry
{
	private readonly Dictionary<string, Func<IServiceProvider, IJobHandler>> _handlers = new(StringComparer.Ordinal);
	private readonly List<RecurringJobDefinition> _recurring = [];

	public IReadOnlyList<RecurringJobDefinition> Recurring => _recurring;

	public void Add(string type, Func<IServiceProvider, IJobHandler> factory)
	{
		ArgumentException.ThrowIfNullOrWhiteSpace(type);
		ArgumentNullException.ThrowIfNull(factory);

		if (!_handlers.TryAdd(type, factory))
		{
			throw new InvalidOperationException($"A handler for job type '{type}' is already registered.");
		}
	}

	public void AddRecurring(RecurringJobDefinition definition)
	{
		ArgumentNullException.ThrowIfNull(definition);

		// Fail at startup, not at the first tick, if the expression is wrong.
		CronExpression.Parse(definition.Cron);
		TimeZones.Resolve(definition.TimeZone);

		if (_recurring.Any(r => r.Name == definition.Name))
		{
			throw new InvalidOperationException($"A recurring job named '{definition.Name}' is already registered.");
		}

		_recurring.Add(definition);
	}

	public bool TryGet(string type, out Func<IServiceProvider, IJobHandler> factory) =>
		_handlers.TryGetValue(type, out factory!);
}
