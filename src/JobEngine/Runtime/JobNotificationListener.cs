using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace JobEngine.Runtime;

/// <summary>
/// A latch that remembers a wake-up until someone waits for it, so a notification that arrives between
/// "claim found nothing" and "start waiting" is not lost.
/// </summary>
public sealed class WakeSignal
{
	private TaskCompletionSource _signal = Create();

	private static TaskCompletionSource Create() => new(TaskCreationOptions.RunContinuationsAsynchronously);

	public void Set() => Volatile.Read(ref _signal).TrySetResult();

	/// <summary>Returns when signaled or when <paramref name="timeout"/> elapses, whichever is first.</summary>
	public async Task WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
	{
		var current = Volatile.Read(ref _signal);

		try
		{
			await current.Task.WaitAsync(timeout, cancellationToken);
		}
		catch (TimeoutException)
		{
			return;
		}

		Interlocked.CompareExchange(ref _signal, Create(), current);
	}
}

/// <summary>
/// Holds one dedicated connection in LISTEN mode and wakes the claim loop when a job becomes runnable.
/// If the connection drops it reconnects and wakes the worker once (to catch anything missed); polling keeps
/// the system correct in the meantime.
/// </summary>
public sealed class JobNotificationListener(JobEngineOptions options, WakeSignal signal, ILogger<JobNotificationListener> logger)
	: BackgroundService
{
	private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(2);

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		if (!options.EnableWorker || !options.UseListenNotify)
		{
			return;
		}

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				await using var connection = new NpgsqlConnection(options.ConnectionString);
				await connection.OpenAsync(stoppingToken);

				connection.Notification += (_, _) => signal.Set();

				await using (var listen = new NpgsqlCommand($"LISTEN \"{options.NotificationChannel}\";", connection))
				{
					await listen.ExecuteNonQueryAsync(stoppingToken);
				}

				logger.LogInformation("Listening on channel {Channel}.", options.NotificationChannel);

				// Jobs may have been enqueued while we were not listening.
				signal.Set();

				while (!stoppingToken.IsCancellationRequested)
				{
					await connection.WaitAsync(stoppingToken);
				}
			}
			catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
			{
				return;
			}
			catch (Exception ex)
			{
				logger.LogWarning(ex, "The notification listener lost its connection; polling continues. Reconnecting.");

				try
				{
					await Task.Delay(ReconnectDelay, stoppingToken);
				}
				catch (OperationCanceledException)
				{
					return;
				}
			}
		}
	}
}
