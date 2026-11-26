using JobEngine.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobEngine.Runtime;

/// <summary>Creates migrations/recurring schedules at startup so workers never race the DDL.</summary>
public sealed class JobEngineMigrator(JobStore store, JobHandlerRegistry registry, JobEngineOptions options) : IHostedService
{
	public async Task StartAsync(CancellationToken cancellationToken)
	{
		if (options.AutoMigrate)
		{
			await store.MigrateAsync(cancellationToken);
		}

		if (options.EnableScheduler)
		{
			foreach (var definition in registry.Recurring)
			{
				await store.UpsertScheduleAsync(definition, cancellationToken);
			}
		}
	}

	public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

/// <summary>
/// Maintenance loop: fires due cron schedules, dead-letters jobs whose final lease expired, and optionally
/// purges old finished jobs. Every step is safe to run in several processes at once.
/// </summary>
public sealed class SchedulerService(JobStore store, JobEngineOptions options, ILogger<SchedulerService> logger) : BackgroundService
{
	private static readonly TimeSpan PurgeEvery = TimeSpan.FromMinutes(1);

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		if (!options.EnableScheduler)
		{
			return;
		}

		var lastPurge = DateTime.MinValue;

		while (!stoppingToken.IsCancellationRequested)
		{
			try
			{
				var fired = await store.FireDueSchedulesAsync(ct: stoppingToken);
				var reaped = await store.ReapExhaustedAsync(stoppingToken);

				if (fired > 0 || reaped > 0)
				{
					logger.LogInformation("Scheduler tick: fired {Fired} schedule(s), dead-lettered {Reaped} exhausted job(s).", fired, reaped);
				}

				if (options.CompletedRetention is { } retention && DateTime.UtcNow - lastPurge >= PurgeEvery)
				{
					var purged = await store.PurgeAsync(retention, stoppingToken);
					lastPurge = DateTime.UtcNow;

					if (purged > 0)
					{
						logger.LogInformation("Purged {Purged} finished job(s).", purged);
					}
				}
			}
			catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
			{
				return;
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Scheduler tick failed.");
			}

			try
			{
				await Task.Delay(options.SchedulerInterval, stoppingToken);
			}
			catch (OperationCanceledException)
			{
				return;
			}
		}
	}
}
