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
		var lastArchive = DateTime.MinValue;

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

				if (DateTime.UtcNow - lastArchive >= PurgeEvery && (options.ArchiveAfter is not null || options.ArchiveRetention is not null))
				{
					lastArchive = DateTime.UtcNow;

					if (options.ArchiveAfter is { } archiveAfter)
					{
						var moved = 0;

						// A bounded number of batches per tick, so one tick never monopolises the database.
						for (var batch = 0; batch < 20; batch++)
						{
							var count = await store.ArchiveAsync(archiveAfter, ct: stoppingToken);
							moved += count;

							if (count == 0)
							{
								break;
							}
						}

						if (moved > 0)
						{
							logger.LogInformation("Archived {Moved} succeeded job(s).", moved);
						}
					}

					if (options.ArchiveRetention is { } archiveRetention)
					{
						await store.PurgeArchiveAsync(archiveRetention, stoppingToken);
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
