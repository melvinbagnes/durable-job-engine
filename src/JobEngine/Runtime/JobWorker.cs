using JobEngine.Jobs;
using JobEngine.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace JobEngine.Runtime;

/// <summary>
/// Claim loop. At most <see cref="JobEngineOptions.Concurrency"/> jobs run at once; the loop only claims as
/// many jobs as it has free slots, so a busy worker never hoards leases it cannot service.
/// </summary>
public sealed class JobWorker(
	JobStore store,
	JobProcessor processor,
	WakeSignal wakeSignal,
	JobEngineOptions options,
	ILogger<JobWorker> logger) : BackgroundService
{
	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		if (!options.EnableWorker)
		{
			return;
		}

		using var slots = new SemaphoreSlim(options.Concurrency, options.Concurrency);

		logger.LogInformation(
			"Worker {WorkerId} started: queues [{Queues}], concurrency {Concurrency}.",
			options.WorkerId,
			string.Join(", ", options.Queues),
			options.Concurrency);

		try
		{
			while (!stoppingToken.IsCancellationRequested)
			{
				await slots.WaitAsync(stoppingToken);

				IReadOnlyList<ClaimedJob> claimed;

				try
				{
					claimed = await TryClaimAsync(slots.CurrentCount + 1, stoppingToken);
				}
				catch
				{
					slots.Release();

					throw;
				}

				if (claimed.Count == 0)
				{
					slots.Release();
					await wakeSignal.WaitAsync(options.PollInterval, stoppingToken);

					continue;
				}

				// One slot is already held from the wait above; take one more per extra job.
				for (var extra = 1; extra < claimed.Count; extra++)
				{
					await slots.WaitAsync(CancellationToken.None);
				}

				foreach (var job in claimed)
				{
					Start(job, slots, stoppingToken);
				}
			}
		}
		catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
		{
			// Normal shutdown.
		}

		// Every running job holds a slot until it has recorded its outcome, so taking all slots back
		// means everything has finished (handlers see the cancellation and release their jobs).
		for (var slot = 0; slot < options.Concurrency; slot++)
		{
			await slots.WaitAsync(CancellationToken.None);
		}

		logger.LogInformation("Worker {WorkerId} stopped.", options.WorkerId);
	}

	private async Task<IReadOnlyList<ClaimedJob>> TryClaimAsync(int free, CancellationToken ct)
	{
		try
		{
			return await store.ClaimAsync(options.WorkerId, options.Queues.ToArray(), free, options.LeaseDuration, ct);
		}
		catch (OperationCanceledException) when (ct.IsCancellationRequested)
		{
			throw;
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Claiming jobs failed; backing off.");

			await Task.Delay(options.PollInterval, ct);

			return [];
		}
	}

	private void Start(ClaimedJob job, SemaphoreSlim slots, CancellationToken stoppingToken)
	{
		_ = Task.Run(async () =>
		{
			try
			{
				await processor.ProcessAsync(job, stoppingToken);
			}
			catch (Exception ex)
			{
				logger.LogError(ex, "Unexpected error while processing job {JobId}.", job.Id);
			}
			finally
			{
				slots.Release();
			}
		});
	}
}
