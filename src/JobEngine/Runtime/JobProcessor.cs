using System.Diagnostics;
using JobEngine.Jobs;
using JobEngine.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace JobEngine.Runtime;

/// <summary>Runs one claimed job: handler + lease heartbeat + fenced outcome write.</summary>
public sealed class JobProcessor(
	JobStore store,
	JobHandlerRegistry registry,
	IServiceProvider services,
	JobEngineOptions options,
	ILogger<JobProcessor> logger)
{
	private const int MaxErrorLength = 4000;

	public async Task ProcessAsync(ClaimedJob job, CancellationToken shutdown)
	{
		using var activity = JobEngineTelemetry.ActivitySource.StartActivity("job process", ActivityKind.Consumer);
		activity?.SetTag("jobengine.job.id", job.Id);
		activity?.SetTag("jobengine.job.type", job.Type);
		activity?.SetTag("jobengine.job.queue", job.Queue);
		activity?.SetTag("jobengine.job.attempt", job.Attempts);

		var started = Stopwatch.GetTimestamp();

		using var handlerCts = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
		using var timeoutCts = new CancellationTokenSource();
		using var heartbeatCts = new CancellationTokenSource();

		var timeout = job.TimeoutMs is { } ms ? TimeSpan.FromMilliseconds(ms) : options.DefaultJobTimeout;

		if (timeout is { } limit)
		{
			timeoutCts.CancelAfter(limit);
		}

		var lease = new LeaseState();
		var heartbeat = HeartbeatAsync(job, lease, handlerCts, heartbeatCts.Token);

		Exception? error = null;

		try
		{
			using var combined = CancellationTokenSource.CreateLinkedTokenSource(handlerCts.Token, timeoutCts.Token);

			await RunWithTimeoutAsync(job, timeout, combined.Token);
		}
		catch (Exception ex)
		{
			error = ex;
		}

		var timedOut = error is OperationCanceledException
			&& timeoutCts.IsCancellationRequested
			&& !shutdown.IsCancellationRequested
			&& !lease.Lost;

		if (timedOut)
		{
			error = new TimeoutException($"The job exceeded its timeout of {timeout}.");
		}

		await heartbeatCts.CancelAsync();
		await heartbeat;

		var outcome = await ReportOutcomeAsync(job, error, lease.Lost, shutdown.IsCancellationRequested, timedOut);

		var tags = new TagList
		{
			{ "queue", job.Queue },
			{ "type", job.Type },
			{ "outcome", outcome }
		};

		JobEngineTelemetry.Processed.Add(1, tags);
		JobEngineTelemetry.Duration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, tags);

		activity?.SetTag("jobengine.job.outcome", outcome);

		if (error is not null && outcome is not "released")
		{
			activity?.SetStatus(ActivityStatusCode.Error, error.Message);
		}
	}

	/// <summary>
	/// Runs the handler. With a timeout, the token is canceled when it elapses; a handler that ignores the
	/// token is abandoned after <see cref="JobEngineOptions.TimeoutGrace"/> so its slot is not held forever.
	/// </summary>
	private async Task RunWithTimeoutAsync(ClaimedJob job, TimeSpan? timeout, CancellationToken ct)
	{
		var run = ExecuteHandlerAsync(job, ct);

		if (timeout is not { } limit)
		{
			await run;

			return;
		}

		using var graceCts = new CancellationTokenSource();
		var grace = Task.Delay(limit + options.TimeoutGrace, graceCts.Token);

		try
		{
			if (await Task.WhenAny(run, grace) == run)
			{
				await run;

				return;
			}
		}
		finally
		{
			await graceCts.CancelAsync();
		}

		_ = run.ContinueWith(
			static task => _ = task.Exception,
			CancellationToken.None,
			TaskContinuationOptions.OnlyOnFaulted,
			TaskScheduler.Default);

		logger.LogError("Job {JobId} ignored cancellation after its timeout and was abandoned.", job.Id);

		throw new OperationCanceledException("The handler did not stop after its timeout.");
	}

	private async Task ExecuteHandlerAsync(ClaimedJob job, CancellationToken ct)
	{
		if (!registry.TryGet(job.Type, out var factory))
		{
			throw new PermanentFailureException($"No handler registered for job type '{job.Type}'.");
		}

		await using var scope = services.CreateAsyncScope();

		var handler = factory(scope.ServiceProvider);
		var context = new JobContext(
			job.Id,
			job.Type,
			job.Queue,
			job.PayloadJson,
			job.Attempts,
			job.MaxAttempts,
			scope.ServiceProvider);

		await handler.HandleAsync(context, ct);
	}

	/// <summary>
	/// Extends the lease while the handler runs. If the lease is gone (another worker reclaimed the job
	/// after a stall) the handler is canceled and its result is discarded: the fence token makes any late
	/// write a no-op, so a zombie worker cannot overwrite the new owner's outcome.
	/// </summary>
	private async Task HeartbeatAsync(ClaimedJob job, LeaseState lease, CancellationTokenSource handlerCts, CancellationToken stop)
	{
		using var timer = new PeriodicTimer(options.LeaseDuration / 3);

		try
		{
			while (await timer.WaitForNextTickAsync(stop))
			{
				try
				{
					if (!await store.ExtendLeaseAsync(job.Id, job.LeaseToken, options.LeaseDuration, stop))
					{
						lease.Lost = true;
						logger.LogWarning("Job {JobId} lost its lease; canceling the handler.", job.Id);
						await handlerCts.CancelAsync();

						return;
					}
				}
				catch (Exception ex) when (ex is not OperationCanceledException)
				{
					logger.LogWarning(ex, "Could not extend the lease of job {JobId}; will retry on the next beat.", job.Id);
				}
			}
		}
		catch (OperationCanceledException)
		{
			// The handler finished; nothing left to heartbeat.
		}
	}

	private async Task<string> ReportOutcomeAsync(ClaimedJob job, Exception? error, bool leaseLost, bool shuttingDown, bool timedOut)
	{
		try
		{
			if (leaseLost)
			{
				logger.LogWarning("Job {JobId} lost its lease; its result was discarded.", job.Id);

				return "lease_lost";
			}

			if (error is null)
			{
				if (!await store.CompleteAsync(job.Id, job.LeaseToken))
				{
					logger.LogWarning("Job {JobId} finished but its lease was already gone.", job.Id);

					return "lease_lost";
				}

				return "succeeded";
			}

			if (error is OperationCanceledException && shuttingDown)
			{
				await store.ReleaseAsync(job.Id, job.LeaseToken);
				logger.LogInformation("Job {JobId} released back to the queue for shutdown.", job.Id);

				return "released";
			}

			var permanent = error is PermanentFailureException;
			var delay = permanent ? TimeSpan.Zero : options.RetryPolicy.GetDelay(job.Attempts);
			var outcome = await store.FailAsync(job.Id, job.LeaseToken, Describe(error), permanent, delay);

			if (outcome is null)
			{
				logger.LogWarning("Job {JobId} failed but its lease was already gone.", job.Id);

				return "lease_lost";
			}

			logger.LogWarning(
				error,
				"Job {JobId} ({Type}) failed on attempt {Attempt}/{MaxAttempts}; now {State}.",
				job.Id,
				job.Type,
				job.Attempts,
				job.MaxAttempts,
				outcome.State);

			return timedOut ? "timed_out" : outcome.State == JobState.Dead ? "dead" : "retried";
		}
		catch (Exception ex)
		{
			logger.LogError(ex, "Could not record the outcome of job {JobId}; its lease will expire and it will be retried.", job.Id);

			return "lease_lost";
		}
	}

	private static string Describe(Exception error)
	{
		var text = error.ToString();

		return text.Length <= MaxErrorLength ? text : text[..MaxErrorLength];
	}

	private sealed class LeaseState
	{
		public volatile bool Lost;
	}
}
