using JobEngine.Runtime;

namespace JobEngine.Demo;

public sealed record EmailPayload(string? To, string? Subject, int FailFirst = 0);

/// <summary>Pretends to send an e-mail. Set "failFirst": N in the payload to watch N retries before success.</summary>
public sealed class SendEmailHandler(ILogger<SendEmailHandler> logger) : IJobHandler
{
	public Task HandleAsync(JobContext context, CancellationToken cancellationToken)
	{
		var payload = context.GetPayload<EmailPayload>()
			?? throw new PermanentFailureException("The payload is missing.");

		if (string.IsNullOrWhiteSpace(payload.To))
		{
			throw new PermanentFailureException("The 'to' address is required; retrying will not help.");
		}

		if (context.Attempt <= payload.FailFirst)
		{
			throw new InvalidOperationException($"SMTP server unavailable (simulated failure on attempt {context.Attempt}).");
		}

		logger.LogInformation("E-mail to {To}: {Subject} (attempt {Attempt})", payload.To, payload.Subject, context.Attempt);

		return Task.CompletedTask;
	}
}

public sealed class GenerateReportHandler(ILogger<GenerateReportHandler> logger) : IJobHandler
{
	public async Task HandleAsync(JobContext context, CancellationToken cancellationToken)
	{
		logger.LogInformation("Generating report for job {JobId}...", context.JobId);
		await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
		logger.LogInformation("Report for job {JobId} done.", context.JobId);
	}
}
