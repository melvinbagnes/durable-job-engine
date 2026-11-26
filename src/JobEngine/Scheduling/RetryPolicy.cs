namespace JobEngine.Scheduling;

/// <summary>
/// Exponential backoff with a cap and symmetric jitter.
/// delay(n) = min(MaxDelay, BaseDelay * Multiplier^(n-1)) * (1 +/- JitterRatio).
/// </summary>
public sealed record RetryPolicy(TimeSpan BaseDelay, TimeSpan MaxDelay, double Multiplier = 2.0, double JitterRatio = 0.2)
{
	/// <param name="attempt">The attempt that just failed, starting at 1.</param>
	/// <param name="jitterSample">A value in [0, 1); 0.5 means "no jitter".</param>
	public TimeSpan GetDelay(int attempt, double jitterSample)
	{
		ArgumentOutOfRangeException.ThrowIfLessThan(attempt, 1);

		var maxMilliseconds = MaxDelay.TotalMilliseconds;
		var exponential = BaseDelay.TotalMilliseconds * Math.Pow(Multiplier, attempt - 1);
		var capped = Math.Min(exponential, maxMilliseconds);
		var jitterFactor = 1 + (((jitterSample * 2) - 1) * JitterRatio);
		var jittered = Math.Clamp(capped * jitterFactor, 0, maxMilliseconds);

		return TimeSpan.FromMilliseconds(jittered);
	}

	public TimeSpan GetDelay(int attempt) => GetDelay(attempt, Random.Shared.NextDouble());
}
