using JobEngine.Scheduling;
using Xunit;

namespace JobEngine.Tests;

public class RetryPolicyTests
{
	private static readonly RetryPolicy Policy = new(TimeSpan.FromSeconds(10), TimeSpan.FromMinutes(5), 2.0, 0.2);

	[Theory]
	[InlineData(1, 10)]
	[InlineData(2, 20)]
	[InlineData(3, 40)]
	[InlineData(4, 80)]
	public void WithoutJitter_DelayDoublesEachAttempt(int attempt, double expectedSeconds)
	{
		Assert.Equal(expectedSeconds, Policy.GetDelay(attempt, 0.5).TotalSeconds, precision: 6);
	}

	[Fact]
	public void DelayIsCappedAtMaxDelay()
	{
		Assert.Equal(300, Policy.GetDelay(20, 0.5).TotalSeconds, precision: 6);
	}

	[Fact]
	public void HugeAttemptNumbersDoNotOverflow()
	{
		Assert.Equal(300, Policy.GetDelay(5000, 0.5).TotalSeconds, precision: 6);
	}

	[Fact]
	public void JitterSpreadsTheDelayAroundTheBase()
	{
		Assert.Equal(8, Policy.GetDelay(1, 0.0).TotalSeconds, precision: 6);
		Assert.Equal(12, Policy.GetDelay(1, 1.0).TotalSeconds, precision: 6);
	}

	[Fact]
	public void JitterNeverPushesTheDelayAboveTheCap()
	{
		Assert.True(Policy.GetDelay(20, 1.0) <= Policy.MaxDelay);
	}

	[Fact]
	public void AttemptMustBePositive()
	{
		Assert.Throws<ArgumentOutOfRangeException>(() => Policy.GetDelay(0));
	}
}
