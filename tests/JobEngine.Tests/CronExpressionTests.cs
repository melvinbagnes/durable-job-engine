using JobEngine.Scheduling;
using Xunit;

namespace JobEngine.Tests;

public class CronExpressionTests
{
	private static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0, int second = 0) =>
		new(year, month, day, hour, minute, second, DateTimeKind.Utc);

	[Fact]
	public void EveryMinute_FiresOnTheNextWholeMinute()
	{
		var next = CronExpression.Parse("* * * * *").GetNextOccurrence(Utc(2026, 1, 1, 12, 0, 30));

		Assert.Equal(Utc(2026, 1, 1, 12, 1), next);
	}

	[Fact]
	public void NextOccurrence_IsStrictlyAfterTheGivenInstant()
	{
		var next = CronExpression.Parse("0 12 * * *").GetNextOccurrence(Utc(2026, 1, 1, 12, 0));

		Assert.Equal(Utc(2026, 1, 2, 12, 0), next);
	}

	[Fact]
	public void Step_FiresEveryFifteenMinutes()
	{
		var next = CronExpression.Parse("*/15 * * * *").GetNextOccurrence(Utc(2026, 1, 1, 12, 7));

		Assert.Equal(Utc(2026, 1, 1, 12, 15), next);
	}

	[Fact]
	public void StartWithStep_ExpandsToTheEndOfTheField()
	{
		var cron = CronExpression.Parse("5/20 * * * *");

		Assert.Equal(Utc(2026, 1, 1, 12, 5), cron.GetNextOccurrence(Utc(2026, 1, 1, 12, 0)));
		Assert.Equal(Utc(2026, 1, 1, 12, 25), cron.GetNextOccurrence(Utc(2026, 1, 1, 12, 5)));
		Assert.Equal(Utc(2026, 1, 1, 12, 45), cron.GetNextOccurrence(Utc(2026, 1, 1, 12, 25)));
		Assert.Equal(Utc(2026, 1, 1, 13, 5), cron.GetNextOccurrence(Utc(2026, 1, 1, 12, 45)));
	}

	[Fact]
	public void WeekdayRange_SkipsTheWeekend()
	{
		// 2026-01-02 is a Friday; the next weekday 09:00 after Friday 10:00 is Monday.
		var next = CronExpression.Parse("0 9 * * 1-5").GetNextOccurrence(Utc(2026, 1, 2, 10, 0));

		Assert.Equal(Utc(2026, 1, 5, 9, 0), next);
	}

	[Fact]
	public void List_PicksTheNextListedDay()
	{
		var next = CronExpression.Parse("0 0 1,15 * *").GetNextOccurrence(Utc(2026, 1, 2));

		Assert.Equal(Utc(2026, 1, 15), next);
	}

	[Fact]
	public void BothDayFieldsRestricted_MatchEither()
	{
		// "the 13th, or any Friday": Friday 2026-01-02 comes before the 13th.
		var next = CronExpression.Parse("0 0 13 * 5").GetNextOccurrence(Utc(2026, 1, 1));

		Assert.Equal(Utc(2026, 1, 2), next);
	}

	[Fact]
	public void StarWithStepInDayOfMonth_StillRestrictsTheDay()
	{
		var next = CronExpression.Parse("0 0 */10 * *").GetNextOccurrence(Utc(2026, 1, 2));

		Assert.Equal(Utc(2026, 1, 11), next);
	}

	[Fact]
	public void SevenMeansSunday()
	{
		// 2026-01-01 is a Thursday; the next Sunday is the 4th.
		var next = CronExpression.Parse("0 0 * * 7").GetNextOccurrence(Utc(2026, 1, 1));

		Assert.Equal(Utc(2026, 1, 4), next);
	}

	[Fact]
	public void LeapDay_FindsTheNextLeapYear()
	{
		var next = CronExpression.Parse("30 2 29 2 *").GetNextOccurrence(Utc(2026, 3, 1));

		Assert.Equal(Utc(2028, 2, 29, 2, 30), next);
	}

	[Fact]
	public void ImpossibleDate_ReturnsNull()
	{
		Assert.Null(CronExpression.Parse("0 0 31 2 *").GetNextOccurrence(Utc(2026, 1, 1)));
	}

	[Fact]
	public void ExtraWhitespace_IsNormalised()
	{
		Assert.Equal("*/5 * * * *", CronExpression.Parse("  */5   *  * * *  ").Expression);
	}

	[Theory]
	[InlineData("* * * *")]
	[InlineData("* * * * * *")]
	[InlineData("60 * * * *")]
	[InlineData("* 24 * * *")]
	[InlineData("* * 0 * *")]
	[InlineData("* * * 13 *")]
	[InlineData("* * * * 8")]
	[InlineData("*/0 * * * *")]
	[InlineData("5-1 * * * *")]
	[InlineData("a * * * *")]
	[InlineData("1- * * * *")]
	[InlineData("1,,2 * * * *")]
	[InlineData("")]
	public void InvalidExpressions_AreRejected(string expression)
	{
		Assert.Throws<FormatException>(() => CronExpression.Parse(expression));
		Assert.False(CronExpression.TryParse(expression, out _));
	}
}
