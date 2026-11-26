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

	// ------------------------------------------------------------ time zones

	private static readonly TimeZoneInfo NewYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
	private static readonly TimeZoneInfo London = TimeZoneInfo.FindSystemTimeZoneById("Europe/London");
	private static readonly TimeZoneInfo Manila = TimeZoneInfo.FindSystemTimeZoneById("Asia/Manila");

	private static List<DateTime> Occurrences(string expression, TimeZoneInfo zone, DateTime afterUtc, DateTime untilUtc)
	{
		var cron = CronExpression.Parse(expression);
		var found = new List<DateTime>();
		var cursor = afterUtc;

		while (cron.GetNextOccurrence(cursor, zone) is { } next && next < untilUtc)
		{
			found.Add(next);
			cursor = next;
		}

		return found;
	}

	private static void AssertSpacing(List<DateTime> runs, TimeSpan expected)
	{
		for (var i = 1; i < runs.Count; i++)
		{
			Assert.Equal(expected, runs[i] - runs[i - 1]);
		}
	}

	[Fact]
	public void TimeZone_FixedOffsetZone_ShiftsTheWallClockTime()
	{
		var next = CronExpression.Parse("0 9 * * *").GetNextOccurrence(Utc(2026, 1, 1, 0, 0), Manila);

		Assert.Equal(Utc(2026, 1, 1, 1, 0), next); // 09:00 in Manila is 01:00 UTC
	}

	[Fact]
	public void TimeZone_NullOrUtcBehavesLikeNoZone()
	{
		var cron = CronExpression.Parse("0 9 * * *");
		var after = Utc(2026, 1, 1, 0, 0);

		Assert.Equal(cron.GetNextOccurrence(after), cron.GetNextOccurrence(after, null));
		Assert.Equal(cron.GetNextOccurrence(after), cron.GetNextOccurrence(after, TimeZoneInfo.Utc));
	}

	[Fact]
	public void TimeZone_DailyScheduleFollowsDaylightSaving()
	{
		var cron = CronExpression.Parse("0 9 * * *");

		Assert.Equal(Utc(2026, 3, 7, 14, 0), cron.GetNextOccurrence(Utc(2026, 3, 6, 15, 0), NewYork)); // EST, UTC-5
		Assert.Equal(Utc(2026, 3, 8, 13, 0), cron.GetNextOccurrence(Utc(2026, 3, 7, 15, 0), NewYork)); // EDT, UTC-4
		Assert.Equal(Utc(2026, 6, 1, 8, 0), CronExpression.Parse("0 9 * * *").GetNextOccurrence(Utc(2026, 6, 1, 0, 0), London));
	}

	[Fact]
	public void TimeZone_TimeInTheSpringGap_RunsOnceShiftedPastTheGap()
	{
		var cron = CronExpression.Parse("30 2 * * *");

		// 02:30 does not exist on 2026-03-08 in New York; it runs at 03:30 EDT (07:30 UTC) and not again that day.
		Assert.Equal(Utc(2026, 3, 8, 7, 30), cron.GetNextOccurrence(Utc(2026, 3, 8, 0, 0), NewYork));
		Assert.Equal(Utc(2026, 3, 9, 6, 30), cron.GetNextOccurrence(Utc(2026, 3, 8, 7, 30), NewYork));
	}

	[Fact]
	public void TimeZone_TimeRepeatedInAutumn_RunsAtItsFirstOccurrenceOnly()
	{
		var cron = CronExpression.Parse("30 1 * * *");

		// 01:30 happens twice on 2026-11-01 in New York (05:30 UTC and 06:30 UTC); only the first counts.
		Assert.Equal(Utc(2026, 11, 1, 5, 30), cron.GetNextOccurrence(Utc(2026, 11, 1, 0, 0), NewYork));
		Assert.Equal(Utc(2026, 11, 2, 6, 30), cron.GetNextOccurrence(Utc(2026, 11, 1, 5, 30), NewYork));
	}

	[Fact]
	public void TimeZone_EveryTenMinutes_KeepsFiringAcrossTheSpringGap()
	{
		var runs = Occurrences("*/10 * * * *", NewYork, Utc(2026, 3, 8, 5, 55), Utc(2026, 3, 8, 9, 0));

		Assert.Equal(18, runs.Count); // 06:00..08:50 UTC, every 10 minutes of real time, nothing skipped
		AssertSpacing(runs, TimeSpan.FromMinutes(10));
		Assert.Contains(Utc(2026, 3, 8, 7, 0), runs); // 03:00 EDT, straight after 01:50 EST
	}

	[Fact]
	public void TimeZone_EveryTenMinutes_FiresThroughTheRepeatedHour()
	{
		var runs = Occurrences("*/10 * * * *", NewYork, Utc(2026, 11, 1, 4, 55), Utc(2026, 11, 1, 8, 0));

		Assert.Equal(18, runs.Count); // 05:00..07:50 UTC; the 01:00-01:59 hour is run twice
		AssertSpacing(runs, TimeSpan.FromMinutes(10));
	}

	[Fact]
	public void TimeZone_Hourly_FiresInBothInstancesOfTheRepeatedHour()
	{
		var runs = Occurrences("0 * * * *", NewYork, Utc(2026, 11, 1, 4, 30), Utc(2026, 11, 1, 8, 0));

		Assert.Equal(new[] { Utc(2026, 11, 1, 5, 0), Utc(2026, 11, 1, 6, 0), Utc(2026, 11, 1, 7, 0) }, runs);
	}
}
