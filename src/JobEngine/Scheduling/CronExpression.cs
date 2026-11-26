using System.Globalization;

namespace JobEngine.Scheduling;

/// <summary>
/// Minimal 5-field cron (minute hour day-of-month month day-of-week), evaluated in UTC or in an IANA time zone.
/// Supports *, lists (a,b), ranges (a-b) and steps (*/n, a-b/n, a/n).
/// As in classic cron, when both day-of-month and day-of-week are restricted a day matches if either does.
/// </summary>
public sealed class CronExpression
{
	private readonly bool[] _minutes = new bool[60];
	private readonly bool[] _hours = new bool[24];
	private readonly bool[] _daysOfMonth = new bool[32];
	private readonly bool[] _months = new bool[13];
	private readonly bool[] _daysOfWeek = new bool[7];
	private readonly bool _dayOfMonthRestricted;
	private readonly bool _dayOfWeekRestricted;
	private readonly bool _everyHour;

	private CronExpression(string expression, string[] fields)
	{
		Expression = expression;

		Fill(_minutes, fields[0], 0, 59, "minute");
		Fill(_hours, fields[1], 0, 23, "hour");
		Fill(_daysOfMonth, fields[2], 1, 31, "day-of-month");
		Fill(_months, fields[3], 1, 12, "month");

		// Day-of-week accepts 0-7 where both 0 and 7 mean Sunday.
		var weekdays = new bool[8];
		Fill(weekdays, fields[4], 0, 7, "day-of-week");

		for (var day = 0; day < 7; day++)
		{
			_daysOfWeek[day] = weekdays[day];
		}

		if (weekdays[7])
		{
			_daysOfWeek[0] = true;
		}

		_everyHour = _hours.All(enabled => enabled);
		_dayOfMonthRestricted = !fields[2].StartsWith('*');
		_dayOfWeekRestricted = !fields[4].StartsWith('*');
	}

	public string Expression { get; }

	public static CronExpression Parse(string expression)
	{
		ArgumentNullException.ThrowIfNull(expression);

		var fields = expression.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);

		if (fields.Length != 5)
		{
			throw new FormatException("A cron expression needs exactly 5 fields: minute hour day-of-month month day-of-week.");
		}

		return new CronExpression(string.Join(' ', fields), fields);
	}

	public static bool TryParse(string? expression, out CronExpression? result)
	{
		try
		{
			result = expression is null ? null : Parse(expression);
		}
		catch (FormatException)
		{
			result = null;
		}

		return result is not null;
	}

	/// <summary>The first matching minute strictly after <paramref name="afterUtc"/>, or null if none within 10 years.</summary>
	/// <param name="timeZone">
	/// The zone the expression is written in; null means UTC. Daylight-saving rules:
	/// a schedule that runs in every hour (e.g. "*/10 * * * *") follows real elapsed time, so it neither skips the
	/// hour that does not exist in spring nor misses the repeated hour in autumn. A schedule that names hours
	/// (e.g. "30 2 * * *") runs once per wall-clock day: a time that does not exist is moved to the same instant
	/// shifted past the gap (02:30 becomes 03:30), and a time that occurs twice runs at its first occurrence only.
	/// </param>
	public DateTime? GetNextOccurrence(DateTime afterUtc, TimeZoneInfo? timeZone = null)
	{
		var after = ToUtc(afterUtc);

		if (timeZone is null || (timeZone.BaseUtcOffset == TimeSpan.Zero && !timeZone.SupportsDaylightSavingTime))
		{
			return NextWallTime(after);
		}

		return _everyHour ? NextOnElapsedTime(after, timeZone) : NextOnWallClock(after, timeZone);
	}

	private static DateTime ToUtc(DateTime value) =>
		value.Kind == DateTimeKind.Local ? value.ToUniversalTime() : DateTime.SpecifyKind(value, DateTimeKind.Utc);

	private DateTime? NextOnWallClock(DateTime afterUtc, TimeZoneInfo timeZone)
	{
		var local = TimeZoneInfo.ConvertTimeFromUtc(afterUtc, timeZone);

		for (var attempt = 0; attempt < 1000; attempt++)
		{
			var wall = NextWallTime(local);

			if (wall is null)
			{
				return null;
			}

			var utc = ResolveToUtc(DateTime.SpecifyKind(wall.Value, DateTimeKind.Unspecified), timeZone);

			if (utc > afterUtc)
			{
				return utc;
			}

			local = wall.Value;
		}

		return null;
	}

	private static DateTime ResolveToUtc(DateTime wallClock, TimeZoneInfo timeZone)
	{
		TimeSpan offset;

		if (timeZone.IsInvalidTime(wallClock))
		{
			offset = timeZone.BaseUtcOffset;
		}
		else if (timeZone.IsAmbiguousTime(wallClock))
		{
			offset = timeZone.GetAmbiguousTimeOffsets(wallClock).Max();
		}
		else
		{
			offset = timeZone.GetUtcOffset(wallClock);
		}

		return DateTime.SpecifyKind(wallClock - offset, DateTimeKind.Utc);
	}

	/// <summary>Walks the zone's UTC-offset segments; inside one segment local time is just UTC plus a fixed offset.</summary>
	private DateTime? NextOnElapsedTime(DateTime afterUtc, TimeZoneInfo timeZone)
	{
		var cursor = new DateTime(afterUtc.Year, afterUtc.Month, afterUtc.Day, afterUtc.Hour, afterUtc.Minute, 0, DateTimeKind.Utc)
			.AddMinutes(1);

		for (var segment = 0; segment < 200; segment++)
		{
			var offset = timeZone.GetUtcOffset(cursor);
			var segmentEnd = NextOffsetChange(cursor, offset, timeZone);
			var wall = NextWallTime(cursor + offset - TimeSpan.FromMinutes(1));

			if (wall is null)
			{
				return null;
			}

			var utc = DateTime.SpecifyKind(wall.Value - offset, DateTimeKind.Utc);

			if (segmentEnd is null || utc < segmentEnd.Value)
			{
				return utc;
			}

			cursor = segmentEnd.Value;
		}

		return null;
	}

	private static DateTime? NextOffsetChange(DateTime from, TimeSpan offset, TimeZoneInfo timeZone)
	{
		if (!timeZone.SupportsDaylightSavingTime)
		{
			return null;
		}

		var low = from;

		for (var days = 1; days <= 400; days++)
		{
			var high = from.AddDays(days);

			if (timeZone.GetUtcOffset(high) == offset)
			{
				low = high;

				continue;
			}

			while (high - low > TimeSpan.FromMinutes(1))
			{
				var middle = low + TimeSpan.FromTicks((high - low).Ticks / 2);

				if (timeZone.GetUtcOffset(middle) == offset)
				{
					low = middle;
				}
				else
				{
					high = middle;
				}
			}

			return new DateTime(high.Year, high.Month, high.Day, high.Hour, high.Minute, 0, DateTimeKind.Utc);
		}

		return null;
	}

	/// <summary>Next match strictly after a wall-clock instant, ignoring time zones (the value's Kind is irrelevant).</summary>
	private DateTime? NextWallTime(DateTime after)
	{
		var candidate = new DateTime(after.Year, after.Month, after.Day, after.Hour, after.Minute, 0, DateTimeKind.Utc).AddMinutes(1);
		var limit = candidate.AddYears(10);

		while (candidate < limit)
		{
			if (!_months[candidate.Month])
			{
				candidate = new DateTime(candidate.Year, candidate.Month, 1, 0, 0, 0, DateTimeKind.Utc).AddMonths(1);
				continue;
			}

			if (!DayMatches(candidate))
			{
				candidate = new DateTime(candidate.Year, candidate.Month, candidate.Day, 0, 0, 0, DateTimeKind.Utc).AddDays(1);
				continue;
			}

			if (!_hours[candidate.Hour])
			{
				candidate = new DateTime(candidate.Year, candidate.Month, candidate.Day, candidate.Hour, 0, 0, DateTimeKind.Utc).AddHours(1);
				continue;
			}

			if (!_minutes[candidate.Minute])
			{
				candidate = candidate.AddMinutes(1);
				continue;
			}

			return candidate;
		}

		return null;
	}

	private bool DayMatches(DateTime value)
	{
		var dayOfMonthMatches = _daysOfMonth[value.Day];
		var dayOfWeekMatches = _daysOfWeek[(int)value.DayOfWeek];

		if (_dayOfMonthRestricted && _dayOfWeekRestricted)
		{
			return dayOfMonthMatches || dayOfWeekMatches;
		}

		return dayOfMonthMatches && dayOfWeekMatches;
	}

	private static void Fill(bool[] target, string field, int min, int max, string name)
	{
		foreach (var part in field.Split(','))
		{
			if (part.Length == 0)
			{
				throw new FormatException($"Empty list item in the {name} field.");
			}

			var stepParts = part.Split('/');

			if (stepParts.Length > 2)
			{
				throw new FormatException($"Invalid step in the {name} field: '{part}'.");
			}

			var step = 1;

			if (stepParts.Length == 2 && (!TryParseInt(stepParts[1], out step) || step < 1))
			{
				throw new FormatException($"Invalid step in the {name} field: '{part}'.");
			}

			var range = stepParts[0];
			int start;
			int end;

			if (range == "*")
			{
				start = min;
				end = max;
			}
			else if (range.Contains('-'))
			{
				var bounds = range.Split('-');

				if (bounds.Length != 2 || !TryParseInt(bounds[0], out start) || !TryParseInt(bounds[1], out end))
				{
					throw new FormatException($"Invalid range in the {name} field: '{part}'.");
				}
			}
			else
			{
				if (!TryParseInt(range, out start))
				{
					throw new FormatException($"Invalid value in the {name} field: '{part}'.");
				}

				end = stepParts.Length == 2 ? max : start;
			}

			if (start < min || end > max || start > end)
			{
				throw new FormatException($"Value out of range in the {name} field: '{part}' (allowed {min}-{max}).");
			}

			for (var value = start; value <= end; value += step)
			{
				target[value] = true;
			}
		}
	}

	private static bool TryParseInt(string text, out int value) =>
		int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value);
}
