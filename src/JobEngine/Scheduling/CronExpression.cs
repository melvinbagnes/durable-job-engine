using System.Globalization;

namespace JobEngine.Scheduling;

/// <summary>
/// Minimal 5-field cron (minute hour day-of-month month day-of-week), evaluated in UTC.
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
	public DateTime? GetNextOccurrence(DateTime afterUtc)
	{
		var after = afterUtc.Kind == DateTimeKind.Local ? afterUtc.ToUniversalTime() : afterUtc;
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
