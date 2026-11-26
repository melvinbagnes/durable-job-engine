namespace JobEngine.Scheduling;

/// <param name="TimeZone">
/// IANA id such as "Asia/Manila" or "America/New_York". The cron expression is evaluated in this zone.
/// Null means UTC.
/// </param>
public sealed record RecurringJobDefinition(
	string Name,
	string Cron,
	string Type,
	string Queue = "default",
	string PayloadJson = "{}",
	int? MaxAttempts = null,
	string? TimeZone = null);

internal static class TimeZones
{
	/// <summary>Null or empty means UTC (returned as null). Throws <see cref="TimeZoneNotFoundException"/> for unknown ids.</summary>
	public static TimeZoneInfo? Resolve(string? id) =>
		string.IsNullOrWhiteSpace(id) ? null : TimeZoneInfo.FindSystemTimeZoneById(id);
}
