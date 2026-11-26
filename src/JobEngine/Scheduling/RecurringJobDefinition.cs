namespace JobEngine.Scheduling;

public sealed record RecurringJobDefinition(
	string Name,
	string Cron,
	string Type,
	string Queue = "default",
	string PayloadJson = "{}",
	int? MaxAttempts = null);
