using System.Text.Json;
using JobEngine;
using JobEngine.Demo;
using JobEngine.Jobs;
using JobEngine.Scheduling;
using JobEngine.Storage;
using JobEngine.Workflows;
using Npgsql;

var builder = WebApplication.CreateBuilder(args);

var connectionString = builder.Configuration.GetConnectionString("Postgres")
	?? "Host=localhost;Port=5432;Username=postgres;Password=postgres;Database=jobengine";

builder.Services.AddSingleton(NpgsqlDataSource.Create(connectionString));

builder.Services
	.AddJobEngine(options =>
	{
		options.ConnectionString = connectionString;
		options.Queues = ["default", "emails"];
		options.Concurrency = 4;
		options.CompletedRetention = TimeSpan.FromDays(7);
	})
	.AddHandler<SendEmailHandler>("email.send")
	.AddHandler<GenerateReportHandler>("report.generate")
	.AddHandler("system.heartbeat", (context, _) =>
	{
		Console.WriteLine($"[{DateTime.UtcNow:O}] heartbeat job {context.JobId}");

		return Task.CompletedTask;
	})
	.AddRecurring(new RecurringJobDefinition("heartbeat", "* * * * *", "system.heartbeat"));

var app = builder.Build();

// The orders table belongs to the "application"; it exists to show the transactional outbox.
await using (var scope = app.Services.CreateAsyncScope())
{
	var dataSource = scope.ServiceProvider.GetRequiredService<NpgsqlDataSource>();
	await using var command = dataSource.CreateCommand(
		"CREATE TABLE IF NOT EXISTS orders (id uuid PRIMARY KEY, customer text NOT NULL, amount numeric NOT NULL, created_at timestamptz NOT NULL DEFAULT now());");
	await command.ExecuteNonQueryAsync();
}

app.MapGet("/", () => Results.Text("Durable job engine demo. See README.md for the endpoints."));

app.MapPost("/jobs", async (EnqueueJobBody body, JobStore store, CancellationToken ct) =>
{
	var result = await store.EnqueueAsync(
		new EnqueueRequest
		{
			Type = body.Type,
			Queue = body.Queue ?? "default",
			PayloadJson = body.Payload?.GetRawText() ?? "{}",
			Priority = body.Priority ?? 0,
			Delay = TimeSpan.FromSeconds(body.DelaySeconds ?? 0),
			MaxAttempts = body.MaxAttempts,
			IdempotencyKey = body.IdempotencyKey
		},
		ct);

	return Results.Accepted($"/jobs/{result.JobId}", result);
});

app.MapPost("/workflows", async (EnqueueWorkflowBody body, JobStore store, CancellationToken ct) =>
{
	var steps = body.Steps.Select(step => new WorkflowStep
	{
		Key = step.Key,
		Type = step.Type,
		Queue = step.Queue ?? "default",
		PayloadJson = step.Payload?.GetRawText() ?? "{}",
		DependsOn = step.DependsOn ?? []
	}).ToList();

	try
	{
		var result = await store.EnqueueWorkflowAsync(steps, body.IdempotencyKey, ct);

		return Results.Accepted(value: result);
	}
	catch (WorkflowValidationException ex)
	{
		return Results.BadRequest(new { error = ex.Message });
	}
});

app.MapGet("/jobs/{id:guid}", async (Guid id, JobStore store, CancellationToken ct) =>
	await store.GetAsync(id, ct) is { } job ? Results.Ok(job) : Results.NotFound());

app.MapPost("/jobs/{id:guid}/requeue", async (Guid id, JobStore store, CancellationToken ct) =>
	await store.RequeueDeadAsync(id, ct) ? Results.Accepted() : Results.Conflict(new { error = "Only dead jobs can be requeued." }));

app.MapGet("/stats", async (JobStore store, CancellationToken ct) =>
	Results.Ok((await store.GetStatsAsync(ct)).ByState.ToDictionary(pair => pair.Key.ToString().ToLowerInvariant(), pair => pair.Value)));

// Transactional outbox: the order and its confirmation e-mail job commit together or not at all.
app.MapPost("/orders", async (CreateOrderBody body, NpgsqlDataSource dataSource, JobStore store, CancellationToken ct) =>
{
	var orderId = Guid.NewGuid();

	await using var connection = await dataSource.OpenConnectionAsync(ct);
	await using var transaction = await connection.BeginTransactionAsync(ct);

	await using (var insert = new NpgsqlCommand("INSERT INTO orders (id, customer, amount) VALUES (@id, @customer, @amount);", connection, transaction))
	{
		insert.Parameters.AddWithValue("id", orderId);
		insert.Parameters.AddWithValue("customer", body.Customer);
		insert.Parameters.AddWithValue("amount", body.Amount);
		await insert.ExecuteNonQueryAsync(ct);
	}

	var job = await store.EnqueueAsync(
		transaction,
		new EnqueueRequest
		{
			Type = "email.send",
			Queue = "emails",
			PayloadJson = JsonSerializer.Serialize(new { to = $"{body.Customer}@example.com", subject = $"Order {orderId} confirmed" }),
			IdempotencyKey = $"order-confirmation:{orderId}"
		},
		ct);

	await transaction.CommitAsync(ct);

	return Results.Created($"/jobs/{job.JobId}", new { orderId, confirmationJobId = job.JobId });
});

app.Run();

namespace JobEngine.Demo
{
	public sealed record EnqueueJobBody(
		string Type,
		string? Queue,
		JsonElement? Payload,
		int? Priority,
		double? DelaySeconds,
		int? MaxAttempts,
		string? IdempotencyKey);

	public sealed record WorkflowStepBody(
		string Key,
		string Type,
		string? Queue,
		JsonElement? Payload,
		List<string>? DependsOn);

	public sealed record EnqueueWorkflowBody(List<WorkflowStepBody> Steps, string? IdempotencyKey);

	public sealed record CreateOrderBody(string Customer, decimal Amount);
}
