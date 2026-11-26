using JobEngine.Runtime;
using JobEngine.Scheduling;
using JobEngine.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace JobEngine;

public sealed class JobEngineBuilder(IServiceCollection services, JobHandlerRegistry registry)
{
	public IServiceCollection Services => services;

	public JobEngineBuilder AddHandler<THandler>(string type)
		where THandler : class, IJobHandler
	{
		services.TryAddScoped<THandler>();
		registry.Add(type, provider => provider.GetRequiredService<THandler>());

		return this;
	}

	public JobEngineBuilder AddHandler(string type, Func<JobContext, CancellationToken, Task> handler)
	{
		ArgumentNullException.ThrowIfNull(handler);
		registry.Add(type, _ => new DelegateJobHandler(handler));

		return this;
	}

	public JobEngineBuilder AddRecurring(RecurringJobDefinition definition)
	{
		registry.AddRecurring(definition);

		return this;
	}
}

public static class JobEngineServiceCollectionExtensions
{
	/// <summary>
	/// Registers the store and (unless disabled in the options) the worker, the scheduler and the startup
	/// migrator. Enqueue-only processes can set EnableWorker/EnableScheduler to false.
	/// </summary>
	public static JobEngineBuilder AddJobEngine(this IServiceCollection services, Action<JobEngineOptions> configure)
	{
		ArgumentNullException.ThrowIfNull(configure);

		services.AddLogging();
		services.AddOptions<JobEngineOptions>().Configure(configure);
		services.AddSingleton(provider => provider.GetRequiredService<IOptions<JobEngineOptions>>().Value);

		var registry = new JobHandlerRegistry();
		services.AddSingleton(registry);

		// No TimeProvider unless the application registered one: timestamps then come from the database clock.
		services.AddSingleton(provider => JobStore.Create(
			provider.GetRequiredService<JobEngineOptions>(),
			provider.GetService<TimeProvider>()));
		services.AddSingleton<WakeSignal>();
		services.AddSingleton<JobProcessor>();

		// Hosted services start in registration order: migrate first, then run.
		services.AddHostedService<JobEngineMigrator>();
		services.AddHostedService<JobNotificationListener>();
		services.AddHostedService<JobWorker>();
		services.AddHostedService<SchedulerService>();

		return new JobEngineBuilder(services, registry);
	}
}
