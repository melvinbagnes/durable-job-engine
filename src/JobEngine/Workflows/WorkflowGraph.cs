namespace JobEngine.Workflows;

public sealed class WorkflowValidationException(string message) : Exception(message);

public sealed class WorkflowStep
{
	public required string Key { get; init; }

	public required string Type { get; init; }

	public string PayloadJson { get; init; } = "{}";

	public string Queue { get; init; } = "default";

	public int Priority { get; init; }

	public int? MaxAttempts { get; init; }

	/// <summary>Keys of the steps that must succeed before this one can run.</summary>
	public IReadOnlyList<string> DependsOn { get; init; } = [];
}

public sealed record WorkflowEnqueueResult(IReadOnlyDictionary<string, Guid> JobIds, bool Created);

public static class WorkflowGraph
{
	/// <summary>
	/// Checks that keys are unique, every dependency exists and the graph has no cycle.
	/// Returns the steps in a valid execution (topological) order.
	/// </summary>
	public static IReadOnlyList<WorkflowStep> Validate(IReadOnlyList<WorkflowStep> steps)
	{
		ArgumentNullException.ThrowIfNull(steps);

		if (steps.Count == 0)
		{
			throw new WorkflowValidationException("A workflow needs at least one step.");
		}

		var byKey = new Dictionary<string, WorkflowStep>(StringComparer.Ordinal);

		foreach (var step in steps)
		{
			if (string.IsNullOrWhiteSpace(step.Key))
			{
				throw new WorkflowValidationException("Every step needs a key.");
			}

			if (!byKey.TryAdd(step.Key, step))
			{
				throw new WorkflowValidationException($"Duplicate step key '{step.Key}'.");
			}
		}

		var remainingDependencies = new Dictionary<string, int>(StringComparer.Ordinal);
		var dependents = new Dictionary<string, List<string>>(StringComparer.Ordinal);

		foreach (var step in steps)
		{
			var distinct = step.DependsOn.Distinct(StringComparer.Ordinal).ToList();
			remainingDependencies[step.Key] = distinct.Count;

			foreach (var dependency in distinct)
			{
				if (dependency == step.Key)
				{
					throw new WorkflowValidationException($"Step '{step.Key}' cannot depend on itself.");
				}

				if (!byKey.ContainsKey(dependency))
				{
					throw new WorkflowValidationException($"Step '{step.Key}' depends on unknown step '{dependency}'.");
				}

				if (!dependents.TryGetValue(dependency, out var list))
				{
					list = [];
					dependents[dependency] = list;
				}

				list.Add(step.Key);
			}
		}

		var ready = new Queue<string>(steps.Where(s => remainingDependencies[s.Key] == 0).Select(s => s.Key));
		var ordered = new List<WorkflowStep>(steps.Count);

		while (ready.Count > 0)
		{
			var key = ready.Dequeue();
			ordered.Add(byKey[key]);

			if (!dependents.TryGetValue(key, out var children))
			{
				continue;
			}

			foreach (var child in children)
			{
				remainingDependencies[child]--;

				if (remainingDependencies[child] == 0)
				{
					ready.Enqueue(child);
				}
			}
		}

		if (ordered.Count != steps.Count)
		{
			throw new WorkflowValidationException("The workflow contains a dependency cycle.");
		}

		return ordered;
	}
}
