using JobEngine.Workflows;
using Xunit;

namespace JobEngine.Tests;

public class WorkflowGraphTests
{
	private static WorkflowStep Step(string key, params string[] dependsOn) =>
		new() { Key = key, Type = key, DependsOn = dependsOn };

	[Fact]
	public void Validate_ReturnsStepsInDependencyOrder()
	{
		var ordered = WorkflowGraph.Validate([Step("d", "b", "c"), Step("b", "a"), Step("c", "a"), Step("a")]);
		var keys = ordered.Select(s => s.Key).ToList();

		Assert.Equal("a", keys[0]);
		Assert.Equal("d", keys[^1]);
		Assert.True(keys.IndexOf("b") < keys.IndexOf("d"));
		Assert.True(keys.IndexOf("c") < keys.IndexOf("d"));
	}

	[Fact]
	public void Validate_AcceptsIndependentSteps()
	{
		Assert.Equal(2, WorkflowGraph.Validate([Step("a"), Step("b")]).Count);
	}

	[Fact]
	public void Validate_ToleratesRepeatedDependencies()
	{
		Assert.Equal(2, WorkflowGraph.Validate([Step("a"), Step("b", "a", "a")]).Count);
	}

	[Fact]
	public void Validate_RejectsCycles()
	{
		var error = Assert.Throws<WorkflowValidationException>(
			() => WorkflowGraph.Validate([Step("a", "c"), Step("b", "a"), Step("c", "b")]));

		Assert.Contains("cycle", error.Message);
	}

	[Fact]
	public void Validate_RejectsSelfDependency()
	{
		Assert.Throws<WorkflowValidationException>(() => WorkflowGraph.Validate([Step("a", "a")]));
	}

	[Fact]
	public void Validate_RejectsUnknownDependency()
	{
		var error = Assert.Throws<WorkflowValidationException>(() => WorkflowGraph.Validate([Step("a", "ghost")]));

		Assert.Contains("ghost", error.Message);
	}

	[Fact]
	public void Validate_RejectsDuplicateKeys()
	{
		Assert.Throws<WorkflowValidationException>(() => WorkflowGraph.Validate([Step("a"), Step("a")]));
	}

	[Fact]
	public void Validate_RejectsEmptyWorkflows()
	{
		Assert.Throws<WorkflowValidationException>(() => WorkflowGraph.Validate([]));
	}
}
