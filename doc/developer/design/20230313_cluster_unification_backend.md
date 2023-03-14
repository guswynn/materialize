- Feature name: Cluster Unification, Backend
- Associated: <https://github.com/MaterializeInc/materialize/issues/17413>

# Summary
[summary]: #summary

Unify the storage and compute timely clusters, and the controllers that interact with them. This design
document does not yet propose change the sql-level policy separating storage and compute objects into
separate clusters.

# Motivation
[motivation]: #motivation

The motivation for this change is not product-level, instead being a technical decision. Compute and storage
perform their respective tasks in the same way: scheduling and managing (through their controllers) dataflows
on remote timely clusters. They have similar code to do the this, but their separate implementations
have drifted apart, as they fix different bugs/problems in different ways, and add different features. We wish
to consolidate these implementations, with a well-considered set of features, to reduce skew and duplicated
work going forward.

Additionally, storage wishes to have _introspection sources_ (see
<https://github.com/MaterializeInc/materialize/pull/17858> for current art), without duplicating the
compute implementation.

# Explanation
[explanation]: #explanation

The compute and storage servers, and the controllers that interact with them have complex, subtle behavior,
and as such, represents a risky set of changes. Ordinarily, we discuss the safety of a design in the
[rollout](#rollout) section, but the scale of risk begs us to discuss this in this section. The unification
of storage and compute will consist of 4 _implementation phases_, with an _evaluation phase_ between each.

An _evaluation phase_ consists of _quantitative and qualitative analysis_ of the state of the codebase.
- _Quantitative analysis_ involves release qualification, nightlies, and experience in production, for
  phases that can be gated behind LaunchDarkly features.
- _Qualitative analysis_ is the storage and compute team determing how _comfortable they feel_ about the
  phase's changes, after implementation. This is to ensure both codebases that are merging remain
  maintainble by its respective team.

After each _evaluation phase_

The _implementation phases_ follow. More details about each will be discussed in
[the reference explanation](#reference-explanation). In this document:

- "consolidate X" means ensuring compute and storage share codepaths for X, but may be run as separate tasks
- "merge X" means ensuring compute and storage manage the X resource in the same place, but may still
  perform different logic.

## Consolidate timely servers

Both compute and storage require consistently distributing new commands to individual workers. The
way they do this has diverged:

- Compute uses a [custom dataflow](https://github.com/MaterializeInc/materialize/blob/main/src/compute/src/server.rs#L237)
  that distributes work from worker 0.
  - Currently, the compute controller ensures only worker 0 receives top level commands.
- Storage uses a [timely Sequencer](https://docs.rs/timely/latest/timely/synchronization/sequence/struct.Sequencer.html) to
  distribute work from worker 0.
  - The storage controller assumes all workers are interested in every command, but all but worker 0 drop new commands on
  on the server-side.
  - Additionally, the storage controller has a channel connected to a side tokio-task, that is used to allow some commands
  to perform asynchronous work before being distributed.

These will be consolidated to use storage's approach, as timely `Sequencer`s are a well-understood abstraction.
The code in storage that sets up the `Sequencer`, the async task, and various channels will be factored out into a
common library (in the `mz_cluster` crate), and used by both storage and compute. Note that compute will not make use
of the provided async task.


## Consolidate logging dataflows


## Consolidate controller replica handling


## Merge command stream and grpc clients


## Merge controller state


## Consolidate controller state


Explain the design as if it were part of Materialize and you were teaching the team about it.
This can mean:

- Introduce new named concepts.
- Explain the feature using examples that demonstrate product-level changes.
- Explain how it builds on the current architecture.
- Explain how engineers and users should think about this change, and how it influences how everyone uses the product.
- If needed, talk though errors, backwards-compatibility, or migration strategies.
- Discuss how this affects maintainability, or whether it introduces concepts that might be hard to change in the future.

# Reference explanation
[reference-explanation]: #reference-explanation

Focus on the implementation of the feature.
This is the technical part of the design.

- Is it reasonably clear how the feature is implemented?
- What dependencies does the feature have and introduce?
- Focus on corner cases.
- How can we test the feature and protect against regressions?

# Rollout
[rollout]: #rollout

Describe what steps are necessary to enable this feature for users.
How do we validate that the feature performs as expected? What monitoring and observability does it require?

## Testing and observability
[testing-and-observability]: #testing-and-observability

Testability and explainability are top-tier design concerns!
Describe how you will test and roll out the implementation.
When the deliverable is a refactoring, the existing tests may be sufficient.
When the deliverable is a new feature, new tests are imperative.

Describe what metrics can be used to monitor and observe the feature.
What information do we need to expose internally, and what information is interesting to the user?
How do we expose the information?

Basic guidelines:

* Nearly every feature requires either Rust unit tests, sqllogictest tests, or testdrive tests.
* Features that interact with Kubernetes additionally need a cloudtest test.
* Features that interact with external systems additionally should be tested manually in a staging environment.
* Features or changes to performance-critical parts of the system should be load tested.

## Lifecycle
[lifecycle]: #lifecycle

If the design is risky or has the potential to be destabilizing, you should plan to roll the implementation out behind a feature flag.
List all feature flags, their behavior and when it is safe to change their value.
Describe the [lifecycle of the feature](https://www.notion.so/Feature-lifecycle-2fb13301803b4b7e9ba0868238bd4cfb).
Will it start as an alpha feature behind a feature flag?
What level of testing will be required to promote to beta?
To stable?

# Drawbacks
[drawbacks]: #drawbacks

Why should we *not* do this?

# Conclusion and alternatives
[conclusion-and-alternatives]: #conclusion-and-alternatives

- Why is the design the best to solve the problem?
- What other designs have been considered, and what were the reasons to not pick any other?
- What is the impact of not implementing this design?

# Unresolved questions
[unresolved-questions]: #unresolved-questions

- What questions need to be resolved to finalize the design?
- What questions will need to be resolved during the implementation of the design?
- What questions does this design raise that we should address separately?

# Future work
[future-work]: #future-work

Describe what work should follow from this design, which new aspects it enables, and how it might affect individual parts of Materialize.
Think in larger terms.
This section can also serve as a place to dump ideas that are related but not part of the design.

If you can't think of any, please note this down.
