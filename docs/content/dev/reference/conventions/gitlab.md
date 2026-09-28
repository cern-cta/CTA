# GitLab CI Conventions

Follow these conventions when adding or changing CI jobs. For running pipelines and investigating their results, see [CI Pipelines](../testing/ci/pipelines.md). Keep explanations of individual jobs and non-obvious implementation choices beside their YAML or scripts.

## Configuration layout

- Keep pipeline inputs, shared variables, workflow rules, and the stage list in `.gitlab-ci.yml`. Place job definitions in `.gitlab/ci/*.gitlab-ci.yml`.
- Each job-definition file MUST contain jobs from only one stage. Its filename should describe that group. Reuse an existing stage when it fits the job's purpose.
- Job names MUST use lowercase alphanumeric characters and hyphens (`example-job`). Grouped or generated jobs may use separators such as colons, slashes, or spaces.
- Group related jobs together, with shared templates before their specializations.
- Job keys MUST follow the order enforced by `ci/checks/check_gitlab_ci_key_order.py`. Use a nearby job as a starting point; the checker reports the expected order when it finds a mismatch.

## Dependencies and merge-train guard

- Jobs MUST declare dependencies through `needs`, directly or through a shared template. Use `needs: []` only when the job has no prerequisites.
- Use stages to group jobs and `needs` to let jobs start as soon as their dependencies are ready.
- Include dependencies whose artifacts the job consumes. Use `artifacts: false` for dependencies needed only for execution order.
- Jobs participating in merge-train validation MUST include an optional dependency on `check-merge-train-duplicate` so its result is available. The dependency is optional because the check is absent from other pipeline types.
- Preserve the default `before_script`, which runs the duplicate-pipeline guard. When overriding it, reference the guard before performing setup, either directly or through a guarded shared template.

For example, a validation job with its own setup should include:

```yaml
example-job:
  stage: validate
  needs:
    - job: check-merge-train-duplicate
      optional: true
  before_script:
    - !reference [.merge-train-guard, before_script]
    - ./ci/example-setup.sh
  script:
    - ./ci/example-check.sh
```

The script names above are illustrative. Add any other dependencies needed by the job. Jobs in stages excluded from merge-train pipelines do not need the guard; the current exceptions are recorded in `ci/checks/check_gitlab_ci_before_script_guard.py`. The duplicate-detection logic belongs in `ci/checks/merge_train_guard.py`; see [CI Pipelines](../testing/ci/pipelines.md#merge-train-validation) for interpreting its results.

## Inputs, images, and scripts

### Pipeline configuration

- Define user-facing settings in `spec.inputs` in `.gitlab-ci.yml`, with a description, type, and appropriate default. Provide `options` when valid values are limited.
- Keep internal variables distinct from inputs. Define shared values centrally rather than repeating them across jobs.
- Secrets MUST NOT appear in job definitions, logs, or artifacts. Supply them through GitLab CI/CD variables or the relevant secret store.
- Choose `rules` for the pipeline sources and inputs the job supports. Jobs SHOULD run on success unless another behaviour is needed.
- Make manual execution and `allow_failure` intentional. Explain non-obvious choices beside the job so a successful pipeline does not misleadingly imply that a required check ran and passed.

### Execution images and scripts

- Reference shared execution images through central image variables. Dockerfiles live under `ci/docker/pipeline/` and the platform directories in `ci/docker/cta/`; follow [Add or update an image dependency](../../contributing/maintainers/ci-maintenance.md#add-or-update-an-image-dependency) when changing their dependencies.
- Install general-purpose tools and stable, shared dependencies **when building the CI image**. Pinned images let package updates be tested before adoption.
- Avoid repeated package installation and broad package upgrades at runtime: they slow down jobs and allow repository updates to break otherwise unchanged CI runs.
- Install dependencies **at job runtime** when their versions belong to the selected source revision or are deliberately varied by a test, such as Python requirements or regression-test dependencies.
- Use declared requirements or version constraints for runtime installations, and explain non-obvious exceptions beside the job. Caching improves speed but does not control dependency versions.
- Keep job definitions focused on invoking scripts. Extract complex logic into reusable scripts beside the relevant tooling.
- Use separate YAML array entries for separate commands. Use block scalars for short conditional blocks or commands where they improve readability.
- Prefer long-form flags when the invoked tool supports them, so the job's intent is clear.

### Reports and artifacts

- Publish test and code-quality reports in GitLab's supported report formats where practical.
- Collect diagnostic logs on failure, using an appropriate `artifacts:when` setting.
- Set a retention period suited to the outputs, and keep artifacts focused on useful results and debugging evidence.

## Validate changes

Run the repository's CI convention checks through [Pre-commit Hooks](../tools-and-environment/pre-commit.md):

```sh
pre-commit run gitlab-ci --all-files
```

This checks job key order, one stage per file, and preservation of the merge-train guard in explicit `before_script` definitions. The `lint-yaml` CI job runs these same checks alongside YAML linting. These checks do not validate every convention or the complete pipeline behaviour.

For changes to job selection or dependencies, inspect the resulting pipeline for the relevant source and inputs. Confirm that intended jobs are included, dependencies are available, and required checks block on failure.
