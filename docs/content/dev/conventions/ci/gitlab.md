# GitLab CI Conventions

- Job definitions MUST be placed in `.gitlab/ci/<stage>.gitlab-ci.yaml`.
- A single `.gitlab/ci/*.gitlab-ci.yaml` file MUST NOT contain jobs from multiple unrelated stages.
- Jobs MUST declare `needs`. If a job has no dependencies, it MUST specify an empty list (`needs: []`).
    - This ensures correct DAG positioning and prevents unnecessary artifact downloads.
- Job names MUST be lowercase and use only alphanumeric characters and hyphens (`example-job`).
    - Slashes (`/`), colons (`:`), or spaces MAY be used only when required to [group jobs together](https://docs.gitlab.com/ee/ci/jobs/#group-jobs-in-a-pipeline).
- Scripts in job definitions MUST list each line as a separate YAML array entry (not using `- |`), except for short conditional blocks where `- |` improves readability.
- Secrets MUST NOT be placed in job definitions. They MUST be defined as GitLab CI/CD variables and referenced securely.
- Global pipeline variables MUST be defined in `.gitlab-ci.yaml`.
    - Public variables MUST include a description and explicit `value`.
    - If the set of valid values is limited, `options` MUST be provided.
- Jobs MUST use long-form flags (e.g., `--namespace`) when invoking scripts.
- By default, jobs MUST run `on_success` unless explicitly justified otherwise.
- Similar jobs SHOULD be grouped into the same stage.
- Jobs within a file SHOULD be grouped logically: general/default jobs toward the top, edge-case/specific jobs toward the bottom.
- Jobs SHOULD reference container images via a global variable, allowing central updates.
- Excessive inline scripting in job definitions SHOULD be avoided. Complex logic SHOULD be extracted into standalone scripts.
- Jobs SHOULD avoid installing dependencies at runtime. Required tools SHOULD be baked into the CI image instead (see [ci/cta-ci-images](https://gitlab.cern.ch/cta/ci/cta-ci-images) repo).

!!! tip

    - Keep if-statements in job scripts short and focused when using `- |` for readability.
    - Add comments in CI configuration files to explain non-obvious job behavior or structure.
    - Prefer caching strategies or prebuilt images over repeated downloads or installations for efficiency.
