# Gitlab CI 

## Skipping CI 


## Pipeline Customization

Some times it might be interesting or necessary  (see: ). To fill this purpose there are two Gitlab CI flags to archive this goal:

- `ONLY_RUN_SYSTEST`: 

  - Default value: `FALSE`
  - Allowed value: A valid image tag obtained from `ctageneric` container registry.
  - Usage: `git push -o ci.vriable="ONLY_RUN_SYSTEST="`

- `SYSTEMTESTS_LIST`: used to run only a subset of the availabe system tests. By default all tests are run. 
  - Default value: `ALL`
  - Allowed values: List, comma separated without white spaces, containing the names of the jobs to run.
  - Usage: `git push -o ci.variable="SYSTEMTESTS_LIST=client-eos5,client-gfal2-eos5"`

When any of these flags is set a dummy fail job will be created to avoid merging into main a pipeline that potentially has only run a system test for one test.   
