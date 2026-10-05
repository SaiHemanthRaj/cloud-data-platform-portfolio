# Cloud and orchestration boundaries

## S3 mirror

The adapter is exercised with botocore Stubber and a fake recorder. Those tests prove request shape,
conditional collision handling, checksum gates, and manifest order, not AWS permissions or deployment.
Only source/curated/quarantine files and the committed manifest are mirrored. Spark outputs and SQLite
canonical state remain local.

Use a dedicated synthetic-data bucket. The publishing identity needs `s3:PutObject` and `s3:GetObject`
on `arn:aws:s3:::YOUR_BUCKET/objects/*`; it does not need bucket administration. The Terraform operator,
if you choose to provision, needs separate administrative permissions. Never commit keys.

The SDK retries transient errors. A 412 response triggers an exact content comparison; a collision fails.
Other errors propagate. A manifest is written only after all local checksums validate and object writes succeed.
An interrupted publish can be safely re-run from the committed local journal.

## Terraform scaffold: not applied

The starter's bucket resource now includes a public-access block, AES256 encryption, versioning,
and seven-day expiration for current/noncurrent demo objects. Terraform is not installed in the verified
environment, so neither validate nor apply is reported as passed. A real operator should run:

```bash
cd terraform
terraform init
terraform fmt -check
terraform validate
terraform plan -var='bucket_name=YOUR_UNIQUE_SANDBOX_BUCKET'
```

Review the plan before an apply. Lifecycle configuration is eventual, not immediate cleanup; nonempty
versioned buckets require explicit object/version cleanup before destruction. No estimated dollar cost is
claimed, and auto-deletion of resources is not implemented.

## Airflow scaffold: not executed

`airflow/retail_events_dag.py` targets Airflow 2.10 decorators. It is syntactically compiled only;
an Airflow scheduler/DAG run has not been tested. Put the repository on the worker's PYTHONPATH,
install the AWS optional requirements, set PIPELINE_SOURCE, PIPELINE_ROOT, AWS_S3_BUCKET and AWS_REGION,
then deploy into your own Airflow environment. It chains ingestion to S3 publication and retries tasks.

The scaffold assumes a shared durable local filesystem between workers. That does not hold for many
distributed executors. Replace the local journal/state design before advertising distributed orchestration.
The Spark job is intentionally run separately and is not part of this DAG.

## Starter changes

The unfinished dbt/Snowflake layer was moved into a complete, focused warehouse project. The original
flat fact projection, unconstrained dependency list, one-column test, and ingestion-only Dockerfile were
replaced. The original five-row sales fixture remains as provenance, but does not feed the new pipeline.
