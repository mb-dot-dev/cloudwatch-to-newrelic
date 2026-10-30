# CloudWatch Logs → New Relic via Firehose — Design

Date: 2026-10-30

## Intent

The account runs a handful of low-traffic Lambda functions (well inside the Lambda free tier). All of
them log with AWS Lambda Powertools structured JSON. The goal is to see those logs in New Relic as
structured, queryable entries — not raw CloudWatch blobs — with a consistent set of attributes to filter
and group by.

Firehose can deliver to New Relic directly, but the raw CloudWatch payload is gzipped batches with
CloudWatch envelope fields. A processor Lambda (this repo) reshapes each log line into a clean New Relic
log entry and adds metadata.

`helsinki-api-gateway-rest-api` is one of the functions whose logs will be forwarded, and is also the
reference project for conventions (see "Conventions").

## Goals

- Every log line from every Lambda in the account lands in New Relic as its own entry.
- Each entry carries: `functionName`, `environment`, `requestId`, `level`, `message`, and the original
  timestamp.
- New Lambdas are forwarded automatically — no change to this repo or the source repo needed.
- Built, tested and deployed the same way as the other `mb-dot-dev` Lambda projects.

## Non-goals

- Metrics or traces — logs only.
- Multiple environments. Only `prod` exists; `environment` is hardcoded to `prod`.
- Non-Lambda log groups (see "Account-wide subscription" for how they're handled).
- High-throughput tuning; volume is tiny.

## Anti-goals

- A forwarding loop: the processor's own logs must never be fed back through the pipeline.
- One malformed line causing a whole batch to be dropped or retried forever.
- The New Relic license key committed to the repo or printed in GitHub logs. (The user is the only person
  with access to the AWS account, so the key living in a plain SSM `String` and in the Firehose config is
  acceptable.)

## Current account state (checked 2026-10-30)

- Account `233927217152`; all 5 Lambda functions are in eu-central-1, so a single-region design covers
  everything.
- No existing account-level subscription filter policy and no log-group subscription filters on
  `/aws/lambda/*` in eu-central-1 — nothing to conflict with.
- The GitHub deploy role (`DEPLOY_ROLE_ARN`, defined in
  `gitea.molnarbence.dev/aws-integration/cfdeploy-iam/templates/iam-role.yml`) already allows
  `firehose:*`, `s3:*`, `logs:*`, `iam:*`, `ssm:*`, `lambda:*`, `cloudformation:*`,
  `resource-groups:*` — no changes needed.

## Architecture

```
Lambda log groups (/aws/lambda/*)
   │  account-level subscription filter policy
   ▼
Kinesis Data Firehose ──invoke──► processor Lambda (this repo)
   │  ◄──transformed records──┘
   ▼
New Relic Firehose log endpoint (HTTP endpoint destination)
   │ failed deliveries / failed transforms
   ▼
S3 backup bucket
```

### Account-wide subscription

- One `AWS::Logs::AccountPolicy` of type `SUBSCRIPTION_FILTER_POLICY`, destination = the Firehose stream,
  empty filter pattern (forward everything).
- Selection criteria **must exclude** `/aws/lambda/cloudwatch-to-newrelic` (the processor's own log
  group) to prevent a loop. Also exclude any log group Firehose itself writes errors to, if error logging
  is enabled.
- Account subscription policies only support exclusion (`LogGroupName NOT IN [...]`), not prefix
  inclusion, so non-Lambda log groups will also reach Firehose. The processor drops records whose log
  group doesn't start with `/aws/lambda/` (marks them `Dropped`). *(Builder's call — settled here as the
  default; verify the selection-criteria syntax at implementation time.)*
- Needs an IAM role that CloudWatch Logs (`logs.amazonaws.com`) assumes to `firehose:PutRecord*` on the
  stream.

### Processor Lambda behaviour

Input: a Firehose transformation event. Each record's `data` is base64 of a gzipped CloudWatch Logs
subscription payload (`messageType`, `logGroup`, `logStream`, `logEvents[]`).

Per record:

1. Base64-decode, gunzip, parse JSON.
2. `messageType == "CONTROL_MESSAGE"` → result `Dropped`.
3. `logGroup` not under `/aws/lambda/` → result `Dropped`.
4. For each `logEvents[]` item, build one New Relic log entry:
   - **Powertools JSON line** (message parses as a JSON object):
     - `message` ← Powertools `message` (stringify if it's not a string)
     - `level` ← Powertools `level`
     - `requestId` ← Powertools `function_request_id`
     - `functionName` ← Powertools `function_name`, falling back to the log group name suffix
     - `timestamp` ← the CloudWatch event `timestamp` (epoch ms)
     - `environment` ← `"prod"`
     - every other Powertools field passed through as-is as an attribute (`service`, `location`,
       `xray_trace_id`, `cold_start`, custom keys, exception fields, …)
   - **Non-JSON line** (platform `START`/`END`/`REPORT`/`INIT_START`, stray prints):
     - `message` ← the raw line
     - `functionName` ← log group name suffix (`/aws/lambda/<name>` → `<name>`)
     - `requestId` ← parsed from `RequestId: <uuid>` when present
     - `level` ← omitted
     - `timestamp`, `environment` as above
   - Also attach `logGroup` and `logStream` as attributes.
5. Pack all entries from the record into one output record (Firehose requires exactly one output record
   per input `recordId`), base64-encode, result `Ok`.
6. Any exception while processing a single record → that record gets `ProcessingFailed` (Firehose sends
   it to the S3 backup); other records in the batch are unaffected.

The exact payload shape the New Relic Firehose endpoint expects for already-transformed records (JSON
array vs. newline-delimited JSON vs. a `{"logs": [...]}` wrapper, and attribute naming such as
`timestamp` units) **must be verified against New Relic's docs or with a quick real send before writing
the encoder**. The attribute set above is fixed; the envelope is left to the builder.

The processor uses Powertools `Logger` for its own logging (consistent with other projects), but those
logs are excluded from forwarding. It needs no secrets and no runtime config — no `mb-config`.

### Firehose stack

Own CloudFormation template (e.g. `aws/firehose.yml`), deployed via the reusable
`deploy-cloudformation` workflow after the Lambda is deployed. Contains:

- `AWS::KinesisFirehose::DeliveryStream`, type `DirectPut`, `HttpEndpointDestinationConfiguration`:
  - endpoint URL = New Relic **EU** Firehose logs endpoint (the New Relic account is in the EU data
    centre): `https://aws-api.eu.newrelic.com/firehose/v1` (verified in New Relic docs). Template
    parameter with the EU URL as default. New Relic recommends GZIP content encoding and a 1 MiB
    buffer (1 MB per-request cap).
  - authentication via `EndpointConfiguration.AccessKey` =
    `{{resolve:ssm:/newrelic/licence-keys/cloudwatch-to-newrelic}}`. The parameter
    (`arn:aws:ssm:eu-central-1:233927217152:parameter/newrelic/licence-keys/cloudwatch-to-newrelic`)
    already exists as a `String` (created 2026-10-30 from 1Password) so CloudFormation can resolve it —
    no Secrets Manager, no secret passing through CI. The Lambda never sees the key.
  - `ProcessingConfiguration` with a `Lambda` processor = the processor function ARN (passed in from the
    `deploy-lambda` job output)
  - `S3BackupMode: FailedDataOnly` + S3 configuration
  - buffering and retry: sensible small defaults (builder's call)
- S3 backup bucket with a lifecycle expiry (builder picks retention; e.g. 30 days to match log
  retention), public access blocked, encryption on.
- Firehose delivery role: invoke the processor Lambda, write to the bucket.
- CloudWatch Logs → Firehose role (for the account policy).
- The account subscription filter policy.
- All resources tagged `Project` / `Name`.

## Conventions (follow `helsinki-api-gateway-rest-api`)

The repo is currently a template skeleton (empty handler, leftover "consumer"/"SQS" wording,
`moto[sqs]`, hand-written deploy steps that only deploy the IAM stack). Align it with helsinki:

- Python 3.15, `uv`, ruff config (same rule set), `ty`, pytest + coverage (fail under 75), Powertools.
- `template.yaml` (rename from `template.yml`), `samconfig.toml` (bucket `molnarbence-prod-lambda-code`,
  eu-central-1), `sonar-project.properties`, `.python-version`, dependabot.
- Template: `ProjectName`, `FunctionName`, `LambdaExecutionRoleArn` parameters (as `deploy-lambda`
  passes them); explicit `LogGroup` with 30-day retention; `POWERTOOLS_SERVICE_NAME`,
  `POWERTOOLS_LOG_LEVEL` env vars; `Project`/`Name` tags. Handler `app.main.lambda_handler`.
  Timeout/memory: builder's call (Firehose invocation, small batches).
- `aws/resource-group.yml` copied from helsinki.
- `aws/iam-role.yml`: basic execution role only (no SSM/KMS needed).
- `.github/workflows/main.yaml` rebuilt on `mb-dot-dev/reusable-workflows`: `prepare`, `sonarqube`,
  `build-lambda-zip`, `cfn-lint`, `deploy-resource-group`, `deploy-iam-role`, `deploy-lambda`, then
  `deploy-firehose` (`deploy-cloudformation`, needs `deploy-lambda` for the function ARN).
- `PROJECT_NAME: cloudwatch-to-newrelic` → function name and log group
  `/aws/lambda/cloudwatch-to-newrelic`.
- Makefile `cfn-lint` target covers `aws/*.yml template.yaml`.

## Testing

- Unit tests for the processor with fixture events: Powertools JSON lines, platform lines, a control
  message, a non-Lambda log group, a corrupt record (→ `ProcessingFailed` without affecting siblings),
  and multi-event records.
- cfn-lint on all templates.
- Manual end-to-end check after first deploy: invoke helsinki, confirm entries in New Relic with the
  expected attributes, confirm the processor's own logs are not there.

## Decisions made by Claude (not explicitly confirmed)

- Non-JSON lines are forwarded as plain messages rather than dropped.
- Non-Lambda log groups are dropped in the processor (consequence of account-policy exclusion-only
  selection).
- Per-record `ProcessingFailed` instead of failing the whole invocation.
- Failed data backed up to S3 (`FailedDataOnly`).
- `logGroup` / `logStream` added as extra attributes.
- Attribute names in camelCase (`functionName`, `requestId`) as the user wrote them.
- Python 3.15 (bumped from the skeleton's 3.14) to match helsinki.
- Firehose stack deployed after `deploy-lambda`, taking the function ARN from its output.

## Left to the builder

- New Relic payload envelope (verify first, see above — plan Task 1 is a spike for this).
- Firehose buffering/retry values, Lambda timeout/memory, bucket retention.
- Module layout inside `app/`.
