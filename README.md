# cloudwatch-to-newrelic

Forwards the CloudWatch Logs of every Lambda function in the account to New Relic as structured,
queryable log entries.

```
Lambda log groups (/aws/lambda/*)
   │  account-level subscription filter policy
   ▼
Amazon Data Firehose ──invoke──► processor Lambda (this repo)
   │  ◄──transformed records──┘
   ▼
New Relic EU Firehose endpoint ── failed records ──► S3 backup bucket
```

New Lambdas are picked up automatically; nothing needs to change here or in the source project.

## What lands in New Relic

Each log line becomes one New Relic log with these attributes:

| Attribute      | Source                                                                  |
| -------------- | ----------------------------------------------------------------------- |
| `message`      | Powertools `message`, or the raw line for non-JSON lines                |
| `timestamp`    | CloudWatch event timestamp (epoch ms)                                   |
| `functionName` | Powertools `function_name`, else derived from the log group name        |
| `requestId`    | Powertools `function_request_id`, or `RequestId:` in platform lines     |
| `level`        | Powertools `level` (absent for plain lines)                             |
| `environment`  | always `prod`                                                           |
| `logGroup`     | CloudWatch log group                                                    |
| `logStream`    | CloudWatch log stream                                                   |

All other Powertools fields (`service`, `cold_start`, `xray_trace_id`, custom keys, …) are passed
through as attributes. Lambda platform lines (`START`/`END`/`REPORT`) and plain prints are forwarded as
plain messages. Logs from non-Lambda log groups are dropped by the processor.

```sql
SELECT functionName, requestId, level, message FROM Log WHERE functionName = 'helsinki' SINCE 1 hour ago
```

## Repository layout

| Path                    | Purpose                                                                   |
| ----------------------- | ------------------------------------------------------------------------- |
| `app/`                  | Processor Lambda: `models.py` (pydantic event models), `entries.py` (log line → entry), `transform.py` (Firehose record → output record), `main.py` (handler) |
| `template.yaml`         | SAM template for the processor Lambda and its log group                   |
| `aws/iam-role.yml`      | Lambda execution role                                                     |
| `aws/firehose.yml`      | Firehose stream, S3 backup bucket, IAM roles, account subscription policy |
| `aws/resource-group.yml`| Tag-based resource group for the project                                  |
| `docs/superpowers/`     | Design spec and implementation plan                                       |

## Deployment

Pushing to `main` runs `.github/workflows/main.yaml`, built on
[`mb-dot-dev/reusable-workflows`](https://github.com/mb-dot-dev/reusable-workflows): SonarQube → build
Lambda ZIP → deploy resource group, IAM role, Lambda, then Firehose (stack `cloudwatch-to-newrelic-*`
in `eu-central-1`).

Prerequisite: the New Relic license key in SSM Parameter Store as a plain `String` at
`/newrelic/licence-keys/cloudwatch-to-newrelic`. CloudFormation resolves it into the Firehose
configuration; the Lambda never sees it.

## Development

```bash
make install-dev   # install dependencies (uv)
make test          # ruff, ty, pytest
make coverage      # pytest with coverage (fails under 75%)
make cfn-lint      # lint CloudFormation templates
make build-lambda-package
```

Requires [uv](https://docs.astral.sh/uv/); the Python version comes from `.python-version` (3.15).

## Operations

- **Health:** Firehose metrics `IncomingRecords`, `SucceedProcessing.Records` and
  `DeliveryToHttpEndpoint.Success` for stream `cloudwatch-to-newrelic`.
- **Failures:** records that fail processing or delivery go to the stack's backup bucket
  (`failed/` and `errors/` prefixes, expired after 30 days). Firehose errors are logged to
  `/aws/kinesisfirehose/cloudwatch-to-newrelic`; processor errors to `/aws/lambda/cloudwatch-to-newrelic`.
- **No loops:** the processor's and Firehose's own log groups are excluded from the subscription
  policy, so their logs never reach New Relic.

## Gotchas

- New Relic's Firehose endpoint keeps only the first line of a newline-delimited record, so the
  processor re-wraps entries in a gzipped CloudWatch-shaped payload instead.
- Firehose's built-in `Decompression` processor isn't supported for HTTP endpoint destinations; the
  processor Lambda gunzips the CloudWatch payloads itself.
- Python 3.15 isn't supported by Dependabot yet, so its uv update runs fail.
