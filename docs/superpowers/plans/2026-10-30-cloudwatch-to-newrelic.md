# CloudWatch Logs → New Relic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forward every Lambda log line in the account to New Relic as a structured entry, via an
account-wide subscription → Firehose → processor Lambda (this repo) → New Relic EU endpoint.

**Architecture:** A pure-Python processor (`app/`) turns each Firehose transformation record (gzipped
CloudWatch Logs payload) into New Relic log entries. Four CloudFormation stacks: resource group, IAM
role, Lambda (SAM, via reusable `deploy-lambda`), and Firehose (stream, S3 backup, roles, account
subscription policy). CI/CD uses `mb-dot-dev/reusable-workflows`, mirroring
`helsinki-api-gateway-rest-api`.

**Tech Stack:** Python 3.15, uv, aws-lambda-powertools (Logger), pytest, ruff, ty, SAM/CloudFormation,
cfn-lint, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-30-cloudwatch-to-newrelic-design.md`

**Reference project:** `/Users/bence/repos/github.com/mb-dot-dev/helsinki-api-gateway-rest-api`
("copy from helsinki" below means this repo).

## Global Constraints

- Python `3.15` (`.python-version`, `requires-python = ">=3.15"`, Lambda `Runtime: python3.15`).
- Region `eu-central-1`, account `233927217152`.
- `PROJECT_NAME: cloudwatch-to-newrelic` → function name `cloudwatch-to-newrelic`, its log group
  `/aws/lambda/cloudwatch-to-newrelic`.
- New Relic endpoint: `https://aws-api.eu.newrelic.com/firehose/v1` (verified in New Relic docs;
  supersedes the URL guessed in the spec).
- License key: `{{resolve:ssm:/newrelic/licence-keys/cloudwatch-to-newrelic}}` (already exists as a
  `String`). Never committed, never echoed in CI.
- `environment` attribute is always `"prod"`.
- Entry attribute names: `message`, `timestamp` (epoch ms), `environment`, `functionName`, `requestId`,
  `level`, `logGroup`, `logStream`.
- No `mb-config`, no runtime config, no secrets in the Lambda.
- All AWS resources tagged `Project` (= project name) and `Name`.
- Ruff/pytest/coverage config identical to helsinki (coverage `fail_under = 75`).

## Review Focus

1. A Powertools line whose `message` is a dict or list (`logger.info({"k": 1})`) — expect `message` to
   be the JSON-stringified value, not dropped or crashing. → Task 2 test
   `test_powertools_dict_message_is_stringified`.
2. A line that is valid JSON but not an object (`"hello"`, `42`, `[1, 2]`) — expect plain-line handling,
   not a crash. → Task 2 test `test_json_non_object_is_plain_line`.
3. A Powertools line carrying keys that collide with canonical ones (custom `environment`, Powertools
   `timestamp` string) — expect canonical values to win (`environment == "prod"`, `timestamp` = CW epoch
   ms). → Task 2 test `test_canonical_keys_win_over_passthrough`.
4. A `DATA_MESSAGE` with an empty `logEvents` list — expect `Dropped`, not an empty `Ok` record.
   → Task 3 test `test_empty_log_events_dropped`.
5. A record that isn't gzip at all (someone `PutRecord`s directly to the stream) — expect
   `ProcessingFailed` for that record only, original data returned. → Task 3 test
   `test_corrupt_record_fails_alone`.

---

### Task 1: Spike — confirm the New Relic payload format (throwaway, nothing committed)

The New Relic docs don't specify how the Firehose endpoint treats already-transformed records. Decide
between two candidate encodings by sending one test request of each straight to the endpoint, shaped
like a Firehose HTTP delivery.

**Files:** scratch only (session scratchpad), e.g. `spike_nr.py`. Do not commit.

- [ ] **Step 1: Write the spike script**

```python
# Run: python -I spike_nr.py
import base64, gzip, json, subprocess, time, uuid, urllib.request

KEY = subprocess.run(
    ["aws", "ssm", "get-parameter", "--profile", "prod-admin_access_sso", "--region", "eu-central-1",
     "--name", "/newrelic/licence-keys/cloudwatch-to-newrelic", "--query", "Parameter.Value",
     "--output", "text"], check=True, capture_output=True, text=True).stdout.strip()
URL = "https://aws-api.eu.newrelic.com/firehose/v1"
now = int(time.time() * 1000)

def entry(variant: str, n: int) -> dict:
    return {"message": f"spike {variant} line {n}", "timestamp": now, "environment": "prod",
            "functionName": "spike-fn", "requestId": "spike-req", "level": "INFO", "spikeVariant": variant}

# A: newline-delimited JSON entries
a = "\n".join(json.dumps(entry("A", n)) for n in (1, 2)).encode()
# B: CloudWatch-shaped gzipped payload, each message = JSON entry
b = gzip.compress(json.dumps({
    "messageType": "DATA_MESSAGE", "owner": "233927217152", "logGroup": "/aws/lambda/spike-fn",
    "logStream": "spike", "subscriptionFilters": [],
    "logEvents": [{"id": str(n), "timestamp": now, "message": json.dumps(entry("B", n))} for n in (1, 2)],
}).encode())

for data in (a, b):
    body = json.dumps({"requestId": str(uuid.uuid4()), "timestamp": now,
                       "records": [{"data": base64.b64encode(data).decode()}]}).encode()
    req = urllib.request.Request(URL, data=body, method="POST", headers={
        "Content-Type": "application/json", "X-Amz-Firehose-Protocol-Version": "1.0",
        "X-Amz-Firehose-Request-Id": str(uuid.uuid4()), "X-Amz-Firehose-Access-Key": KEY})
    with urllib.request.urlopen(req) as resp:
        print(resp.status, resp.read().decode())
```

- [ ] **Step 2: Run it**

Run: `python -I spike_nr.py` (user may need `! aws sso login --profile prod-admin_access_sso` first)
Expected: two `200` responses.

- [ ] **Step 3: Ask the user to check New Relic**

Ask the user to run in New Relic Logs (EU): `SELECT * FROM Log WHERE message LIKE 'spike%' SINCE 30 minutes ago`.
A variant passes if it produced **two separate** entries, each with `functionName`, `requestId`,
`level`, `environment` as top-level attributes and the expected `message`.

- [ ] **Step 4: Record the decision**

Prefer A if it passes (simpler). Else B. If neither passes, stop and report to the user — do not guess a
third format. Append one line to the spec's "Processor Lambda behaviour" section stating the chosen
envelope, and commit only the spec change:

```bash
git add docs/superpowers/specs/2026-10-30-cloudwatch-to-newrelic-design.md
git commit -m "docs: record New Relic payload envelope from spike"
```

---

### Task 2: Project alignment + log entry building

Bring the skeleton in line with helsinki and implement the pure mapping from one CloudWatch log event
to one New Relic entry.

**Files:**
- Modify: `pyproject.toml` — `requires-python = ">=3.15"`; `dependencies = ["aws-lambda-powertools>=3.31.1"]`;
  dev group as helsinki minus `moto[sqs]`; add `pythonpath = ["."]` to pytest options; copy helsinki's
  `[tool.ruff*]` sections verbatim (drop helsinki's Powertools-metrics `filterwarnings`). Set
  `description`. Regenerate `uv.lock` with `uv lock`.
- Create: `.python-version` (`3.15`), `.github/dependabot.yml` (copy from helsinki),
  `sonar-project.properties` (copy from helsinki, `sonar.projectKey=mb-dot-dev_cloudwatch-to-newrelic`).
- Modify: `Makefile` — `--python "$$(cat .python-version)"` in `install-pip-ci` (as helsinki).
- Create: `app/entries.py`
- Create: `tests/__init__.py` (empty), `tests/test_entries.py`

**Interfaces:**
- Produces (`app/entries.py`):
  - `ENVIRONMENT: Final = "prod"`
  - `LogEntry = dict[str, Any]` (type alias)
  - `function_name_from_log_group(log_group: str) -> str` — `/aws/lambda/<name>` → `<name>`
  - `build_entry(event: dict[str, Any], log_group: str, log_stream: str) -> LogEntry` — `event` is one
    CloudWatch `logEvents[]` item (`id`, `timestamp`, `message`).

Mapping rules (from spec):
- Powertools line = `event["message"]` parses (`json.loads`) to a `dict`:
  - `message` ← its `message` (`json.dumps` if not a `str`); `level` ← `level`;
    `requestId` ← `function_request_id`; `functionName` ← `function_name`, else from log group.
  - every other key passed through, **except** the consumed keys `message`, `level`,
    `function_request_id`, `function_name`, `timestamp`.
- Otherwise plain line: `message` ← raw line with trailing newline stripped; `functionName` from log
  group; `requestId` from regex `RequestId: ([0-9a-f-]{36})` when it matches; no `level` key.
- Always: `timestamp` ← `event["timestamp"]`, `environment` ← `"prod"`, `logGroup`, `logStream`.
  Canonical keys are written **after** passthrough so they win. Omit `requestId`/`level` keys when
  there's no value (don't emit `None`).

- [ ] **Step 1: Align project files** as listed above, then run `uv sync` and `make lint`.
  Expected: lint passes on the existing `app/`.

- [ ] **Step 2: Write the failing tests** in `tests/test_entries.py`

```python
LOG_GROUP = "/aws/lambda/helsinki"
STREAM = "2026/10/30/[$LATEST]abc"

def ev(message: str, ts: int = 1_761_800_000_000) -> dict:
    return {"id": "1", "timestamp": ts, "message": message}

def test_function_name_from_log_group():
    assert function_name_from_log_group("/aws/lambda/helsinki") == "helsinki"

def test_powertools_line_maps_canonical_fields():
    line = json.dumps({"level": "INFO", "message": "hi", "function_name": "helsinki",
                       "function_request_id": "req-1", "service": "helsinki", "cold_start": True,
                       "timestamp": "2026-10-30 10:00:00,000+0000"})
    assert build_entry(ev(line), LOG_GROUP, STREAM) == {
        "message": "hi", "level": "INFO", "requestId": "req-1", "functionName": "helsinki",
        "service": "helsinki", "cold_start": True, "timestamp": 1_761_800_000_000,
        "environment": "prod", "logGroup": LOG_GROUP, "logStream": STREAM}

def test_powertools_without_function_name_uses_log_group():
    e = build_entry(ev(json.dumps({"level": "ERROR", "message": "x"})), LOG_GROUP, STREAM)
    assert e["functionName"] == "helsinki" and "requestId" not in e

def test_powertools_dict_message_is_stringified():
    e = build_entry(ev(json.dumps({"level": "INFO", "message": {"k": 1}})), LOG_GROUP, STREAM)
    assert e["message"] == '{"k": 1}'

def test_canonical_keys_win_over_passthrough():
    line = json.dumps({"level": "INFO", "message": "m", "environment": "staging", "logGroup": "other"})
    e = build_entry(ev(line), LOG_GROUP, STREAM)
    assert e["environment"] == "prod" and e["logGroup"] == LOG_GROUP and e["timestamp"] == 1_761_800_000_000

def test_platform_line_parses_request_id():
    line = "END RequestId: 8f507cfc-1234-4abc-9def-0123456789ab\n"
    assert build_entry(ev(line), LOG_GROUP, STREAM) == {
        "message": "END RequestId: 8f507cfc-1234-4abc-9def-0123456789ab",
        "requestId": "8f507cfc-1234-4abc-9def-0123456789ab", "functionName": "helsinki",
        "timestamp": 1_761_800_000_000, "environment": "prod", "logGroup": LOG_GROUP, "logStream": STREAM}

def test_plain_line_without_request_id():
    e = build_entry(ev("INIT_START Runtime Version: python:3.15\n"), LOG_GROUP, STREAM)
    assert "requestId" not in e and "level" not in e and e["message"].startswith("INIT_START")

@pytest.mark.parametrize("line", ['"hello"', "42", "[1, 2]", "{not json"])
def test_json_non_object_is_plain_line(line):
    assert build_entry(ev(line), LOG_GROUP, STREAM)["message"] == line
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/test_entries.py`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.entries'`

- [ ] **Step 4: Implement `app/entries.py`** per the Interfaces and mapping rules above.

- [ ] **Step 5: Run tests and lint**

Run: `make test`
Expected: all tests PASS, ruff/ty clean.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "feat: align project with helsinki and map CloudWatch events to New Relic entries"
```

---

### Task 3: Firehose record transformation + Lambda handler

**Files:**
- Create: `app/transform.py`
- Modify: `app/main.py`
- Create: `tests/conftest.py`, `tests/test_transform.py`, `tests/test_main.py`

**Interfaces:**
- Consumes: `build_entry`, `LogEntry` from Task 2.
- Produces (`app/transform.py`):
  - `encode_entries(entries: list[LogEntry], payload: dict[str, Any]) -> bytes` — the envelope chosen
    in Task 1. A: `"\n".join(json.dumps(e) for e in entries)` encoded UTF-8. B: gzipped CloudWatch-shaped
    JSON copying `messageType`, `owner`, `logGroup`, `logStream`, `subscriptionFilters` from `payload`,
    with `logEvents[i] = {"id": <original id>, "timestamp": entry["timestamp"], "message": json.dumps(entry)}`.
  - `transform_record(record: dict[str, Any]) -> dict[str, str]` — input is one Firehose record
    (`recordId`, `data` base64); returns `{"recordId", "result", "data"}` with `result` in
    `"Ok" | "Dropped" | "ProcessingFailed"`.
- Produces (`app/main.py`): `lambda_handler(event: dict[str, Any], context: LambdaContext) -> dict[str, Any]`
  returning `{"records": [transform_record(r) for r in event["records"]]}`; module-level
  `logger = Logger()`; decorated with `@logger.inject_lambda_context` (as helsinki).
- Produces (`tests/conftest.py`): fixture `make_record` → `(payload: dict | bytes, record_id: str = "r1") -> dict`
  (dict is JSON-dumped, gzipped, base64'd; bytes are base64'd as-is); `lambda_context` fixture (as helsinki).

`transform_record` rules, in order:
1. Decode base64 → gunzip → `json.loads`. Any exception → `ProcessingFailed`, `data` = original input
   data; log it with `logger.exception` including `recordId`.
2. `messageType == "CONTROL_MESSAGE"` → `Dropped`.
3. `logGroup` not starting with `/aws/lambda/` → `Dropped`.
4. Empty `logEvents` → `Dropped`.
5. Else `Ok`, `data` = base64 of `encode_entries([build_entry(e, logGroup, logStream) for e in logEvents], payload)`.

For `Dropped`/`ProcessingFailed`, return the original `data` unchanged.

- [ ] **Step 1: Write the failing tests** in `tests/test_transform.py`

```python
def payload(log_group="/aws/lambda/helsinki", events=None, message_type="DATA_MESSAGE"):
    return {"messageType": message_type, "owner": "233927217152", "logGroup": log_group,
            "logStream": "s", "subscriptionFilters": ["f"],
            "logEvents": events if events is not None else [
                {"id": "1", "timestamp": 1, "message": '{"level":"INFO","message":"a"}'},
                {"id": "2", "timestamp": 2, "message": "START RequestId: 8f507cfc-1234-4abc-9def-0123456789ab"}]}

def decoded_entries(result) -> list[dict]:
    """Decode result['data'] back to entries for the envelope chosen in Task 1."""
    ...  # A: split lines + json.loads; B: gunzip, json.loads, json.loads each logEvents[].message

def test_ok_record_has_one_entry_per_event(make_record):
    rec = make_record(payload())
    out = transform_record(rec)
    assert out["recordId"] == "r1" and out["result"] == "Ok"
    entries = decoded_entries(out)
    assert [e["message"] for e in entries] == ["a", "START RequestId: 8f507cfc-1234-4abc-9def-0123456789ab"]
    assert all(e["environment"] == "prod" and e["functionName"] == "helsinki" for e in entries)

def test_control_message_dropped(make_record):
    rec = make_record(payload(message_type="CONTROL_MESSAGE"))
    assert transform_record(rec) == {"recordId": "r1", "result": "Dropped", "data": rec["data"]}

def test_non_lambda_log_group_dropped(make_record):
    assert transform_record(make_record(payload(log_group="/aws/rds/x")))["result"] == "Dropped"

def test_empty_log_events_dropped(make_record):
    assert transform_record(make_record(payload(events=[])))["result"] == "Dropped"

def test_corrupt_record_fails_alone(make_record):
    rec = make_record(b"not gzip")
    assert transform_record(rec) == {"recordId": "r1", "result": "ProcessingFailed", "data": rec["data"]}
```

And `tests/test_main.py`:

```python
def test_handler_processes_each_record_independently(make_record, lambda_context):
    event = {"invocationId": "i", "deliveryStreamArn": "arn", "region": "eu-central-1",
             "records": [make_record(payload(), "a"), make_record(b"junk", "b"),
                         make_record(payload(message_type="CONTROL_MESSAGE"), "c")]}
    out = lambda_handler(event, lambda_context)
    assert [(r["recordId"], r["result"]) for r in out["records"]] == [
        ("a", "Ok"), ("b", "ProcessingFailed"), ("c", "Dropped")]
```

(`payload` helper: put it in `conftest.py` as a plain function or fixture so both test files share it.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/test_transform.py tests/test_main.py`
Expected: FAIL — `ImportError` for `app.transform` / `lambda_handler`.

- [ ] **Step 3: Implement `app/transform.py` and `app/main.py`** per Interfaces and rules.

- [ ] **Step 4: Run all tests, lint, coverage**

Run: `make test && make coverage`
Expected: PASS, coverage ≥ 75%.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: transform Firehose records into New Relic log entries"
```

---

### Task 4: Lambda, IAM and resource-group templates

**Files:**
- Rename: `template.yml` → `template.yaml`
- Modify: `template.yaml`
- Modify: `aws/iam-role.yml` (wording + `Name` tag only)
- Create: `aws/resource-group.yml` (copy from helsinki verbatim)
- Create: `samconfig.toml` (copy from helsinki verbatim)
- Modify: `Makefile` — `cfn-lint` target: `uv run cfn-lint aws/*.yml template.yaml`

**Interfaces:**
- Produces: stack `cloudwatch-to-newrelic-lambda` outputs `FunctionArn`, `FunctionName` (names
  required by reusable `deploy-lambda`).

`template.yaml` contents (follow helsinki's template shape, drop API Gateway / SQS parts):
- Description: "Firehose transformation Lambda that reshapes CloudWatch Logs for New Relic".
- Parameters: `ProjectName`, `FunctionName`, `LambdaExecutionRoleArn` (all `String`; these are what
  `deploy-lambda` passes).
- `Function`: `Runtime: python3.15`, `Handler: app.main.lambda_handler`, `CodeUri: lambda.zip`,
  `Timeout: 60`, `MemorySize: 128`, env `POWERTOOLS_SERVICE_NAME: !Ref FunctionName`,
  `POWERTOOLS_LOG_LEVEL: INFO`, tags `Project`/`Name`. No `Events`.
- `LogGroup`: `/aws/lambda/${FunctionName}`, `RetentionInDays: 30`, tags `Project`/`Name`.
- Outputs `FunctionArn` (`!GetAtt Function.Arn`), `FunctionName` (`!Ref FunctionName`).

`aws/iam-role.yml`: keep basic execution only; replace "consumer" wording; add `Name` tag.

- [ ] **Step 1: Make the changes above.**

- [ ] **Step 2: Lint**

Run: `make cfn-lint && make build-lambda-package && unzip -l lambda.zip | grep -E "app/(main|entries|transform).py|aws_lambda_powertools/__init__.py"`
Expected: cfn-lint clean; all four paths listed. Then `rm -rf build lambda.zip requirements.txt`.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: SAM template for the processor Lambda, resource group stack"
```

---

### Task 5: Firehose stack

**Files:**
- Create: `aws/firehose.yml`

**Interfaces:**
- Consumes: processor function ARN (Task 4 output `FunctionArn`, delivered via workflow in Task 6).
- Produces: stack `cloudwatch-to-newrelic-firehose`, output `DeliveryStreamArn`.

Parameters:
- `ProjectName` (`String`)
- `ProcessorFunctionArn` (`String`)
- `NewRelicEndpointUrl` (`String`, default `https://aws-api.eu.newrelic.com/firehose/v1`)

Resources:
- `BackupBucket` (`AWS::S3::Bucket`): public access block all true, SSE-S3, lifecycle expire after
  30 days, tags.
- `FirehoseErrorLogGroup`: `/aws/kinesisfirehose/${ProjectName}`, retention 30 days; plus
  `AWS::Logs::LogStream` `HttpDelivery` in it.
- `DeliveryRole` (trust `firehose.amazonaws.com`, condition `sts:ExternalId` = `${AWS::AccountId}`):
  `lambda:InvokeFunction`, `lambda:GetFunctionConfiguration` on `ProcessorFunctionArn`;
  `s3:AbortMultipartUpload`, `s3:GetBucketLocation`, `s3:GetObject`, `s3:ListBucket`,
  `s3:ListBucketMultipartUploads`, `s3:PutObject` on the bucket and `bucket/*`;
  `logs:PutLogEvents` on the error log group.
- `DeliveryStream` (`AWS::KinesisFirehose::DeliveryStream`), name `${ProjectName}`, `DirectPut`,
  `HttpEndpointDestinationConfiguration`:
  - `EndpointConfiguration`: `Name: NewRelic`, `Url: !Ref NewRelicEndpointUrl`,
    `AccessKey: "{{resolve:ssm:/newrelic/licence-keys/cloudwatch-to-newrelic}}"`
  - `RequestConfiguration.ContentEncoding: GZIP`
  - `BufferingHints`: `SizeInMBs: 1`, `IntervalInSeconds: 60` (New Relic docs: 1 MiB cap)
  - `RetryOptions.DurationInSeconds: 60`
  - `ProcessingConfiguration`: enabled, one `Lambda` processor with `LambdaArn`,
    `BufferSizeInMBs: 1`, `BufferIntervalInSeconds: 60`
  - `S3BackupMode: FailedDataOnly`, `S3Configuration` → bucket, `RoleARN`, prefix `failed/`,
    `CompressionFormat: GZIP`
  - `CloudWatchLoggingOptions` → error log group / stream; `RoleARN: DeliveryRole`
- `LogsToFirehoseRole` (trust `logs.amazonaws.com`, condition `aws:SourceArn` like
  `arn:aws:logs:${AWS::Region}:${AWS::AccountId}:*`): `firehose:PutRecord`, `firehose:PutRecordBatch`
  on the stream ARN.
- `SubscriptionPolicy` (`AWS::Logs::AccountPolicy`): `PolicyName: ${ProjectName}`,
  `PolicyType: SUBSCRIPTION_FILTER_POLICY`, `Scope: ALL`,
  `PolicyDocument` = JSON string `{"DestinationArn": <stream arn>, "RoleArn": <LogsToFirehoseRole arn>, "FilterPattern": "", "Distribution": "Random"}` (build with `!Sub`),
  `SelectionCriteria: 'LogGroupName NOT IN ["/aws/lambda/${ProjectName}", "/aws/kinesisfirehose/${ProjectName}"]'`.
  Check the property names against the current `AWS::Logs::AccountPolicy` docs; if cfn-lint rejects
  them, fix to match the docs, don't drop the exclusions.
- All taggable resources: `Project`, `Name`.

- [ ] **Step 1: Write `aws/firehose.yml`** as above.

- [ ] **Step 2: Lint**

Run: `make cfn-lint`
Expected: no errors.

- [ ] **Step 3: Commit**

```bash
git add aws/firehose.yml
git commit -m "feat: Firehose stack with New Relic destination and account subscription policy"
```

---

### Task 6: CI/CD on reusable workflows

**Files:**
- Modify: `.github/workflows/main.yaml` (replace jobs; keep `name`, `run-name`, `on`, `concurrency`,
  `env.PROJECT_NAME: cloudwatch-to-newrelic`, `permissions`)

**Interfaces:**
- Consumes: reusable workflows at `mb-dot-dev/reusable-workflows/.github/workflows/*.yaml@main`;
  `deploy-lambda` output `function-arn`; `deploy-cloudformation` input names `stack-name`, `template`,
  `project`, `parameter-overrides`.

Jobs (copy helsinki's job shapes; drop unused `jwt_*` outputs from `prepare`):
- `prepare` (push only, outputs `project_name`)
- `sonarqube` → `python-test-sonar.yaml`, `secrets: SONAR_TOKEN`
- `build-lambda-zip` (needs `sonarqube`) → `build-lambda-zip.yaml`
- `cfn-lint` → `cfn-lint.yaml`
- `deploy-resource-group` (needs `prepare`, `cfn-lint`) → `deploy-cloudformation.yaml`,
  stack `${project}-resource-group`, template `aws/resource-group.yml`
- `deploy-iam-role` (needs `prepare`, `cfn-lint`) → stack `${project}-iam-role`, template
  `aws/iam-role.yml`, overrides `ProjectName=<project>,RoleName=<project>-lambda-execution`
  (keep the existing role name to avoid replacing the deployed role)
- `deploy-lambda` (needs `prepare`, `build-lambda-zip`, `deploy-iam-role`, `cfn-lint`) →
  `deploy-lambda.yaml`, `lambda-execution-role-arn` from iam stack outputs (as helsinki),
  `secrets: DEPLOY_ROLE_ARN`
- `deploy-firehose` (needs `prepare`, `deploy-lambda`) → `deploy-cloudformation.yaml`,
  stack `${project}-firehose`, template `aws/firehose.yml`,
  overrides `ProjectName=<project>,ProcessorFunctionArn=${{ needs.deploy-lambda.outputs.function-arn }}`,
  `secrets: inherit`

The user must add the `SONAR_TOKEN` repo secret and create the SonarCloud project
`mb-dot-dev_cloudwatch-to-newrelic` if they don't exist — tell them; don't attempt it.

- [ ] **Step 1: Rewrite `main.yaml`** as above.

- [ ] **Step 2: Validate YAML**

Run: `uv run python -I -c "import yaml,sys; yaml.safe_load(open('.github/workflows/main.yaml'))" && echo ok`
(if PyYAML isn't available, `uvx --from pyyaml python -I -c ...`)
Expected: `ok`. Also eyeball that every `needs.*.outputs.*` reference names a job listed in that job's `needs`.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/main.yaml
git commit -m "ci: deploy via reusable workflows, add Firehose stack"
```

---

### Task 7: Deploy and verify end to end (with the user)

Deployment happens on push to `main` (the `prepare` job gates on `push`). Merging is the user's call.

- [ ] **Step 1:** Push the branch, open a PR, confirm the PR run passes `sonarqube`, `build-lambda-zip`,
  `cfn-lint`. Ask the user to merge.
- [ ] **Step 2:** After merge, confirm all deploy jobs succeed:
  `gh run watch` on the `main` run. Expected: `deploy-firehose` green.
- [ ] **Step 3:** Confirm the policy exists:
  `aws logs describe-account-policies --policy-type SUBSCRIPTION_FILTER_POLICY --profile prod-admin_access_sso --region eu-central-1`
  Expected: one policy named `cloudwatch-to-newrelic` with the two exclusions.
- [ ] **Step 4:** Ask the user to call helsinki once, then wait ~2 minutes and run in New Relic:
  `SELECT functionName, requestId, level, environment, message FROM Log WHERE functionName = 'helsinki' SINCE 10 minutes ago`
  Expected: Powertools lines with all four attributes; `START`/`END`/`REPORT` lines with `requestId`.
- [ ] **Step 5:** Confirm no loop: `SELECT count(*) FROM Log WHERE functionName = 'cloudwatch-to-newrelic' SINCE 10 minutes ago` → `0`.
- [ ] **Step 6:** Confirm nothing failed: `aws s3 ls s3://<backup bucket>/failed/ --recursive --profile prod-admin_access_sso` → empty.
