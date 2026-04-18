# FirstLight

**FirstLight** is a low-latency, auditable transient triage pipeline built on public alert streams to identify, report, monitor, and prioritize high-value **hostless** candidates for realistic independent follow-up.

## Current Status

- Public repository sanitized and aligned with the production workflow.
- Production TNS reporting validated with capped nightly dispatch.
- Follow-up workflow merged into `main` through **v1.3.0**.
- The project now covers both:
  - **discovery/reporting**
  - **follow-up triage and dossier preparation**
- Current next milestone: improve monitoring continuity, capture repeat activity earlier, and support the first real manual follow-up path toward classification.

## Project Scope

The public scope of the project is now:

- Real-time ingestion of public Fink/ZTF alerts.
- Normalization and niche filtering for hostless candidates.
- Anti-duplication and capped TNS reporting.
- SQLite-backed auditing and reproducibility.
- Follow-up mirroring and rescoring of submitted candidates.
- Remote follow-up prioritization for a small scientific shortlist.
- Manual observation logging, post-observation review, and dossier preparation.

## Why This Project Exists

Modern public alert streams produce more transient candidates than a single observer can realistically follow.

FirstLight is **not** meant to maximize the number of reports. Its goal is to:

1. Identify a small number of high-value hostless candidates in real time.
2. Report them with traceability.
3. Maintain a realistic shortlist of candidates worth limited follow-up resources.
4. Progress from raw reporting toward classification-oriented decision support.

## High-Level Architecture

FirstLight currently operates as:

`Fink alert stream -> AVRO capture -> Replay / Normalization -> Hostless niche filter -> SQLite decisions -> TNS dispatch -> Follow-up mirror -> Scoring -> Promotion -> Daily report -> Manual observation logging -> Post-observation review -> Dossier preparation`

![FirstLight architecture](docs/architecture.png)

Core design principles:

- **Low latency**
- **Auditability**
- **Replayability**
- **Small operational footprint**
- **Clear separation between discovery/reporting and follow-up prioritization**

## Repository Structure

```text
FirstLight/
  config/
    n1.example.yaml
    remote_followup.example.yaml
    tns.example.env

  docs/
    architecture.png
    remote_followup_strategy.md
    followup_daily_report.md
    dossiers/
      ...generated candidate dossiers

  scripts/
    run_night.ps1
    run_night_wrapper.ps1
    replay_avro_dir.py
    morning_check.ps1
    sweep_replies_once.ps1
    followup_backfill.py
    followup_sync_tns_state.py
    followup_score.py
    followup_promote.py
    followup_daily_refresh.py
    followup_daily_report.py
    followup_record_observation.py
    followup_post_observation_review.py
    followup_prepare_dossier.py

  sql/
    001_followup_schema.sql

  src/firstlight/
    __init__.py
    __main__.py
    cli.py
    niches/
      n1_hostless_fast.py
    pipeline/
      normalize.py
    storage/
      db.py
    tns/
      client.py
      dispatch.py
    utils/
      time.py

  deploy/windows/
    task.example.xml

  legacy/
    ...older or non-production paths kept for reference only
```

## Production-Relevant Components

The production-oriented discovery/reporting workflow relies mainly on:

- `scripts/run_night.ps1`
- `scripts/replay_avro_dir.py`
- `scripts/morning_check.ps1`
- `src/firstlight/cli.py`
- `src/firstlight/storage/db.py`
- `src/firstlight/tns/client.py`
- `src/firstlight/tns/dispatch.py`
- `src/firstlight/pipeline/normalize.py`
- `src/firstlight/niches/n1_hostless_fast.py`

The follow-up workflow merged into `main` relies mainly on:

- `sql/001_followup_schema.sql`
- `scripts/followup_backfill.py`
- `scripts/followup_sync_tns_state.py`
- `scripts/followup_score.py`
- `scripts/followup_promote.py`
- `scripts/followup_daily_refresh.py`
- `scripts/followup_daily_report.py`
- `scripts/followup_record_observation.py`
- `scripts/followup_post_observation_review.py`
- `scripts/followup_prepare_dossier.py`

## Environment And Configuration

Example configuration files included in the repository:

- `config/n1.example.yaml`
- `config/remote_followup.example.yaml`
- `config/tns.example.env`

Sensitive runtime files such as local databases, `.env`, `.env.prod`, logs, and operational artifacts are intentionally excluded from version control.

## TNS Notes

The project supports TNS-related actions from the CLI, including:

- Environment checks
- Payload inspection
- Minimal payload printing
- Report reply polling
- Capped dispatch from the local SQLite database

Example:

```powershell
python -m firstlight --env .env.prod tns envcheck
python -m firstlight --env .env.prod tns submit-min --print-payload
```

## Nightly Operation

Typical production-oriented usage is based on the nightly runner:

```powershell
& .\scripts\run_night.ps1 `
  -MaxHours 10 `
  -EnvFile .env.prod `
  -CfgPath .\config\n1.example.yaml `
  -DbPath .\firstlight_prod.sqlite `
  -PythonExe C:\path\to\python.exe `
  -FinkConsumerExe C:\path\to\fink_consumer.exe `
  -DispatchMaxSubmit 1 `
  -DispatchSkipReply:$true
```

A quick morning review can be done with:

```powershell
.\scripts\morning_check.ps1 -Db .\firstlight_prod.sqlite
```

## Follow-up Daily Workflow

A typical follow-up morning workflow is:

```powershell
python .\scripts\followup_daily_refresh.py --source-db .\firstlight_prod.sqlite --target-db .\firstlight_followup_prod.sqlite
python .\scripts\followup_daily_report.py --db .\firstlight_followup_prod.sqlite --cfg .\config\remote_followup.example.yaml
```

If a real manual observation exists, the operational workflow extends to:

```powershell
python .\scripts\followup_record_observation.py ...
python .\scripts\followup_score.py --db .\firstlight_followup_prod.sqlite --cfg .\config\remote_followup.example.yaml
python .\scripts\followup_promote.py --db .\firstlight_followup_prod.sqlite --cfg .\config\remote_followup.example.yaml
python .\scripts\followup_post_observation_review.py --db .\firstlight_followup_prod.sqlite --object-id <OBJ> --candid <CANDID> --cfg .\config\remote_followup.example.yaml --dry-run
python .\scripts\followup_prepare_dossier.py --db .\firstlight_followup_prod.sqlite --object-id <OBJ> --candid <CANDID> --cfg .\config\remote_followup.example.yaml
```

## Follow-up Philosophy

The follow-up layer is intentionally conservative.

It is not designed to label many candidates as actionable. It is designed to:

- keep a very small shortlist,
- penalize stale objects,
- distinguish scientific interest from operational feasibility,
- and avoid pretending that a candidate is close to classification when it is not.

This is especially important for an independent workflow with limited follow-up access.

## Roadmap

The current roadmap is:

1. Keep the hostless discovery/reporting workflow stable.
2. Improve continuity of monitoring and repeat detection coverage.
3. Build a shadow/monitoring layer that can detect repeated promising candidates before or beyond formal TNS submission.
4. Execute and document the first real manual remote follow-up attempts.
5. Use real cases to refine the escalation path from shortlist -> imaging -> spectroscopy -> dossier -> classification.
6. Accumulate a small but scientifically defensible set of follow-up outcomes.

## What This Repository Is Not

This is **not** a polished general-purpose astronomy platform, and it is **not yet** a full automated classification system.

It is an independent, evolving project focused on:

- Operational reliability
- Transparent candidate selection
- Realistic progression toward classification-oriented follow-up
- Auditable decision support for a narrow hostless transient niche

## Personal Context

FirstLight is a fully independent personal project developed out of a long-standing interest in astrophysics and time-domain astronomy.

It is being developed alongside part-time physics studies and focuses on the intersection of:

- Software engineering
- Real-time data pipelines
- Practical transient selection and follow-up strategy

## Publication Path

A formal publication path will only make sense once the current follow-up layer is exercised on real cases with documented outcomes.

At this stage, the repository should be read as:

- a real working system,
- with a validated production reporting stage,
- with a first integrated follow-up workflow,
- and with the next scientific step centered on real monitored cases and real follow-up outcomes.
