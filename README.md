# FirstLight

**FirstLight** is a low-latency, auditable transient triage pipeline built on public alert streams to identify, report and prioritize high-value **hostless** candidates for follow-up.

The current public focus of the project is:

- real-time ingestion of public Fink/ZTF alerts
- normalization + niche filtering for hostless candidates
- anti-duplication and TNS reporting
- SQLite-backed auditing and reproducibility
- transition from pure first-report automation to **follow-up prioritization**

## Current status

- public repository sanitized and aligned with the production workflow
- first successful **production** TNS report achieved
- nightly capped reporting validated
- next milestone: prioritization of hostless candidates for independent follow-up and eventual classification

## Why this project exists

Modern public alert streams produce more transient candidates than a single observer can realistically follow.

FirstLight is not meant to maximize the number of reports.  
Its goal is to:

1. identify a small number of high-value hostless candidates in real time
2. report them with traceability
3. prioritize the subset that is realistically followable and classifiable with limited independent resources

## High-level architecture

FirstLight currently operates as:

`Fink alert stream -> AVRO capture -> replay/normalization -> niche filter -> SQLite decisions -> TNS dispatch -> auditing`

Core design principles:

- **low latency**
- **auditability**
- **replayability**
- **small operational footprint**
- **clear separation between discovery/reporting and follow-up prioritization**

## Repository structure

```text
FirstLight/
  config/
    n1.example.yaml
    tns.example.env

  scripts/
    run_night.ps1
    run_night_wrapper.ps1
    replay_avro_dir.py
    morning_check.ps1
    sweep_replies_once.ps1

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

## What is considered production-relevant

The current production-oriented workflow relies mainly on:

- `scripts/run_night.ps1`
- `scripts/replay_avro_dir.py`
- `scripts/morning_check.ps1`
- `src/firstlight/cli.py`
- `src/firstlight/storage/db.py`
- `src/firstlight/tns/client.py`
- `src/firstlight/tns/dispatch.py`
- `src/firstlight/pipeline/normalize.py`
- `src/firstlight/niches/n1_hostless_fast.py`

## Environment and configuration

Two example config files are included:

- `config/n1.example.yaml`
- `config/tns.example.env`

Sensitive runtime files such as local databases, `.env.prod`, logs, and operational artifacts are intentionally excluded from version control.

## TNS notes

The project supports TNS-related actions from the CLI, including:

- environment checks
- payload inspection
- minimal payload printing
- report reply polling
- capped dispatch from the local SQLite database

Example:

```powershell
python -m firstlight --env .env.prod tns envcheck
python -m firstlight --env .env.prod tns submit-min --print-payload
```

## Nightly operation

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

## Roadmap

The current roadmap is:

1. keep the hostless discovery/reporting workflow stable
2. add a follow-up prioritization layer within the hostless niche
3. track which submitted candidates are realistically classifiable
4. build a small, well-documented set of follow-up outcomes
5. transition from reporting infrastructure to a scientifically defensible follow-up selection story

## What this repository is not

This is **not** a polished general-purpose astronomy platform, and it is **not yet** a full automated classification system.

It is an independent, evolving project focused on:

- operational reliability
- transparent candidate selection
- realistic progression toward classification-oriented follow-up

## Personal note

FirstLight is an **independent personal project**, developed outside my day job and outside any official employer scope.

It sits at the intersection of:
- software engineering
- real-time data pipelines
- personal work in astrophysics / time-domain astronomy

## Public communication policy

The project is communicated publicly by milestones, not by nightly noise.

The current public milestone is:
- first successful production TNS report
- repository cleanup and public documentation alignment
- transition toward hostless follow-up prioritization

## License / publication

A formal publication path will only make sense once the follow-up prioritization layer is frozen and evaluated on real cases with documented outcomes.

Until then, the repository should be read as:
- a real working system
- with a validated first production stage
- and an explicitly defined next scientific stage
