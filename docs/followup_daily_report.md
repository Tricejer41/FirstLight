# FirstLight Follow-up Daily Report

Generated UTC: `2026-04-17T02:55:28+00:00`

DB: `firstlight_followup_prod.sqlite`

## Strategy thresholds

- shortlist_floor: `62.0`
- primary_min_score: `82.0`
- backup_min_score: `72.0`
- spectroscopy_min_score: `88.0`
- spectroscopy_min_effective_evidence: `1`

## Queue status counts
- watch: 9
- watch_high: 1

## Decision summary

- ready_spectroscopy: `0`
- actionable_now: `0`
- actionable_backup: `0`
- watch_high: `1`


## ready_spectroscopy (0)

No candidates in this section.


## actionable_now (0)

No candidates in this section.


## actionable_backup (0)

No candidates in this section.


## watch_high (1)

### 1. ZTF26aarheku

- candid: `3381527690715015005`
- report_id: `271233`
- tns_name: `-`
- submitted_utc: `2026-04-06T01:53:58+00:00`
- status: `watch_high`
- priority_bucket: `high`
- score: `62.0` (best: `87.0`)
- score_version: `classifiability_v3_remote_followup`
- mag: `15.714`
- effective_freshness_days: `11.030`
- age_since_submission_days: `11.030`
- nmtchps: `2`
- distpsnr1: `16.373`
- srmag1: `-`
- dec_deg: `12.257`
- science_score: `42.0`
- remote_imaging_score: `16.0`
- remote_spectroscopy_score: `56.2`
- remote_bonus_score: `4.0`
- evidence: effective=`0` (survey=`0`, manual_phot=`0`)
- last_action: `status_change` at `2026-04-06T12:57:44+00:00`
- last_action_reason: score=87.0 remains in scientific shortlist
- primary_blockers: score 62.0 < primary threshold 82.0; freshness 11.03 d > primary window 4.0 d
- backup_blockers: score 62.0 < backup threshold 72.0; freshness 11.03 d > backup window 8.0 d
- spectroscopy_blockers: remote spectroscopy score 56.2 < threshold 88.0; effective evidence 0 < required 1; manual imaging missing before spectroscopy
- recommendation: WATCH HIGH: buen candidato científico, pero aún sin evidencia nueva post-report.
