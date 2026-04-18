# FirstLight Remote Follow-up Strategy

## Purpose

This document defines the operational strategy for remote follow-up in FirstLight.

The goal is not to optimize for local observing from Banyoles.
The goal is to optimize for a realistic remote workflow where:
- FirstLight reports very few hostless candidates to TNS
- A small scientific shortlist is maintained
- At most one primary remote-imaging target and one backup target are selected
- Spectroscopy is considered only after sufficient follow-up justification

This document is a strategy freeze for the remote workflow.
It should guide future scoring, promotion, and daily reporting logic.

---

## Core principle

The workflow is split into two clearly different phases:

### 1. Pre-observation phase
This phase decides whether a reported object deserves remote follow-up.

Inputs may include:
- scientific score from survey data
- freshness
- brightness
- field cleanliness / hostless cleanliness
- evidence from new survey detections
- remote follow-up feasibility proxies

This phase does **not** use manual follow-up observations, because those observations do not exist yet.

### 2. Post-observation phase
This phase happens only after a real remote follow-up attempt or real remote observation.

At this point, the system should answer:
- close the case
- continue with more imaging
- escalate toward spectroscopy
- prepare a classification-ready package

---

## Status model

### watch
Interesting object, but no money or effort should be spent on it.
It remains in the queue and is re-evaluated by the daily refresh.

### watch_high
Scientific shortlist.
This object is one of the stronger reported hostless candidates, but it does not yet justify paying for remote follow-up.

### actionable_backup
Backup remote-imaging target.
This object is good enough that it could justify a paid remote imaging attempt if the primary target becomes unavailable, weaker, or less practical.

### actionable_now
Primary remote-imaging target.
If one object is chosen for a paid remote imaging attempt, it should be this one.

### ready_spectroscopy
The object has enough justification to consider a remote spectroscopy attempt.
This should be rare and should usually require:
- strong scientific interest
- good enough brightness
- good enough operational feasibility
- at least one useful imaging/follow-up step or equivalent justification

### closed
The object no longer justifies more operational attention.

---

## Daily selection philosophy

The queue should stay small and strict.

Preferred shape of the queue:
- many `watch`
- very few `watch_high`
- at most one `actionable_now`
- at most one `actionable_backup`
- normally zero `ready_spectroscopy`

This is intentional.
The project is not optimized for volume.
It is optimized for picking a very small number of cases that are realistic to follow remotely as a solo workflow.

---

## Meaning of "remote follow-up feasibility"

Remote follow-up feasibility is not "visible from Banyoles".

It means:

> Is this object a realistic target for a paid remote follow-up attempt with a remote telescope workflow?

This should depend mainly on:
- brightness
- freshness
- scientific priority
- whether it remains active/interesting
- whether it is in a sky region that is not obviously impractical for a typical remote setup

Local Banyoles observability may still be kept as a weak diagnostic proxy,
but it must not be the dominant operational criterion.

---

## Pre-observation decision flow

1. First report is already done by the production workflow.
2. The object enters the follow-up mirror.
3. Daily refresh re-scores it.
4. Daily report shows shortlist objects.
5. Promotion logic selects:
   - one primary target at most
   - one backup target at most
6. Manual review may confirm or reject that choice.
7. Only then may a real remote imaging attempt be considered.

---

## Post-observation decision flow

After a real remote follow-up attempt, the object should be re-evaluated.

Possible outcomes:
- `closed`
- `watch_high`
- `actionable_backup`
- `ready_spectroscopy`

Important distinction:
- "continue" does **not** automatically mean spectroscopy
- "continue" may simply mean another imaging attempt or another photometric epoch

This is expected in a solo remote workflow.

---

## Operational meaning of "continue"

A candidate may remain scientifically interesting after one follow-up attempt without being ready for spectroscopy.

Examples:
- first imaging was too weak or inconclusive
- follow-up confirms the object is still active, but not enough for spectroscopy
- a second imaging epoch would be more useful than jumping directly to a spectrum
- the object remains viable, but the cost/benefit of spectroscopy is still not justified

Therefore:
- `continue` means the case stays alive
- `ready_spectroscopy` means the case is strong enough for an actual spectroscopy decision

---

## Separation from scientific publication goals

Preprint and paper goals are intentionally outside the operational workflow.

Operational workflow focuses on:
- selecting
- reporting
- prioritizing
- following up
- re-evaluating
- classifying if possible

Scientific output goals are separate:

### Preprint
Reasonable after the first strong classification-ready case or first truly successful follow-up case.

### Paper
Reasonable only after a group of classified objects exists with meaningful scientific similarities and a defendable common narrative.

These goals should not influence daily queue mechanics.

---

## Frozen assumptions for next development steps

The next development steps should assume:

- the workflow is remote-first, not local-observing-first
- manual observations happen only after selection, never before
- primary and backup targets must remain very limited
- spectroscopy is downstream from imaging and reassessment, not automatic
- publication planning is external to operational logic