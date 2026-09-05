# How You Build

## Dev loop and agentic tools used

I built this pipeline with Claude as the primary agentic dev tool, and used ChatGPT on the side to get oriented on parts of the task. The loop looked like:

1. **Source investigation first, code second.** Before writing any SQL, I
   had Claude fetch and read har.fyi's actual guides (release cycle,
   minimizing costs, the `pages` table reference, the `technology` struct
   reference) rather than relying on general knowledge of "BigQuery public
   datasets." This is what surfaced the exact mechanics of the timing trap
   (CrUX lands 2nd Tuesday, not the 1st) and the UNNEST-drops-empty-arrays
   gotcha that shaped the two-query extract design -- neither of those
   would have been obvious from the take-home brief's text alone.
2. **Build one pipeline stage at a time, test it immediately, then move
   on.** Each of `normalize_domain.py`, `arrival_check.py`, `extract.py`,
   `build_snapshot.py`, `validate.py`, and `diff_snapshots.py` was written,
   then immediately smoke-tested against synthetic fixtures before the next
   stage was built on top of it. This caught real bugs early -- e.g. the
   initial guardrail thresholds (`min_row_count`, `min_domain_count`) were
   tuned for production scale and failed against the deliberately small
   mock fixtures on first run, exactly the kind of thing worth catching in
   a 2-second local test rather than after wiring the whole orchestrator
   together.
3. **Wrote a synthetic HTTP Archive fixture generator
   (`pipeline/mock_source.py`) as its own deliberate step.** This wasn't a
   fallback bolted on after the fact -- I designed it to inject exactly the
   failure modes the assignment cares about (grain collisions, a
   not-yet-landed month, mid-stream tech churn) so the rest of the pipeline
   could be exercised end-to-end against realistic-shaped data, with a
   proper pytest suite, before ever touching real BigQuery.
4. **Orchestrator last, not first.** `run_pipeline.py` was written after
   every individual stage already worked in isolation, so it's a thin
   composition layer (arrival check -> extract -> normalize/aggregate ->
   validate -> load -> diff) rather than a monolith I'd have had to debug
   as one unit.

## Where AI saved the most time

- **Reading and synthesizing har.fyi's documentation quickly.** I'd
  already read through har.fyi's cost-optimization guide myself and knew
  it had the specific mechanics I needed (cluster column order, the exact
  free-tier number, the "`LIMIT` doesn't reduce scan cost" gotcha), so I
  handed Claude the actual guide links to fetch and pull load-bearing facts
  out of, rather than asking it to reason about BigQuery cost behavior from
  general knowledge. Doing the reading myself first meant I knew which
  pages actually mattered and could point Claude at exactly those, instead
  of it guessing at what to look up -- and it meant I could sanity-check
  what came back against what I'd already read, rather than taking it on
  faith. That combination (I'd found and read the source, Claude extracted
  and structured the specific numbers into the design doc) is what makes
  the cost model and trap-handling defensible rather than generic.
- **Writing the full pytest suite alongside the code**, not after it --
  each pipeline stage's tests were written and run within the same step as
  the implementation, which meant regressions (like the threshold mismatch
  above) were caught in seconds, not discovered later during an end-to-end
  run.
- **Boilerplate the assignment doesn't actually care about** -- argparse
  wiring, config loading, JSON I/O plumbing -- went fast, leaving more time
  for the parts that are actually being assessed (the trap-handling logic
  and the reasoning behind each design decision).

## Where AI cost more than doing it by hand

- **BigQuery access itself, initially.** The AI-agent sandbox this was
  developed in has no network path to `googleapis.com`, so the first pass
  of every dry-run/cost claim in `docs/design_doc.md` §2 was necessarily
  sourced from the take-home brief's own stated numbers (~150-250 GB/month)
  or from the har.fyi docs' publicly stated table sizes -- not from a live
  query. That's the one place agentic tooling couldn't substitute for
  hands-on access. I closed that gap myself by running the real dry-runs
  and a real, cost-controlled extraction from a machine with actual GCP
  credentials (`sql/exploration/04_dry_run_notes.md`, ~29.84-30.70 GB
  measured per query, in the same range as the estimate) -- but that step
  genuinely had to happen outside the agentic dev loop, on real
  infrastructure, and it's worth being upfront that the two verification
  passes (estimate, then real dry-run) happened days apart rather than in
  the same sitting.
- **Local disk space, for the warehouse-landing step.** Landing a full
  production month (millions of rows) to local JSON before uploading to
  GCS isn't something a laptop's disk comfortably holds. Rather than
  redesigning the extract path around this under deadline pressure, I used
  the same `--limit-rows` mechanism already built for proving the pipeline
  against real production data (500 rows/month) and landed that for real,
  into BigQuery. This is a legitimate proof of the mechanism end-to-end
  (arrival check through BigQuery `MERGE`), but it's not the same as having
  landed a real production-scale month -- flagged again below, and the
  reasoning for why it's scoped this way (and how it would scale) is
  written out in `design_doc.md` §3.4.
- **Deciding the grain-aggregation rule (union vs. pick-one)** was a
  judgment call about the business use case, not something to delegate --
  it needed a human to reason about what a sales rep actually wants, and
  to accept the named trade-off in writing. I made that call myself after
  walking through a few concrete examples, and also ran it past a friend
  with backend/data experience as an outside sanity check before
  committing to it in the design doc -- a second opinion felt worth having
  for a decision that shapes the whole product's output, even though it
  didn't change the conclusion. AI tooling was good at implementing the
  rule once decided and at listing the candidate rules; deciding which one
  was _right_ for this product was mine to make.

## One known weakness to flag to a teammate

**The row-count thresholds in `config.yaml` (`min_row_count`,
`min_domain_count`, and the drift percentages) are static numbers, not
derived from real production-scale HTTP Archive data.** I calibrated them
against the take-home brief's stated ballpark figures (12-16M pages/month,
~150-250 GB filtered scans) and against this repo's own small mock
fixtures, but I haven't run this against the actual `crawl.pages` table
across enough months yet to see what a real desktop-only, root-page-only
monthly row count looks like, or how much it naturally fluctuates month to
month. Before this runs unattended in production, someone should run it
for real for 2-3 consecutive months, look at the actual row-count
variance, and recalibrate `guardrails.min_row_count` and
`guardrails.max_row_count_drop_pct` / `max_row_count_spike_pct` off real
numbers instead of estimates -- as written, there's a real risk the
thresholds are either too loose (missing a genuine anomaly) or too tight
(false-alarming on normal month-to-month variation) in ways that can only
be found by watching it run for real.

**A second weakness, related to the first:** `pipeline/land_to_warehouse.py`
and `sql/warehouse/*.sql` have been run for real against a live GCP trial
account -- tables created, and both real production snapshots plus the
change-events batch actually landed and verified with `SELECT`s against
the warehouse -- but only against `--limit-rows 500` extracts (see "Where
AI cost more than doing it by hand" above), not a full production month.
The `MERGE` logic and staging/idempotency behavior are exercised for real,
but I haven't watched this land tens of thousands of domains and confirmed
load-job duration, `MERGE` cost, or BigQuery quota behavior at that scale.
`land_to_warehouse.py` has test coverage (`tests/test_land_to_warehouse.py`,
12 tests, mocking `google.cloud.bigquery`/`storage` so nothing touches
real GCP) covering the orchestration order (upload -> load staging ->
merge, never merge-before-load), the dry-run path, missing-file handling,
`WRITE_TRUNCATE` on staging loads, and that `create_tables.sql` is
non-destructive. What that suite does _not_ cover -- and can't, by design,
since it mocks the BigQuery client -- is real load-job duration, `MERGE`
cost, or quota behavior at production scale. A teammate picking this up
should still treat "land a real full month, not just 500 rows" as the
next thing to do before this runs unattended.

**v2, the agentic autopilot layer -- a third weakness, and where the dev
loop actually helped.** My first draft of `orchestrator_agent.py` was
architecturally wrong: I'd relabeled the existing deterministic control
flow with agentic-sounding function names without changing what the system
does, which is the exact anti-pattern section 5 warns about. Talking
through the design before writing code -- specifically, asking "what is
the human still doing manually every month that an agent could take over"
instead of "where can I put an LLM call" -- is what surfaced the actual
gap (self-triggering) instead of a cosmetic one. That's the clearest
example in this whole project of AI-assisted *architecture* review paying
off, separate from AI-assisted code generation.

Where it cost more than doing it by hand: getting the GitHub Actions
two-job, environment-gated approval flow right (artifact hand-off between
the `check` and `approve` jobs, the `if: needs.check.outputs.pending_approval
== 'true'` condition, the environment protection rule itself) took more
back-and-forth than the Python code did -- YAML workflow syntax is exactly
the kind of thing that looks plausible and is subtly wrong, and I ended up
verifying the job-output plumbing by hand rather than trusting a first
draft.
