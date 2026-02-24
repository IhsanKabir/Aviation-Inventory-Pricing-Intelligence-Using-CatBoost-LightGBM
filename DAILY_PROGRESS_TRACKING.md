<!-- cspell:disable -->

# Daily Progress Tracking Template

Use this to monitor remediation work and maintain accountability.

---

## Week of February 23-27, 2026

### Day 1: February 24 (Monday) - Phase 1: Validation Framework

**Target:** 3 hours / Get validation + identity gates working
**Team:** Dev A + Dev B (together)

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| 1a - Create validator.py | 10:30 AM | Dev A |  | Schema validations (7 checks) |
| 1b - Create unit tests | 11:00 AM | Dev A |  | 12+ test cases |
| Run test suite | 11:15 AM | Dev A |  | `pytest validation/` |
| 1c - Integrate into db.py | 11:30 AM | Dev B |  | Hook bulk_insert_offers() |
| 1d - Add logging to run_all.py | 12:00 PM | Dev B |  | Rejection report JSON |
| Test with mock data | 12:30 PM | Both |  | E2E: scrape → validate → insert |
| Code review + sign-off | 1:00 PM | Both |  | All tests passing? |
| **PHASE 1 COMPLETE** | **1:30 PM** | ✅ | | Validation gate live |

**Success Criteria (Yes/No):**
- [ ] validator.py passes all unit tests (12/12)
- [ ] db.py modified, no syntax errors
- [ ] run_all.py logs rejection summary
- [ ] Zero negative prices inserted into test DB
- [ ] Manual test run produces rejection JSON

**Handoff to Phase 2:**
⏳ Once Phase 1 passes, split team:

- Dev A → Airline Parsers + Timezone
- Dev B → Session Management

---

### Day 2: February 25 (Tuesday) - Phase 2A: Airline Parsers & Phase 2B: Session Mgmt

**Dev A: Airline Parsers (High)**
**Dev B: Session Management (High)**

#### Dev A Track: Airline Parsers + Timezone Helper

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| 3a - Audit airline modules | 9:00 AM | Dev A |  | Which are empty/partial/complete? |
| 3d - Create timezone_helper.py | 10:00 AM | Dev A |  | `apply_timezone_offsets()` function |
| Test timezone helper | 10:30 AM | Dev A |  | 5 sample flights, verify UTC conversion |
| 3b - Start US Bangla parser | 10:45 AM | Dev A |  | Reverse-engineer from Codex work |
| 3c - Start Novo Air parser | 12:00 PM | Dev A |  | Parallel; reuse patterns from BG |
| Parsers: Add timezone calls | 2:00 PM | Dev A |  | Each parser uses timezone_helper |
| Parsers: Add soldout logic | 3:00 PM | Dev A |  | Check `seat_available == 0` |
| Test fixture responses | 4:00 PM | Dev A |  | `pytest airlines/` |
| Code review | 5:00 PM | Dev A |  | All tests passing? |

**Success Criteria (Yes/No):**
- [ ] Audit doc shows status of all modules
- [ ] timezone_helper.py works correctly (sample test)
- [ ] US Bangla parser scrapes 1 test flight
- [ ] Novo Air parser scrapes 1 test flight
- [ ] Both parsers return UTC + local times
- [ ] All soldout logic implemented

#### Dev B Track: Session Management

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| 4a - Design SessionManager | 9:00 AM | Dev B |  | Interfaces: load_cookies(), refresh() |
| 4b - Create session_manager.py | 10:00 AM | Dev B |  | load_cookies(airline), set_cookies() |
| 4c - Implement 401 auto-retry | 11:00 AM | Dev B |  | Retry loop, max 2 attempts |
| Test SessionManager unit tests | 11:30 AM | Dev B |  | Mock 401 responses |
| 4d - Consolidate cookies files | 12:00 PM | Dev B |  | Create cookies/>{airline}_cookies.json |
| 4e - Update all airline modules | 2:00 PM | Dev B |  | Import SessionManager, use it |
| Test end-to-end | 3:00 PM | Dev B |  | Mock 401 → auto-refresh → retry |
| Code review | 4:00 PM | Dev B |  | All tests passing? |

**Success Criteria (Yes/No):**
- [ ] SessionManager.load_cookies() works
- [ ] 401 response triggers auto-refresh
- [ ] Retry succeeds on 2nd attempt (mock)
- [ ] All airlines using SessionManager
- [ ] cookies/ folder created with airline files

**Daily Standup 4:00 PM:**
- Dev A: Which parsers still need work? (blockers?)
- Dev B: SessionManager tested with real 401?
- Both: Any shared dependencies?

---

### Day 3: February 26 (Wednesday) - Phase 2 Completion + Phase 3 Start

**Dev A: Finish Parsers + Timezone**
**Dev B: Add Quality Gates + Prediction**

#### Dev A Track: Complete Day 2 Unfinished

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| Complete US Bangla parser | 10:00 AM | Dev A |  | Handle edge cases |
| Complete Novo Air parser | 11:00 AM | Dev A |  | Sample scrape test |
| 7b-7d - Update comparison engine for UTC | 12:00 PM | Dev A |  | Always compare UTC |
| 7e - Timezone integration test | 1:00 PM | Dev A |  | DAC→CXB flight, verify local times |
| Final parser sign-off | 2:00 PM | Dev A |  | All 5+ airlines working? |

#### Dev B Track: Quality Gates + Prediction

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| 5a - Create data_quality_gates.json | 9:00 AM | Dev B |  | Define SLA thresholds |
| 5b - Create data_quality_evaluator.py | 10:00 AM | Dev B |  | Compute metrics + pass/fail |
| 5c - Add validation metrics to generate_reports.py | 11:00 AM | Dev B |  | null %, duplicate %, outlier % |
| 5d - Hook DQ gates into run_all.py | 12:00 PM | Dev B |  | Warn on condition violations |
| Test DQ gates | 1:00 PM | Dev B |  | Simulate DQ violation |
| 6a - Add --run-prediction flag | 2:00 PM | Dev B |  | Off by default |
| 6b - Wire predict_next_day.py | 2:30 PM | Dev B |  | Call at pipeline end |
| Test prediction flow | 3:00 PM | Dev B |  | Query outputs saved |
| Code review | 4:00 PM | Dev B |  | Ready for 3-day test run? |

**Success Criteria (Yes/No):**
- [ ] DQ rule violations trigger warning in logs
- [ ] DQ report generated automatically
- [ ] --run-prediction flag works (test mode)
- [ ] Predictions saved to output/reports/predictions/
- [ ] All modules integrated, no syntax errors

**Daily Standup 4:00 PM:**
- Both: Ready to start 3-day test run tomorrow?
- Any unresolved blockers?

---

### Day 4: February 27 (Thursday) - 3-Day Test Run Start + Phase 4: Testing

**All:** Run live pipeline with validation/parsers/quality gates
**Log:** Collect data for forecast baseline

| Event | Time | Action | Success = |
|-------|------|--------|-----------|
| 7:00 AM | Start run_all.py | First live scrape with validation |  |
| 8:00 AM | Review validation report | Check rejection rate < 10% |  |
| 9:00 AM | Check airline parse counts | All 5+ airlines have rows |  |
| 10:00 AM | Verify timezone offsets | Sample 5 flights, verify local times |  |
| 11:00 AM | Check session health | No 401 errors (or auto-recovered) |  |
| Noon | Run 2nd scrape | Should complete without issues |  |
| 1:00 PM | Generate DQ report | Run: `python generate_reports.py --timespan=24h` |  |
| 2:00 PM | Review DQ metrics | Null rates < 5%, duplicates < 1% |  |

Meanwhile: Dev A & B work on **Phase 4: Testing (Low)**

| Task | Deadline | Owner | Status | Notes |
|------|----------|-------|--------|-------|
| 8a - Create E2E test | 10:00 AM | Dev A |  | Mock scrape → validate → report |
| Test E2E flow | 11:00 AM | Dev A |  | All stages pass |
| 8b - Edge case tests | 12:00 PM | Dev A |  | Empty response, auth failure, etc. |
| 8c - GitHub Actions setup | 1:00 PM | Dev B |  | pytest on every commit |
| Test CI/CD workflow | 2:00 PM | Dev B |  | Push dummy commit, watch pytest run |
| Code coverage report | 3:00 PM | Dev B |  | Aim for 80%+ on critical modules |
| Final sign-off | 4:00 PM | Both |  | Project ready for thesis? |

**Success Criteria (Yes/No):**
- [ ] First live scrape completes without critical errors
- [ ] Validation rejects < 10% of rows
- [ ] All 5+ airlines have non-zero row counts
- [ ] DQ report passes (PASS/WARN, not FAIL)
- [ ] Timezone test passes (local times correct)
- [ ] E2E test passing
- [ ] CI/CD pipeline working

**End of Day 4: STATUS → "OPERATIONAL TEST MODE"**

---

### Day 5: February 28 (Friday) - 3-Day Test Data Collection + Documentation

**All:** Continue 3-day test run; collect baseline data for forecasting
**Collect baseline predictions on Day 7**

| Task | Owner | Deadline | Status |
|------|-------|----------|--------|
| Run 3 scrapes (Fri/Sat/Sun) at regular intervals | Dev A | 5:00 PM |  |
| Monitor for failures/rejections | Dev B | Daily |  |
| Collect all DQ/validation reports | Both | Daily |  |
| Document any issues found | Both | 4:00 PM |  |
| Update OPERATIONS_RUNBOOK.md | Dev A | 3:00 PM |  |
| Update PROJECT_DECISIONS.md | Dev B | 3:00 PM |  |
| Final sign-off | Both | 5:00 PM |  |

---

## Overall Progress Dashboard

```text
┌──────────────────────────────────────────────────────────┐
│  REMEDIATION WEEK: Feb 23-27, 2026                       │
│  ════════════════════════════════════════════════════════ │
│                                                          │
│  Phase 1: Validation (CRITICAL)                          │
│  ════════════════════════════════════════════════════════ │
│  Days:   Mon 2/24                                        │
│  Target: 3 hours                                         │
│  Status: ░░░░░░░░░░░░░░░░░░ 0% (not started)            │
│  Owner:  Dev A + Dev B (together)                        │
│  Blockers: None (can start immediately)                 │
│                                                          │
│  Phase 2: Airline Parsers + Session + Timezone (HIGH)    │
│  ════════════════════════════════════════════════════════ │
│  Days:   Tue 2/25 → Thu 2/26                            │
│  Target: 19 hours (split 2 people)                      │
│  Status: ░░░░░░░░░░░░░░░░░░ 0% (blocked by Phase 1)    │
│  Owner:  Dev A (parsers) + Dev B (session)              │
│  Blockers: Phase 1 must complete first                  │
│                                                          │
│  Phase 3: Quality Gates + Prediction (MEDIUM)           │
│  ════════════════════════════════════════════════════════ │
│  Days:   Tue 2/25 → Thu 2/26 (parallel)                 │
│  Target: 7 hours                                        │
│  Status: ░░░░░░░░░░░░░░░░░░ 0% (blocked by Phase 1)    │
│  Owner:  Dev B (during Phase 2)                         │
│  Blockers: Phase 1 must complete first                  │
│                                                          │
│  Phase 4: Test + CI/CD (LOW)                            │
│  ════════════════════════════════════════════════════════ │
│  Days:   Thu 2/26 → Fri 2/27                            │
│  Target: 8 hours                                        │
│  Status: ░░░░░░░░░░░░░░░░░░ 0% (blocked by Phases 1-3) │
│  Owner:  Dev A + Dev B (together)                       │
│  Blockers: Phases 1-3 must be stable first              │
│                                                          │
│  ════════════════════════════════════════════════════════ │
│  TOTAL EFFORT: 37 hours (1 person serial)               │
│              : 19 hours (2 people parallel) ← PLANNED    │
│  ════════════════════════════════════════════════════════ │
│                                                          │
│  GO-LIVE: Mon 3/2 (after 3-7 day test run)              │
│  THESIS READY: Fri 2/28                                 │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

---

## Daily Standup Template (2:00 PM each day)

**Everyone answers:**

1. **Yesterday's commitments** - Did you complete them? Why/why not?
2. **Blockers** - What's stuck? Who can help?
3. **Note: What surprised you?** Any unexpected findings?
4. **Today's plan** - What will you finish TODAY?

**Example:**

```text
MONDAY 2/24 Standup:
  Dev A: "Finished validator.py + tests (12/12 passing).
         Starting db.py integration. No blockers."
  Dev B: "Added logging to run_all.py. Tested E2E with
         mock data; zero negative prices in DB.
         Phase 1 ready for sign-off at 1:30 PM."
  Blocker: None. Phase 1 on track.
  Today: Dev A + Dev B complete Phase 1 sign-off.

TUESDAY 2/25 Standup:
  Dev A: "Timeline updated. timezone_helper done.
         Working on US Bangla parser; need Codex API docs.
         Estimate: done today 3 PM."
  Dev B: "SessionManager design complete.
         Implementing load_cookies() + 401 retry logic.
         Estimate: testing by 2 PM."
  Blocker: Dev A needs Codex API docs for Novo Air API.
  Action: Check Codex logs / API docs folder.
  Today: Both track finishes by EOD (code review 4 PM).
```

---

## Weekly Go/No-Go Gate

**Every Friday, 5:00 PM:**

```text
✅ GO (Ready to move forward)  if:
  □ Phase just completed has 100% success criteria met
  □ All unit/integration tests passing
  □ Code review sign-off from peer
  □ No critical blockers for next phase

⚠️  PROCEED-WITH-CAUTION if:
  □ Phase ~90% complete (minor issues only)
  □ Blockers don't affect next phase
  □ Workarounds available

❌ NO-GO (Halt, replan) if:
  □ Phase < 70% complete
  □ Critical blocker for next phase
  □ Tests failing
  □ Fundamental design issue found
```

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| Parser reverse-engineering takes too long | Medium | High | Start with US Bangla only, mark others as "stub" |
| DB schema change breaks backward compat | Low | High | Test on fresh DB first, validate rollback |
| 401 retry logic causes infinite loop | Low | Critical | Cap max retries = 2, log all retry attempts |
| Timezone conversion errors silently corrupt data | Medium | High | Unit test every timezone offset before deploy |
| Parsers inconsistent after unification | Medium | Medium | Create comprehensive fixture tests |

---

## Success Email Template (For Friday 5 PM)

```text
Subject: Airline Scraper Remediation - Week Completed ✅

Hi Team,

This week we completed [choose: Phase 1 / Phases 1-2 / All phases].

Metrics:
  • Validation framework live: Rejects [X]% bad data per run
  • Airlines operational: [5/10] scraping without errors
  • Session auto-refresh: Tested, working
  • DQ gates: [PASS/WARN/FAIL] status automated
  • Timezone handling: UTC standard applied, local times correct
  • Test coverage: [X]% on critical modules

Next steps: [3-7 day test run / Full production deployment / ...]

Remaining work:
  • [ ] Task A
  • [ ] Task B

Go-live target: [Date]

Thanks for the focus this week!
```

---

## Post-Mortems (If things go wrong)

**If Phase 1 test fails:**
- Revert validator changes
- Debug specific test case
- Fix + re-test = usually 30 min

**If parser returns wrong format:**
- Check against test fixture
- Update parser logic
- Re-run fixture test = usually 1 hour

**If timezone offset is wrong:**
- Verify airport code in config
- Verify offset value
- Test UTC conversion = usually 1 hour

**If DQ gates are too strict:**
- Loosen thresholds in config
- Re-run reports
- Document new thresholds = 30 min

---

## Final Checklist Before "Go-Live"

Phases 1-3 complete? Check:

- [ ] Validation passing (< 10% rejection rate normal?)
- [ ] All 5+ airlines scraping
- [ ] Session auto-refresh working
- [ ] Timezone offsets correct
- [ ] DQ gates in place + passing
- [ ] Prediction integrated (optional flag)
- [ ] E2E tests passing
- [ ] Operators trained on new features
- [ ] Runbook updated
- [ ] 3-7 days of good data collected for ML baseline

**If all 10 boxes checked → READY FOR THESIS**
