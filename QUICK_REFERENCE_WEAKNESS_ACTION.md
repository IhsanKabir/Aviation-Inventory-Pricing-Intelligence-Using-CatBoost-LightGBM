# Quick Reference: Weakness Summary & Action Checklist

## 🚨 Critical Issues (24 Hours)

### Issue #1: No Input Validation
- **Problem**: Bad data enters DB (negative prices, missing IDs, type mismatches)
- **Solution**: Create `validation/flight_offer_validator.py` with strict schema checks
- **Time**: ~2 hours
- **Test**: `pytest validation/`

### Issue #2: Identity Tracking Broken  
- **Problem**: `identity_valid` flag exists but never used; invalid records in change events
- **Solution**: Set flag in validator, filter in comparison engine, report in DQ metrics
- **Time**: ~1 hour
- **Test**: Verify comparison engine filters invalid records

---

## ⚠️ High Priority (48 Hours)

### Issue #3: 50% of Airlines Can't Scrape
- **Problem**: `us-bangla.py`, `novoair.py` are EMPTY; others have missing timezone/soldout logic
- **Solution**: Complete parsers for all enabled airlines
- **Time**: ~14 hours (mostly reverse-engineering)
- **Blockers**: Needs Codex API docs or network inspection

### Issue #4: Fragmented Session Management
- **Problem**: Multiple cookie/session files; no auto-refresh on 401; manual Cloudflare bypass required
- **Solution**: Create `SessionManager` class, consolidate cookies, add auto-retry
- **Time**: ~3 hours
- **Test**: Mock 401 response, verify auto-refresh works

### Issue #5: Timezone Chaos
- **Problem**: Each airline handles timestamps differently; no UTC standardization
- **Solution**: Create timezone helper, standardize all parsers to return UTC + local
- **Time**: ~2 hours
- **Impact**: Change detection currently compares different timezones!

---

## 📊 Medium Priority (72 Hours)

### Issue #6: No Data Quality Gates
- **Problem**: DQ metrics collected but no automated thresholds; garbage silently enters reports
- **Solution**: Define SLA gates in config, implement auto pass/fail evaluator
- **Time**: ~4 hours
- **Examples**: `null_rate < 5%`, `duplicate_rate < 1%`, abort on FAIL

### Issue #7: Prediction Engine Orphaned
- **Problem**: `predict_next_day.py` is feature-complete (1183 lines) but NOT integrated into pipeline
- **Solution**: Wire flag into `run_all.py`, auto-run on-demand, capture outputs
- **Time**: ~3 hours
- **Benefit**: ML forecasts become part of daily reports

---

## 📈 Effort Summary

```
Critical  (1-2)   →   3-4 hours   →  0.5 days  ← DO THIS FIRST
High      (3-5,7) →  17-20 hours  →  2-3 days
Medium    (6,8)   →   7-10 hours  →  1-2 days
Low       (9)     →   6-8 hours   →  1 day

Total: 33-42 hours ≈ 4-5 days of focused work
```

---

## 🎯 Recommended Attack Order

### Day 1 (Morning): Validation Framework
- [ ] 1a - Create `validation/flight_offer_validator.py`
- [ ] 1b - Add identity key validation
- [ ] 1c - Add price sanity checks (no negatives/zeros/outliers)
- [ ] 1d - Hook into `bulk_insert_offers()`
- [ ] **Test**: Unit tests passing

### Day 1 (Afternoon): Identity Gate
- [ ] 2a - Validator sets `identity_valid` flag
- [ ] 2b - Comparison engine filters invalid records
- [ ] 2c - Reports show `identity_valid` metrics per airline
- [ ] **Test**: Invalid records excluded from change events

### Day 2: Airline Parsers
- [ ] 3a - Full audit: which modules are empty/partial/complete
- [ ] 3d - Create `timezone_helper.py` 
- [ ] 3b - Complete US Bangla parser (or mark as stub)
- [ ] 3c - Complete Novo Air parser (or mark as stub)
- [ ] 3e - Add timezone + soldout logic to all parsers
- [ ] **Test**: Fixture tests 100% passing, sample scrapes work

### Day 2 (Evening): Sessions
- [ ] 4a-4b - Create `SessionManager` with `load_cookies(airline)`
- [ ] 4c - Implement 401 auto-refresh retry loop
- [ ] 4d - Consolidate cookies to `cookies/{airline}_cookies.json`
- [ ] 4f - Update all airlines to use SessionManager
- [ ] **Test**: Manual 401 mock test passing

### Day 3: Quality Gates
- [ ] 5a - Create `config/data_quality_gates.json` with thresholds
- [ ] 5b - Implement `engines/data_quality_evaluator.py`
- [ ] 5c - Auto-compute metrics (null %, duplicates %, outliers)
- [ ] 5d - Add DQ report generation to `run_all.py`
- [ ] 5e - Abort on FAIL, warn on condition violations
- [ ] **Test**: DQ report generates correctly, thresholds trigger

### Day 3 (Evening): Predictions
- [ ] 6a - Add `--run-prediction` flag to `run_all.py`
- [ ] 6b - Wire into pipeline (disabled by default)
- [ ] 6c - Outputs saved to `output/reports/predictions/`
- [ ] 6d - Create forecast evaluation metrics
- [ ] **Test**: Predictions generate without errors

### Day 4: Polish & Testing
- [ ] 8a - Create E2E pipeline test
- [ ] 8c - Set up GitHub Actions (`pytest` on push)
- [ ] Documentation updates
- [ ] Final integration testing

---

## 📋 Status Board Template

Copy this and update daily:

```
┌─────────────────────────────────────────────────────────┐
│  AIRLINE SCRAPER REMEDIATION - WEEK OF 2026-02-23       │
├─────────────────────────────────────────────────────────┤
│ PHASE 1: Stabilization (Critical)          [████░░░░░░] │
│   ☐ Validation Framework                 ETA: 2026-02-24 │
│   ☐ Identity Gate                        ETA: 2026-02-24 │
│                                                           │
│ PHASE 2: Operationalization (High)        [░░░░░░░░░░░░] │
│   ☐ Airline Parsers (5 modules)          ETA: 2026-02-25 │
│   ☐ Session Manager                      ETA: 2026-02-25 │
│   ☐ Timezone Handling                    ETA: 2026-02-25 │
│                                                           │
│ PHASE 3: Intelligence (Medium)            [░░░░░░░░░░░░] │
│   ☐ Data Quality Gates                   ETA: 2026-02-26 │
│   ☐ Prediction Integration               ETA: 2026-02-26 │
│                                                           │
│ PHASE 4: Polish (Low)                     [░░░░░░░░░░░░] │
│   ☐ E2E Tests + CI/CD                    ETA: 2026-02-27 │
└─────────────────────────────────────────────────────────┘
```

---

## 🔗 Key Files to Touch

**Create (NEW):**
- `validation/flight_offer_validator.py`
- `validation/test_flight_offer_validator.py`
- `core/session_manager.py`
- `airlines/timezone_helper.py`
- `config/data_quality_gates.json`
- `engines/data_quality_evaluator.py`
- `engines/forecast_evaluator.py`
- `tests/test_identity_validation.py`
- `tests/test_session_manager.py`
- `tests/test_timezone_handling.py`
- `tests/test_e2e_pipeline.py`

**Modify (EXISTING):**
- `db.py` - Add validator to `bulk_insert_offers()`
- `run_all.py` - Add validation logging, DQ gates, prediction flag
- `comparison_engine.py` - Add `identity_valid` filter
- `generate_reports.py` - Add `identity_valid` metrics, DQ automation
- All `airlines/*.py` - Add timezone helper, soldout logic, SessionManager
- `models.py` - Add/verify `identity_valid` column
- `OPERATIONS_RUNBOOK.md` - Document new features

---

## 💡 Pro Tips

1. **Start Small**: Begin with validation (most impactful, 2 hours)
2. **Test Early**: Unit test each component before integration
3. **Mock External**: Mock HTTP 401s, API responses for testing
4. **Parallelize**: Work on parsers while someone else does SessionManager
5. **Iterate**: After Phase 1, you'll have 90% improvement; phases 2-4 are refinement

---

## 📞 Questions to Answer Before Starting

1. **Which airlines are priority?** (Top 5 vs all 10+)
2. **API credentials available?** (Needed for reverse-engineering parsers)
3. **DB access?** (Needed for schema changes)
4. **Deployment frequency?** (Daily runs, hourly, on-demand?)
5. **SLA thresholds?** (What's max acceptable null %, duplicate %?)

---

## ✨ Expected Impact After Remediation

| Metric | Before | After |
|--------|--------|-------|
| Garbage row rate | Unknown (5-10%?) | < 1% (validated) |
| Airlines working | 2/10 (20%) | 5+/10 (50%+) |
| Session failures | 30% (manual fixes) | < 5% (auto-refresh) |
| Timezone errors | High (silent bugs) | None (UTC standard) |
| Data quality gates | Manual review | Automated pass/fail |
| Prediction integration | 0% (orphaned) | 100% (in reports) |
| Regression protection | 0% (no CI/CD) | 100% (GitHub Actions) |

---

## 🚀 Go-Live Checklist

Before running daily scheduled scrapes:

- [ ] Validation framework catches 5+ types of bad data
- [ ] All 5+ priority airlines scraping without 401 errors
- [ ] DQ gates pass on 3 consecutive runs
- [ ] Timezone offsets correct for all route airports
- [ ] Session auto-refresh works (mock 401 test)
- [ ] E2E test pipeline passes
- [ ] Operators trained on new validation/DQ outputs
- [ ] Runbook updated with new features
