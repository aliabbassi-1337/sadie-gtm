# Codebase Concerns

**Analysis Date:** 2026-01-20

## Tech Debt

**Code Duplication: Detector Logic**
- Issue: Booking engine detection logic is duplicated between `services/leadgen/detector.py` (1749 lines) and `scripts/pipeline/detect.py` (2168 lines)
- Files: `services/leadgen/detector.py`, `scripts/pipeline/detect.py`
- Impact: Changes to detection patterns must be applied in two places; divergence has already occurred (different ENGINE_PATTERNS, different timeouts)
- Fix approach: Deprecate `scripts/pipeline/detect.py` entirely; ensure all workflows use `services/leadgen/detector.py`. The scripts version appears to be legacy code that should be archived.

**Code Duplication: Chain/Non-Hotel Filters**
- Issue: SKIP_CHAINS, SKIP_NON_HOTELS, SKIP_DOMAINS lists are duplicated between `services/leadgen/detector.py` and `services/leadgen/grid_scraper.py`
- Files: `services/leadgen/detector.py:63-208`, `services/leadgen/grid_scraper.py:94-199`
- Impact: Filter updates must be applied in two places; risk of inconsistency
- Fix approach: Extract shared filter constants to `services/leadgen/constants.py` and import in both modules

**Global Mutable State: Engine Patterns**
- Issue: Engine patterns are stored in module-level global `_engine_patterns` dict and modified via `set_engine_patterns()`
- Files: `services/leadgen/detector.py:44-60`
- Impact: Not thread-safe; patterns must be set before each batch; error-prone in concurrent scenarios
- Fix approach: Pass patterns as constructor argument to `BatchDetector` and `HotelProcessor`; remove global state

**Silent Exception Swallowing**
- Issue: Many `except Exception: pass` blocks silently ignore errors without logging
- Files:
  - `services/leadgen/detector.py` (20+ instances, lines 538, 558, 572, 760, 782, 796, 970, 983, 1164, 1176, 1179, 1436, 1457, 1547)
  - `services/enrichment/service.py:33-65` (entire interface has pass stubs)
  - `services/reporting/service.py:28-108` (entire interface has pass stubs)
- Impact: Silent failures make debugging difficult; errors are lost
- Fix approach: Add `logger.debug()` or `logger.warning()` calls in catch blocks; at minimum log the exception type

**Bare except: Block**
- Issue: `scripts/pipeline/export_excel.py:44` uses `except:` without exception type
- Files: `scripts/pipeline/export_excel.py:44`
- Impact: Catches all exceptions including KeyboardInterrupt and SystemExit
- Fix approach: Change to `except Exception:` at minimum

**Missing Newline Before Function**
- Issue: `services/leadgen/repo.py:200` - function `insert_hotels_bulk` starts immediately after previous function without blank line separator
- Files: `services/leadgen/repo.py:200`
- Impact: Code style inconsistency; minor readability issue
- Fix approach: Add blank line before function definition

## Known Bugs

**None identified during analysis.**

The codebase appears functionally correct. No obvious logic bugs were found, though the silent exception handling could mask bugs in production.

## Security Considerations

**SSL Verification Disabled**
- Risk: HTTP client disables SSL verification (`verify=False`)
- Files:
  - `services/leadgen/detector.py:290` - httpx client for precheck
  - `services/enrichment/service.py:113` - httpx client for room count enrichment
- Current mitigation: None
- Recommendations: Enable SSL verification in production; only disable for known problematic sites with explicit list

**API Keys in Environment Variables**
- Risk: API keys stored in environment variables (standard practice, but noted)
- Files:
  - `services/leadgen/grid_scraper.py:313` - SERPER_API_KEY
  - `services/enrichment/room_count_enricher.py:22` - ROOM_COUNT_ENRICHER_AGENT_GROQ_KEY
  - `db/client.py:35` - SADIE_DB_PASSWORD
  - `infra/s3.py`, `infra/sqs.py` - AWS credentials (via environment)
- Current mitigation: Using dotenv, env vars
- Recommendations: Consider AWS Secrets Manager for production deployment; ensure .env is in .gitignore (appears to be)

**Hardcoded Country Default**
- Risk: Country defaults to "USA" in multiple places
- Files:
  - `services/leadgen/repo.py:32` - `country: str = "USA"`
  - `services/leadgen/service.py:227` - `"country": "USA"`
- Current mitigation: None
- Recommendations: Consider making country a required parameter or configurable default for international expansion

**No Rate Limit Enforcement**
- Risk: No client-side rate limiting for external APIs beyond semaphores
- Files: `services/leadgen/grid_scraper.py:54-55` - only uses semaphore concurrency control
- Current mitigation: Semaphore limits concurrent requests to 4
- Recommendations: Add exponential backoff; add rate limit tracking per time window

## Performance Bottlenecks

**Sequential Database Inserts**
- Problem: `insert_hotels_bulk` inserts hotels one at a time in a loop
- Files: `services/leadgen/repo.py:200-246`
- Cause: Individual INSERT calls instead of bulk insert
- Improvement path: Use `executemany()` or `COPY` command for batch inserts; would significantly improve scrape-to-database throughput

**Browser Context Pool Size**
- Problem: Fixed pool of browser contexts (equal to concurrency setting)
- Files: `services/leadgen/detector.py:1706-1716`
- Cause: Context reuse strategy ties pool size to concurrency
- Improvement path: Allow context pool to be sized independently; consider lazy context creation

**Large File: detector.py**
- Problem: `services/leadgen/detector.py` is 1749 lines in a single file
- Files: `services/leadgen/detector.py`
- Cause: All detection logic in one module
- Improvement path: Split into submodules: `detector/config.py`, `detector/engines.py`, `detector/contacts.py`, `detector/processor.py`

**Large File: grid_scraper.py**
- Problem: `services/leadgen/grid_scraper.py` is 1057 lines
- Files: `services/leadgen/grid_scraper.py`
- Cause: Scraping, filtering, and grid logic combined
- Improvement path: Extract filter logic to shared module; extract grid generation to separate module

## Fragile Areas

**BookingButtonFinder JavaScript Evaluation**
- Files: `services/leadgen/detector.py:578-678`
- Why fragile: Complex JavaScript executed in page context; relies on specific DOM structure; hardcoded CSS selectors
- Safe modification: Test against real hotel websites after any changes; maintain integration tests
- Test coverage: Integration tests exist in `services/leadgen/detector_test.py` but only cover 4 hotels

**HTML Pattern Scanning**
- Files: `services/leadgen/detector.py:1182-1304`
- Why fragile: `_scan_html_for_engines` uses hardcoded keyword patterns that may not match new booking engines
- Safe modification: Add new patterns to database `booking_engines.domains` rather than hardcoding
- Test coverage: No unit tests for HTML scanning specifically

**Address Parsing**
- Files: `services/leadgen/grid_scraper.py:1044-1057`
- Why fragile: `_parse_address` assumes US-style address format (City, State ZIP)
- Safe modification: Use a proper address parsing library for international support
- Test coverage: No unit tests for address parsing

**Room Count Extraction**
- Files: `services/leadgen/detector.py:423-479`
- Why fragile: Regex-based extraction from HTML; easily confused by page content
- Safe modification: Add more test cases; consider using LLM fallback like room_count_enricher
- Test coverage: Unit tests exist but limited patterns tested

## Scaling Limits

**Database Connection Pool**
- Current capacity: 10 connections max (`db/client.py:37`)
- Limit: Supavisor transaction mode imposes additional limits
- Scaling path: Increase pool size; consider connection per worker model for EC2 scaling

**Scraper API Credits**
- Current capacity: Serper API plan limits (varies by subscription)
- Limit: Out-of-credits detection exists (`grid_scraper.py:860-863`)
- Scaling path: Multiple API keys; credit monitoring dashboard

**Single-Region Database**
- Current capacity: Single Supabase instance
- Limit: Geographic latency for global operations
- Scaling path: Read replicas; regional routing

## Dependencies at Risk

**Python 3.9**
- Risk: Using Python 3.9 which is approaching end-of-life (October 2025 security-only)
- Impact: No new features; security patches only
- Migration plan: Upgrade to Python 3.11 or 3.12; update `.venv` and any version pins

**Playwright Version Pinning**
- Risk: No visible version pin for playwright in requirements
- Impact: Browser automation could break with major updates
- Migration plan: Pin playwright version in requirements; test before updating

## Missing Critical Features

**No Retry Queue**
- Problem: Failed detections are marked as non-retriable immediately
- Files: `services/leadgen/service.py:299-314`
- Blocks: Transient failures (network issues) permanently mark hotels as failed
- Recommendation: Add retry queue with exponential backoff; separate permanent vs transient errors

**No Health Checks**
- Problem: No endpoint or mechanism to verify service health
- Blocks: Production monitoring; deployment verification
- Recommendation: Add `/health` endpoint for each service; include DB connectivity check

**No Metrics Collection**
- Problem: No instrumentation for observability
- Blocks: Performance monitoring; cost tracking; alerting
- Recommendation: Add Prometheus metrics or CloudWatch metrics for key operations

## Test Coverage Gaps

**Service Layer Tests**
- What's not tested: `services/leadgen/service.py` has only stub tests in `service_test.py`
- Files: `services/leadgen/service.py`, `services/leadgen/service_test.py`
- Risk: Core business logic untested; regressions could ship
- Priority: High

**Grid Scraper Unit Tests**
- What's not tested: `services/leadgen/grid_scraper.py` has no test file
- Files: `services/leadgen/grid_scraper.py`
- Risk: Scraping logic changes could break without detection
- Priority: High

**Repository Layer Tests**
- What's not tested: Many repo functions in `services/leadgen/repo.py`
- Files: `services/leadgen/repo.py`, `services/leadgen/repo_test.py`
- Risk: Database operations could fail silently
- Priority: Medium

**Workflow Tests**
- What's not tested: No tests for any workflow scripts in `workflows/`
- Files: `workflows/*.py` (13 files)
- Risk: CLI workflows could break; deployment issues
- Priority: Medium

**Enrichment Service Tests**
- What's not tested: `services/enrichment/service.py` operations
- Files: `services/enrichment/service.py`, `services/enrichment/repo_test.py` (partial)
- Risk: Room count and proximity enrichment untested
- Priority: Medium

**Integration Test Coverage**
- What's not tested: Only 4 hotels in `detector_test.py` integration tests
- Files: `services/leadgen/detector_test.py`
- Risk: Detection accuracy regressions for edge cases
- Priority: Medium - expand to 20+ diverse hotels

---

*Concerns audit: 2026-01-20*
