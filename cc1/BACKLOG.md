# BACKLOG
_Future features and improvements. Update when: ideas emerge, features are planned._

## Ideas
_Features under consideration_

- Adaptive rate limiting based on API response patterns (monitor 503 error frequency and automatically adjust batch size, delays, and concurrency) #impact:medium #effort:large #type:improvement
- RSA slot management for 3-ad limit (automatically remove old theme ads when ad group is full; implementation started in rsa_management.py but not yet integrated) #impact:high #effort:medium #type:feature
- Automated gap detection and repair (periodic audit to detect ad groups with THEME_*_DONE labels but missing theme ads; ALL_THEMES_DONE label now automatically applied by fill_missing_themed_ads_parallel_v3.py; Check-up function now optimized with 12-24x performance improvement and THEMES_CHECK_DONE tracking; could further enhance with automatic scheduling and email reports) #impact:medium #effort:small #type:improvement #status:enhanced
- Performance metrics dashboard for audit operations (track execution time per customer, query counts, label operations, show optimization breakdown to identify bottlenecks) #impact:low #effort:small #type:improvement

## Planned
_Approved features ready for development_

## Technical Debt
_Known issues to address_

- Integrate RSA management into main processor (rsa_management.py created with manage_ad_slots() function, but not called from main_optimized.py; need to add before ad creation, apply THEMA_ORIGINAL label to paused ads, set new ads to PAUSED status) #priority:high #effort:medium
- Gap-filler script location and integration (fill_missing_themed_ads_parallel_v3.py currently in project root as utility script; consider: integrate into main automation flow vs keep as manual utility; if integrated, how to trigger - automatic after discovery? manual button in UI? scheduled job?) #priority:medium #effort:medium

## Maybe Later
_Low priority or uncertain features_

---

## Backlog Tags Guide
- `#impact:` high | medium | low - Expected user impact
- `#effort:` small | medium | large - Development effort
- `#type:` feature | improvement | fix - Type of work
