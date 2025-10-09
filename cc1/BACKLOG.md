# BACKLOG
_Future features and improvements. Update when: ideas emerge, features are planned._

## Ideas
_Features under consideration_

- Adaptive rate limiting based on API response patterns (monitor 503 error frequency and automatically adjust batch size, delays, and concurrency) #impact:medium #effort:large #type:improvement
- RSA slot management for 3-ad limit (automatically remove old theme ads when ad group is full; implementation started in rsa_management.py but not yet integrated) #impact:high #effort:medium #type:feature

## Planned
_Approved features ready for development_

## Technical Debt
_Known issues to address_

- Integrate RSA management into main processor (rsa_management.py created with manage_ad_slots() function, but not called from main_optimized.py; need to add before ad creation, apply THEMA_ORIGINAL label to paused ads, set new ads to PAUSED status) #priority:high #effort:medium

## Maybe Later
_Low priority or uncertain features_

---

## Backlog Tags Guide
- `#impact:` high | medium | low - Expected user impact
- `#effort:` small | medium | large - Development effort
- `#type:` feature | improvement | fix - Type of work
