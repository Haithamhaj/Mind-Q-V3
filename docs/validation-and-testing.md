# Validation & Testing

## Phase Validators
Use the validation scripts under `backend/validation_scripts/` to verify each phase before moving on.

```bash
# Examples
python backend/validation_scripts/validate_phase0.py
python backend/validation_scripts/validate_phase1.py
python backend/validation_scripts/validate_phase12.py
```

Project root also contains helper validation scripts for full pipeline checks.

## Tests
Run backend tests:
```bash
cd backend
pytest -q
```

Recommended workflow:
1. Implement or adjust a phase service in `app/services/`.
2. Run its validator.
3. Add/update tests under `backend/tests/`.
4. Ensure all tests pass before committing.
