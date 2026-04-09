# imou_info

## Railway cost / RAM optimization notes

This service is usually memory-bound on Railway, so these options give the biggest savings:

1. **Use bounded charts cache** (implemented in `main.py`):
   - `CHARTS_CACHE_TTL_SEC` (default `300`)
   - `CHARTS_CACHE_MAX_ITEMS` (default `128`, new)
2. **Tune Gunicorn threads** in `nixpacks.toml`:
   - Current value is `--threads 8`. For this app, `2-4` threads is usually enough and uses less RAM.
3. **Disable optional features if not needed**:
   - `DTEK_FORECAST_ENABLED=0`
   - `GDRIVE_EVENTS_ENABLED=0`

### Suggested low-cost env baseline

```env
CHARTS_CACHE_TTL_SEC=120
CHARTS_CACHE_MAX_ITEMS=64
DTEK_FORECAST_ENABLED=0
GDRIVE_EVENTS_ENABLED=1
```
