"""
conftest.py
─────────────
Minimal env so shared.config.settings (required by every module that
imports shared.logging.logger at module scope, e.g. token_router.py) can be
constructed during unit tests without a real .env / Mongo / broker
credentials — pure business-logic modules shouldn't need those to be tested.
"""

import os
import tempfile

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret-key")
os.environ.setdefault("AUTH_ENFORCEMENT_ENABLED", "true")
# Keep test log lines out of the real service log (../logs), which the
# running algo.trade writes to — test runs were mixing fake brokers/legs
# ("bconf1", "L1", ...) into it.
os.environ.setdefault("LOG_DIR", tempfile.mkdtemp(prefix="algo2_test_logs_"))


import itertools

import pytest


@pytest.fixture(autouse=True)
def _ticks_ten_seconds_apart(monkeypatch):
    """risk.evaluator.PeakWindow only lets a profit level raise the peak once
    it has held ~1s (filters half-repriced multi-leg ticks). Unit tests feed
    ticks back-to-back, so by default model them as 10s apart — a sustained
    move. Tests about the spike filter itself set evaluator.clock directly."""
    from risk import evaluator

    counter = itertools.count(start=1000, step=10)
    monkeypatch.setattr(evaluator, "clock", lambda: next(counter))


@pytest.fixture(autouse=True)
def _fresh_peak_windows():
    """Peak windows are module-level (per broker/strategy/scope id); tests
    reuse ids like "s1", so start each test clean."""
    from risk import broker_risk, runtime_risk, strategy_risk

    for mod in (broker_risk, runtime_risk, strategy_risk):
        mod._PEAK_WINDOWS.clear()
    yield
