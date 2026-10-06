"""
models.py
───────────
Master-doc (Unified Strategy+Broker Risk Engine) §8-10/§45: ONE RiskConfig
shape shared by Strategy and Broker scope — "do not duplicate this
configuration model separately."
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class TrailingMode(str, Enum):
    NONE = "NONE"
    LOCK = "LOCK"
    LOCK_AND_TRAIL = "LOCK_AND_TRAIL"
    TRAIL_STOP_LOSS = "TRAIL_STOP_LOSS"


class RiskDecision(str, Enum):
    NONE = "NONE"
    STOP_LOSS_HIT = "STOP_LOSS_HIT"
    TARGET_HIT = "TARGET_HIT"
    LOCK_HIT = "LOCK_HIT"
    TRAILING_HIT = "TRAILING_HIT"


@dataclass
class RiskConfig:
    stop_loss_enabled: bool = False
    stop_loss_amount: float = 0.0

    target_enabled: bool = False
    target_amount: float = 0.0

    trailing_mode: TrailingMode = TrailingMode.NONE
    activation_profit: float = 0.0  # "ProfitReaches"
    lock_profit: float = 0.0
    profit_step: float = 0.0  # "IncreaseInProfitBy" / TrailForEvery
    trail_by: float = 0.0  # "TrailProfitBy"
    trailing_distance: float = 0.0  # TRAIL_STOP_LOSS mode only

    # Broker "OverallTrailSL" standalone (old check_broker_sl_target Case A):
    # for every `stop_loss_trail_every` of peak profit, the SL amount shrinks
    # by `stop_loss_trail_by` (tighten only, never loosen).
    stop_loss_trail_every: float = 0.0
    stop_loss_trail_by: float = 0.0
