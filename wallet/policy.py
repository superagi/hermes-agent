"""Policy engine — evaluates transactions against configurable rules.

Policies are checked in order. The first ``block`` or ``require_approval``
result wins.  If all policies pass, the transaction is auto-approved.

For v1, policies are in-memory (loaded from config.yaml).  A future
version will persist per-wallet policies in the keystore.
"""

import json
import logging
import os
import time
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from wallet.file_state import read_json, update_json

logger = logging.getLogger(__name__)


class PolicyVerdict(str, Enum):
    ALLOW = "allow"
    REQUIRE_APPROVAL = "require_approval"
    BLOCK = "block"


@dataclass
class PolicyResult:
    """Result of evaluating all policies for a transaction."""
    verdict: PolicyVerdict
    reason: str = ""
    checked: List[str] = field(default_factory=list)  # Policy names that passed
    failed: str = ""     # Policy name that blocked/required approval


@dataclass
class TxRequest:
    """A pending transaction to evaluate."""
    wallet_id: str
    wallet_type: str     # "user" | "agent"
    chain: str
    to_address: str
    amount: Decimal
    symbol: str


# ---------------------------------------------------------------------------
# Individual policy checks
# ---------------------------------------------------------------------------

def _check_spending_limit(tx: TxRequest, config: dict) -> Optional[PolicyVerdict]:
    """Block if single transaction exceeds max amount."""
    max_amount = Decimal(str(config.get("max_native", "0")))
    if max_amount > 0 and tx.amount > max_amount:
        return PolicyVerdict.BLOCK
    return None


def _check_daily_limit(tx: TxRequest, config: dict, daily_totals: Dict[str, Decimal]) -> Optional[PolicyVerdict]:
    """Block if daily aggregate exceeds limit."""
    max_daily = Decimal(str(config.get("max_native", "0")))
    if max_daily <= 0:
        return None
    today_key = f"{tx.wallet_id}:{time.strftime('%Y-%m-%d')}"
    current_total = daily_totals.get(today_key, Decimal("0"))
    if current_total + tx.amount > max_daily:
        return PolicyVerdict.BLOCK
    return None


def _check_rate_limit(tx: TxRequest, config: dict, tx_timestamps: Dict[str, list]) -> Optional[PolicyVerdict]:
    """Block if too many transactions in the time window."""
    max_txns = config.get("max_txns", 0)
    window = config.get("window_seconds", 3600)
    if max_txns <= 0:
        return None

    key = tx.wallet_id
    now = time.time()
    timestamps = tx_timestamps.get(key, [])
    # Prune old timestamps
    timestamps = [t for t in timestamps if now - t < window]
    tx_timestamps[key] = timestamps

    if len(timestamps) >= max_txns:
        return PolicyVerdict.BLOCK
    return None


def _check_cooldown(tx: TxRequest, config: dict, last_tx_time: Dict[str, float]) -> Optional[PolicyVerdict]:
    """Block if not enough time since last transaction."""
    min_seconds = config.get("min_seconds", 0)
    if min_seconds <= 0:
        return None
    key = tx.wallet_id
    last = last_tx_time.get(key, 0)
    if time.time() - last < min_seconds:
        return PolicyVerdict.BLOCK
    return None


def _check_allowed_recipients(tx: TxRequest, config: dict) -> Optional[PolicyVerdict]:
    """Block if recipient not in allowlist (when configured)."""
    addresses = config.get("addresses", [])
    if not addresses:
        return None  # No allowlist = allow all
    if tx.to_address.lower() not in [a.lower() for a in addresses]:
        return PolicyVerdict.BLOCK
    return None


def _check_blocked_recipients(tx: TxRequest, config: dict) -> Optional[PolicyVerdict]:
    """Block if recipient is in blocklist."""
    addresses = config.get("addresses", [])
    if tx.to_address.lower() in [a.lower() for a in addresses]:
        return PolicyVerdict.BLOCK
    return None


def _check_require_approval(tx: TxRequest, config: dict) -> Optional[PolicyVerdict]:
    """Require owner approval if amount exceeds threshold."""
    above = Decimal(str(config.get("above_native", "-1")))
    if above < 0:
        return None  # Not configured
    if tx.amount > above:
        return PolicyVerdict.REQUIRE_APPROVAL
    return None


# ---------------------------------------------------------------------------
# Policy engine
# ---------------------------------------------------------------------------

# Default policies for agent wallets (can be tightened, not loosened)
AGENT_WALLET_DEFAULTS = {
    "spending_limit": {"max_native": "1.0"},      # Max per tx (in native token)
    "daily_limit": {"max_native": "5.0"},          # Max per day
    "rate_limit": {"max_txns": 5, "window_seconds": 3600},
    "cooldown": {"min_seconds": 30},
    "require_approval": {"above_native": "0.5"},   # Require approval above this
}

# User wallets always require approval by default
USER_WALLET_DEFAULTS = {
    "require_approval": {"above_native": "0"},     # Always require approval
}


class PolicyEngine:
    """Evaluates transactions against a set of policies.

    State for freeze/rate-limit/daily-limit is persisted to a JSON file so
    CLI invocations and approval replays share the same safeguards.
    """

    def __init__(self, policies: Optional[Dict[str, dict]] = None, state_path: Optional[Path] = None):
        self._policies = policies or {}
        self._state_path = Path(state_path) if state_path else Path(os.getenv("HERMES_HOME", Path.home() / ".hermes")) / "wallet" / "policy_state.json"
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        # Tracking state for rate-based policies
        self._daily_totals: Dict[str, Decimal] = defaultdict(Decimal)
        self._tx_timestamps: Dict[str, list] = defaultdict(list)
        self._last_tx_time: Dict[str, float] = {}
        self._frozen = False
        self._load_state()

    @property
    def is_frozen(self) -> bool:
        return self._frozen

    def freeze(self) -> None:
        """Kill switch — block all transactions."""
        self._frozen = True
        self._save_state()
        logger.warning("Wallet FROZEN — all transactions blocked")

    def unfreeze(self) -> None:
        """Unfreeze — resume normal operation."""
        self._frozen = False
        self._save_state()
        logger.info("Wallet unfrozen")

    def evaluate(self, tx: TxRequest) -> PolicyResult:
        """Evaluate a transaction against all policies.

        Returns PolicyResult with the final verdict.
        """
        # Refresh persisted state so multiple processes share freeze/rate/daily/cooldown.
        self._load_state()
        if self._frozen:
            return PolicyResult(
                verdict=PolicyVerdict.BLOCK,
                reason="Wallet is frozen (kill switch active)",
                failed="freeze",
            )

        # Select policy set based on wallet type
        if tx.wallet_type == "agent":
            policies = {**AGENT_WALLET_DEFAULTS, **self._policies}
        else:
            policies = {**USER_WALLET_DEFAULTS, **self._policies}

        checked = []

        _CHECKS = {
            "spending_limit": lambda cfg: _check_spending_limit(tx, cfg),
            "daily_limit": lambda cfg: _check_daily_limit(tx, cfg, self._daily_totals),
            "rate_limit": lambda cfg: _check_rate_limit(tx, cfg, self._tx_timestamps),
            "cooldown": lambda cfg: _check_cooldown(tx, cfg, self._last_tx_time),
            "allowed_recipients": lambda cfg: _check_allowed_recipients(tx, cfg),
            "blocked_recipients": lambda cfg: _check_blocked_recipients(tx, cfg),
            "require_approval": lambda cfg: _check_require_approval(tx, cfg),
        }

        # Hard blocks always run before approval policies.
        policy_names = [p for p in policies.keys() if p != "require_approval"]
        if "require_approval" in policies:
            policy_names.append("require_approval")

        pending_approval = None
        for policy_name in policy_names:
            config = policies[policy_name]
            check_fn = _CHECKS.get(policy_name)
            if not check_fn:
                continue

            result = check_fn(config)
            if result == PolicyVerdict.BLOCK:
                return PolicyResult(
                    verdict=PolicyVerdict.BLOCK,
                    reason=f"Blocked by {policy_name} policy",
                    checked=checked,
                    failed=policy_name,
                )
            elif result == PolicyVerdict.REQUIRE_APPROVAL:
                pending_approval = PolicyResult(
                    verdict=PolicyVerdict.REQUIRE_APPROVAL,
                    reason=f"Requires approval ({policy_name} policy)",
                    checked=checked.copy(),
                    failed=policy_name,
                )
            else:
                checked.append(policy_name)

        if pending_approval is not None:
            return pending_approval

        return PolicyResult(
            verdict=PolicyVerdict.ALLOW,
            reason="All policies passed",
            checked=checked,
        )

    def record_transaction(self, tx: TxRequest) -> None:
        """Update tracking state after a successful transaction.

        Uses locked read-modify-write so updates from separate processes merge
        instead of clobbering each other.
        """
        now = time.time()
        today_key = f"{tx.wallet_id}:{time.strftime('%Y-%m-%d')}"

        def _merge(existing):
            existing = existing or {}
            daily = dict(existing.get("daily_totals", {}) or {})
            prev = Decimal(str(daily.get(today_key, "0")))
            daily[today_key] = str(prev + tx.amount)

            timestamps = dict(existing.get("tx_timestamps", {}) or {})
            vals = list(timestamps.get(tx.wallet_id, []) or [])
            vals = [t for t in vals if now - t < 86400]
            vals.append(now)
            timestamps[tx.wallet_id] = vals

            last = dict(existing.get("last_tx_time", {}) or {})
            last[tx.wallet_id] = now

            return {
                "frozen": bool(existing.get("frozen", self._frozen)),
                "daily_totals": daily,
                "tx_timestamps": timestamps,
                "last_tx_time": last,
            }

        new_state = update_json(self._state_path, {}, _merge)
        self._frozen = bool(new_state.get("frozen", False))
        self._daily_totals = defaultdict(Decimal, {k: Decimal(str(v)) for k, v in new_state.get("daily_totals", {}).items()})
        self._tx_timestamps = defaultdict(list, new_state.get("tx_timestamps", {}) or {})
        self._last_tx_time = new_state.get("last_tx_time", {}) or {}

    def _load_state(self) -> None:
        try:
            data = read_json(self._state_path, {})
            self._frozen = bool(data.get("frozen", False))
            self._daily_totals = defaultdict(
                Decimal,
                {k: Decimal(str(v)) for k, v in (data.get("daily_totals", {}) or {}).items()},
            )
            self._tx_timestamps = defaultdict(list, data.get("tx_timestamps", {}) or {})
            self._last_tx_time = data.get("last_tx_time", {}) or {}
        except Exception as e:
            logger.warning("Failed to load wallet policy state: %s", e)

    def _save_state(self) -> None:
        try:
            frozen = self._frozen
            daily = {k: str(v) for k, v in self._daily_totals.items()}
            timestamps = {k: list(v) for k, v in self._tx_timestamps.items()}
            last = dict(self._last_tx_time)

            def _merge(existing):
                existing = existing or {}
                # freeze/unfreeze should not destroy other fields; keep latest known
                return {
                    "frozen": frozen,
                    "daily_totals": daily or dict(existing.get("daily_totals", {}) or {}),
                    "tx_timestamps": timestamps or dict(existing.get("tx_timestamps", {}) or {}),
                    "last_tx_time": last or dict(existing.get("last_tx_time", {}) or {}),
                }

            update_json(self._state_path, {}, _merge)
        except Exception as e:
            logger.warning("Failed to save wallet policy state: %s", e)
