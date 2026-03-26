"""Agent-facing wallet tools.

These are the tools the LLM can call.  They go through the wallet manager
and policy engine — the agent never has access to private keys.

All handlers return JSON strings per Hermes convention.

Toolset: ``wallet`` (optional install: ``pip install 'hermes-agent[wallet]'``)

The toolset is gated on:
  1. keystore + wallet packages being installed
  2. The keystore being initialized and unlocked
  3. At least one wallet existing

Tools:
  wallet_list         — List wallets with addresses and balances
  wallet_balance      — Check balance of a specific wallet
  wallet_send         — Send native tokens (policy-gated)
  wallet_history      — Transaction history
  wallet_estimate_gas — Fee estimation
  wallet_address      — Get a wallet's deposit address (for sharing/receiving)
  wallet_networks     — List supported and active blockchain networks
"""

import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Optional

from tools.registry import registry

logger = logging.getLogger(__name__)

# Lazy-loaded singleton (initialized on first tool call)
_wallet_manager = None
_policy_engine = None


def _get_manager():
    """Lazy-init the wallet manager + policy engine."""
    global _wallet_manager, _policy_engine
    if _wallet_manager is not None:
        return _wallet_manager, _policy_engine

    try:
        from keystore.client import get_keystore
        from wallet.manager import WalletManager
        from wallet.policy import PolicyEngine

        ks = get_keystore()
        if not ks.is_unlocked:
            return None, None

        _wallet_manager = WalletManager(ks)
        _policy_engine = PolicyEngine()

        # Register available chain providers, respecting config.yaml overrides
        _register_providers(_wallet_manager)

        return _wallet_manager, _policy_engine
    except ImportError:
        return None, None
    except Exception as e:
        logger.debug("Wallet manager init failed: %s", e)
        return None, None


def _load_rpc_overrides() -> dict:
    """Load user RPC endpoint overrides from config.yaml."""
    try:
        from hermes_cli.config import load_config
        cfg = load_config()
        return cfg.get("wallet", {}).get("rpc_endpoints", {})
    except Exception:
        return {}


def _register_providers(mgr):
    """Register chain providers based on installed deps + config overrides."""
    overrides = _load_rpc_overrides()

    try:
        from wallet.chains.evm import EVMProvider, EVM_CHAINS
        for chain_id, config in EVM_CHAINS.items():
            rpc = overrides.get(chain_id, "")
            mgr.register_provider(chain_id, EVMProvider(config, rpc_url_override=rpc))
    except ImportError:
        pass

    try:
        from wallet.chains.solana import SolanaProvider, SOLANA_CHAINS
        for chain_id, config in SOLANA_CHAINS.items():
            rpc = overrides.get(chain_id, "")
            mgr.register_provider(chain_id, SolanaProvider(config, rpc_url_override=rpc))
    except ImportError:
        pass


def _check_wallet_available() -> bool:
    """Check if wallet functionality is available."""
    mgr, _ = _get_manager()
    return mgr is not None


# =========================================================================
# Tool handlers
# =========================================================================

def wallet_list(task_id: str = None, **kw) -> str:
    """List all wallets with their addresses and balances."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available. Run 'hermes wallet create' first."})

    wallets = mgr.list_wallets()
    if not wallets:
        return json.dumps({
            "wallets": [],
            "message": "No wallets found. Create one with 'hermes wallet create'.",
        })

    result = []
    for w in wallets:
        entry = {
            "wallet_id": w.wallet_id,
            "label": w.label,
            "chain": w.chain,
            "address": w.address,
            "type": w.wallet_type,
        }
        # Try to fetch balance (non-blocking, skip on error)
        try:
            bal = mgr.get_balance(w.wallet_id)
            entry["balance"] = str(bal.balance)
            entry["symbol"] = bal.symbol
        except Exception:
            entry["balance"] = "unavailable"
            entry["symbol"] = ""
        result.append(entry)

    return json.dumps({"wallets": result})


def wallet_balance(args: dict, task_id: str = None, **kw) -> str:
    """Check wallet balance."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    wallet_id = args.get("wallet_id")
    chain = args.get("chain")

    try:
        wallet = mgr.resolve_wallet(wallet_id=wallet_id, chain=chain)
        bal = mgr.get_balance(wallet.wallet_id)
        return json.dumps({
            "wallet": wallet.label,
            "address": wallet.address,
            "chain": wallet.chain,
            "balance": str(bal.balance),
            "symbol": bal.symbol,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


def wallet_send(args: dict, task_id: str = None, **kw) -> str:
    """Request a token transfer.  Subject to policy engine approval."""
    mgr, policy = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    to_address = args.get("to", "")
    amount_str = args.get("amount", "")
    wallet_id = args.get("wallet_id")
    chain = args.get("chain")

    if not to_address or not amount_str:
        return json.dumps({"error": "Both 'to' and 'amount' are required"})

    try:
        amount = Decimal(amount_str)
    except InvalidOperation:
        return json.dumps({"error": f"Invalid amount: {amount_str}"})

    if amount <= 0:
        return json.dumps({"error": "Amount must be positive"})

    try:
        wallet = mgr.resolve_wallet(wallet_id=wallet_id, chain=chain)
    except Exception as e:
        return json.dumps({"error": str(e)})

    # Get chain symbol
    try:
        provider = mgr.get_provider(wallet.chain)
        symbol = provider.config.symbol
    except Exception:
        symbol = "?"

    # Evaluate policy
    from wallet.policy import TxRequest, PolicyVerdict
    tx_req = TxRequest(
        wallet_id=wallet.wallet_id,
        wallet_type=wallet.wallet_type,
        chain=wallet.chain,
        to_address=to_address,
        amount=amount,
        symbol=symbol,
    )

    if policy:
        result = policy.evaluate(tx_req)

        if result.verdict == PolicyVerdict.BLOCK:
            return json.dumps({
                "status": "blocked",
                "reason": result.reason,
                "policy": result.failed,
            })

        if result.verdict == PolicyVerdict.REQUIRE_APPROVAL:
            # For v1: return pending status — the CLI/gateway approval flow
            # will handle the user interaction
            return json.dumps({
                "status": "pending_approval",
                "reason": result.reason,
                "transaction": {
                    "from": wallet.address,
                    "to": to_address,
                    "amount": str(amount),
                    "symbol": symbol,
                    "chain": wallet.chain,
                    "wallet": wallet.label,
                },
                "message": (
                    f"Transaction requires owner approval: send {amount} {symbol} "
                    f"to {to_address} on {wallet.chain}. "
                    "The owner will be prompted to approve or deny."
                ),
            })

    # Policy passed — execute
    try:
        tx_result = mgr.send(wallet.wallet_id, to_address, amount, decided_by="policy_auto")
        if policy:
            policy.record_transaction(tx_req)

        if tx_result.status == "failed":
            return json.dumps({
                "status": "failed",
                "error": tx_result.error,
            })

        return json.dumps({
            "status": "submitted",
            "tx_hash": tx_result.tx_hash,
            "explorer_url": tx_result.explorer_url,
            "chain": tx_result.chain,
            "from": wallet.address,
            "to": to_address,
            "amount": str(amount),
            "symbol": symbol,
        })
    except Exception as e:
        return json.dumps({"error": f"Transaction failed: {e}"})


def wallet_history(args: dict, task_id: str = None, **kw) -> str:
    """Get transaction history."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    wallet_id = args.get("wallet_id")
    limit = args.get("limit", 20)

    records = mgr.get_tx_history(wallet_id=wallet_id, limit=limit)
    if not records:
        return json.dumps({"transactions": [], "message": "No transaction history"})

    return json.dumps({
        "transactions": [
            {
                "tx_id": r.tx_id,
                "chain": r.chain,
                "to": r.to_address,
                "amount": r.amount,
                "symbol": r.symbol,
                "tx_hash": r.tx_hash,
                "status": r.status,
                "time": r.requested_at,
            }
            for r in records
        ],
    })


def wallet_estimate_gas(args: dict, task_id: str = None, **kw) -> str:
    """Estimate transaction fee."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    to_address = args.get("to", "")
    amount_str = args.get("amount", "0.01")
    wallet_id = args.get("wallet_id")
    chain = args.get("chain")

    try:
        amount = Decimal(amount_str)
        wallet = mgr.resolve_wallet(wallet_id=wallet_id, chain=chain)
        estimate = mgr.estimate_fee(wallet.wallet_id, to_address, amount)
        return json.dumps({
            "chain": estimate.chain,
            "estimated_fee": str(estimate.estimated_fee),
            "symbol": estimate.symbol,
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


def wallet_address(args: dict, task_id: str = None, **kw) -> str:
    """Get a wallet's deposit address — for sharing with others or receiving funds."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    wallet_id = args.get("wallet_id")
    chain = args.get("chain")

    try:
        wallet = mgr.resolve_wallet(wallet_id=wallet_id, chain=chain)
        provider = mgr.get_provider(wallet.chain)
        return json.dumps({
            "wallet": wallet.label,
            "wallet_id": wallet.wallet_id,
            "chain": wallet.chain,
            "network": provider.config.display_name,
            "address": wallet.address,
            "type": wallet.wallet_type,
            "is_testnet": provider.config.is_testnet,
            "message": (
                f"Address for {wallet.label} on {provider.config.display_name}: "
                f"{wallet.address}"
            ),
        })
    except Exception as e:
        return json.dumps({"error": str(e)})


def wallet_networks(args: dict = None, task_id: str = None, **kw) -> str:
    """List supported blockchain networks and which have active wallets."""
    mgr, _ = _get_manager()
    if mgr is None:
        return json.dumps({"error": "Wallet not available"})

    wallets = mgr.list_wallets()
    wallet_chains = {w.chain for w in wallets}

    networks = []
    for chain_id in mgr.supported_chains:
        try:
            provider = mgr.get_provider(chain_id)
            cfg = provider.config
            networks.append({
                "chain_id": chain_id,
                "name": cfg.display_name,
                "symbol": cfg.symbol,
                "is_testnet": cfg.is_testnet,
                "has_wallet": chain_id in wallet_chains,
                "rpc_url": cfg.rpc_url,
            })
        except Exception:
            networks.append({"chain_id": chain_id, "status": "error"})

    # Group by mainnet/testnet for readability
    mainnets = [n for n in networks if not n.get("is_testnet")]
    testnets = [n for n in networks if n.get("is_testnet")]

    return json.dumps({
        "mainnets": mainnets,
        "testnets": testnets,
        "total_networks": len(networks),
        "networks_with_wallets": len(wallet_chains),
    })


# =========================================================================
# Tool registration
# =========================================================================

registry.register(
    name="wallet_list",
    toolset="wallet",
    schema={
        "name": "wallet_list",
        "description": (
            "List all crypto wallets with their addresses and balances. "
            "Shows wallet ID, label, chain, address, and current balance."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    handler=lambda args, **kw: wallet_list(**kw),
    check_fn=_check_wallet_available,
    emoji="💰",
)

registry.register(
    name="wallet_balance",
    toolset="wallet",
    schema={
        "name": "wallet_balance",
        "description": "Check the native token balance of a crypto wallet.",
        "parameters": {
            "type": "object",
            "properties": {
                "wallet_id": {
                    "type": "string",
                    "description": "Wallet ID (optional — uses default if only one wallet exists)",
                },
                "chain": {
                    "type": "string",
                    "description": "Chain name (e.g. 'ethereum', 'solana', 'base')",
                },
            },
            "required": [],
        },
    },
    handler=lambda args, **kw: wallet_balance(args, **kw),
    check_fn=_check_wallet_available,
    emoji="💰",
)

registry.register(
    name="wallet_send",
    toolset="wallet",
    schema={
        "name": "wallet_send",
        "description": (
            "Send native tokens (ETH, SOL, etc.) to an address. "
            "Subject to spending limits and may require owner approval for large amounts. "
            "Returns transaction hash on success or pending_approval status."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "to": {
                    "type": "string",
                    "description": "Recipient wallet address",
                },
                "amount": {
                    "type": "string",
                    "description": "Amount to send in native token units (e.g. '0.01' for 0.01 ETH)",
                },
                "wallet_id": {
                    "type": "string",
                    "description": "Wallet ID to send from (optional — uses default if only one)",
                },
                "chain": {
                    "type": "string",
                    "description": "Chain name (optional if wallet_id is provided)",
                },
            },
            "required": ["to", "amount"],
        },
    },
    handler=lambda args, **kw: wallet_send(args, **kw),
    check_fn=_check_wallet_available,
    emoji="📤",
)

registry.register(
    name="wallet_history",
    toolset="wallet",
    schema={
        "name": "wallet_history",
        "description": "Get recent transaction history for a wallet.",
        "parameters": {
            "type": "object",
            "properties": {
                "wallet_id": {
                    "type": "string",
                    "description": "Wallet ID (optional — shows all if omitted)",
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of transactions to return (default: 20)",
                },
            },
            "required": [],
        },
    },
    handler=lambda args, **kw: wallet_history(args, **kw),
    check_fn=_check_wallet_available,
    emoji="📋",
)

registry.register(
    name="wallet_estimate_gas",
    toolset="wallet",
    schema={
        "name": "wallet_estimate_gas",
        "description": "Estimate the transaction fee for sending tokens.",
        "parameters": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "Recipient address"},
                "amount": {"type": "string", "description": "Amount to send"},
                "wallet_id": {"type": "string", "description": "Wallet ID"},
                "chain": {"type": "string", "description": "Chain name"},
            },
            "required": ["to"],
        },
    },
    handler=lambda args, **kw: wallet_estimate_gas(args, **kw),
    check_fn=_check_wallet_available,
    emoji="⛽",
)

registry.register(
    name="wallet_address",
    toolset="wallet",
    schema={
        "name": "wallet_address",
        "description": (
            "Get a wallet's deposit address for receiving funds. "
            "Use this to share your wallet address with others or to check "
            "which address to send funds to."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "wallet_id": {
                    "type": "string",
                    "description": "Wallet ID (optional — uses default if only one wallet exists)",
                },
                "chain": {
                    "type": "string",
                    "description": "Chain name (e.g. 'solana', 'ethereum', 'base')",
                },
            },
            "required": [],
        },
    },
    handler=lambda args, **kw: wallet_address(args, **kw),
    check_fn=_check_wallet_available,
    emoji="📬",
)

registry.register(
    name="wallet_networks",
    toolset="wallet",
    schema={
        "name": "wallet_networks",
        "description": (
            "List all supported blockchain networks and which ones have active wallets. "
            "Shows mainnets and testnets separately with their native token symbols."
        ),
        "parameters": {"type": "object", "properties": {}, "required": []},
    },
    handler=lambda args, **kw: wallet_networks(**kw),
    check_fn=_check_wallet_available,
    emoji="🌐",
)
