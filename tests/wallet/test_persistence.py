"""Tests for wallet persistence: tx log, policy freeze, export path."""

import json
from decimal import Decimal

import pytest

nacl = pytest.importorskip("nacl")
argon2 = pytest.importorskip("argon2")

from keystore.client import KeystoreClient
from wallet.manager import WalletManager
from wallet.policy import PolicyEngine, TxRequest, PolicyVerdict
from wallet.chains import ChainProvider, Balance, TransactionResult, GasEstimate, ChainConfig


class FakeProvider(ChainProvider):
    def __init__(self):
        super().__init__(ChainConfig(
            chain_id="test-chain",
            display_name="Test Chain",
            symbol="TEST",
            decimals=18,
            rpc_url="http://localhost:8545",
            explorer_url="https://testscan.io",
            is_testnet=True,
        ))

    def get_balance(self, address: str) -> Balance:
        return Balance("test-chain", address, Decimal("10"), 10, "TEST", 18)

    def send_transaction(self, from_private_key, to_address, amount) -> TransactionResult:
        return TransactionResult(
            tx_hash="0xabc",
            chain="test-chain",
            status="submitted",
            explorer_url="https://testscan.io/tx/0xabc",
        )

    def estimate_fee(self, from_address, to_address, amount) -> GasEstimate:
        return GasEstimate("test-chain", Decimal("0.001"), 1, "TEST")

    def validate_address(self, address: str) -> bool:
        return True

    def generate_keypair(self):
        return ("0x" + "A" * 40, "deadbeef" * 8)

    @staticmethod
    def address_from_key(private_key: str) -> str:
        return "0x" + "B" * 40


@pytest.fixture
def ks(tmp_path):
    db = tmp_path / "keystore" / "secrets.db"
    client = KeystoreClient(db)
    client.initialize("test-pass")
    return client


def test_export_private_key_for_cli_export_requester(ks, tmp_path):
    mgr = WalletManager(ks, state_dir=tmp_path / "wallet")
    mgr.register_provider("test-chain", FakeProvider())
    w = mgr.create_wallet(chain="test-chain", label="Exportable")
    exported = mgr.export_private_key(w.wallet_id)
    assert exported
    assert isinstance(exported, str)


def test_tx_history_persists_across_manager_instances(ks, tmp_path):
    state_dir = tmp_path / "wallet"
    mgr1 = WalletManager(ks, state_dir=state_dir)
    mgr1.register_provider("test-chain", FakeProvider())
    w = mgr1.create_wallet(chain="test-chain")
    mgr1.send(w.wallet_id, "0xreceiver", Decimal("1.0"), decided_by="owner_cli", policy_result='{}')

    mgr2 = WalletManager(ks, state_dir=state_dir)
    mgr2.register_provider("test-chain", FakeProvider())
    hist = mgr2.get_tx_history(w.wallet_id)
    assert len(hist) == 1
    assert hist[0].tx_hash == "0xabc"


def test_tx_history_merges_across_multiple_manager_instances(ks, tmp_path):
    state_dir = tmp_path / "wallet"
    mgr1 = WalletManager(ks, state_dir=state_dir)
    mgr1.register_provider("test-chain", FakeProvider())
    w = mgr1.create_wallet(chain="test-chain")

    mgr2 = WalletManager(ks, state_dir=state_dir)
    mgr2.register_provider("test-chain", FakeProvider())

    mgr1.send(w.wallet_id, "0xreceiver1", Decimal("1.0"), decided_by="owner_cli", policy_result='{}')
    mgr2.send(w.wallet_id, "0xreceiver2", Decimal("2.0"), decided_by="owner_cli", policy_result='{}')

    mgr3 = WalletManager(ks, state_dir=state_dir)
    mgr3.register_provider("test-chain", FakeProvider())
    hist = mgr3.get_tx_history(w.wallet_id, limit=10)
    assert len(hist) == 2


def test_policy_freeze_persists_across_instances(tmp_path):
    state = tmp_path / "wallet" / "policy_state.json"
    p1 = PolicyEngine(state_path=state)
    p1.freeze()

    p2 = PolicyEngine(state_path=state)
    tx = TxRequest(
        wallet_id="w1", wallet_type="agent", chain="test-chain",
        to_address="0xreceiver", amount=Decimal("0.1"), symbol="TEST",
    )
    result = p2.evaluate(tx)
    assert result.verdict == PolicyVerdict.BLOCK
    assert "frozen" in result.reason.lower()


def test_policy_record_transaction_persists(tmp_path):
    state = tmp_path / "wallet" / "policy_state.json"
    p1 = PolicyEngine(state_path=state)
    tx = TxRequest(
        wallet_id="w1", wallet_type="agent", chain="test-chain",
        to_address="0xreceiver", amount=Decimal("0.5"), symbol="TEST",
    )
    p1.record_transaction(tx)

    p2 = PolicyEngine(state_path=state)
    p2._policies = {"cooldown": {"min_seconds": 99999}}
    result = p2.evaluate(tx)
    assert result.verdict == PolicyVerdict.BLOCK
    assert result.failed == "cooldown"


def test_policy_record_transaction_merges_across_instances(tmp_path):
    state = tmp_path / "wallet" / "policy_state.json"
    tx = TxRequest(
        wallet_id="w1", wallet_type="agent", chain="test-chain",
        to_address="0xreceiver", amount=Decimal("1.0"), symbol="TEST",
    )
    p1 = PolicyEngine(state_path=state)
    p2 = PolicyEngine(state_path=state)
    p1.record_transaction(tx)
    p2.record_transaction(tx)

    p3 = PolicyEngine(state_path=state, policies={"daily_limit": {"max_native": "1.5"}})
    result = p3.evaluate(tx)
    assert result.verdict == PolicyVerdict.BLOCK
    assert result.failed == "daily_limit"


def test_user_wallet_hard_blocks_run_before_require_approval(tmp_path):
    state = tmp_path / "wallet" / "policy_state.json"
    p = PolicyEngine(
        state_path=state,
        policies={
            "spending_limit": {"max_native": "0.5"},
            "blocked_recipients": {"addresses": ["0xblocked"]},
        },
    )
    tx = TxRequest(
        wallet_id="w1", wallet_type="user", chain="test-chain",
        to_address="0xblocked", amount=Decimal("1.0"), symbol="TEST",
    )
    result = p.evaluate(tx)
    assert result.verdict == PolicyVerdict.BLOCK
    assert result.failed in {"spending_limit", "blocked_recipients"}
