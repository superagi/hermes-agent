"""Tests for wallet.manager — wallet lifecycle and operations."""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

nacl = pytest.importorskip("nacl")
argon2 = pytest.importorskip("argon2")

from keystore.client import KeystoreClient
from wallet.manager import WalletManager, WalletInfo, WalletNotFound, WalletError
from wallet.chains import ChainProvider, Balance, TransactionResult, GasEstimate, ChainConfig


class FakeProvider(ChainProvider):
    """Mock chain provider for testing."""

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
        self._balances = {}
        self._next_tx_hash = "0xabcdef1234567890"
        self._counter = 0

    def get_balance(self, address: str) -> Balance:
        bal = self._balances.get(address, Decimal("1.5"))
        return Balance(
            chain="test-chain", address=address,
            balance=bal, balance_raw=int(bal * 10**18),
            symbol="TEST", decimals=18,
        )

    def send_transaction(self, from_private_key, to_address, amount) -> TransactionResult:
        return TransactionResult(
            tx_hash=self._next_tx_hash,
            chain="test-chain",
            status="submitted",
            explorer_url=f"https://testscan.io/tx/{self._next_tx_hash}",
        )

    def estimate_fee(self, from_address, to_address, amount) -> GasEstimate:
        return GasEstimate(
            chain="test-chain",
            estimated_fee=Decimal("0.001"),
            estimated_fee_raw=1000000000000000,
            symbol="TEST",
        )

    def validate_address(self, address: str) -> bool:
        return address.startswith("0x") and len(address) == 42

    def generate_keypair(self):
        self._counter += 1
        suffix = format(self._counter, "040x")[-40:]
        return ("0x" + suffix.upper(), ("deadbeef" * 7) + format(self._counter, "08x"))

    @staticmethod
    def address_from_key(private_key: str) -> str:
        return "0x" + "B" * 40


@pytest.fixture
def ks(tmp_path):
    """Initialized and unlocked keystore."""
    db = tmp_path / "keystore" / "secrets.db"
    client = KeystoreClient(db)
    client.initialize("test-pass")
    return client


@pytest.fixture
def mgr(ks):
    """Wallet manager with a fake test chain provider."""
    m = WalletManager(ks)
    m.register_provider("test-chain", FakeProvider())
    return m


class TestWalletCreation:
    def test_create_wallet(self, mgr):
        w = mgr.create_wallet(chain="test-chain", label="My Test Wallet")
        assert w.label == "My Test Wallet"
        assert w.chain == "test-chain"
        assert w.address.startswith("0x")
        assert w.wallet_type == "user"

    def test_create_agent_wallet(self, mgr):
        w = mgr.create_wallet(chain="test-chain", wallet_type="agent")
        assert w.wallet_type == "agent"

    def test_create_unsupported_chain(self, mgr):
        with pytest.raises(WalletError, match="No provider"):
            mgr.create_wallet(chain="nonexistent")

    def test_list_wallets(self, mgr):
        mgr.create_wallet(chain="test-chain", label="W1")
        mgr.create_wallet(chain="test-chain", label="W2")
        wallets = mgr.list_wallets()
        assert len(wallets) == 2
        labels = {w.label for w in wallets}
        assert "W1" in labels
        assert "W2" in labels

    def test_get_wallet(self, mgr):
        w = mgr.create_wallet(chain="test-chain", label="Find Me")
        found = mgr.get_wallet(w.wallet_id)
        assert found.label == "Find Me"
        assert found.address == w.address

    def test_get_wallet_not_found(self, mgr):
        with pytest.raises(WalletNotFound):
            mgr.get_wallet("w_nonexistent")

    def test_delete_wallet(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        assert mgr.delete_wallet(w.wallet_id)
        assert len(mgr.list_wallets()) == 0


class TestImport:
    def test_import_wallet(self, mgr):
        w = mgr.import_wallet(
            chain="test-chain",
            private_key="deadbeef" * 8,
            label="Imported",
        )
        assert w.label == "Imported"
        assert w.address == "0x" + "B" * 40  # from FakeProvider.address_from_key

    def test_duplicate_import_same_address_rejected(self, mgr):
        mgr.import_wallet(chain="test-chain", private_key="deadbeef" * 8, label="Imported")
        with pytest.raises(WalletError, match="already exists"):
            mgr.import_wallet(chain="test-chain", private_key="deadbeef" * 8, label="Imported Again")

    def test_delete_does_not_remove_shared_key_of_other_wallet(self, mgr):
        # Create one wallet via import, then manually create a second metadata alias to same address
        w = mgr.import_wallet(chain="test-chain", private_key="deadbeef" * 8, label="Imported")
        # Direct metadata insert simulates legacy duplicate state
        mgr._ks.set_secret(
            "wallet:meta:w_dup",
            json.dumps({
                "wallet_id": "w_dup",
                "label": "Duplicate",
                "chain": w.chain,
                "address": w.address,
                "wallet_type": "user",
                "created_at": w.created_at,
            }),
            category="sealed",
        )
        assert mgr.delete_wallet(w.wallet_id)
        # Duplicate metadata should still be able to export/read key
        dup = mgr.get_wallet("w_dup")
        assert mgr.export_private_key(dup.wallet_id)


class TestBalance:
    def test_get_balance(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        bal = mgr.get_balance(w.wallet_id)
        assert bal.symbol == "TEST"
        assert bal.balance == Decimal("1.5")


class TestSend:
    def test_send_success(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        result = mgr.send(w.wallet_id, "0x" + "C" * 40, Decimal("0.1"))
        assert result.status == "submitted"
        assert result.tx_hash == "0xabcdef1234567890"

    def test_send_invalid_address(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        result = mgr.send(w.wallet_id, "invalid", Decimal("0.1"))
        assert result.status == "failed"
        assert "Invalid address" in result.error

    def test_tx_history(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        mgr.send(w.wallet_id, "0x" + "C" * 40, Decimal("0.1"))
        mgr.send(w.wallet_id, "0x" + "D" * 40, Decimal("0.2"))
        history = mgr.get_tx_history()
        assert len(history) == 2


class TestResolve:
    def test_resolve_single_wallet(self, mgr):
        w = mgr.create_wallet(chain="test-chain")
        resolved = mgr.resolve_wallet()
        assert resolved.wallet_id == w.wallet_id

    def test_resolve_by_chain(self, mgr):
        mgr.create_wallet(chain="test-chain", label="A")
        resolved = mgr.resolve_wallet(chain="test-chain")
        assert resolved.label == "A"

    def test_resolve_no_wallets(self, mgr):
        with pytest.raises(WalletNotFound, match="No wallets"):
            mgr.resolve_wallet()

    def test_resolve_ambiguous(self, mgr):
        mgr.create_wallet(chain="test-chain", label="A")
        mgr.create_wallet(chain="test-chain", label="B")
        with pytest.raises(WalletError, match="Multiple"):
            mgr.resolve_wallet()
