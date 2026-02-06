from typing import Iterable, List, Optional, Sequence

from web3 import Web3

# see documentation & how to deploy here: https://github.com/growthepie/backend-internal/tree/main/SupplyReader

DEFAULT_SUPPLY_READER_ADDRESS = "0x36D0094A899045f6E1a2DC0Aa470ba980b6F297F"

SUPPLY_READER_ABI = [
    {
        "inputs": [{"internalType": "address[]", "name": "tokenAddresses", "type": "address[]"}],
        "name": "getTotalSupplies",
        "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address[]", "name": "tokenAddresses", "type": "address[]"}],
        "name": "getDecimals",
        "outputs": [{"internalType": "uint8[]", "name": "", "type": "uint8[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address[]", "name": "addresses", "type": "address[]"}],
        "name": "etherBalances",
        "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "owner", "type": "address"},
            {"internalType": "address[]", "name": "tokens", "type": "address[]"},
        ],
        "name": "tokensBalance",
        "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address[]", "name": "owners", "type": "address[]"},
            {"internalType": "address", "name": "token", "type": "address"},
        ],
        "name": "tokenBalances",
        "outputs": [{"internalType": "uint256[]", "name": "", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]


class SupplyReaderAdapter:
    """
    Thin wrapper around the SupplyReader contract. Expects a Web3 instance
    (already configured for the target chain) at init.
    """

    def __init__(self, web3: Web3, contract_address: str = DEFAULT_SUPPLY_READER_ADDRESS):
        self.web3 = web3
        self.contract_address = self._checksum_address(contract_address)
        self.contract = self.web3.eth.contract(
            address=self.contract_address,
            abi=SUPPLY_READER_ABI,
        )

    def get_total_supplies(
        self,
        token_addresses: Sequence[str],
        block_number: Optional[int] = None,
    ) -> List[int]:
        tokens = self._checksum_addresses(token_addresses)
        call = self.contract.functions.getTotalSupplies(tokens)
        return call.call(block_identifier=block_number) if block_number is not None else call.call()

    def get_decimals(
        self,
        token_addresses: Sequence[str],
        block_number: Optional[int] = None,
    ) -> List[int]:
        tokens = self._checksum_addresses(token_addresses)
        call = self.contract.functions.getDecimals(tokens)
        return call.call(block_identifier=block_number) if block_number is not None else call.call()

    def ether_balances(
        self,
        addresses: Sequence[str],
        block_number: Optional[int] = None,
    ) -> List[int]:
        owners = self._checksum_addresses(addresses)
        call = self.contract.functions.etherBalances(owners)
        return call.call(block_identifier=block_number) if block_number is not None else call.call()

    def tokens_balance(
        self,
        owner: str,
        tokens: Sequence[str],
        block_number: Optional[int] = None,
    ) -> List[int]:
        owner_checksum = self._checksum_address(owner)
        tokens_checksum = self._checksum_addresses(tokens)
        call = self.contract.functions.tokensBalance(owner_checksum, tokens_checksum)
        return call.call(block_identifier=block_number) if block_number is not None else call.call()

    def token_balances(
        self,
        owners: Sequence[str],
        token: str,
        block_number: Optional[int] = None,
    ) -> List[int]:
        owners_checksum = self._checksum_addresses(owners)
        token_checksum = self._checksum_address(token)
        call = self.contract.functions.tokenBalances(owners_checksum, token_checksum)
        return call.call(block_identifier=block_number) if block_number is not None else call.call()

    @staticmethod
    def _checksum_address(address: str) -> str:
        return address if Web3.is_checksum_address(address) else Web3.to_checksum_address(address)

    def _checksum_addresses(self, addresses: Iterable[str]) -> List[str]:
        return [self._checksum_address(addr) for addr in addresses]
