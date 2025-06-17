rpc_config = {
    "zksync_era": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "mode": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "base": {"processors": "evm", "sleeper": 2, "calc_fees": False},
    "linea": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "optimism": {"processors": "evm", "sleeper": 3, "calc_fees": True},
    "ethereum": {"processors": "evm", "sleeper": 6, "calc_fees": True},
    "blast": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "scroll": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "arbitrum": {"processors": "evm", "sleeper": 2, "calc_fees": False},
    "unichain": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "taiko": {"processors": "evm", "sleeper": 6, "calc_fees": False},
    "manta": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "redstone": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "soneium": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "worldchain": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "arbitrum_nova": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "zircuit": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "swell": {"processors": "evm", "sleeper": 3, "calc_fees": False},
    "ink": {"processors": "evm", "sleeper": 3, "calc_fees": False},

    # Custom Gas EVM chains
    "mantle": {"processors": "evm_custom_gas", "sleeper": 3, "calc_fees": False},
     "celo": {"processors": "evm_custom_gas", "sleeper": 2, "calc_fees": False},

    # Non-EVM chains
    "starknet": {"processors": "starknet", "sleeper": 5, "calc_fees": False},
}
