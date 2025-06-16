rpc_config = {
    "zksync_era": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "mode": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "base": {"prep_script": "evm", "sleeper": 2, "calc_fees": False},
    "linea": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "optimism": {"prep_script": "evm", "sleeper": 3, "calc_fees": True},
    "ethereum": {"prep_script": "evm", "sleeper": 6, "calc_fees": True},
    "blast": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "scroll": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "arbitrum": {"prep_script": "evm", "sleeper": 2, "calc_fees": False},
    "unichain": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "taiko": {"prep_script": "evm", "sleeper": 6, "calc_fees": False},
    "manta": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "redstone": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "soneium": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "worldchain": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "arbitrum_nova": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "zircuit": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "swell": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},
    "ink": {"prep_script": "evm", "sleeper": 3, "calc_fees": False},

    # Custom Gas EVM chains
    "mantle": {"prep_script": "evm_custom_gas", "sleeper": 3, "calc_fees": False},
     "celo": {"prep_script": "evm_custom_gas", "sleeper": 2, "calc_fees": False},

    # Non-EVM chains
    "starknet": {"prep_script": "starknet", "sleeper": 5, "calc_fees": False},
}
