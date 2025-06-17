rpc_config = {
    "ethereum": {
        "processors": "evm",
        "cluster": "l1",
        "sleeper": 6,
        "calc_fees": True,
    },
    "zksync_era": {
        "processors": "evm",
        "cluster": "elastic",
        "sleeper": 3,
        "calc_fees": False,
    },
    "mode": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    "base": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 2,
        "calc_fees": False,
    },
    "linea": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    "optimism": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "blast": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    "scroll": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    "arbitrum": {
        "processors": "evm",
        "sleeper": 2,
        "calc_fees": False,
    },
    "unichain": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "taiko": {
        "processors": "evm",
        "sleeper": 6,
        "calc_fees": False,
    },
    "manta": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    "redstone": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    "soneium": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "worldchain": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "arbitrum_nova": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    "zircuit": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    "swell": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },
    "ink": {
        "processors": "evm",
        "cluster": "op_stack",
        "sleeper": 3,
        "calc_fees": False,
    },

    # Custom Gas EVM chains
    "mantle": {
        "processors": "evm_custom_gas",
        "sleeper": 3,
        "calc_fees": False,
    },
    "celo": {
        "processors": "evm_custom_gas",
        "sleeper": 2,
        "calc_fees": False,
    },

    # Non-EVM chains
    "starknet": {
        "processors": "starknet",
        "sleeper": 5,
        "calc_fees": False,
    },
}