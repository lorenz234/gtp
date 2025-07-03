rpc_config = {
    ## L1
    "ethereum": {
        "processors": "evm",
        "stack": "l1",
        "sleeper": 6,
        "calc_fees": True,
    },
    
    ## OP Stack chains
    "mode": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "base": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 2,
        "calc_fees": True,
    },
    "optimism": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "blast": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "unichain": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "redstone": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "soneium": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 10, # Longer sleeper due to limited RPC access via Startale
        "calc_fees": True,
    },
    "worldchain": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "ink": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    "zora": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 5,
        "calc_fees": True,
    },
    "swell": {
        "processors": "evm",
        "stack": "op_stack",
        "sleeper": 3,
        "calc_fees": True,
    },
    
    ## Nitro/Arbitrum Orbit chains
    "arbitrum": {
        "processors": "evm",
        "stack": "nitro",
        "sleeper": 2,
        "calc_fees": True,
    },
    
    "arbitrum_nova": {
        "processors": "evm",
        "stack": "nitro",
        "sleeper": 3,
        "calc_fees": True,
    },
    
    ## Elastic chains
    "zksync_era": {
        "processors": "evm",
        "stack": "elastic",
        "sleeper": 3,
        "calc_fees": False,
    },
    
    ## Others
    "linea": {
        "processors": "evm",
        "stack": "basic",
        "sleeper": 5,
        "calc_fees": True,
    },
    
    "scroll": {
        "processors": "evm",
        "stack": "basic",
        "sleeper": 5,
        "calc_fees": True,
    },
    
    "taiko": {
        "processors": "evm",
        "stack": "basic",
        "sleeper": 6,
        "calc_fees": True,
    },
    
    "manta": {
        "processors": "evm",
        "sleeper": 3,
        "calc_fees": False,
    },
    
    "zircuit": {
        "processors": "evm",
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