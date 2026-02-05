# Token Metadata

# Rule 1: No value increasing stablecoins allowed! e.g. sUSDS
# Rule 2: Stablecoins that mainly wrap other stablecoins are not allowed, unless it is a bridge! e.g. Aave aUSDC, IUSD, dtrinity USD
# Rule 3: Only stablecoins that anyone can own are allowed! e.g. Blackrock BUIDL

coin_mapping = [
    # circlefin
    {
        "owner_project": "circlefin",
        "token_id": "circlefin_usdc", # primary key
        "symbol": "USDC",
        "coingecko_id": ["usd-coin"],
        "metric_key": "direct", # either minted 'direct' on each chain or 'bridged' from one bridged_origin_chain
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694"
    },
    {
        "owner_project": "circlefin",
        "token_id": "circlefin_usdce",
        "symbol": "USDC.e",
        "coingecko_id": ["usd-coin-ethereum-bridged", "bridged-usdc-polygon-pos-bridge", "stargate-bridged-usdc", "bridged-usd-coin-starkgate", "soneium-bridged-usdc-soneium", "binance-bridged-usdc-bnb-smart-chain", "cronos-bridged-usdc-cronos", "bridged-usd-coin-linea", "bridged-usd-coin-optimism", "bridged-usd-coin-scroll", "zksync-bridged-usdc-zksync", "mantle-bridged-usdc-mantle", "bridged-usd-coin-base", "metis-bridged-usdc-metis", "mode-bridged-usdc-mode", "fraxtal-bridged-usdc-fraxtal", "bridged-usd-coin-manta-pacific", "polygon-hermez-bridged-usdc-polygon-zkevm"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum", # origin chain where the supply is bridged from
        "bridged_origin_token_id": "circlefin_usdc", # supply will be deducted from origin chain USDC supply
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694"
    },
    {
        "owner_project": "circlefin",
        "token_id": "circlefin_eurc",
        "symbol": "EURC",
        "coingecko_id": ["euro-coin"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": "https://coin-images.coingecko.com/coins/images/26045/large/euro.png?1696525125"
    },
    # tetherto
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_usdt",
        "symbol": "USDT",
        "coingecko_id": ["tether"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_usdt0",
        "symbol": "USDT0",
        "coingecko_id": ["usdt0"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "tetherto_usdt",
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_usdte",
        "symbol": "USDT.e",
        "coingecko_id": ["binance-bridged-usdt-bnb-smart-chain", "cronos-bridged-usdt-cronos", "bridged-tether-linea", "tether-rainbow-bridge", "sui-bridged-usdt-sui", "l2-standard-bridged-usdt-base", "polygon-hermez-bridged-usdt-polygon-zkevm", "soneium-bridged-usdt-soneium", "bridged-tether-scroll", "zircuit-bridged-usdt-zircuit", "mode-bridged-usdt-mode", "bridged-tether-manta-pacific"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "tetherto_usdt",
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_eurt",
        "symbol": "EURT",
        "coingecko_id": ["tether-eurt"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_usat",
        "symbol": "USAT",
        "coingecko_id": ["usa"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "tetherto",
        "token_id": "tetherto_ausdt",
        "symbol": "AUSDT",
        "coingecko_id": ["usa"],
        "metric_key": "alloy-tether",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # makerdao (now Sky)
    {
        "owner_project": "makerdao",
        "token_id": "makerdao_dai",
        "symbol": "DAI",
        "coingecko_id": ["dai"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "makerdao",
        "token_id": "makerdao_usds",
        "symbol": "USDS",
        "coingecko_id": ["usds"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "makerdao",
        "token_id": "makerdao_daie",
        "symbol": "DAI.e",
        "coingecko_id": ["makerdao-arbitrum-bridged-dai-arbitrum-one", "polygon-pos-bridged-dai-polygon-pos", "makerdao-optimism-bridged-dai-optimism", "bridged-dai-starkgate", "starkgate-bridged-dai-v2-starknet", "l2-standard-bridged-dai-base", "bridged-dai-stablecoin-linea", "zksync-erc20-bridged-dai-zksync", "omnibridge-bridged-dai-gnosis-chain", "polygon-zkevm-bridged-dai-polygon-zkevm"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "makerdao_dai",
        "fiat": "usd",
        "logo": ""
    },
    # ethena-labs
    {
        "owner_project": "ethena-labs",
        "token_id": "ethena_usde",
        "symbol": "USDE",
        "coingecko_id": ["ethena-usde"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "ethena-labs",
        "token_id": "ethena_usdtb",
        "symbol": "USDtb",
        "coingecko_id": ["usdtb"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # binance
    {
        "owner_project": "binance",
        "token_id": "binance_busd",
        "symbol": "BUSD",
        "coingecko_id": ["binance-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "binance",
        "token_id": "binance_busde",
        "symbol": "BUSD.e",
        "coingecko_id": ["binance-peg-busd"],
        "metric_key": "direct",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "first_digital_labs_fdusd",
        "fiat": "usd",
        "logo": ""
    },
    # first-digital-labs
    {
        "owner_project": "first-digital-labs",
        "token_id": "first_digital_labs_fdusd",
        "symbol": "FDUSD",
        "coingecko_id": ["first-digital-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # trueusd
    {
        "owner_project": "trueusd",
        "token_id": "trueusd_tusd",
        "symbol": "TUSD",
        "coingecko_id": ["true-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "trueusd",
        "token_id": "trueusd_tusde",
        "symbol": "TUSD.e",
        "coingecko_id": ["bridged-trueusd"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "trueusd_tusd",
        "fiat": "usd",
        "logo": ""
    },
    # fraxfinance
    {
        "owner_project": "fraxfinance",
        "token_id": "fraxfinance_frax",
        "symbol": "FRAX",
        "coingecko_id": ["frax"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "fraxfinance",
        "token_id": "fraxfinance_frxusd",
        "symbol": "FRXUSD",
        "coingecko_id": ["frax-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # paxosglobal
    {
        "owner_project": "paxosglobal",
        "token_id": "paxosglobal_usdp",
        "symbol": "USDP",
        "coingecko_id": ["paxos-standard"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "paxosglobal",
        "token_id": "paxosglobal_usdg",
        "symbol": "USDG",
        "coingecko_id": ["global-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # gemini-ex
    {
        "owner_project": "gemini-ex",
        "token_id": "gemini_gusd",
        "symbol": "GUSD",
        "coingecko_id": ["gemini-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # paypal
    {
        "owner_project": "paypal",
        "token_id": "paypal_pyusd",
        "symbol": "PYUSD",
        "coingecko_id": ["paypal-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # liquity
    {
        "owner_project": "liquity",
        "token_id": "liquity_lusd",
        "symbol": "LUSD",
        "coingecko_id": ["liquity-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "liquity",
        "token_id": "liquity_lusde",
        "symbol": "LUSD.e",
        "coingecko_id": [None],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "liquity_lusd",
        "fiat": "usd",
        "logo": ""
    },
    # uncap-finance
    {
        "owner_project": "uncap-finance",
        "token_id": "uncap_usdu",
        "symbol": "USDU",
        "coingecko_id": ["uncap-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # fidelity
    {
        "owner_project": "fidelity",
        "token_id": "fidelity_fidd",
        "symbol": "FIDD",
        "coingecko_id": ["fidelity-digital-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # blast-io
    {
        "owner_project": "blast-io",
        "token_id": "blast_io_usdb",
        "symbol": "USDB",
        "coingecko_id": ["usdb"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # mountainprotocol
    {
        "owner_project": "mountainprotocol",
        "token_id": "mountainprotocol_usdm",
        "symbol": "USDM",
        "coingecko_id": ["mountain-protocol-usdm"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # izumi-finance
    {
        "owner_project": "izumi-finance",
        "token_id": "izumi_iusd",
        "symbol": "IUSD",
        "coingecko_id": ["izumi-bond-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # reserve-protocol
    {
        "owner_project": "reserve-protocol",
        "token_id": "reserve_eusd",
        "symbol": "eUSD",
        "coingecko_id": ["electronic-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # openusdt (assume all USDT are wrapped on Ethereum)
    {
        "owner_project": "openusdt",
        "token_id": "openusdt",
        "symbol": "oUSDT",
        "coingecko_id": ["openusdt"],
        "metric_key": "direct",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "tetherto_usdt",
        "fiat": "usd",
        "logo": ""
    },
    # curve
    {
        "owner_project": "curve",
        "token_id": "curve_crvusd",
        "symbol": "crvUSD",
        "coingecko_id": ["crvusd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "curve",
        "token_id": "curve_crvusde",
        "symbol": "crvUSD.e",
        "coingecko_id": [None],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "curve_crvusd",
        "fiat": "usd",
        "logo": ""
    },
    # worldwide-stablecoin-payment-network
    {
        "owner_project": "worldwide-stablecoin-payment-network",
        "token_id": "worldwide_wusd",
        "symbol": "WUSD",
        "coingecko_id": ["worldwide-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # inverse-finance
    {
        "owner_project": "inverse-finance",
        "token_id": "inverse_dola",
        "symbol": "DOLA",
        "coingecko_id": ["dola-usd"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "inverse_dola",
        "fiat": "usd",
        "logo": ""
    },
    # alchemix
    {
        "owner_project": "alchemix",
        "token_id": "alchemix_alusd",
        "symbol": "ALUSD",
        "coingecko_id": ["alchemix-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # usual-money
    {
        "owner_project": "usual-money",
        "token_id": "usual_usd0",
        "symbol": "USD0",
        "coingecko_id": ["usual-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # mento-protocol
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_usdm",
        "symbol": "USDm",
        "coingecko_id": ["celo-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_eurm",
        "symbol": "EURm",
        "coingecko_id": ["celo-euro"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_brlm",
        "symbol": "BRLm",
        "coingecko_id": ["celo-real-creal"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "brl",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_kesm",
        "symbol": "KESm",
        "coingecko_id": ["celo-kenyan-shilling"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "kes",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_phpm",
        "symbol": "PHPm",
        "coingecko_id": ["puso"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "php",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_copm",
        "symbol": "COPm",
        "coingecko_id": ["ccop"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "cop",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_chfm",
        "symbol": "CHFm",
        "coingecko_id": ["cchf"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "chf",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_jpym",
        "symbol": "JPYm",
        "coingecko_id": ["celo-japanese-yen"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "jpy",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_cadm",
        "symbol": "CADm",
        "coingecko_id": ["celo-canadian-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "cad",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_ngcm",
        "symbol": "NGNm",
        "coingecko_id": ["celo-nigerian-naira"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "ngn",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_audm",
        "symbol": "AUDm",
        "coingecko_id": ["celo-australian-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "aud",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_zarm",
        "symbol": "ZARm",
        "coingecko_id": ["celo-south-african-rand"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "zar",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_gbpm",
        "symbol": "GBPm",
        "coingecko_id": ["celo-british-pound"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "gbp",
        "logo": ""
    },
    {
        "owner_project": "mento-protocol",
        "token_id": "mento_xofm",
        "symbol": "XOFm",
        "coingecko_id": [None],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "xof",
        "logo": ""
    },
    # transfero
    {
        "owner_project": "transfero",
        "token_id": "transfero_brz",
        "symbol": "BRZ",
        "coingecko_id": ["brz"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "brl",
        "logo": ""
    },
    {
        "owner_project": "transfero",
        "token_id": "transfero_arz",
        "symbol": "ARZ",
        "coingecko_id": [None],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "ars",
        "logo": ""
    },
    # elixir-protocol
    {
        "owner_project": "elixir-protocol",
        "token_id": "elixir_deusd",
        "symbol": "deUSD",
        "coingecko_id": ["elixir-deusd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # glo-foundation
    {
        "owner_project": "glo-foundation",
        "token_id": "glo_usdglo",
        "symbol": "USDGLO",
        "coingecko_id": ["glo-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # usdxtoken (stables labs), went to 0
    {
        "owner_project": "usdxtoken",
        "token_id": "usdxtoken_usdx",
        "symbol": "USDX",
        "coingecko_id": ["usdx-money-usdx"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # agora-finance
    {
        "owner_project": "agora-finance",
        "token_id": "agora_ausd",
        "symbol": "AUSD",
        "coingecko_id": ["agora-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # resolv-usr
    {
        "owner_project": "resolv-usr",
        "token_id": "resolv_usr",
        "symbol": "USR",
        "coingecko_id": ["resolv-usr"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # plumenetwork
    {
        "owner_project": "plumenetwork",
        "token_id": "plumenetwork_pusd",
        "symbol": "pUSD",
        "coingecko_id": ["plume-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "circlefin_usdc",
        "fiat": "usd",
        "logo": ""
    },
    # stablecoinxyz
    {
        "owner_project": "stablecoinxyz",
        "token_id": "stablecoinxyz_sbc",
        "symbol": "SBC",
        "coingecko_id": ["stable-coin-2"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # metamask
    {
        "owner_project": "metamask",
        "token_id": "metamask_musd",
        "symbol": "mUSD",
        "coingecko_id": ["metamask-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # ripple
    {
        "owner_project": "ripple",
        "token_id": "ripple_rlusd",
        "symbol": "RLUSD",
        "coingecko_id": ["ripple-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # avalon
    {
        "owner_project": "avalon",
        "token_id": "avalon_usda",
        "symbol": "USDa",
        "coingecko_id": ["usda-2"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # worldlibertyfinancial
    {
        "owner_project": "worldlibertyfinancial",
        "token_id": "worldlibertyfinancial_usd1",
        "symbol": "USD1",
        "coingecko_id": ["usd1-wlfi"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # usdd
    {
        "owner_project": "usdd",
        "token_id": "usdd_usdd",
        "symbol": "USDD",
        "coingecko_id": ["usdd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # aave
    {
        "owner_project": "aave",
        "token_id": "aave_gho",
        "symbol": "GHO",
        "coingecko_id": ["gho"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # mzero-labs (normal M is a wrapper of USDM, USDAI...)
    {
        "owner_project": "mzero-labs",
        "token_id": "mzero_wm",
        "symbol": "wM",
        "coingecko_id": ["wrappedm-by-m0"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # mnee
    {
        "owner_project": "mnee",
        "token_id": "mnee_mnee",
        "symbol": "MNEE",
        "coingecko_id": ["mnee-usd-stablecoin"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # startale-usd
    {
        "owner_project": "startale-usd",
        "token_id": "startale_usd",
        "symbol": "USDSC",
        "coingecko_id": ["startale-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # megaeth
    {
        "owner_project": "megaeth",
        "token_id": "megaeth_usdm",
        "symbol": "USDm",
        "coingecko_id": [None],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # cap-money
    {
        "owner_project": "cap-money",
        "token_id": "cap_cusd",
        "symbol": "cUSD",
        "coingecko_id": ["cap-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # monerium
    {
        "owner_project": "monerium",
        "token_id": "monerium_eure",
        "symbol": "EURe",
        "coingecko_id": ["monerium-eur-money-2"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    {
        "owner_project": "monerium",
        "token_id": "monerium_eure_old",
        "symbol": "EURe.old",
        "coingecko_id": ["monerium-eur-money"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    # stasis
    {
        "owner_project": "stasis",
        "token_id": "stasis_eurs",
        "symbol": "EURS",
        "coingecko_id": ["stasis-eurs"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "stasis_eurs",
        "fiat": "eur",
        "logo": ""
    },
    # angle-protocol
    {
        "owner_project": "angle-protocol",
        "token_id": "angle_eura",
        "symbol": "EURA",
        "coingecko_id": ["ageur"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "angle_eura",
        "fiat": "eur",
        "logo": ""
    },
    # allunity
    {
        "owner_project": "allunity",
        "token_id": "allunity_eurau",
        "symbol": "EURAU",
        "coingecko_id": ["allunity-eur"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    # coinvertible
    {
        "owner_project": "coinvertible",
        "token_id": "coinvertible_eurcv",
        "symbol": "EURCV",
        "coingecko_id": ["societe-generale-forge-eurcv"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    {
        "owner_project": "coinvertible",
        "token_id": "coinvertible_usdcv",
        "symbol": "USDCV",
        "coingecko_id": ["usd-coinvertible"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # noon-capital
    {
        "owner_project": "noon-capital",
        "token_id": "noon_capital_usn",
        "symbol": "USN",
        "coingecko_id": ["noon-usn"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # eurite
    {
        "owner_project": "eurite",
        "token_id": "eurite_euri",
        "symbol": "EURI",
        "coingecko_id": ["eurite"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    # anchored-coins
    {
        "owner_project": "anchored-coins",
        "token_id": "anchored_coins_aeur",
        "symbol": "AEUR",
        "coingecko_id": ["anchored-coins-eur"],
        "metric_key": "",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "",
        "logo": ""
    },
    {
        "owner_project": "anchored-coins",
        "token_id": "anchored_coins_achf",
        "symbol": "ACHF",
        "coingecko_id": ["anchored-coins-chf"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "chf",
        "logo": ""
    },
    # stablr
    {
        "owner_project": "stablr",
        "token_id": "stablr_eurr",
        "symbol": "EURR",
        "coingecko_id": ["stablr-euro"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "eur",
        "logo": ""
    },
    {
        "owner_project": "stablr",
        "token_id": "stablr_usdr",
        "symbol": "USDR",
        "coingecko_id": ["stablr-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # frankencoin
    {
        "owner_project": "frankencoin",
        "token_id": "frankencoin_zchf",
        "symbol": "ZCHF",
        "coingecko_id": ["frankencoin"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "frankencoin_zchf",
        "fiat": "chf",
        "logo": ""
    },
    # straitsx
    {
        "owner_project": "straitsx",
        "token_id": "straitsx_xsgd",
        "symbol": "XSGD",
        "coingecko_id": ["xsgd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "sgd",
        "logo": ""
    },
    {
        "owner_project": "straitsx",
        "token_id": "straitsx_xusd",
        "symbol": "XUSD",
        "coingecko_id": ["straitsx-xusd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    {
        "owner_project": "straitsx",
        "token_id": "straitsx_xidr",
        "symbol": "XIDR",
        "coingecko_id": ["straitsx-indonesia-rupiah"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "idr",
        "logo": ""
    },
    # a7a5
    {
        "owner_project": "a7a5",
        "token_id": "a7a5_a7a5",
        "symbol": "A7A5",
        "coingecko_id": ["a7a5"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "rub",
        "logo": ""
    },
    # apacx
    {
        "owner_project": "apacx",
        "token_id": "apacx_pht",
        "symbol": "PHT",
        "coingecko_id": ["pht-stablecoin"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "php",
        "logo": ""
    },
    # audd-digital
    {
        "owner_project": "audd-digital",
        "token_id": "audd-digital_audd",
        "symbol": "AUDD",
        "coingecko_id": ["novatti-australian-digital-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "aud",
        "logo": ""
    },
    # tokenised-gbp
    {
        "owner_project": "tokenised-gbp",
        "token_id": "tokenised-gbp_tgbp",
        "symbol": "tGBP",
        "coingecko_id": ["tokenised-gbp"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "gbp",
        "logo": ""
    },
    # bilira-org
    {
        "owner_project": "bilira-org",
        "token_id": "bilira-org_tryb",
        "symbol": "TRYb",
        "coingecko_id": ["bilira"],
        "metric_key": "bridged",
        "bridged_origin_chain": "ethereum",
        "bridged_origin_token_id": "bilira-org_tryb",
        "fiat": "try",
        "logo": ""
    },
    # falconfinance
    {
        "owner_project": "falconfinance",
        "token_id": "falconfinance_usdf",
        "symbol": "USDf",
        "coingecko_id": ["falcon-finance"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # usdai
    {
        "owner_project": "usdai",
        "token_id": "usdai_usdai",
        "symbol": "USDai",
        "coingecko_id": ["usdai"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # neutrl
    {
        "owner_project": "neutrl",
        "token_id": "neutrl_nusd",
        "symbol": "NUSD",
        "coingecko_id": ["nusd-2"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # satoshi-protocol
    {
        "owner_project": "satoshi-protocol",
        "token_id": "satoshi_satusd",
        "symbol": "SATUSD",
        "coingecko_id": ["satoshi-stablecoin"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # anzen-finance
    {
        "owner_project": "anzen-finance",
        "token_id": "anzen_usdz",
        "symbol": "USDz",
        "coingecko_id": ["anzen-usdz"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # arks-labs
    {
        "owner_project": "arks-labs",
        "token_id": "arks_cgusd",
        "symbol": "cgUSD",
        "coingecko_id": ["cygnus-finance-global-usd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # openedenhq
    {
        "owner_project": "openedenhq",
        "token_id": "openedenhq_usdo",
        "symbol": "USDO",
        "coingecko_id": ["openeden-open-dollar"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # usdkg
    {
        "owner_project": "usdkg",
        "token_id": "usdkg_usdkg",
        "symbol": "USDKG",
        "coingecko_id": ["usdkg"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
    # aegis-im
    {
        "owner_project": "aegis-im",
        "token_id": "aegis-im_yusd",
        "symbol": "YUSD",
        "coingecko_id": ["aegis-yusd"],
        "metric_key": "direct",
        "bridged_origin_chain": None,
        "bridged_origin_token_id": None,
        "fiat": "usd",
        "logo": ""
    },
]

# postgres table for fact_stables:
# date | token_id | metric_key | address | decimals | value

# postgres table for sys_stables:
# owner_project | token_id | symbol | coingecko_id | metric_key | bridged_origin_chain | bridged_origin_token_id | fiat | logo

# Token Address Mapping
address_mapping = {
    "ethereum": {
        "circlefin_usdc": {
            "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "decimals": 6
        },
        "circlefin_eurc": {
            "address": "0x1abaea1f7c830bd89acc67ec4af516284b1bc33c",
            "decimals": 6
        },
        "tetherto_usdt": {
            "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "decimals": 6
        },
        "tetherto_eurt": {
            "address": "0xc581b735a1688071a1746c968e0798d642ede491",
            "decimals": 6
        },
        "tetherto_usat": {
            "address": "0x07041776f5007aca2a54844f50503a18a72a8b68",
            "decimals": 6
        },
        "tetherto_ausdt": {
            "address": "0x07041776f5007aca2a54844f50503a18a72a8b68",
            "decimals": 6
        },
        "makerdao_dai": {
            "address": "0x6b175474e89094c44da98b954eedeac495271d0f",
            "decimals": 18
        },
        "makerdao_usds": {
            "address": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
            "decimals": 18
        },
        "ethena_usde": {
            "address": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
            "decimals": 18
        },
        "ethena_usdtb": {
            "address": "0xc139190f447e929f090edeb554d95abb8b18ac1c",
            "decimals": 18
        },
        "binance_busd": {
            "address": "0x4fabb145d64652a948d72533023f6e7a623c7c53",
            "decimals": 18
        },
        "first_digital_labs_fdusd": {
            "address": "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
            "decimals": 18
        },
        "trueusd_tusd": {
            "address": "0x0000000000085d4780b73119b644ae5ecd22b376",
            "decimals": 18
        },
        "fraxfinance_frax": {
            "address": "0x853d955acef822db058eb8505911ed77f175b99e",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0xcacd6fd266af91b8aed52accc382b4e165586e29",
            "decimals": 18
        },
        "paxosglobal_usdp": {
            "address": "0x8e870d67f660d95d5be530380d0ec0bd388289e1",
            "decimals": 18
        },
        "paxosglobal_usdg": {
            "address": "0xe343167631d89b6ffc58b88d6b7fb0228795491d",
            "decimals": 6
        },
        "gemini_gusd": {
            "address": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
            "decimals": 2
        },
        "paypal_pyusd": {
            "address": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
            "decimals": 6
        },
        "liquity_lusd": {
            "address": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "izumi_iusd": {
            "address": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
            "decimals": 18
        },
        "reserve_eusd": {
            "address": "0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e",
            "decimals": 18
        },
        "worldwide_wusd": {
            "address": "0x7cd017ca5ddb86861fa983a34b5f495c6f898c41",
            "decimals": 18
        },
        "inverse_dola": {
            "address": "0x865377367054516e17014ccded1e7d814edc9ce4",
            "decimals": 18
        },
        "alchemix_alusd": {
            "address": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",
            "decimals": 18
        },
        "usual_usd0": {
            "address": "0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "usdxtoken_usdx": {
            "address": "0xf3527ef8de265eaa3716fb312c12847bfba66cef",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "resolv_usr": {
            "address": "0x66a1e37c9b0eaddca17d3662d6c05f4decf3e110",
            "decimals": 18
        },
        "plumenetwork_pusd": {
            "address": "0xdddd73f5df1f0dc31373357beac77545dc5a6f3f",
            "decimals": 6
        },
        "stablecoinxyz_sbc": {
            "address": "0xf9fb20b8e097904f0ab7d12e9dbee88f2dcd0f16",
            "decimals": 18
        },
        "metamask_musd": {
            "address": "0xaca92e438df0b2401ff60da7e4337b687a2435da",
            "decimals": 6
        },
        "ripple_rlusd": {
            "address": "0x8292bb45bf1ee4d140127049757c2e0ff06317ed",
            "decimals": 18
        },
        "avalon_usda": {
            "address": "0x8a60e489004ca22d775c5f2c657598278d17d9c2",
            "decimals": 18
        },
        "worldlibertyfinancial_usd1": {
            "address": "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
            "decimals": 18
        },
        "usdd_usdd": {
            "address": "0x4f8e5de400de08b164e7421b3ee387f461becd1a",
            "decimals": 18
        },
        "aave_gho": {
            "address": "0x40d16fc0246ad3160ccc09b8d0d3a2cd28ae6c2f",
            "decimals": 18
        },
        "mzero_wm": {
            "address": "0x437cc33344a0b27a429f795ff6b469c72698b291",
            "decimals": 6
        },
        "mnee_mnee": {
            "address": "0x8ccedbae4916b79da7f3f612efb2eb93a2bfd6cf",
            "decimals": 18
        },
        "cap_cusd": {
            "address": "0xcccc62962d17b8914c62d74ffb843d73b2a3cccc",
            "decimals": 18
        },
        "monerium_eure": {
            "address": "0x39b8b6385416f4ca36a20319f70d28621895279d",
            "decimals": 18
        },
        "EURe_eureold": {
            "address": "0x3231cb76718cdef2155fc47b5286d82e6eda273f",
            "decimals": 18
        },
        "stasis_eurs": {
            "address": "0xdb25f211ab05b1c97d595516f45794528a807ad8",
            "decimals": 2
        },
        "angle_eura": {
            "address": "0x1a7e4e63778b4f12a199c062f3efdd288afcbce8",
            "decimals": 18
        },
        "allunity_eurau": {
            "address": "0x4933a85b5b5466fbaf179f72d3de273c287ec2c2",
            "decimals": 6
        },
        "coinvertible_eurcv": {
            "address": "0x5f7827fdeb7c20b443265fc2f40845b715385ff2",
            "decimals": 18
        },
        "coinvertible_usdcv": {
            "address": "0x5422374b27757da72d5265cc745ea906e0446634",
            "decimals": 18
        },
        "eurite_euri": {
            "address": "0x9d1a7a3191102e9f900faa10540837ba84dcbae7",
            "decimals": 18
        },
        "anchored_coins_aeur": {
            "address": "0xa40640458fbc27b6eefedea1e9c9e17d4cee7a21",
            "decimals": 18
        },
        "anchored_coins_achf": {
            "address": "0x653aab62056b92641116d63927de6141d780e596",
            "decimals": 18
        },
        "stablr_eurr": {
            "address": "0x50753cfaf86c094925bf976f218d043f8791e408",
            "decimals": 6
        },
        "stablr_usdr": {
            "address": "0x7b43e3875440b44613dc3bc08e7763e6da63c8f8",
            "decimals": 6
        },
        "frankencoin_zchf": {
            "address": "0xb58e61c3098d85632df34eecfb899a1ed80921cb",
            "decimals": 18
        },
        "straitsx_xsgd": {
            "address": "0x70e8de73ce538da2beed35d14187f6959a8eca96",
            "decimals": 6
        },
        "straitsx_xusd": {
            "address": "0xc08e7e23c235073c6807c2efe7021304cb7c2815",
            "decimals": 6
        },
        "a7a5_a7a5": {
            "address": "0x6fa0be17e4bea2fcfa22ef89bf8ac9aab0ab0fc9",
            "decimals": 6
        },
        "apacx_pht": {
            "address": "0xbe370ad45d44eb45174c4ec60b88839fef32c077",
            "decimals": 18
        },
        "audd-digital_audd": {
            "address": "0x4cce605ed955295432958d8951d0b176c10720d5",
            "decimals": 6
        },
        "tokenised-gbp_tgbp": {
            "address": "0x27f6c8289550fce67f6b50bed1f519966afe5287",
            "decimals": 18
        },
        "bilira-org_tryb": {
            "address": "0x2c537e5624e4af88a7ae4060c022609376c8d0eb",
            "decimals": 6
        },
        "falconfinance_usdf": {
            "address": "0xfa2b947eec368f42195f24f36d2af29f7c24cec2",
            "decimals": 18
        },
        "neutrl_nusd": {
            "address": "0xe556aba6fe6036275ec1f87eda296be72c811bce",
            "decimals": 18
        },
        "satoshi_satusd": {
            "address": "0x1958853a8be062dc4f401750eb233f5850f0d0d2",
            "decimals": 18
        },
        "anzen_usdz": {
            "address": "0xa469b7ee9ee773642b3e93e842e5d9b5baa10067",
            "decimals": 18
        },
        "openedenhq_usdo": {
            "address": "0x8238884ec9668ef77b90c6dff4d1a9f4f4823bfe",
            "decimals": 18
        },
        "usdkg_usdkg": {
            "address": "0xe820c06321e60d36257c666643fa5436643445e3",
            "decimals": 6
        },
        "aegis-im_yusd": {
            "address": "0x4274cd7277c7bb0806bd5fe84b9adae466a8da0a",
            "decimals": 18
        },
        "fidelity_fidd": {
            "address": "0x7c135549504245b5eae64fc0e99fa5ebabb8e35d",
            "decimals": 18
        },
        "transfero_brz": {
            "address": "0x01d33fd36ec67c6ada32cf36b31e88ee190b1839",
            "decimals": 18
        },
        "elixir_deusd": {
            "address": "0x15700b564ca08d9439c58ca5053166e8317aa138",
            "decimals": 18
        },
        "monerium_eure_old": {
            "address": "0x3231cb76718cdef2155fc47b5286d82e6eda273f",
            "decimals": 18
        },
        "noon_capital_usn": {
            "address": "0xda67b4284609d2d48e5d10cfac411572727dc1ed",
            "decimals": 18
        },
        "straitsx_xidr": {
            "address": "0xebf2096e01455108badcbaf86ce30b6e5a72aa52",
            "decimals": 6
        }
    },
    "zksync_era": {
        "circlefin_usdc": {
            "address": "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x3355df6d4c9c3035724fd0e3914de96a5a83aaf4",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x493257fD37EDB34451f62EDf8D2a0C418852bA4C",
            "decimals": 6
        },
        "curve_crvusde": {
            "address": "0x43cD37CC4B9EC54833c8aC362Dd55E58bFd62b86",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0x4b9eb6c0b6ea15176bbf62841c6b2a8a398cb656",
            "decimals": 18
        },
        "ethena_usde": {
            "address": "0x39fe7a0dacce31bd90418e3e659fb0b5f0b3db0d",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0xea77c590bb36c43ef7139ce649cfbcfd6163170d",
            "decimals": 18
        },
        "liquity_lusd": {
            "address": "0x503234f203fc7eb888eec8513210612a43cf6115",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x7715c206a14ac93cb1a6c0316a6e5f8ad7c9dc31",
            "decimals": 18
        },
        "noon_capital_usn": {
            "address": "0x0469d9d1de0ee58fa1153ef00836b9bbcb84c0b6",
            "decimals": 18
        }
    },
    "ink": {
        "circlefin_usdc": {
            "address": "0x2d270e6886d130d724215a266106e6832161eaed",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0xf1815bd50389c46847f0bda824ec8da914045d14",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0x0200c29006150606b650577bbe7b6248f58470c1",
            "decimals": 6
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "paxosglobal_usdg": {
            "address": "0xe343167631d89b6ffc58b88d6b7fb0228795491d",
            "decimals": 6
        },
        "aave_gho": {
            "address": "0xfc421ad3c883bf9e7c4f42de845c4e4405799e73",
            "decimals": 18
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "unichain": {
        "circlefin_usdc": {
            "address": "0x078d782b760474a361dda0af3839290b0ef57ad6",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0x9151434b16b9763660705744891fa906f660ecc5",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x588CE4F028D8e7B53B687865d6A67b3A54C75518",
            "decimals": 6
        },
        "makerdao_usds": {
            "address": "0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0x20CAb320A855b39F724131C69424240519573f81",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "plume": {
        "circlefin_usdc": {
            "address": "0x222365ef19f7947e5484218551b56bb3965aa7af",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x78add880a697070c1e765ac44d65323a0dcce913",
            "decimals": 6
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "plumenetwork_pusd": {
            "address": "0xdddd73f5df1f0dc31373357beac77545dc5a6f3f",
            "decimals": 6
        },
        "worldlibertyfinancial_usd1": {
            "address": "0x111111d2bf19e43c34263401e0cad979ed1cdb61",
            "decimals": 18
        },
        "mzero_wm": {
            "address": "0x437cc33344a0b27a429f795ff6b469c72698b291",
            "decimals": 6
        },
        "elixir_deusd": {
            "address": "0x1271656f45e251f588847721ba2c561dd1f0223f",
            "decimals": 18
        }
    },
    "base": {
        "circlefin_usdc": {
            "address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            "decimals": 6
        },
        "circlefin_eurc": {
            "address": "0x60a3e35cc302bfa44cb288bc5a4f316fdb1adb42",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
            "decimals": 6
        },
        "makerdao_usds": {
            "address": "0x820c137fa70c8691f0e44dc420a5e53c168921dc",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0x50c5725949a6f0c72e6c4a641f24049a917db0cb",
            "decimals": 18
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0xe5020a6d073a794b6e7f05678707de47986fb0b6",
            "decimals": 18
        },
        "liquity_lusd": {
            "address": "0x368181499736d0c0cc614dbb145e2ec1ac86b8c6",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "reserve_eusd": {
            "address": "0xcfa3ef56d303ae4faaba0592388f19d7c3399fb4",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0x417ac0e078398c154edfadd9ef675d30be60af93",
            "decimals": 18
        },
        "inverse_dola": {
            "address": "0x4621b7a9c75199271f773ebd9a499dbd165c3191",
            "decimals": 18
        },
        "usual_usd0": {
            "address": "0x758a3e0b1f842c9306b783f8a4078c6c8c03a270",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "usdxtoken_usdx": {
            "address": "0xf3527ef8de265eaa3716fb312c12847bfba66cef",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "resolv_usr": {
            "address": "0x35e5db674d8e93a03d814fa0ada70731efe8a4b9",
            "decimals": 18
        },
        "stablecoinxyz_sbc": {
            "address": "0xfdcc3dd6671eab0709a4c0f3f53de9a333d80798",
            "decimals": 18
        },
        "aave_gho": {
            "address": "0x6bb7a212910682dcfdbd5bcbb3e28fb4e8da10ee",
            "decimals": 18
        },
        "angle_eura": {
            "address": "0xa61beb4a3d02decb01039e378237032b351125b4",
            "decimals": 18
        },
        "allunity_eurau": {
            "address": "0x4933a85b5b5466fbaf179f72d3de273c287ec2c2",
            "decimals": 6
        },
        "frankencoin_zchf": {
            "address": "0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553",
            "decimals": 18
        },
        "straitsx_xsgd": {
            "address": "0x0a4c9cb2778ab3302996a34befcf9a8bc288c33b",
            "decimals": 6
        },
        "audd-digital_audd": {
            "address": "0x449b3317a6d1efb1bc3ba0700c9eaa4ffff4ae65",
            "decimals": 6
        },
        "tokenised-gbp_tgbp": {
            "address": "0x27f6c8289550fce67f6b50bed1f519966afe5287",
            "decimals": 18
        },
        "bilira-org_tryb": {
            "address": "0xfb8718a69aed7726afb3f04d2bd4bfde1bdcb294",
            "decimals": 6
        },
        "satoshi_satusd": {
            "address": "0x70654aad8b7734dc319d0c3608ec7b32e03fa162",
            "decimals": 18
        },
        "anzen_usdz": {
            "address": "0x04d5ddf5f3a8939889f11e97f8c4bb48317f1938",
            "decimals": 18
        },
        "arks_cgusd": {
            "address": "0xca72827a3d211cfd8f6b00ac98824872b72cab49",
            "decimals": 6
        },
        "openedenhq_usdo": {
            "address": "0xad55aebc9b8c03fc43cd9f62260391c13c23e7c0",
            "decimals": 18
        },
        "circlefin_usdce": {
            "address": "0xd9aaec86b65d86f6a7b5b1b0c42ffa531710b6ca",
            "decimals": 6
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "celo": {
        "circlefin_usdc": {
            "address": "0xceba9300f2b948710d2653dd7b07f33a8b32118c",
            "decimals": 6
        },
        "tetherto_usdt": {
            "address": "0x48065fbbe25f71c9282ddf5e1cd6d6a887483d5e",
            "decimals": 6
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "mento_usdm": {
            "address": "0x765de816845861e75a25fca122bb6898b8b1282a",
            "decimals": 18
        },
        "mento_eurm": {
            "address": "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73",
            "decimals": 18
        },
        "mento_brlm": {
            "address": "0xe8537a3d056da446677b9e9d6c5db704eaab4787",
            "decimals": 18
        },
        "mento_kesm": {
            "address": "0x456a3d042c0dbd3db53d5489e98dfb038553b0d0",
            "decimals": 18
        },
        "mento_phpm": {
            "address": "0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b",
            "decimals": 18
        },
        "mento_copm": {
            "address": "0x8a567e2ae79ca692bd748ab832081c45de4041ea",
            "decimals": 18
        },
        "mento_chfm": {
            "address": "0xb55a79f398e759e43c95b979163f30ec87ee131d",
            "decimals": 18
        },
        "mento_jpym": {
            "address": "0xc45ecf20f3cd864b32d9794d6f76814ae8892e20",
            "decimals": 18
        },
        "mento_cadm": {
            "address": "0xff4ab19391af240c311c54200a492233052b6325",
            "decimals": 18
        },
        "mento_ngcm": {
            "address": "0xe2702bd97ee33c88c8f6f92da3b733608aa76f71",
            "decimals": 18
        },
        "mento_audm": {
            "address": "0x7175504c455076f15c04a2f90a8e352281f492f9",
            "decimals": 18
        },
        "mento_zarm": {
            "address": "0x4c35853a3b4e647fd266f4de678dcc8fec410bf6",
            "decimals": 18
        },
        "mento_gbpm": {
            "address": "0xccf663b1ff11028f0b19058d0f7b674004a40746",
            "decimals": 18
        },
        "mento_xofm": {
            "address": "0x73f93dcc49cb8a239e2032663e9475dd5ef29a08",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "angle_eura": {
            "address": "0xc16b81af351ba9e64c1a069e3ab18c244a1e3049",
            "decimals": 18
        }
    },
    "worldchain": {
        "circlefin_usdc": {
            "address": "0x79a02482a880bce3f13e09da970dc34db4cd24d1",
            "decimals": 6
        },
        "circlefin_eurc": {
            "address": "0x1c60ba0a0ed1019e8eb035e6daf4155a5ce2380b",
            "decimals": 6
        },
        "bilira-org_tryb": {
            "address": "0x2c537e5624e4af88a7ae4060c022609376c8d0eb",
            "decimals": 6
        }
    },
    "polygon_pos": {
        "circlefin_usdc": {
            "address": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
            "decimals": 18
        },
        "gemini_gusd": {
            "address": "0xc8a94a3d3d2dabc3c1caffffdca6a7543c3e3e65",
            "decimals": 2
        },
        "binance_busde": {
            "address": "0x9c9e5fd8bbc25984b178fdce6117defa39d2db39",
            "decimals": 18
        },
        "trueusd_tusde": {
            "address": "0x2e1ad108ff1d8c782fcbbb89aad783ac49586756",
            "decimals": 18
        },
        "fraxfinance_frax": {
            "address": "0x45c32fa6df82ead1e2ef74d17b76547eddfaff89",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "liquity_lusd": {
            "address": "0x23001f892c0c82b79303edc9b9033cd190bb21c7",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "izumi_iusd": {
            "address": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0xc4ce1d6f5d98d65ee25cf85e9f2e9dcfee6cb5d6",
            "decimals": 18
        },
        "worldwide_wusd": {
            "address": "0x7cd017ca5ddb86861fa983a34b5f495c6f898c41",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "stablecoinxyz_sbc": {
            "address": "0xfdcc3dd6671eab0709a4c0f3f53de9a333d80798",
            "decimals": 18
        },
        "monerium_eure": {
            "address": "0xe0aea583266584dafbb3f9c3211d5588c73fea8d",
            "decimals": 18
        },
        "EURe_eureold": {
            "address": "0x18ec0a6e18e5bc3784fdd3a3634b31245ab704f6",
            "decimals": 18
        },
        "stasis_eurs": {
            "address": "0xe111178a87a3bff0c8d18decba5798827539ae99",
            "decimals": 2
        },
        "angle_eura": {
            "address": "0xe0b52e49357fd4daf2c15e02058dce6bc0057db4",
            "decimals": 18
        },
        "allunity_eurau": {
            "address": "0x4933a85b5b5466fbaf179f72d3de273c287ec2c2",
            "decimals": 6
        },
        "frankencoin_zchf": {
            "address": "0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553",
            "decimals": 18
        },
        "straitsx_xsgd": {
            "address": "0xdc3326e71d45186f113a2f448984ca0e8d201995",
            "decimals": 6
        },
        "apacx_pht": {
            "address": "0xe75220cb014dfb2d354bb59be26d7458bb8d0706",
            "decimals": 18
        },
        "tokenised-gbp_tgbp": {
            "address": "0x27f6c8289550fce67f6b50bed1f519966afe5287",
            "decimals": 18
        },
        "bilira-org_tryb": {
            "address": "0x4fb71290ac171e1d144f7221d882becac7196eb5",
            "decimals": 6
        },
        "transfero_brz": {
            "address": "0x4ed141110f6eeeaba9a1df36d8c26f684d2475dc",
            "decimals": 18
        },
        "monerium_eure_old": {
            "address": "0x18ec0a6e18e5bc3784fdd3a3634b31245ab704f6",
            "decimals": 18
        },
        "straitsx_xidr": {
            "address": "0x2c826035c1c36986117a0e949bd6ad4bab54afe2",
            "decimals": 6
        }
    },
    "arbitrum": {
        "circlefin_usdc": {
            "address": "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
            "decimals": 6
        },
        "makerdao_usds": {
            "address": "0x6491c05a82219b8d1479057361ff1654749b876b",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
            "decimals": 18
        },
        "stasis_eurs": {
            "address": "0xd22a58f79e9481d1a88e00c343885a588b34b68b",
            "decimals": 2
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "first_digital_labs_fdusd": {
            "address": "0x93c9932e4afa59201f0b5e63f7d816516f1669fe",
            "decimals": 18
        },
        "trueusd_tusde": {
            "address": "0x4d15a3a2286d883af0aa1b3f21367843fac63e07",
            "decimals": 18
        },
        "fraxfinance_frax": {
            "address": "0x17fc002b466eec40dae837fc4be5c67993ddbd6f",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "paypal_pyusd": {
            "address": "0x46850ad61c2b7d64d08c9c754f45254596696984",
            "decimals": 6
        },
        "liquity_lusd": {
            "address": "0x93b346b6bc2548da6a1e7d98e9a421b42541425b",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "izumi_iusd": {
            "address": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
            "decimals": 18
        },
        "reserve_eusd": {
            "address": "0x12275dcb9048680c4be40942ea4d92c74c63b844",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0x498bf2b1e120fed3ad3d42ea2165e9b73f99c1e5",
            "decimals": 18
        },
        "inverse_dola": {
            "address": "0x6a7661795c374c0bfc635934efaddff3a7ee23b6",
            "decimals": 18
        },
        "alchemix_alusd": {
            "address": "0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a",
            "decimals": 18
        },
        "usual_usd0": {
            "address": "0x35f1c5cb7fb977e669fd244c567da99d8a3a6850",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "usdxtoken_usdx": {
            "address": "0xf3527ef8de265eaa3716fb312c12847bfba66cef",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "resolv_usr": {
            "address": "0x2492d0006411af6c8bbb1c8afc1b0197350a79e9",
            "decimals": 18
        },
        "usdd_usdd": {
            "address": "0x680447595e8b7b3aa1b43beb9f6098c79ac2ab3f",
            "decimals": 18
        },
        "aave_gho": {
            "address": "0x7dff72693f6a4149b17e7c6314655f6a9f7c8b33",
            "decimals": 18
        },
        "mzero_wm": {
            "address": "0x437cc33344a0b27a429f795ff6b469c72698b291",
            "decimals": 6
        },
        "monerium_eure": {
            "address": "0x0c06ccf38114ddfc35e07427b9424adcca9f44f8",
            "decimals": 18
        },
        "angle_eura": {
            "address": "0xfa5ed56a203466cbbc2430a43c66b9d8723528e7",
            "decimals": 18
        },
        "allunity_eurau": {
            "address": "0x4933a85b5b5466fbaf179f72d3de273c287ec2c2",
            "decimals": 6
        },
        "frankencoin_zchf": {
            "address": "0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553",
            "decimals": 18
        },
        "straitsx_xsgd": {
            "address": "0xe333e7754a2dc1e020a162ecab019254b9dab653",
            "decimals": 6
        },
        "usdai_usdai": {
            "address": "0x0a1a1a107e45b7ced86833863f482bc5f4ed82ef",
            "decimals": 18
        },
        "satoshi_satusd": {
            "address": "0xb4818bb69478730ef4e33cc068dd94278e2766cb",
            "decimals": 18
        }
    },
    "starknet": {
        "circlefin_usdc": {
            "address": "0x33068f6539f8e6e6b131e6b2b814e6c34a5224bc66947c47dab9dfee93b35fb",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x53c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x068F5c6a61780768455de69077E07e89787839bf8166dEcfBf92B645209c0fB8",
            "decimals": 6
        },
        "liquity_lusde": {
            "address": "0x070a76fd48ca0Ef910631754d77DD822147Fe98A569b826ec85e3c33fde586aC",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x04887629C229B4eE8E82f3dB4cDdEB1B2c0F084d46f229672623Bd1282Df5931",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0x5574eb6b8789a91466f902c380d978e472db68170ff82a5b650b95a58ddf4ad",
            "decimals": 18
        },
        "uncap_usdu": {
            "address": "0x2F94539F80158f9a48a7acF3747718dfBec9B6f639E2742c1FB44aE7ab5AA04",
            "decimals": 18
        },
        "noon_capital_usn": {
            "address": "0x1e6545cab7ba4ac866768ba5e1bd540893762286ed3fea7f9c02bfa147e135b",
            "decimals": 18
        }
    },
    "optimism": {
        "circlefin_usdc": {
            "address": "0x0b2c639c533813f4aa9d7837caf62653d097ff85",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x7f5c764cbc14f9669b88837ca1490cca17c31607",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0x01bff41798a0bcf287b996046ca68b395dbc1071",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
            "decimals": 18
        },
        "makerdao_usds": {
            "address": "0x4f13a96ec5c4cf34e442b46bbd98a0791f20edc3",
            "decimals": 18
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "binance_busde": {
            "address": "0x9c9e5fd8bbc25984b178fdce6117defa39d2db39",
            "decimals": 18
        },
        "trueusd_tusde": {
            "address": "0xcb59a0a753fdb7491d5f3d794316f1ade197b21e",
            "decimals": 18
        },
        "fraxfinance_frax": {
            "address": "0x2e3d870790dc77a83dd1d18184acc7439a53f475",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "liquity_lusd": {
            "address": "0xc40f949f8a4e094d1b49a23ea9241d289b7b2819",
            "decimals": 18
        },
        "mountainprotocol_usdm": {
            "address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0xc52d7f23a2e460248db6ee192cb23dd12bddcbf6",
            "decimals": 18
        },
        "inverse_dola": {
            "address": "0x8ae125e8653821e851f12a49f7765db9a9ce7384",
            "decimals": 18
        },
        "alchemix_alusd": {
            "address": "0xcb8fa9a76b8e203d8c3797bf438d8fb81ea3326a",
            "decimals": 18
        },
        "glo_usdglo": {
            "address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "decimals": 18
        },
        "allunity_eurau": {
            "address": "0x4933a85b5b5466fbaf179f72d3de273c287ec2c2",
            "decimals": 6
        },
        "frankencoin_zchf": {
            "address": "0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553",
            "decimals": 18
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "taiko": {
        "circlefin_usdc": {
            "address": "0x07d83526730c7438048d55a4fc0b850e2aab6f0b",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0x19e26b0638bf63aa9fa4d14c6baf8d52ebe86c5c",
            "decimals": 6
        },
        "curve_crvusd": {
            "address": "0xc8f4518ed4bab9a972808a493107926ce8237068",
            "decimals": 18
        },
        "tetherto_usdte": {
            "address": "0x2def195713cf4a606b49d07e520e22c17899a736",
            "decimals": 6
        }
    },
    "gravity": {
        "circlefin_usdce": {
            "address": "0xfbda5f676cb37624f28265a144a48b0d6e87d3b6",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x816E810f9F787d669FB71932DeabF6c83781Cd48",
            "decimals": 6
        }
    },
    "soneium": {
        "circlefin_usdce": {
            "address": "0xba9986d2381edf1da03b0b9c1f8b00dc4aacc369",
            "decimals": 6
        },
        "resolv_usr": {
            "address": "0xb1b385542b6e80f77b94393ba8342c3af699f15c",
            "decimals": 18
        },
        "startale_usd": {
            "address": "0x3f99231dd03a9f0e7e3421c92b7b90fbe012985a",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x3A337a6adA9d885b6Ad95ec48F9b75f197b5AE35",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0x102d758f688a4C1C5a80b116bD945d4455460282",
            "decimals": 6
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "linea": {
        "circlefin_usdce": {
            "address": "0x176211869ca2b568f2a7d4ee941e073a821ee1ff",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0xa219439258ca9da29e9cc4ce5596924745e12b93",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0x4af15ec2a0bd43db75dd04e62faa3b8ef36b00d5",
            "decimals": 18
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0xc7346783f5e645aa998b106ef9e7f499528673d8",
            "decimals": 18
        },
        "metamask_musd": {
            "address": "0xaca92e438df0b2401ff60da7e4337b687a2435da",
            "decimals": 6
        },
        "monerium_eure": {
            "address": "0x3ff47c5bf409c86533fe1f4907524d304062428d",
            "decimals": 18
        },
        "paxosglobal_usdp": {
            "address": "0xd2bc272ea0154a93bf00191c8a1db23e67643ec5",
            "decimals": 18
        }
    },
    "scroll": {
        "circlefin_usdce": {
            "address": "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4",
            "decimals": 6
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x397f939c3b91a74c321ea7129396492ba9cdce82",
            "decimals": 18
        },
        "izumi_iusd": {
            "address": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
            "decimals": 18
        },
        "monerium_eure": {
            "address": "0xd7bb130a48595fcdf9480e36c1ae97ff2938ac21",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0xcA77eB3fEFe3725Dc33bccB54eDEFc3D9f764f97",
            "decimals": 18
        },
        "liquity_lusd": {
            "address": "0xeDEAbc3A1e7D21fE835FFA6f83a710c70BB1a051",
            "decimals": 18
        },
        "tetherto_usdte": {
            "address": "0xf55bec9cafdbe8730f096aa55dad6d22d44099df",
            "decimals": 6
        }
    },
    "mantle": {
        "circlefin_usdce": {
            "address": "0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9",
            "decimals": 6
        },
        "tetherto_usdt0": {
            "address": "0x779ded0c9e1022225f8e0630b35a9b54be713736",
            "decimals": 6
        },
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "avalon_usda": {
            "address": "0x075df695b8e7f4361fa7f8c1426c63f11b06e326",
            "decimals": 18
        },
        "worldlibertyfinancial_usd1": {
            "address": "0x111111d2bf19e43c34263401e0cad979ed1cdb61",
            "decimals": 18
        }
    },
    "megaeth": {
        "tetherto_usdt0": {
            "address": "0xb8ce59fc3717ada4c02eadf9682a9e934f625ebb",
            "decimals": 6
        },
        "megaeth_usdm": {
            "address": "0xFAfDdbb3FC7688494971a79cc65DCa3EF82079E7",
            "decimals": 18
        },
        "cap_cusd": {
            "address": "0xcCcc62962d17b8914c62D74FfB843d73B2a3cccC",
            "decimals": 18
        }
    },
    "zircuit": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "circlefin_usdc": {
            "address": "0x3b952c8C9C44e8Fe201e2b26F6B2200203214cfF",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x46dda6a5a559d861c06ec9a95fb395f5c3db0742",
            "decimals": 6
        }
    },
    "metis": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "alchemix_alusd": {
            "address": "0x303241e2b3b4aed0bb0f8623e7442368fed8faf3",
            "decimals": 18
        },
        "tetherto_usdte": {
            "address": "0xbB06DCA3AE6887fAbF931640f67cab3e3a16F4dC",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0x4651b38e7ec14bb3db731369bfe5b08f2466bd0a",
            "decimals": 18
        },
        "binance_busde": {
            "address": "0xb809cda0c2f79f43248C32b5DcB09d5cD26BbF10",
            "decimals": 18
        },
        "circlefin_usdce": {
            "address": "0xea32a96608495e54156ae48931a7c20f0dcc1a21",
            "decimals": 6
        }
    },
    "swell": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        }
    },
    "mode": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "tetherto_usdt0": {
            "address": "0x102d758f688a4C1C5a80b116bD945d4455460282",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0xd988097fb8612cc24eec14542bc03424c656005f",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0xf0f161fda2712db8b566946122a5af183995e2ed",
            "decimals": 6
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "fraxtal": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0xfc00000000000000000000000000000000000001",
            "decimals": 18
        },
        "curve_crvusd": {
            "address": "0xb102f7efa0d5de071a8d37b3548e1c7cb148caf3",
            "decimals": 18
        },
        "agora_ausd": {
            "address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x4d15ea9c2573addaed814e48c148b5262694646a",
            "decimals": 6
        },
        "circlefin_usdce": {
            "address": "0xdcc0f2d8f90fde85b10ac1c8ab57dc0ae946a543",
            "decimals": 6
        },
        "openusdt": {
            "address": "0x1217bfe6c773eec6cc4a38b5dc45b92292b6e189",
            "decimals": 6
        }
    },
    "manta": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "izumi_iusd": {
            "address": "0x078f712f038a95beea94f036cadb49188a90604b",
            "decimals": 18
        },
        "makerdao_daie": {
            "address": "0x1c466b9371f8aba0d7c458be10a62192fcb8aa71",
            "decimals": 18
        },
        "circlefin_usdce": {
            "address": "0xb73603c5d87fa094b7314c74ace2e64d165016fb",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0xf417f5a458ec102b90352f697d6e2ac3a3d2851f",
            "decimals": 6
        }
    },
    "blast": {
        "ethena_usde": {
            "address": "0x5d3a1ff2b6bab83b63cd9ad0787074081a52ef34",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "anzen_usdz": {
            "address": "0x52056ed29fe015f4ba2e3b079d10c0b87f46e8c6",
            "decimals": 18
        },
        "blast_io_usdb": {
            "address": "0x4300000000000000000000000000000000000003",
            "decimals": 18
        }
    },
    "polygon_zkevm": {
        "fraxfinance_frax": {
            "address": "0xff8544fed5379d9ffa8d47a74ce6b91e632ac44d",
            "decimals": 18
        },
        "fraxfinance_frxusd": {
            "address": "0x80eede496655fb9047dd39d9f418d5483ed600df",
            "decimals": 18
        },
        "circlefin_usdce": {
            "address": "0xa8ce8aee21bc2a48a5ef670afcc9274c7bbbc035",
            "decimals": 6
        },
        "tetherto_usdte": {
            "address": "0x1e4a5963abfd975d8c9021ce480b42188849d41d",
            "decimals": 6
        },
        "makerdao_daie": {
            "address": "0xc5015b9d9161dca7e18e32f6f25c4ad850731fd4",
            "decimals": 18
        }
    }
}