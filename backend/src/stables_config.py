# Token metadata
# a dictionary of tokens that can be considered stablecoins
# each token has a name, symbol, decimals, coingecko_id, fiat currency, logo and addresses
# the addresses are the contract addresses for the token on the ethereum network

# name: the name is the name of the token
# symbol: the symbol is the ticker symbol of the token
# decimals: the number of decimals the token has
# coingecko_id: the coingecko_id is the id of the token on coingecko
# fiat: the fiat currency is the currency that the token is pegged to
# logo: (optional) the logo is a link to the token's logo 
# addresses: the addresses are the contract addresses for the token on the Ethereum network
# exceptions: (optional) a dictionary of exceptions for the token on different chains, e.g. if the token is bridged and natively minted on a chain (example: USDT0 on Arbitrum)

#TODO: add logic for non-usd backed stables (should be enough to add the token to the total supply calculation)
# NOTE: Non-USD stables (EUR, BRL) added below - require currency conversion logic in adapter

stables_metadata = {
    "usdc": {
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "coingecko_id": "usd-coin",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6319/large/usdc.png?1696506694",
        "addresses": {
            "ethereum": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        }
    },
    "tether": {
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "coingecko_id": "tether",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/325/large/Tether.png?1696501661",
        "addresses": {
            "ethereum": "0xdac17f958d2ee523a2206206994597c13d831ec7",
            "celo": "0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e",
        },
        "exceptions": {
            "ethereum": {
                "arbitrum": {
                    "start_date": "2025-02-06",  # USDT0 native on Arbitrum, don't double count USDT on Ethereum bridge before Feb 6th, 2025
                }
            }
        }
    },
    "dai": {
        "name": "Dai",
        "symbol": "DAI",
        "decimals": 18,
        "coingecko_id": "dai",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/9956/large/Badge_Dai.png?1696509996",
        "addresses": {
            "ethereum": "0x6b175474e89094c44da98b954eedeac495271d0f",
        }
    },
    "usds": {
        "name": "USDS (former DAI)",
        "symbol": "USDS",
        "decimals": 18,
        "coingecko_id": "usds",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/39926/large/usds.webp?1726666683",
        "addresses": {
            "ethereum": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
        }
    },
    "ethena-usde": {
        "name": "Ethena USDe",
        "symbol": "USDe",
        "decimals": 18,
        "coingecko_id": "ethena-usde",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/33613/large/usde.png?1733810059",
        "addresses": {
            "ethereum": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
        }
    },
    "ethena-usdtb": {
        "name": "Ethena USDtb",
        "symbol": "USDtb",
        "decimals": 18,
        "coingecko_id": "usdtb",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0xC139190F447e929f090Edeb554D95AbB8b18aC1C",
        }
    },
    "binance_usd": {
        "name": "Binance USD",
        "symbol": "BUSD",
        "decimals": 18,
        "coingecko_id": "binance-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/9576/large/BUSDLOGO.jpg?1696509654",
        "addresses": {
            "ethereum": "0x4fabb145d64652a948d72533023f6e7a623c7c53",
        }
    },
    "true_usd": {
        "name": "TrueUSD",
        "symbol": "TUSD",
        "decimals": 18,
        "coingecko_id": "true-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/3449/large/tusd.png?1696504140",
        "addresses": {
            "ethereum": "0x0000000000085d4780b73119b644ae5ecd22b376",
        }
    },
    "frax": {
        "name": "Frax",
        "symbol": "FRAX",
        "decimals": 18,
        "coingecko_id": "frax",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/13422/large/FRAX_icon.png?1696513182",
        "addresses": {
            "ethereum": "0x853d955acef822db058eb8505911ed77f175b99e",
        }
    },
    "pax-dollar": {
        "name": "Pax Dollar",
        "symbol": "USDP",
        "decimals": 18,
        "coingecko_id": "paxos-standard",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/6013/large/Pax_Dollar.png?1696506427",
        "addresses": {
            "ethereum": "0x8e870d67f660d95d5be530380d0ec0bd388289e1",
        }
    },
    "gemini-usd": {
        "name": "Gemini Dollar",
        "symbol": "GUSD",
        "decimals": 2,
        "coingecko_id": "gemini-dollar",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/5992/large/gemini-dollar-gusd.png?1696506408",
        "addresses": {
            "ethereum": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",
        }
    },
    "paypal-usd": {
        "name": "PayPal USD",
        "symbol": "PYUSD",
        "decimals": 6,
        "coingecko_id": "paypal-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",
        }
    },
    "liquity-usd": {
        "name": "Liquity USD",
        "symbol": "LUSD",
        "decimals": 18,
        "coingecko_id": "liquity-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/14666/large/Group_3.png?1696514341",
        "addresses": {
            "ethereum": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",
        }
    },
    "mountain-protocol-usdm": {
        "name": "Mountain Protocol USD",
        "symbol": "USDM",
        "decimals": 18,
        "coingecko_id": "mountain-protocol-usdm",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/31719/large/usdm.png?1696530540",
        "addresses": {
            "ethereum": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",
        }
    },
    "izumi-bond-usd": {
        "name": "iZUMi Bond USD",
        "symbol": "IUSD",
        "decimals": 18,
        "coingecko_id": "izumi-bond-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/25388/large/iusd-logo-symbol-10k%E5%A4%A7%E5%B0%8F.png?1696524521",
        "addresses": {
            "ethereum": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",
        }
    },
    "electronic-usd": {
        "name": "Electronic USD",
        "symbol": "eUSD",
        "decimals": 18,
        "coingecko_id": "electronic-usd",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/28445/large/0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f.png?1696527441",
        "addresses": {
            "ethereum": "0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f",
        }
    },
    "curve-usd": {
        "name": "Curve USD",
        "symbol": "crvUSDC",
        "decimals": 18,
        "coingecko_id": "crvusd",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/30118/large/crvusd.jpeg?1696529040",
        "addresses": {
            "ethereum": "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e",
        }
    },
    "wusd": {
        "name": "Worldwide USD",
        "symbol": "WUSD",
        "decimals": 18,
        "coingecko_id": "worldwide-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/35358/standard/WUSD-logo.png?1755754866",
        "addresses": {
            "ethereum": "0x7cd017ca5ddb86861fa983a34b5f495c6f898c41",
        }
    },
    "dola": {
        "name": "Dola",
        "symbol": "DOLA",
        "decimals": 18,
        "coingecko_id": "dola-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x865377367054516e17014ccded1e7d814edc9ce4",
        }
    },
    "alchemix-usd": {
        "name": "Alchemix USD",
        "symbol": "ALUSD",
        "decimals": 18,
        "coingecko_id": "alchemix-usd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/14114/large/Alchemix_USD.png?1696513835",
        "addresses": {
            "ethereum": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",
        }
    },
    "first-digital-usd": {
        "name": "First Digital USD",
        "symbol": "FDUSD",
        "decimals": 18,
        "coingecko_id": "first-digital-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",
        }
    },
    "usual-usd": {
        "name": "Usual USD",
        "symbol": "USD0",
        "decimals": 18,
        "coingecko_id": "usual-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5",
        }
    },
    "celo-dollar": {
        "name": "Celo Dollar",
        "symbol": "cUSD",
        "decimals": 18,
        "coingecko_id": "celo-dollar",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "celo": "0x765DE816845861e75A25fCA122bb6898B8B1282a",
        }
    },
    "glo-dollar": {
        "name": "Glo Dollar",
        "symbol": "USDGLO",
        "decimals": 18,
        "coingecko_id": "glo-dollar",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",
            "celo": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3"
        }
    },
    "usdx": {
        "name": "Stables Labs USDX",
        "symbol": "USDX",
        "decimals": 18,
        "coingecko_id": "usdx-money-usdx",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "arbitrum": "0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef",
        }
    },
    "buidl": {
        "name": "BlackRock USD Institutional Digital Liquidity Fund",
        "symbol": "BUIDL",
        "decimals": 6,
        "coingecko_id": "blackrock-usd-institutional-digital-liquidity-fund",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x7712c34205737192402172409a8F7ccef8aA2AEc", 
        }
    },
    "ausd": {
        "name": "aUSD",
        "symbol": "AUSD",
        "decimals": 6,
        "coingecko_id": "ausd",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/39284/standard/AUSD_1024px.png?1764684132",
        "addresses": {
            "ethereum": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
        }
    },
    "usdb": {
        "name": "USDB",
        "symbol": "USDB",
        "decimals": 18,
        "coingecko_id": "usdb",
        "fiat": "usd",
        "logo": "https://assets.coingecko.com/coins/images/35595/large/65c67f0ebf2f6a1bd0feb13c_usdb-icon-yellow.png?1709255427",
        "addresses": {
            "blast": "0x4300000000000000000000000000000000000003",
        }
    },
    "openusdt": {
        "name": "OpenUSDT",
        "symbol": "oUSDT",
        "decimals": 6,
        "coingecko_id": "openusdt",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/54815/large/ousdt.jpg?1741848258",
        "addresses": {
            "base": "0x1217BfE6c773EEC6cc4A38b5Dc45B92292B6E189"
        }
    },
    "resolv-usr": {
        "name": "Resolv USD",
        "symbol": "USR",
        "decimals": 18,
        "coingecko_id": "resolv-usr",
        "fiat": "usd",
        "logo": "https://coin-images.coingecko.com/coins/images/40008/large/USR_LOGO.png?1725222638",
        "addresses": {
            "base": "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9"
        }
    },
    "pusd": {
        "name": "Plume USD",
        "symbol": "pUSD",
        "decimals": 6,
        "coingecko_id": "plume-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "plume": "0xdddD73F5Df1F0DC31373357beAC77545dC5A6f3F"
        }
    },
    "metamask-usd": {
        "name": "MetaMask USD",
        "symbol": "mUSD",
        "decimals": 6,
        "coingecko_id": "metamask-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0xaca92e438df0b2401ff60da7e4337b687a2435da"
        }
    },
    "ripple-usd": {
        "name": "Ripple USD",
        "symbol": "RLUSD",
        "decimals": 18,
        "coingecko_id": "ripple-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD"
        }
    },
    "usda": {
        "name": "Avalon Labs USDa",
        "symbol": "USDa",
        "decimals": 18,
        "coingecko_id": "usda-2",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x8A60E489004Ca22d775C5F2c657598278d17D9c2"
        }
    },
    "usd1": {
        "name": "World Liberty Financial USD",
        "symbol": "USD1",
        "decimals": 18,
        "coingecko_id": "usd1-wlfi",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d"
        }
    },
    "global_dollar": {
        "name": "Global Dollar",
        "symbol": "USDG",
        "decimals": 6,
        "coingecko_id": "global-dollar",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0xe343167631d89B6Ffc58B88d6b7fB0228795491D"
        }
    },
    "usdd": {
        "name": "Decentralized USD",
        "symbol": "USDD",
        "decimals": 18,
        "coingecko_id": "usdd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x4f8e5DE400DE08B164E7421B3EE387f461beCD1A"
        }
    },
    "gho_aave": {
        "name": "GHO Aave",
        "symbol": "GHO",
        "decimals": 18,
        "coingecko_id": "gho",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f"
        }
    },
    "m_zero": {
        "name": "M by M0",
        "symbol": "M",
        "decimals": 6,
        "coingecko_id": "m-2",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x866A2BF4E572CbcF37D5071A7a58503Bfb36be1b"
        }
    },
    "mnee": {
        "name": "MNEE USD",
        "symbol": "MNEE",
        "decimals": 18,
        "coingecko_id": "mnee-usd-stablecoin",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x8ccedbae4916b79da7f3f612efb2eb93a2bfd6cf"
        }
    },
    "gooddollar": { # impossible to track correct "totalSupply", ignored for now! 
        "name": "GoodDollar",
        "symbol": "G$",
        "decimals": 2, # on celo 18...! Big mess.
        "coingecko_id": "gooddollar",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "ethereum": "0x67c5870b4a41d4ebef24d2456547a03f1f3e094b",
            "celo": "0x62b8b11039fcfe5ab0c56e502b1c372a3d2a9c7a"
        }
    },
    "startale_usd": { 
        "name": "Startale USD",
        "symbol": "USDSC",
        "decimals": 6,
        "coingecko_id": "startale-usd",
        "fiat": "usd",
        "logo": None,
        "addresses": {
            "soneium": "0x3f99231dD03a9F0E7e3421c92B7b90fbe012985a"
        }
    },

    ## EUR

    "euro-coin": {
        "name": "Euro Coin",
        "symbol": "EURC",
        "decimals": 6,
        "coingecko_id": "euro-coin",
        "fiat": "eur",
        "logo": "https://assets.coingecko.com/coins/images/26045/large/euro-coin.png?1696525125",
        "addresses": {
            "ethereum": "0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c"
        }
    },
    "monerium-eur-money": {
        "name": "Monerium EUR emoney",
        "symbol": "EURe",
        "decimals": 18,
        "coingecko_id": "monerium-eur-money",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x3231Cb76718CDeF2155FC47b5286d82e6eDA273f"
        }
    },
    "stasis-eurs": {
        "name": "STASIS EURS",
        "symbol": "EURS",
        "decimals": 2,
        "coingecko_id": "stasis-eurs",
        "fiat": "eur",
        "logo": "https://assets.coingecko.com/coins/images/5164/large/EURS_300x300.png?1696505756",
        "addresses": {
            "ethereum": "0xdB25f211AB05b1c97D595516F45794528a807ad8",
            "polygon": "0xE111178A87A3BfF0c8d18dECBa5798827539Ae99"
        }
    },
    "angle_eura": {
        "name": "Angle EURA",
        "symbol": "EURA",
        "decimals": 18,
        "coingecko_id": "ageur",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8"
        }
    },
    "all_unity": {
        "name": "AllUnity EUR",
        "symbol": "EURAU",
        "decimals": 6,
        "coingecko_id": "allunity-eur",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x4933A85b5b5466Fbaf179F72D3DE273c287EC2c2"
        }
    },
    "coinvertible": {
        "name": "CoinVertible EUR",
        "symbol": "EURCV",
        "decimals": 18,
        "coingecko_id": "societe-generale-forge-eurcv",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x5F7827FDeb7c20b443265Fc2F40845B715385Ff2"
        }
    },
    "eurite": {
        "name": "Eurite",
        "symbol": "EURI",
        "decimals": 18,
        "coingecko_id": "eurite",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x9d1A7A3191102e9F900Faa10540837ba84dCBAE7"
        }
    },
    "anchored_coins_eur": {
        "name": "Anchored Coins EUR",
        "symbol": "AEUR",
        "decimals": 18,
        "coingecko_id": "anchored-coins-eur",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0xA40640458FBc27b6EefEdeA1E9C9E17d4ceE7a21"
        }
    },
    "stablr_eur": {
        "name": "StablR Euro",
        "symbol": "EURR",
        "decimals": 6,
        "coingecko_id": "stablr-euro",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0x50753CfAf86c094925Bf976f218D043f8791e408"
        }
    },
    "tether_eur": {
        "name": "Tether Euro",
        "symbol": "EURT",
        "decimals": 6,
        "coingecko_id": "tether-eurt",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "ethereum": "0xc581b735a1688071a1746c968e0798d642ede491"
        }
    },
    "celo_eur": {
        "name": "Celo EUR",
        "symbol": "cEUR",
        "decimals": 18,
        "coingecko_id": "celo-euro",
        "fiat": "eur",
        "logo": None,
        "addresses": {
            "celo": "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73"
        }
    },

    ## CHF
    "frankencoin": {
        "name": "Frankencoin",
        "symbol": "ZCHF",
        "decimals": 18,
        "coingecko_id": "frankencoin",
        "fiat": "chf",
        "logo": None,
        "addresses": {
            "ethereum": "0xB58E61C3098d85632Df34EecfB899A1Ed80921cB"
        }
    },

    ## SGD
    "xsgd": {
        "name": "Straitsx SGD",
        "symbol": "XSGD",
        "decimals": 6,
        "coingecko_id": "xsgd",
        "fiat": "sgd",
        "logo": None,
        "addresses": {
            "ethereum": "0x70e8de73ce538da2beed35d14187f6959a8eca96"
        }
    },

    ## BRL
    "brz": {
        "name": "Brazilian Digital Token",
        "symbol": "BRZ",
        "decimals": 18,
        "coingecko_id": "brz",
        "fiat": "brl",
        "logo": None,
        "addresses": {
            "ethereum": "0x01d33FD36ec67c6Ada32cf36b31e88EE190B1839"
        }
    },
    "celo_brl": {
        "name": "Celo Real",
        "symbol": "cREAL",
        "decimals": 18,
        "coingecko_id": "celo-real-creal",
        "fiat": "brl",
        "logo": None,
        "addresses": {
            "celo": "0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787"
        }
    },

    ## RUB
    "a7a5": {
        "name": "A7A5 Ruble",
        "symbol": "A7A5",
        "decimals": 6,
        "coingecko_id": "a7a5",
        "fiat": "rub",
        "logo": None,
        "addresses": {
            "ethereum": "0x6fa0be17e4bea2fcfa22ef89bf8ac9aab0ab0fc9"
        }
    },

    ## KES
    "cKES": {
        "name": "Celo Kenyan Shilling",
        "symbol": "cKES",
        "decimals": 18,
        "coingecko_id": "celo-kenyan-shilling",
        "fiat": "kes",
        "logo": None,
        "addresses": {
            "celo": "0x456a3D042C0DbD3db53D5489e98dFb038553B0d0"
        }
    },

    ## XOF
    "eXOF": {
        "name": "Celo West African CFA Franc",
        "symbol": "eXOF",
        "decimals": 18,
        "coingecko_id": None,
        "fiat": "xof",
        "logo": None,
        "addresses": {
            "celo": "0x73f93dcc49cb8a239e2032663e9475dd5ef29a08"
        }
    },

    ## PHP
    "apacx_pht": {
        "name": "Apex PHP",
        "symbol": "PHT",
        "decimals": 18,
        "coingecko_id": "pht-stablecoin",
        "fiat": "php",
        "logo": None,
        "addresses": {
            "ethereum": "0xbe370ad45d44eb45174c4ec60b88839fef32c077"
        }
    },
    "puso": {
        "name": "PUSO Stablecoin",
        "symbol": "PUSO",
        "decimals": 18,
        "coingecko_id": "puso",
        "fiat": "php",
        "logo": None,
        "addresses": {
            "celo": "0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b"
        }
    },

    ## AUD
    "audd": {
        "name": "Australian Digital Dollar",
        "symbol": "AUDD",
        "decimals": 6,
        "coingecko_id": "novatti-australian-digital-dollar",
        "fiat": "aud",
        "logo": None,
        "addresses": {
            "ethereum": "0x4cce605ed955295432958d8951d0b176c10720d5"
        }
    },

    ## COP
    "cCOP": {
        "name": "Celo Colombian Peso",
        "symbol": "cCOP",
        "decimals": 18,
        "coingecko_id": "ccop",
        "fiat": "cop",
        "logo": None,
        "addresses": {
            "celo": "0x8A567e2aE79CA692Bd748aB832081C45de4041eA"
        }
    },

    ## GBP
    "true_gbp": {
        "name": "Tokenised GBP",
        "symbol": "TGBP",
        "decimals": 18,
        "coingecko_id": "tokenised-gbp",
        "fiat": "gbp",
        "logo": None,
        "addresses": {
            "ethereum": "0x27f6c8289550fce67f6b50bed1f519966afe5287"  # USDP contract also used for TGBP
        }
    },
    "cGBP": {
        "name": "Celo British Pound",
        "symbol": "cGBP",
        "decimals": 18,
        "coingecko_id": "celo-british-pound",
        "fiat": "gbp",
        "logo": None,
        "addresses": {
            "celo": "0xCCF663b1fF11028f0b19058d0f7B674004a40746"
        }
    },
}

# Bridge or direct token mapping
## bridged: locked value is calculated based on bridge contracts on source chains
### Check for any stable that we track if it is locked in here

## direct: token is directly minted on a chain, so we can get the total supply directly from the token contract
### Call method to get total supply of the token on the chain

## locked_supply: list of tokens that should be subtracted from the total supply
### This is useful for tokens that are locked in contracts of the issuer

stables_mapping = {
    ## Ethereum Mainnet is a slightly special case, as it is the source chain for most bridged stablecoins
    "ethereum": {
        "direct": {

            ## USD 
            "usdc": {
                "token_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC native on Ethereum
                "method_name": "totalSupply",
            },
            "tether": { 
                "token_address": "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT native on Ethereum
                "method_name": "totalSupply",
            },
            "buidl": {
                "token_address": "0x7712c34205737192402172409a8F7ccef8aA2AEc",  # BUIDL native on Ethereum
                "method_name": "totalSupply",
            },
            "dai": {
                "token_address": "0x6b175474e89094c44da98b954eedeac495271d0f",  # DAI native on Ethereum
                "method_name": "totalSupply",
            },
            "usds": {
                "token_address": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",  # USDS native on Ethereum
                "method_name": "totalSupply",
            },
            "ethena-usde": {
                "token_address": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",  # USDe native on Ethereum
                "method_name": "totalSupply",
            },
            "ethena-usdtb": {
                "token_address": "0xC139190F447e929f090Edeb554D95AbB8b18aC1C",  # USDtb native on Ethereum
                "method_name": "totalSupply",
            },
            "binance_usd": {
                "token_address": "0x4fabb145d64652a948d72533023f6e7a623c7c53",  # BUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "true_usd": {
                "token_address": "0x0000000000085d4780b73119b644ae5ecd22b376",  # TUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "frax": {
                "token_address": "0x853d955acef822db058eb8505911ed77f175b99e",  # FRAX native on Ethereum
                "method_name": "totalSupply",
            },
            "pax-dollar": {
                "token_address": "0x8e870d67f660d95d5be530380d0ec0bd388289e1",  # USDP native on Ethereum
                "method_name": "totalSupply",
            },
            "gemini-usd": {
                "token_address": "0x056fd409e1d7a124bd7017459dfea2f387b6d5cd",  # GUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "wusd": {
                "token_address": "0x7cd017ca5ddb86861fa983a34b5f495c6f898c41",  # WUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "paypal-usd": {
                "token_address": "0x6c3ea9036406852006290770bedfcaba0e23a0e8",  # PYUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "liquity-usd": {
                "token_address": "0x5f98805a4e8be255a32880fdec7f6728c6568ba0",  # LUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "mountain-protocol-usdm": {
                "token_address": "0x59d9356e565ab3a36dd77763fc0d87feaf85508c",  # USDM native on Ethereum
                "method_name": "totalSupply",
            },
            "izumi-bond-usd": {
                "token_address": "0x0a3bb08b3a15a19b4de82f8acfc862606fb69a2d",  # IUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "electronic-usd": {
                "token_address": "0xa0d69e286b938e21cbf7e51d71f6a4c8918f482f",  # eUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "curve-usd": {
                "token_address": "0xf939e0a03fb07f59a73314e73794be0e57ac1b4e",  # crvUSDC native on Ethereum
                "method_name": "totalSupply",
            },
            "dola": {
                "token_address": "0x865377367054516e17014ccded1e7d814edc9ce4",  # DOLA native on Ethereum
                "method_name": "totalSupply",
            },
            "alchemix-usd": {
                "token_address": "0xbc6da0fe9ad5f3b0d58160288917aa56653660e9",  # ALUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "first-digital-usd": {
                "token_address": "0xc5f0f7b66764f6ec8c8dff7ba683102295e16409",  # FDUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "usual-usd": {
                "token_address": "0x73a15fed60bf67631dc6cd7bc5b6e8da8190acf5",  # USD0 native on Ethereum
                "method_name": "totalSupply",
            },
            "glo-dollar": {
                "token_address": "0x4f604735c1cf31399c6e711d5962b2b3e0225ad3",  # USDGLO native on Ethereum
                "method_name": "totalSupply",
            },
            "metamask-usd": {
                "token_address": "0xaca92e438df0b2401ff60da7e4337b687a2435da",  # mUSD native on Ethereum
                "method_name": "totalSupply",
            },
            "ripple-usd": {
                "token_address": "0x8292Bb45bf1Ee4d140127049757C2E0fF06317eD",  # RLUSD on Ethereum
                "method_name": "totalSupply",
            },
            "usd1": {
                "token_address": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",  # USD1 on Ethereum
                "method_name": "totalSupply",
            },
            "global_dollar": {
                "token_address": "0xe343167631d89B6Ffc58B88d6b7fB0228795491D",  # USDG on Ethereum
                "method_name": "totalSupply",
            },
            "usdd": {
                "token_address": "0x4f8e5DE400DE08B164E7421B3EE387f461beCD1A",  # USDD on Ethereum
                "method_name": "totalSupply",
            },
            "gho_aave": {
                "token_address": "0x40D16FC0246aD3160Ccc09B8D0D3A2cD28aE6C2f",  # GHO on Ethereum
                "method_name": "totalSupply",
            },
            "m_zero": {
                "token_address": "0x866A2BF4E572CbcF37D5071A7a58503Bfb36be1b",  # M on Ethereum
                "method_name": "totalSupply",
            },
            "mnee": {
                "token_address": "0x8ccedbae4916b79da7f3f612efb2eb93a2bfd6cf",  # MNEE on Ethereum
                "method_name": "totalSupply",
            },
            "ausd": {
                "token_address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",  # aUSD on Ethereum
                "method_name": "totalSupply",
            },
            
            ## EUR
            "euro-coin": {
                "token_address": "0x1aBaEA1f7C830bD89Acc67eC4af516284b1bC33c",  # EURC native on Ethereum
                "method_name": "totalSupply",
            },
            "stasis-eurs": {
                "token_address": "0xdB25f211AB05b1c97D595516F45794528a807ad8",  # EURS native on Ethereum
                "method_name": "totalSupply",
            },
            "monerium-eur-money": {
                "token_address": "0x3231Cb76718CDeF2155FC47b5286d82e6eDA273f",  # EURe native on Ethereum
                "method_name": "totalSupply",
            },
            "angle_eura": {
                "token_address": "0x1a7e4e63778B4f12a199C062f3eFdD288afCBce8",  # EURA native on Ethereum
                "method_name": "totalSupply",
            },
            "all_unity": {
                "token_address": "0x4933A85b5b5466Fbaf179F72D3DE273c287EC2c2",  # EURAU native on Ethereum
                "method_name": "totalSupply",
            },
            "coinvertible": {
                "token_address": "0x5F7827FDeb7c20b443265Fc2F40845B715385Ff2",  # EURCV native on Ethereum
                "method_name": "totalSupply",
            },
            "eurite": {
                "token_address": "0x9d1A7A3191102e9F900Faa10540837ba84dCBAE7",  # EURI native on Ethereum
                "method_name": "totalSupply",
            },
            "anchored_coins_eur": {
                "token_address": "0xA40640458FBc27b6EefEdeA1E9C9E17d4ceE7a21",  # AEUR native on Ethereum
                "method_name": "totalSupply",
            },
            "stablr_eur": {
                "token_address": "0x50753CfAf86c094925Bf976f218D043f8791e408",  # EURR native on Ethereum
                "method_name": "totalSupply",
            },
            "tether_eur": {
                "token_address": "0xc581b735a1688071a1746c968e0798d642ede491",  # EURT native on Ethereum
                "method_name": "totalSupply",
            },

            ## CHF
            "frankencoin": {
                "token_address": "0xB58E61C3098d85632Df34EecfB899A1Ed80921cB",  # ZCHF native on Ethereum
                "method_name": "totalSupply",
            },

            ## SGD 
            "xsgd": {
                "token_address": "0x70e8de73ce538da2beed35d14187f6959a8eca96",  # XSGD native on Ethereum
                "method_name": "totalSupply",
            },

            ## RUB
            "a7a5": {
                "token_address": "0x6fa0be17e4bea2fcfa22ef89bf8ac9aab0ab0fc9",  # A7A5 native on Ethereum
                "method_name": "totalSupply",
            },

            ## PHP
            "apacx_pht": {
                "token_address": "0xbe370ad45d44eb45174c4ec60b88839fef32c077",  # PHT native on Ethereum
                "method_name": "totalSupply",
            },

            ## BRL
            "brz": {
                "token_address": "0x01d33FD36ec67c6Ada32cf36b31e88EE190B1839",  # BRZ native on Ethereum
                "method_name": "totalSupply",
            },

            ## AUD
            "audd": {
                "token_address": "0x4cce605ed955295432958d8951d0b176c10720d5",  # AUDD native on Ethereum
                "method_name": "totalSupply",
            },

            ## GBP
            "true_gbp": {
                "token_address": "0x27f6c8289550fce67f6b50bed1f519966afe5287",  # TGBP native on Ethereum
                "method_name": "totalSupply",
            },

        },

        "locked_supply": {
            "tether": {
                "ethereum": [
                    "0x5754284f345afc66a98fbB0a0Afe71e0F007B949",  # Tether Treasury
                    "0x6C96dE32CEa08842dcc4058c14d3aaAD7Fa41dee", # USDT Layer Zero Bridge https://layerzeroscan.com/oft/USDT0/USDT0
                    "0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf"  # USDT Polygon Bridge (until Aug. 2025)
                ]
            },
            "usdc": {
                "ethereum": [
                    "0x55fe002aeff02f77364de339a1292923a15844b8",  # Circle Treasury
                ]
            },
            "euro-coin": {
                "ethereum": [
                    "0x55FE002aefF02F77364de339a1292923A15844B8"  # EURC Treasury
                ]
            },
            "angle_eura": {
                "ethereum": [
                    "0x41D5D79431A913C4aE7d693E99eDB3D6fA258C5"  # EURA Layer Zero Bridge
                ]
            }
        }  
    },

    ## Layer 2s
    "swell": {
        "bridged": {
            "ethereum": [
                "0x7aA4960908B13D104bf056B23E2C76B43c5AACc8" ##proxy, 0 tokens in it on March 3rd, 2025
            ], 
        },
        "direct": {
            "ethena-usde": {
                "token_address" : "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            },
            "tether": {
                "token_address" : "0xb89c6ED617f5F46175E41551350725A09110bbCE",
                "method_name": "totalSupply",
            }
            ## staked ethena-staked-usde??
        }
    },
    "soneium": {
        "bridged": {
            "ethereum": [
                "0xeb9bf100225c214Efc3E7C651ebbaDcF85177607", ## Generic escrow contract
                "0xC67A8c5f22b40274Ca7C4A56Db89569Ee2AD3FAb" ## Escrow for USDC
            ],
        },
        "direct": {
            "startale_usd": {
                "token_address": "0x3f99231dD03a9F0E7e3421c92B7b90fbe012985a",
                "method_name": "totalSupply",
            },
        }
    },
    "arbitrum": {
        "bridged": {
            "ethereum": [
                "0xa3A7B6F88361F48403514059F1F16C8E78d60EeC", # Arbitrum L1 ERC20 Gateway
                "0xcEe284F754E854890e311e3280b767F80797180d", # Arbitrum: L1 Arb-Custom Gateway
                "0xA10c7CE4b876998858b1a9E12b10092229539400", # DAI Escrow contract
                "0xD925C84b55E4e44a53749fF5F2a5A13F63D128fd", # M by M0 Escrow contract
            ],
        },
        "direct": {
            "usdc": {
                "token_address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC native on Arbitrum
                "method_name": "totalSupply",
            },
            "usdx": {
                "token_address": "0xf3527ef8dE265eAa3716FB312c12847bFBA66Cef",  # USDX native on Arbitrum
                "method_name": "totalSupply",
            },
            "frax": {
                "token_address": "0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F",  # FRAX native on Arbitrum
                "method_name": "totalSupply",
            },
            "mountain-protocol-usdm": {
                "token_address": "0x59D9356E565Ab3A36dD77763Fc0d87fEaf85508C",  # USDM native on Arbitrum
                "method_name": "totalSupply",
            },
            "tether": {
                "token_address": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT0 native on Arbitrum, don't double count USDT on Ethereum bridge before Feb 6th, 2025
                "method_name": "totalSupply",
            },
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",  # USDe native on Arbitrum
                "method_name": "totalSupply",
            },
            "usds": {
                "token_address": "0x6491c05A82219b8D1479057361ff1654749b876b",  # USDS native on Arbitrum
                "method_name": "totalSupply",
            },
            ## EUR
            "monerium-eur-money": {
                "token_address": "0x0c06cCF38114ddfc35e07427B9424adcca9F44F8",  # EURe native on Arbitrum
                "method_name": "totalSupply",
            },
            "angle_eura": {
                "token_address": "0xFA5Ed56A203466CbBC2430a43c66b9D8723528E7",  # EURA native on Arbitrum
                "method_name": "totalSupply",
            },
            ## CHF
            "frankencoin": {
                "token_address": "0xd4dd9e2f021bb459d5a5f6c24c12fe09c5d45553",  # ZCHF native on Arbitrum
                "method_name": "totalSupply",
            },
            ## SGD
            "xsgd": {
                "token_address": "0xe333e7754a2dc1e020a162ecab019254b9dab653",  # XSGD native on Arbitrum
                "method_name": "totalSupply",
            },
        }

    },
    "arbitrum_nova": {
        "bridged": {
            "ethereum": [
                "0xB2535b988dcE19f9D71dfB22dB6da744aCac21bf", # Arbitrum Nova L1 ERC20 Gateway
                "0x23122da8C581AA7E0d07A36Ff1f16F799650232f", # Arbitrum Nova: L1 Arb-Custom Gateway
                "0xA2e996f0cb33575FA0E36e8f62fCd4a9b897aAd3" # DAI Escrow contract
            ], 
        },
    },
    "zksync_era": {
        "bridged": {
            "ethereum": [
                "0xbeD1EB542f9a5aA6419Ff3deb921A372681111f6",  # Shared brigde with other zkStack chains
                "0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063",  # Legacy escrow bridge
                "0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB"
            ],
        },
        "direct": {
            "usdc": {
                "token_address": "0x1d17CBcF0D6D143135aE902365D2E5e2A16538D4",  # USDC native on zkSync Era
                "method_name": "totalSupply",
            },
            "mountain-protocol-usdm": {
                "token_address": "0x7715c206A14Ac93Cb1A6c0316A6E5f8aD7c9Dc31",  # USDM
                "method_name": "totalSupply",
            },
        }

    },
    "unichain": {
        "bridged": {
            "ethereum": [
                "0x81014F44b0a345033bB2b3B21C7a1A308B35fEeA"  # Bridge contract locking USDT and DAI for Unichain
            ]
        },
        "direct": {
            "usdc": {
                "token_address": "0x078D782b760474a361dDA0AF3839290b0EF57AD6",  # USDC native on Unichain
                "method_name": "totalSupply",
            },
            "usds": {
                "token_address": "0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C",  # USDS native on Unichain
                "method_name": "totalSupply",
            },
            "tether": {
                "token_address": "0x9151434b16b9763660705744891fA906F660EcC5",  # USDT0
                "method_name": "totalSupply",
            }
        }
    },
    "lisk": {
        "bridged": {
            "ethereum": [
                "0x2658723Bf70c7667De6B25F99fcce13A16D25d08", # Canonical: Generic escrow (L1StandardBridge) - holds LSK, USDT, WBTC, TRB
                "0xE3622468Ea7dD804702B56ca2a4f88C0936995e6"  # External: Escrow for USDC (L1OpUSDCBridgeAdapter) - holds USDC
            ],
        },
        # LSK is primarily accounted for via its balance in the L1StandardBridge.
        "direct": {},
    },
    "celo": {
        "direct": {
            "tether": {
                "token_address": "0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e", 
                "method_name": "totalSupply",
            },
            "usdc": {
                "token_address": "0xcebA9300f2b948710d2653dD7B07f33A8B32118C",  
                "method_name": "totalSupply",
            },
            "celo-dollar": {
                "token_address": "0x765DE816845861e75A25fCA122bb6898B8B1282a",  
                "method_name": "totalSupply",
            },
            "glo-dollar": {
                "token_address": "0x4F604735c1cF31399C6E711D5962b2B3E0225AD3",  
                "method_name": "totalSupply",
            },
            "angle_eura": { # Eura on Celo bridged via Layer Zero
                "token_address": "0xf1dDcACA7D17f8030Ab2eb54f2D9811365EFe123",
                "method_name": "totalSupply",
            },
            "angle_eura": { # Eura on Celo native
                "token_address": "0xC16B81Af351BA9e64C1a069E3Ab18c244A1E3049",
                "method_name": "totalSupply",
            },
            "celo_eur": {
                "token_address": "0xd8763cba276a3738e6de85b4b3bf5fded6d6ca73",  
                "method_name": "totalSupply",
            },
            "celo_brl": {
                "token_address": "0xe8537a3d056DA446677B9E9d6c5dB704EaAb4787",  
                "method_name": "totalSupply",
            },
            "cKES": {
                "token_address": "0x456a3D042C0DbD3db53D5489e98dFb038553B0d0",  
                "method_name": "totalSupply",
            },
            "puso": {
                "token_address": "0x105d4a9306d2e55a71d2eb95b81553ae1dc20d7b",  
                "method_name": "totalSupply",
            },
            "cGBP": {
                "token_address": "0xCCF663b1fF11028f0b19058d0f7B674004a40746",  
                "method_name": "totalSupply",
            },
            "cCOP": {
                "token_address": "0x8A567e2aE79CA692Bd748aB832081C45de4041eA",  
                "method_name": "totalSupply",
            },
            "eXOF": {
                "token_address": "0x73f93dcc49cb8a239e2032663e9475dd5ef29a08",  
                "method_name": "totalSupply",
            },
        },
        "locked_supply": {
            "tether": {
                "celo": [
                    "0x5754284f345afc66a98fbB0a0Afe71e0F007B949"  # Tether Treasury
                ]
            }
        }
    },
    "mantle": {
        "bridged": {
            "ethereum": [
                "0x95fC37A27a2f68e3A647CDc081F0A89bb47c3012"  # Canonical L1StandardBridge escrow
            ]
        },

        "direct": {
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply"
            }
        }
    },
    "metis": {
        "bridged": {
            "ethereum": [
                "0x3980c9ed79d2c191A89E02Fa3529C60eD6e9c04b"   # Metis Canonical L1StandardBridge escrow
            ]
        }
    },
    "mode": {
        "bridged": {
            "ethereum": [
                "0x735aDBbE72226BD52e818E7181953f42E3b0FF21",  # Mode Canonical L1StandardBridge
                "0x8B34b14c7c7123459Cf3076b8Cb929BE097d0C07"   # (also holds ETH & may custody stables)
            ]
        },

        "direct": {
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply"
            },
            "savings-dai": {
                "token_address": "0x3f51c6c5927B88CDEc4b61e2787F9BD0f5249138",
                "method_name": "totalSupply"
            }
        }
    },
    "taiko": {
        "bridged": {
            "ethereum": [
                
                "0x996282cA11E5DEb6B5D122CC3B9A1FcAAD4415Ab", # Main canonical bridge (ERC-20 & ETH)
                "0xd60247c6848B7Ca29eDdF63AA924E53dB6Ddd8EC"
            ]
        },
        "direct": {
            # Circle‐bridged USDC.e (Stargate).  
            "usdc": {
                "token_address": "0x19e26B0638bf63aa9fa4d14c6baF8D52eBE86C5C",  # USDC.e
                "method_name": "totalSupply"
            },

            # Native USDT contract deployed May 2024.  
            "tether": {
                "token_address": "0x9c2dc7377717603eB92b2655c5f2E7997a4945BD",  # USDT
                "method_name": "totalSupply"
            }
        }
    },
    "redstone": {
        "bridged": {
            "ethereum": [
                # Canonical rollup bridge escrows
                "0xc473ca7E02af24c129c2eEf51F2aDf0411c1Df69",
                "0xC7bCb0e8839a28A1cFadd1CF716de9016CdA51ae"
            ]
        }
    },
    "mint": {
        "bridged": {
            "ethereum": [
                # Primary L1→Mint bridge escrow (holds USDC, USDT, WBTC, etc.)
                "0x2b3F201543adF73160bA42E1a5b7750024F30420",

                # Auxiliary genesis escrow (mostly ETH but include for completeness)
                "0x59625d1FE0Eeb8114a4d13c863978F39b3471781"
            ]
        }
    },
    "worldchain": {
        "bridged": {
            "ethereum": [
                "0x470458C91978D2d929704489Ad730DC3E3001113",  # Main L1StandardBridge escrow
                "0xd5ec14a83B7d95BE1E2Ac12523e2dEE12Cbeea6C"   # Early/aux escrow still holding ETH & some stables
            ]
        },
        "direct": {
            # Circle-bridged USDC (contract lives on World Chain, track via totalSupply)
            "usdc": {
                "token_address": "0x79A02482A880bCE3F13e09Da970dC34db4CD24d1",
                "method_name": "totalSupply"
            }
        }
    },
    "ink": {
        "bridged": {
            "ethereum": [
                "0x88FF1e5b602916615391F55854588EFcBB7663f0",
            ]
        },
        "direct": {
            "tether": { # USDT0
                "token_address": "0x0200C29006150606B650577BBE7B6248F58470c1",
                "method_name":   "totalSupply",
            },
        },
    },
    "blast": {
        "bridged": {
            "ethereum": [
                # Main Blast L1StandardBridge escrow (holds the vast majority)
                "0x697402166Fbf2F22E970df8a6486Ef171dbfc524",
                # Legacy / ETH-specific escrow (still retains ETH + stETH)
                "0x98078db053902644191f93988341E31289E1C8FE",
                # Older stETH escrow (small residual balance but include for
                # completeness so nothing slips through the cracks)
                "0x5F6AE08B8AeB7078cf2F96AFb089D7c9f51DA47d",
            ]
        },
        "direct": {
            # Blast’s own yield-bearing stablecoin
            "usdb": {
                "token_address": "0x4300000000000000000000000000000000000003",
                "method_name": "totalSupply",
            },
            # Ethena’s synthetic dollar (native deployment)
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            },
        }
    },
    "gravity": {
        "bridged": {
            "ethereum": [
                "0xa4108aA1Ec4967F8b52220a4f7e94A8201F2D906",   # canonical Gravity bridge
            ],
        }
    },
    "linea": {
        "bridged": {
            "ethereum": [
                # Linea L1StandardBridge ERC-20 escrow
                "0x051F1D88f0aF5763fB888eC4378b4D8B29ea3319",
            ]
        },
        "direct": {
            "usdc": {
                "token_address": "0x176211869cA2b568f2A7D4EE941E073a821EE1ff",
                "method_name":   "totalSupply",
            },
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name":   "totalSupply",
            },
            "metamask-usd": {
                "token_address": "0xacA92E438df0B2401fF60dA7E4337B687a2435DA",
                "method_name":   "totalSupply",
            },
            ## EUR
            "monerium-eur-money": {
                "token_address": "0x3ff47c5Bf409C86533FE1f4907524d304062428D",
                "method_name":   "totalSupply",
            },
        }
    },
    "manta": {
        "bridged": {
            "ethereum": [
                # Main Manta Pacific L1StandardBridge escrow
                "0x3B95bC951EE0f553ba487327278cAc44f29715E5",

                # Early/auxiliary escrow (still holds ETH & some stables)
                "0x9168765EE952de7C6f8fC6FaD5Ec209B960b7622",
            ]
        },
        "direct": {
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            },
            "manta-musd": {
                "token_address": "0x649d4524897cE85A864DC2a2D5A11Adb3044f44a",
                "method_name": "totalSupply",
            },
        }
    },
    "fraxtal": {
        "bridged": {
            "ethereum": [
                # Generic Fraxtal bridge escrow
                "0x34C0bD5877A5Ee7099D0f5688D65F4bB9158BDE2",
                "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3",
            ],
        },
        "direct": {
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            }
        },
    },
    "scroll": {
        "bridged": {
            "ethereum": [
                # Canonical L1StandardBridge escrow (majority of bridged value)
                "0xD8A791fE2bE73eb6E6cF1eb0cb3F36adC9B3F8f9",
                # Additional Scroll escrows that hold USDC / DAI / wstETH / pufETH, etc.
                "0xb2b10a289A229415a124EFDeF310C10cb004B6ff",
                "0x67260A8B73C5B77B55c1805218A42A7A6F98F515",
                "0x6774Bcbd5ceCeF1336b5300fb5186a12DDD8b367",
                "0xA033Ff09f2da45f0e9ae495f525363722Df42b2a",
                "0xf1AF3b23DE0A5Ca3CAb7261cb0061C0D779A5c7B",
                "0x6625C6332c9F91F2D27c304E729B86db87A3f504"
            ]
        },
        "direct": {
            # Ethena USDe (fully-backed synthetic dollar)
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply",
            },
            # EUR
            "monerium-eur-money": {
                "token_address": "0xd7bb130a48595fcdf9480e36c1ae97ff2938ac21",
                "method_name": "totalSupply",
            },
        }
    },
    "zora": {
        "bridged": {
            "ethereum": [
                "0x3e2Ea9B92B7E48A52296fD261dc26fd995284631", # Zora's bridge escrow holding USDC
            ],
        }
    },
    "polygon_zkevm": {
        "direct": {
            "usdc": {
                "token_address": "0xA8CE8aee21bC2A48a5EF670afCc9274C7bbbC035",
                "method_name": "totalSupply"
            },
            "dai": {
                "token_address": "0xC5015b9d9161Dca7e18e32f6f25C4aD850731Fd4",
                "method_name": "totalSupply"
            },
            "liquity-usd": {
                "token_address": "0x01E9A866c361eAd20Ab4e838287DD464dc67A50e",
                "method_name": "totalSupply"
            },
            "pax-dollar": {
                "token_address": "0x9F171f7d8445a2ab62747C3f3115362a1D51FC50",
                "method_name": "totalSupply"
            },
            "frax": {
                "token_address": "0xFf8544feD5379D9ffa8D47a74cE6b91e632AC44D",
                "method_name": "totalSupply"
            },
            "tether": {
                "token_address": "0x1E4a5963aBFD975d8c9021ce480b42188849D41d",
                "method_name": "totalSupply"
            }
        },
        "bridged": {
            "ethereum": [
                "0x4A27aC91c5cD3768F140ECabDe3FC2B2d92eDb98",  # DAI + sDAI
                "0x70E70e58ed7B1Cec0D8ef7464072ED8A52d755eB"   # USDC
            ]
        }
    },
    "imx": {
        "bridged": {
            "ethereum": [
                "0x5FDCCA53617f4d2b9134B29090C87D01058e27e9"  # ImmutableX Ethereum escrow contract
            ]
        },
    },
    "loopring": {
        "bridged": {
            "ethereum": [
                "0x674bdf20A0F284D710BC40872100128e2d66Bd3f",  # Loopring L1 escrow contract for tokens
                "0x7D3D221A8D8AbDd868E8e88811fFaF033e68E108",  # Old Loopring USDT escrow
                "0xD97D09f3bd931a14382ac60f156C1285a56Bb51B"   # Old Loopring USDT escrow
            ]
        },
    },
    "gitcoin_pgn": {
        "bridged": {
            "ethereum": [
                "0xD0204B9527C1bA7bD765Fa5CCD9355d38338272b"
            ]
        }
    },
    "starknet": {
        "bridged": {
            "ethereum": [
                "0x0437465dfb5B79726e35F08559B0cBea55bb585C",  # DAI
                "0xF3F62F23dF9C1D2C7C63D9ea6B90E8d24c7E3DF5",  # LUSD
                "0xF6080D9fbEEbcd44D89aFfBFd42F098cbFf92816",  # USDC.e
                "0xbb3400F107804DFB482565FF1Ec8D8aE66747605"   # USDT
            ]
        }
    },
    "optimism": {
        "direct": {
            "usdc": {
                "token_address": "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
                "method_name": "totalSupply"
            },
            "tether": {
                "token_address": "0x3e7ef4eebe34441227df5e8bcb86d04564d45f3e",
                "method_name": "totalSupply"
            },
            "frax": {
                "token_address": "0x2e3D870790dC77A83DD1d18184Acc7439A53f475",
                "method_name": "totalSupply"
            },
            "liquity-usd": {
                "token_address": "0x93b346b6bc2548da6a1e7d98e9a421b42541425b",
                "method_name": "totalSupply"
            },
            "curve-usd": {
                "token_address": "0x3ab04f007ca9c72dca2d298b9e9c38426794d47d",
                "method_name": "totalSupply"
            },
            "true_usd": {
                "token_address": "0xE7F58A92476056627f9F2dD71a2d38F164A6eF0D",
                "method_name": "totalSupply"
            },
            "frankencoin": {
                "token_address": "0xD4dD9e2F021BB459D5A5f6c24C12fE09c5D45553",
                "method_name": "totalSupply"
            }
        },
        "bridged": {
            "ethereum": [
                "0x99C9fc46f92E8a1c0deC1b1747d010903E884bE1", # Optimism L1 StandardBridge USDC vault
                "0x467194771dAe2967Aef3ECbEDD3Bf9a310C76C65" # Optimism L1 StandardBridge DAI vault
            ]
        }
    },
    "base": {
        # ────────────────────── bridge escrows on Ethereum (locked value) ──────────────────────
        "bridged": {
            "ethereum": [
                "0x49048044D57e1C92A77f79988d21Fa8fAF74E97e",  # Base: L1StandardBridge (ETH + ERC-20)
                "0x3154Cf16ccdb4C6d922629664174b904d80F2C35",  # Base: L1 ERC-20 bridge vault
            ]
        },

        # ────────────────────── tokens natively minted on Base ──────────────────────
        "direct": {
            "usdc": {                    # USDC
                "token_address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                "method_name":  "totalSupply",
            },
            "ethena-usde": {            # USDe
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name":  "totalSupply",
            },
            "usds": {                    # USDS
                "token_address": "0x820C137fa70C8691f0e44Dc420a5e53c168921Dc",
                "method_name":  "totalSupply",
            },
            "euro-coin": {              # EURC
                "token_address": "0x60a3E35Cc302bFA44Cb288Bc5a4F316Fdb1adb42",
                "method_name":  "totalSupply",
            },
            "openusdt": {               # oUSDT
                "token_address": "0x1217BfE6c773EEC6cc4A38b5Dc45B92292B6E189",
                "method_name":  "totalSupply",
            },
            "resolv-usr": {             # USR
                "token_address": "0x35E5dB674D8e93a03d814FA0ADa70731efe8a4b9",
                "method_name":  "totalSupply",
            },
            "angle_eura": {              # EURA native on Base
                "token_address": "0xA61BeB4A3d02decb01039e378237032B351125B4",
                "method_name":  "totalSupply",
            },
            "frankencoin": {            # ZCHF
                "token_address": "0xD4dD9e2F021BB459D5A5f6c24C12fE09c5D45553",
                "method_name":  "totalSupply",
            },
            "xsgd": {                    # XSGD
                "token_address": "0x0A4C9cb2778aB3302996A34BeFCF9a8Bc288C33b",
                "method_name":  "totalSupply",
            },
        },

    },  
    "plume": {
        "bridged": {
            "ethereum": [
                "0xE2C902BC61296531e556962ffC81A082b82f5F28",  # Generic escrow contract for bridged tokens
            ]
        },
        "direct": {
            "usdc": {
                "token_address": "0x78adD880A697070c1e765Ac44D65323a0DcCE913",  # USDC native on Plume
                "method_name": "totalSupply",
            },
            "pusd": {
                "token_address": "0xdddD73F5Df1F0DC31373357beAC77545dC5A6f3F",  # PUSD native on Plume
                "method_name": "totalSupply",
            },
        }
    },
    "zircuit": {
        "bridged": {
            "ethereum": [
                "0x386B76D9cA5F5Fb150B6BFB35CF5379B22B26dd8",  
            ]
        },
        "direct": {
            "usdc": {
                "token_address": "0x3b952c8C9C44e8Fe201e2b26F6B2200203214cfF",
                "method_name": "totalSupply"
            },
            "ethena-usde": {
                "token_address": "0x5d3a1Ff2b6BAb83b63cd9AD0787074081a52ef34",
                "method_name": "totalSupply"
            },
            "usda": {
                "token_address": "0xff12470a969Dd362EB6595FFB44C82c959Fe9ACc",
                "method_name": "totalSupply"
            }
        }
    },
    "polygon_pos": {
        "bridged": {
            "ethereum": [
                "0x40ec5B33f54e0E8A33A975908C5BA1c14e5BbbDf",  # ERC20 bridge escrow (USDC, DAI, USDT before Sep. 2025)
            ]
        },
        "direct": {
            "tether": { # USDT0, and before USDT bridged canonically
                "token_address": "0xc2132D05D31c914a87C6611C10748AEb04B58e8F",
                "method_name": "totalSupply"
            },
            "buidl": {
                "token_address": "0x2893Ef551B6dD69F661Ac00F11D93E5Dc5Dc0e99",
                "method_name": "totalSupply"
            },
            "ausd": {
                "token_address": "0x00000000efe302beaa2b3e6e1b18d08d69a9012a",
                "method_name": "totalSupply"
            }, 
            "wusd": {
                "token_address": "0x7cd017ca5ddb86861fa983a34b5f495c6f898c41",
                "method_name": "totalSupply"
            }
        }
    }
}