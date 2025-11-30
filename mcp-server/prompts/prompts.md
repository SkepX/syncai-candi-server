# Tool Prompts for Cardano Data

## NFT-Related Prompts

| **Tool Name** | **Prompt** |
| --- | --- |
| **market_stats** | Get the aggregated market stats for ADA, including the 24-hour DEX volume and total number of active addresses |
| **nft_sale_history** | Get the sale history for a specific asset under the "ClayNation3725" collection. The policy ID for Clay Nation is `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` |
| **nft_stats** | Get stats for "ClayNation3725" including sales volume and listings. The policy ID for Clay Nation is `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` |
| **nft_traits** | Fetch the traits for the "ClayNation3725" NFTs and get trait prices. The policy ID for Clay Nation is `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` |
| **collection_assets** | Fetch all NFTs from the "ClayNation" collection, filtering by whether they are on sale. The policy ID for Clay Nation is `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` |
| **holder_distribution** | Retrieve the holder distribution for the "ClayNation3725" collection to see how many NFTs are held by different wallets. The policy ID for Clay Nation is `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` |
| **nft_top_holders** | Get the top NFT holders for the collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. Fetch 10 results per page, excluding exchanges |
| **nft_holder_trend** | Fetch the daily trend of NFT holders for the collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728' over the last 30 days |
| **nft_collection_info** | Fetch the basic details for the NFT collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728', including the name, social links, and logo |
| **number_of_active_listing** | Show the number of active listings and total supply for the NFT collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728' |
| **nft_listings_depth** | Fetch the cumulative listing depth for the collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728', with a maximum of 500 items |
| **nft_active_listings** | Get a list of active NFT listings for the collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. The results are sorted by price in ascending order, with pagination for 100 items per page |
| **nft_listings_trended** | Fetch trended data for the number of listings and floor price for the NFT collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. Show results for a daily interval over the last 30 days |
| **nft_floor_price_ohlcv** | Get OHLCV (open, high, low, close, volume) data for the floor price of the NFT collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. Use a daily interval for the past 30 days, quoted in ADA |
| **nft_collection_stats** | Fetch basic statistics for the NFT collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. Include floor price, volume, and total supply details |
| **nft_collection_stats_extended** | Get extended statistics for the specified NFT collection with the policy ID `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728`. This includes details about the collection's market performance and other metrics |
| **nft_collection_trades** | Retrieve NFT trades for the collection with the policy ID `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` over the last 30 days. The data will be sorted by time, showing the most recent trades first, filtered by minimum trade amount and other criteria |
| **nft_collection_trades_stats** | Get trading statistics for the collection with the policy ID `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728` over the last 24 hours. This includes volume of trades, price fluctuations, and other relevant trading data |
| **nft_collection_traits_price** | Fetch price data for specific traits within the collection with the policy ID `40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728`. This will show trends in the prices of different traits within the collection |
| **nft_collection_metadata_rarity** | Retrieve metadata rarity information for the collection with policy ID '40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728'. This includes details on how traits are distributed across NFTs, helping to understand their rarity |
| **nft_rarity_rank** | What is the rarity rank of the NFT named "ClayNation3725" in the policy "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728"? |
| **nft_volume_trended** | Show me the trended volume and sales data for the policy "40fa2aa67258b4ce7b5782f74831d46a84c59a0ff0c28262fab21728" over a 1-week interval |
| **nft_market_stats** | Give me high-level NFT market stats for the last 24 hours |
| **nft_market_stats_extended** | Provide detailed NFT market stats with percentage changes over the past 7 days |
| **nft_market_volume_trended** | Show trended NFT market volume over the last 30 days |
| **nft_marketplace_stats** | What are the NFT stats for jpg.store over the past 7 days? |
| **nft_top_rankings** | Get top NFT rankings based on total market cap with a limit of 25 items |
| **nft_top_volume_collections** | Retrieve the top 10 NFT collections by trading volume in the last 24 hours |
| **nft_top_volume_collections_extended** | Show the top 10 NFT collections by 24-hour trading volume with percentage change metrics included |

## Token-Related Prompts

| **Tool Name** | **Prompt** |
| --- | --- |
| **token_links** | Find all social media links and websites for AnetaBTC's cBTC token |
| **token_market_cap** | Provide detailed supply and market capitalization information for SNEK token |
| **token_price_ohlcv** | Get OHLCV data for Minswap (MIN) for the past week with 4-hour intervals |
| **token_liquidity_pools** | What liquidity pools are available for AnetaBTC's cBTC token? |
| **token_prices** | What are the current prices for HOSKY token? |
| **token_price_percent_change** | How much has Minswap (MIN) price changed in the last 24 hours? |
| **quote_price** | What's the current ADA price in USD compared to the wrapped cBTC? |
| **available_quote_currencies** | What quote currencies can I use to price the SNEK token? |
| **top_liquidity_tokens** | What are the top 10 tokens by liquidity and how do they compare to HOSKY? |
| **top_market_cap_tokens** | What are the top 20 tokens by market cap and where does MIN rank? |
| **top_volume_tokens** | What tokens have the highest trading volume in the last 24 hours compared to SNEK? |
| **token_trades** | Show me recent large trades for Hosky |
| **trading_stats** | What are the trading stats for HOSKY token in the last 24 hours? |
| **asset_supply** | What is the onchain supply for Minswap (MIN)? |
| **token_database_lookup** | Find information about Cardano USDC in the token database |
| **token_price_indicators** | Analyze the price indicators for MIN token using the MA indicator with professional analysis |
| **active_loans_analysis** | What are the active P2P loans for cBTC token? |
| **loan_offers_analysis** | Analyze P2P loan offers for SNEK token |
| **token_holders_analysis** | Who are the top holders of HOSKY token? |

## Address and Portfolio-Related Prompts

| **Tool Name** | **Prompt** |
| --- | --- |
| **address_info** | Get the payment credential, stake address, and balance details for address addr1q8elqhkuvtyelgcedpup58r893awhg3l87a4rz5d5acatuj9y84nruafrmta2rewd5l46g8zxy4l49ly8kye79ddr3ksqal35g |
| **address_utxos** | Show me all current UTxOs at address addr1q8elqhkuvtyelgcedpup58r893awhg3l87a4rz5d5acatuj9y84nruafrmta2rewd5l46g8zxy4l49ly8kye79ddr3ksqal35g |
| **transaction_utxos** | Get the UTxOs (inputs and outputs) from transaction hash 8be33680ec04da1cc98868699c5462fbbf6975529fb6371669fa735d2972d69b |
| **get_portfolio_positions** | Show positions of SNEK, HOSKY, MIN, and cBTC tokens in this portfolio: stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc |
| **get_token_trade_history** | Show me the token trade history for HOSKY at this address: stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc |
| **get_portfolio_trended_value** | Display the portfolio value trend over the last 30 days for stake address stake1u8rphunzxm9lr4m688peqmnthmap35yt38rgvaqgsk5jcrqdr2vuc in ADA |

## Blockchain and DEX-Related Prompts

| **Tool Name** | **Prompt** |
| --- | --- |
| **get_token_by_id** | Get detailed information for AnetaBTC's cBTC |
| **get_block** | What is the information for block number 10937538? |
| **get_events** | List all events that occurred between blocks 10937538 and 10937542 with a limit of 1000 |
| **get_dex** | Get details for DEX with ID 7 |
| **get_latest_block** | What is the latest block processed in the blockchain? |
| **get_pair_by_id** | Show me details for the trading pair between ADA and SNEK |