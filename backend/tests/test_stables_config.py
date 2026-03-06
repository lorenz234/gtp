#!/usr/bin/env python3
"""
Manual test script for stablecoin adapter v2.
It extracts stablecoin supplies and compares chain totals using
`vw_fact_stables_mcap_v2` grouped by `origin_key` and `date`.
"""

import sys
import os
import pandas as pd
from datetime import datetime, date
sys.path.append(f"{os.getcwd()}/backend/")

from src.db_connector import DbConnector
from src.adapters.adapter_stables_v2 import AdapterStablecoinSupply


def calculate_days_from_date(start_date_str):
    """
    Calculate number of days from start_date to today

    Args:
        start_date_str: Date string in format "YYYY-MM-DD" (e.g., "2024-05-14")

    Returns:
        int: Number of days from start_date to today
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    today = date.today()
    days = (today - start_date).days + 1  # +1 to include today
    
    print(f"Start date: {start_date}")
    print(f"Today: {today}")
    print(f"Days between dates: {days}")

    return days


def _build_origin_filter(origin_keys):
    if not origin_keys:
        return ""
    escaped = [origin_key.replace("'", "''") for origin_key in origin_keys]
    return "AND origin_key IN ('" + "', '".join(escaped) + "')"


def get_current_total_supply(db_connector, days=3, origin_keys=None):
    """
    Calculate current total supply by summing value_usd in vw_fact_stables_mcap_v2.
    """
    print("\nCurrent total supply from vw_fact_stables_mcap_v2")
    print("=" * 60)

    origin_filter = _build_origin_filter(origin_keys)
    query = f"""
        SELECT
            "date",
            origin_key,
            SUM(value_usd) AS value
        FROM public.vw_fact_stables_mcap_v2
        WHERE
            "date" >= current_date - interval '{days} days'
            {origin_filter}
        GROUP BY 1, 2
        ORDER BY 1, 2;
    """
    totals_df = db_connector.execute_query(query, load_df=True)
    if totals_df.empty:
        print("No stable supply data found in vw_fact_stables_mcap_v2.")
        return pd.DataFrame()

    totals_df["date"] = pd.to_datetime(totals_df["date"])
    latest_date = totals_df["date"].max()
    latest_totals = totals_df[totals_df["date"] == latest_date].sort_values("value", ascending=False)

    print(f"Latest totals by chain (date: {latest_date.date()}):")
    for _, row in latest_totals.iterrows():
        print(f"  {row['origin_key']}: ${row['value']:,.2f}")
    print(f"Grand total: ${latest_totals['value'].sum():,.2f}")

    return totals_df


def summarize_extracted_data(extracted_df):
    print("\nExtract summary")
    print("=" * 60)
    print(f"Records extracted: {len(extracted_df)}")
    print(f"Metrics: {sorted(extracted_df['metric_key'].dropna().unique().tolist())}")

    metric_counts = extracted_df.groupby("metric_key").size().sort_values(ascending=False)
    print("Rows per metric_key:")
    for metric_key, count in metric_counts.items():
        print(f"  {metric_key}: {count}")

    origin_counts = extracted_df.groupby("origin_key").size().sort_values(ascending=False).head(10)
    print("Top origins by row count:")
    for origin_key, count in origin_counts.items():
        print(f"  {origin_key}: {count}")

    if "date" in extracted_df.columns and not extracted_df["date"].empty:
        date_min = pd.to_datetime(extracted_df["date"]).min()
        date_max = pd.to_datetime(extracted_df["date"]).max()
        print(f"Date range in extract: {date_min.date()} -> {date_max.date()}")


def compare_totals(before_totals, after_totals):
    print("\nComparison: before vs after load")
    print("=" * 60)

    if before_totals.empty or after_totals.empty:
        print("Comparison skipped because one side is empty.")
        return

    latest_date = after_totals["date"].max()
    before_latest = before_totals[before_totals["date"] == latest_date][["origin_key", "value"]].copy()
    after_latest = after_totals[after_totals["date"] == latest_date][["origin_key", "value"]].copy()

    before_latest = before_latest.rename(columns={"value": "value_before"})
    after_latest = after_latest.rename(columns={"value": "value_after"})
    merged = before_latest.merge(after_latest, on="origin_key", how="outer").fillna(0)
    merged["delta"] = merged["value_after"] - merged["value_before"]
    merged = merged.sort_values("delta", key=lambda s: s.abs(), ascending=False)

    print(f"Compared date: {latest_date.date()}")
    for _, row in merged.iterrows():
        print(
            f"  {row['origin_key']}: "
            f"before=${row['value_before']:,.2f}, "
            f"after=${row['value_after']:,.2f}, "
            f"delta=${row['delta']:,.2f}"
        )

    before_total = merged["value_before"].sum()
    after_total = merged["value_after"].sum()
    print(f"Grand total before: ${before_total:,.2f}")
    print(f"Grand total after:  ${after_total:,.2f}")
    print(f"Grand delta:        ${after_total - before_total:,.2f}")


def test_stables_config(chains_to_test=None, start_date=None, load_full_history=False):
    """
    Test stablecoin v2 extraction and optionally load results.

    Args:
        chains_to_test: list of origin_keys to test, or None for all
        start_date: kept for compatibility with old script (not used by v2 extract)
        load_full_history: if True, load extracted rows into fact_stables_v2
    """
    db_connector = DbConnector()
    comparison_days = 3

    print("\nTesting stablecoin configuration (v2)")
    print("=" * 60)
    if start_date:
        calculate_days_from_date(start_date)
        print("Note: v2 adapter backfills from DB progress; start_date is informational only.")

    current_totals = get_current_total_supply(
        db_connector,
        days=comparison_days,
        origin_keys=chains_to_test,
    )

    print("\nInitializing adapter...")
    stablecoin_adapter = AdapterStablecoinSupply({}, db_connector)
    selected_chains = chains_to_test if chains_to_test else ["*"]
    print(f"Chains requested: {selected_chains}")

    try:
        extract_params = {
            "origin_keys": selected_chains,
            "token_ids": ["*"],
            "metric_keys": ["*"],
        }
        extracted_df = stablecoin_adapter.extract(extract_params)

        if extracted_df is None or extracted_df.empty:
            print("No new stablecoin rows extracted.")
            return None

        summarize_extracted_data(extracted_df)

        if load_full_history:
            print("\nLoading extracted rows into fact_stables_v2...")
            stablecoin_adapter.load(extracted_df, table_name="fact_stables_v2")

            updated_totals = get_current_total_supply(
                db_connector,
                days=comparison_days,
                origin_keys=chains_to_test,
            )
            compare_totals(current_totals, updated_totals)
            print("\nLoad + comparison finished.")
        else:
            print("\nDry run only: extracted data was not loaded.")
            print("Set load_full_history=True to load into fact_stables_v2 and compare totals.")

        return extracted_df

    except Exception as e:
        print(f"\nError during testing: {e}")
        import traceback
        traceback.print_exc()
        return None


def verify_chain_config(chain_name):
    """
    Verify that a chain is configured in stables_config_v2.
    """
    from src.stables_config_v2 import address_mapping

    print(f"\nVerifying configuration for {chain_name}...")

    if chain_name not in address_mapping:
        print(f"{chain_name} not found in address_mapping")
        return False

    config = address_mapping[chain_name]
    print(f"{chain_name} found in address_mapping")

    if "track_on_l1" in config:
        addresses = config["track_on_l1"]
        print(f"track_on_l1 chain with {len(addresses)} bridge address(es)")
        for address in addresses:
            print(f"  - {address}")
        return True

    token_count = len(config)
    print(f"total_supply chain with {token_count} token(s)")
    for token_id, token_data in list(config.items())[:10]:
        print(f"  - {token_id}: {token_data['address']} (decimals={token_data['decimals']})")
    if token_count > 10:
        print(f"  ... and {token_count - 10} more")

    return True


if __name__ == "__main__":
    # CONFIGURATION
    chains_to_test = ["scroll"]  # e.g. ['metis', 'taiko', 'mantle'] or None for all
    start_date = "2023-10-10"    # Informational only in v2 (kept for compatibility)
    load_full_history = True     # True = load into fact_stables_v2, False = dry run

    print("Testing Stablecoin Configuration v2")

    if load_full_history and start_date:
        print("Load mode")
        print(f"start_date hint: {start_date}")
        print("This run will load extracted rows into fact_stables_v2")
    else:
        print("Dry-run mode")
        print("This run extracts only and does not write to DB")

    if chains_to_test:
        for chain in chains_to_test:
            verify_chain_config(chain)

    extracted_df = test_stables_config(
        chains_to_test=chains_to_test,
        start_date=start_date,
        load_full_history=load_full_history,
    )

    if load_full_history:
        if extracted_df is not None:
            print("\nLoad completed.")
        else:
            print("\nNo rows loaded.")
    elif extracted_df is not None:
        print("\nDry run completed successfully.")
        print("Set load_full_history=True to load extracted rows.")
    else:
        print("\nTest failed - check the error output above.")
