
import pandas as pd

# ================= CONFIG =================
SKU_FILE = "sku_with_ai_priority.csv"   # from Part 2 – AI agent output
LOCATION_FILE = "location_master.csv"   # static warehouse layout
OUTPUT_FILE = "sku_slot_assignment.csv"

# Max different SKUs that can share same location
MAX_SKUS_PER_LOCATION = 5


def load_data():
    sku_df = pd.read_csv(SKU_FILE)
    loc_df = pd.read_csv(LOCATION_FILE)
    print(f"Loaded {len(sku_df)} SKUs with AI priority and {len(loc_df)} locations.")
    return sku_df, loc_df


def normalise_cols(sku_df, loc_df):
    # Ensure these columns exist
    if "AI_Priority_Score_100" not in sku_df.columns:
        raise ValueError(
            "SKU file must contain 'AI_Priority_Score_100'. "
            "Make sure you ran the AI agent and used 'sku_with_ai_priority.csv'."
        )
    return sku_df.copy(), loc_df.copy()


def size_fits(sku_size, allowed):
    """
    Simple size compatibility logic.
    sku_size: 'Small'/'Medium'/'Large'
    allowed: 'S','M','L','M+L', or similar.
    """
    if pd.isna(allowed) or allowed is None:
        return True

    size_map = {
        "Small": "S",
        "Medium": "M",
        "Large": "L"
    }
    sku_code = size_map.get(str(sku_size).strip(), None)
    if sku_code is None:
        return True  # if unknown, don't block

    allowed = str(allowed).strip().upper()

    if allowed == sku_code:
        return True
    if allowed == "M+L" and sku_code in {"M", "L"}:
        return True

    return False


def can_store(sku_row, loc_row):
    """
    Basic compatibility check between SKU and location.
    You can mention this logic in the report as “slotting constraints”.
    """
    # Size constraint
    if not size_fits(sku_row.get("SKU_Size_Class"), loc_row.get("Allowed_Size_Class")):
        return False

    # Fragile handling – fragile-only racks must carry fragile SKUs
    handling = str(sku_row.get("Handling_Type", "")).strip().lower()
    special = str(loc_row.get("Special_Constraint", "")).strip().lower()

    if "fragile-only" in special and handling != "fragile":
        return False

    # Example: avoid putting hazardous items in chilled chambers
    family = str(sku_row.get("Family_or_Category", "")).strip().lower()
    if "chilled" in special and "hazard" in family:
        return False

    return True


def prepare_locations(loc_df):
    loc_df = loc_df.copy()
    # Helper numeric aisle for sorting
    import re

    def aisle_num(a):
        if pd.isna(a):
            return 0
        m = re.search(r"(\d+)", str(a))
        return int(m.group(1)) if m else 0

    loc_df["AisleNum"] = loc_df["Aisle"].apply(aisle_num)

    zone_rank = {"Golden": 0, "Standard": 1, "Overflow": 2}
    loc_df["ZoneRank"] = loc_df["Zone"].map(zone_rank).fillna(3)
    loc_df["Used_Slots"] = 0

    # Sort locations: Golden first, then Standard, then Overflow, then by level and aisle
    loc_df = loc_df.sort_values(
        by=["ZoneRank", "Level", "AisleNum", "Rack"],
        ascending=[True, True, True, True]
    ).reset_index(drop=True)

    return loc_df


def assign_skus(sku_df, loc_df):
    """
    Greedy assignment: go through SKUs in descending AI priority
    and assign to the best available compatible location.
    """
    # Sort SKUs by priority (highest first)
    if "AI_Priority_Score_100" in sku_df.columns:
        sku_df = sku_df.sort_values("AI_Priority_Score_100", ascending=False)
    else:
        sku_df = sku_df.sort_values("AI_Priority_Score", ascending=False)

    assignments = []

    for _, sku_row in sku_df.iterrows():
        assigned = False

        for loc_idx, loc_row in loc_df.iterrows():
            # Check capacity by number of SKUs
            if loc_row["Used_Slots"] >= MAX_SKUS_PER_LOCATION:
                continue

            # Check basic compatibility
            if not can_store(sku_row, loc_row):
                continue

            # If we reach here, we can assign
            assignments.append({
                "SKU_ID": sku_row["SKU_ID"],
                "Zone": loc_row["Zone"],
                "Aisle": loc_row["Aisle"],
                "Rack": loc_row["Rack"],
                "Level": loc_row["Level"],
                "Location_ID": loc_row["Location_ID"],
                "Priority_Score": sku_row.get(
                    "AI_Priority_Score_100",
                    sku_row.get("AI_Priority_Score", None)
                ),
                "Handling_Type": sku_row.get("Handling_Type"),
                "SKU_Size_Class": sku_row.get("SKU_Size_Class")
            })

            loc_df.at[loc_idx, "Used_Slots"] = loc_row["Used_Slots"] + 1
            assigned = True
            break

        if not assigned:
            # Keep record of unassigned SKU for dashboard & report
            assignments.append({
                "SKU_ID": sku_row["SKU_ID"],
                "Zone": None,
                "Aisle": None,
                "Rack": None,
                "Level": None,
                "Location_ID": None,
                "Priority_Score": sku_row.get(
                    "AI_Priority_Score_100",
                    sku_row.get("AI_Priority_Score", None)
                ),
                "Handling_Type": sku_row.get("Handling_Type"),
                "SKU_Size_Class": sku_row.get("SKU_Size_Class")
            })

    return pd.DataFrame(assignments)


def main():
    sku_df, loc_df = load_data()
    sku_df, loc_df = normalise_cols(sku_df, loc_df)
    loc_df = prepare_locations(loc_df)

    assignment_df = assign_skus(sku_df, loc_df)

    assignment_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nAssignment complete. Saved to {OUTPUT_FILE}")
    print("Total SKUs in priority file :", len(sku_df))
    print("Assigned SKUs (have Location_ID):", assignment_df["Location_ID"].notna().sum())
    print("Unassigned SKUs:", assignment_df["Location_ID"].isna().sum())


if __name__ == "__main__":
    main()
