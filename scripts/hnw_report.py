import argparse
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from itertools import combinations

from pdf_utils import (
    add_comparison_charts,
    add_key_value_table,
    add_section_header,
    add_side_by_side_sections,
    add_table,
)

import pandas as pd
from fpdf import FPDF


class HNWReport(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 16)
        self.set_text_color(30, 30, 30)
        self.cell(0, 10, "HNW INVESTOR REPORT", ln=True, align="L")
        self.set_font("helvetica", "", 10)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, datetime.now().strftime("%B %d, %Y"), ln=True, align="L")
        self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("helvetica", "I", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Page {self.page_no()}", align="C")



def format_currency(value):
    if pd.isna(value):
        return "N/A"
    return f"${value:,.0f}"


def strip_parens(text):
    if pd.isna(text):
        return ""
    return re.sub(r"\s*\(.*?\)\s*", "", str(text)).strip()


def parse_contacts(contacts_cell):
    if pd.isna(contacts_cell) or str(contacts_cell).strip() == "":
        return []
    raw = str(contacts_cell).split(";")
    cleaned = [re.sub(r'\s+', ' ', strip_parens(x)).strip() for x in raw]
    seen = set()
    out = []
    for name in cleaned:
        if name and name not in seen:
            out.append(name)
            seen.add(name)
    return out


def clean_amount_series(series):
    if pd.api.types.is_numeric_dtype(series):
        return series
    out = (
        series.astype(str)
        .str.replace(r"[\$,]", "", regex=True)
        .str.strip()
        .replace({"": None, "nan": None, "None": None})
    )
    return pd.to_numeric(out, errors="coerce")

def clean_accounts(acc):
    acc = acc[[c for c in acc.columns if "electronic tax documents" not in c.lower()]]
    acc = acc.copy()
    acc.columns = acc.columns.str.strip()

    preferred_id_vars = [
        "Account ID", "Legal name", "Contacts", "Contact locations", "Account notes",
        "Active commitment", "Total commitment", "# of Positions", "Legal entity type",
        "Account Marketplace", "Foreign Investor", "Yardi Vendor Code for Positions",
        "Accredited Investor", "Close Date", "Outside Investor", "Institutional Investor",
    ]
    remove_cols = [c for c in acc.columns if c not in preferred_id_vars]

    acc = acc.drop(columns = remove_cols)
    return acc


def build_investor_outputs(acc):
    acc = acc[[c for c in acc.columns if "electronic tax documents" not in c.lower()]]
    acc = acc.copy()
    acc.columns = acc.columns.str.strip()
    acc["Contacts_list"] = acc["Contacts"].apply(parse_contacts)
    acc["Primary_investor"] = acc["Contacts_list"].apply(lambda lst: lst[0] if lst else None)

    acc = acc.loc[
        ~acc["Legal name"].astype(str).str.strip().str.lower().str.startswith("transfer", na=False)
    ].copy()

    preferred_id_vars = [
        "Account ID", "Legal name", "Contacts", "Contact locations", "Account notes",
        "Active commitment", "Total commitment", "# of Positions", "Legal entity type",
        "Account Marketplace", "Foreign Investor", "Yardi Vendor Code for Positions",
        "Accredited Investor", "Close Date", "Outside Investor", "Institutional Investor",
    ]
    id_vars = [c for c in preferred_id_vars if c in acc.columns]
    for c in ["Legal name", "Contacts", "Active commitment", "Total commitment", "# of Positions"]:
        if c not in id_vars:
            id_vars.append(c)

    loc_cols = [c for c in acc.columns if c not in id_vars and c not in ["Contacts_list", "Primary_investor"]]
    df_all = acc.melt(id_vars=id_vars, value_vars=loc_cols, var_name="Location", value_name="Amount")
    df_all["Amount"] = clean_amount_series(df_all["Amount"])
    df_all = df_all.dropna(subset=["Amount"])
    df_all = df_all[df_all["Amount"] != 0]

    investors = defaultdict(
        lambda: {
            "entities": [],
            "num_entities": 0,
            "total_commitment": 0.0,
            "active_commitment": 0.0,
            "total_positions": 0,
            "co_investors": set(),
            "locations": Counter(),
        }
    )

    entity_loc_amounts = (
        df_all.groupby(["Legal name", "Location"], dropna=False)["Amount"].sum().reset_index()
    )
    loc_map = defaultdict(dict)
    for _, row in entity_loc_amounts.iterrows():
        loc_map[row["Legal name"]][row["Location"]] = float(row["Amount"])

    for _, row in acc.iterrows():
        primary = row["Primary_investor"]
        if not primary:
            continue

        total_commit = row["Total commitment"] if pd.notna(row["Total commitment"]) else 0
        active_commit = row["Active commitment"] if pd.notna(row["Active commitment"]) else 0
        positions = row["# of Positions"] if pd.notna(row["# of Positions"]) else 0

        inv = investors[primary]
        inv["entities"].append(
            {
                "entity": row["Legal name"],
                "total_commitment": float(total_commit),
                "active_commitment": float(active_commit),
                "positions": int(positions),
                "locations": loc_map.get(row["Legal name"], {}),
            }
        )
        inv["num_entities"] += 1
        inv["total_commitment"] += float(total_commit)
        inv["active_commitment"] += float(active_commit)
        inv["total_positions"] += int(positions)

    edge_counts = Counter()
    neighbors = defaultdict(set)
    real_investors = set(acc["Primary_investor"].dropna().unique())
    for _, row in acc.iterrows():
        contacts = row["Contacts_list"]
        if len(contacts) <= 1:
            continue
        for a, b in combinations(sorted(contacts), 2):
            edge_counts[(a, b)] += 1
            neighbors[a].add(b)
            neighbors[b].add(a)

    co_investors = set()
    all_contacts = set()
    for lst in acc["Contacts_list"]:
        all_contacts.update(lst)
    for contact in all_contacts:
        if contact in real_investors:
            continue
        if any(n in real_investors for n in neighbors.get(contact, set())):
            co_investors.add(contact)
    for inv_name in real_investors:
        for neighbor in neighbors.get(inv_name, set()):
            if neighbor in co_investors:
                investors[inv_name]["co_investors"].add(neighbor)

    investors_df = pd.DataFrame(
        [
            {
                "name": name,
                "num_entities": data["num_entities"],
                "total_commitment": data["total_commitment"],
                "active_commitment": data["active_commitment"],
                "total_positions": data["total_positions"],
                "co_investors": "; ".join(sorted(list(data["co_investors"]))),
            }
            for name, data in investors.items()
        ]
    ).sort_values(["total_commitment", "num_entities"], ascending=False)

    investors_json = []
    for name, data in investors.items():
        investors_json.append(
            {
                "name": name,
                "entities": [e["entity"] for e in data["entities"]],
                "no_of_entities": data["num_entities"],
                "commitment": data["total_commitment"],
                "co_investor": sorted(list(data["co_investors"])),
            }
        )

    return investors_json, investors_df



def compute_account_metrics(accounts_df):
    scoped = accounts_df.copy()
    scoped.columns = scoped.columns.str.strip()
    scoped["Active commitment"] = pd.to_numeric(scoped["Active commitment"], errors="coerce")
    scoped["Total commitment"] = pd.to_numeric(scoped["Total commitment"], errors="coerce")
    scoped["Close Date"] = pd.to_datetime(scoped["Close Date"], errors="coerce")

    if "Account ID" in scoped.columns:
        account_series = scoped["Account ID"]
    else:
        account_series = pd.Series(scoped.index, index=scoped.index)

    active_mask = scoped["Active commitment"].fillna(0) > 0
    total_mask = scoped["Total commitment"].fillna(0) > 0
    total_accounts_mask = active_mask #| total_mask
    # Match investor_metrics_report dormant rule: prior to 2023 with positions > 0.
    # dormant_mask = (
    #     (pd.to_numeric(scoped["# of Positions"], errors="coerce").fillna(0) > 0)
    #     & (scoped["Close Date"] < pd.Timestamp("2023-01-01"))
    # )


    total_accounts = account_series[total_accounts_mask].nunique()
    #total_active_accounts = account_series[active_mask].nunique()
    #total_dormant_accounts = account_series[dormant_mask].nunique()
    total_active_commitment = scoped.loc[active_mask, "Active commitment"].sum()
    total_total_commitment = scoped.loc[total_mask, "Total commitment"].sum()
    average_active_commitment = scoped.loc[active_mask, "Active commitment"].mean()
    median_active_commitment = scoped.loc[active_mask, "Active commitment"].median()
    average_total_commitment = scoped.loc[total_mask, "Total commitment"].mean()
    median_total_commitment = scoped.loc[total_mask, "Total commitment"].median()
    average_active_commitment_per_position = scoped.loc[active_mask, "Active commitment"].sum() / pd.to_numeric(scoped.loc[active_mask, "# of Positions"], errors="coerce").sum()
    average_total_commitment_per_position = scoped.loc[total_mask, "Total commitment"].sum() / pd.to_numeric(scoped.loc[total_mask, "# of Positions"], errors="coerce").sum()
    return {
        "total_accounts": total_accounts,
        #"total_active_accounts": total_active_accounts,
        #"total_dormant_accounts": total_dormant_accounts,
        "average_active_commitment": average_active_commitment,
        "median_active_commitment": median_active_commitment,
        "average_total_commitment": average_total_commitment,
        "median_total_commitment": median_total_commitment,
        "total_active_commitment": total_active_commitment,
        "total_total_commitment": total_total_commitment,
        "average_active_commitment_per_position": average_active_commitment_per_position,
        "average_total_commitment_per_position": average_total_commitment_per_position,
    }


def create_quarterly_reports(contacts, new_contacts):
    contacts = contacts.copy()
    contacts["Contact Created At Date"] = pd.to_datetime(contacts["Contact Created At Date"], errors="coerce")
    contacts["Count of Positions"] = pd.to_numeric(contacts["Count of Positions"], errors="coerce").fillna(0)
    contacts["Total Commitment"] = pd.to_numeric(contacts["Total Commitment"].astype(str).str.replace(r"[\$,]", "", regex=True), errors="coerce")

    # Map Contact Type and Investor Group from js_contacts into contacts using Contact ID
    contacts["Contact ID"] = contacts["Contact ID"].astype(str).str.strip()
    nc = new_contacts.copy()
    nc["Contact ID"] = nc["Contact ID"].astype(str).str.strip()
    nc["Contact Type"] = nc["Contact Type"].fillna("").astype(str).str.strip()
    nc["Investor Group"] = nc["Investor Group"].fillna("").astype(str).str.strip()

    contact_type_lookup = (
        nc.loc[nc["Contact Type"] != "", ["Contact ID", "Contact Type"]]
        .drop_duplicates(subset=["Contact ID"], keep="first")
        .set_index("Contact ID")["Contact Type"]
    )
    contacts["Type of Contact"] = contacts["Contact ID"].map(contact_type_lookup).fillna("")

    investor_group_lookup = (
        nc.loc[nc["Investor Group"] != "", ["Contact ID", "Investor Group"]]
        .drop_duplicates(subset=["Contact ID"], keep="first")
        .set_index("Contact ID")["Investor Group"]
    )
    contacts["Investor Group"] = contacts["Contact ID"].map(investor_group_lookup)

    contact_type = contacts["Type of Contact"].astype(str).str.strip().str.lower()

    hnw_contacts = contacts.loc[
        (~contact_type.isin(["accumulator", "institutional"]))
        & (contacts["Investor Group"].isna())
        & (contacts["Count of Positions"] > 0)
    ].copy()

    # Quarterly data
    q1_2026_inv = hnw_contacts.loc[(hnw_contacts["Contact Created At Date"] > "2026-01-01") & (hnw_contacts["Count of Positions"] > 0)]
    q1_2026_inv_count    = q1_2026_inv['Full Contact Name'].nunique()
    q1_2026_inv_total_c  = q1_2026_inv['Total Commitment'].sum()
    q1_2026_inv_mean_c   = q1_2026_inv['Total Commitment'].mean()
    q1_2026_inv_median_c = q1_2026_inv['Total Commitment'].median()

    q1_2025_inv = hnw_contacts.loc[(hnw_contacts["Contact Created At Date"] > "2025-01-01") & (hnw_contacts["Contact Created At Date"] <= "2025-03-31") & (hnw_contacts['Count of Positions'] > 0)]
    q1_2025_inv_count = q1_2025_inv['Full Contact Name'].nunique()
    q1_2025_inv_total_c = q1_2025_inv['Total Commitment'].sum()
    q1_2025_inv_mean_c = q1_2025_inv['Total Commitment'].mean()
    q1_2025_inv_median_c = q1_2025_inv['Total Commitment'].median()

    q4_2025_inv = hnw_contacts.loc[(hnw_contacts["Contact Created At Date"] > "2025-10-01") & (hnw_contacts["Contact Created At Date"] <= "2025-12-31") & (hnw_contacts["Count of Positions"] > 0)]
    q4_2025_inv_count = q4_2025_inv['Full Contact Name'].nunique()
    q4_2025_inv_total_c = q4_2025_inv['Total Commitment'].sum()
    q4_2025_inv_mean_c = q4_2025_inv['Total Commitment'].mean()
    q4_2025_inv_median_c = q4_2025_inv['Total Commitment'].median()

    ytd_2025_inv = hnw_contacts.loc[(hnw_contacts["Contact Created At Date"] > "2025-01-01") & (hnw_contacts["Contact Created At Date"] <= "2025-12-31") & (hnw_contacts["Count of Positions"] > 0)]
    ytd_2025_inv_count = ytd_2025_inv['Full Contact Name'].nunique()
    ytd_2025_inv_total_c = ytd_2025_inv['Total Commitment'].sum()
    ytd_2025_inv_mean_c = ytd_2025_inv['Total Commitment'].mean()
    ytd_2025_inv_median_c = ytd_2025_inv['Total Commitment'].median()


    return {
        "q1_2026_inv": q1_2026_inv,
        "q1_2026_inv_count" : q1_2026_inv_count,   
        "q1_2026_inv_total_c" : q1_2026_inv_total_c,
        "q1_2026_inv_mean_c" : q1_2026_inv_mean_c,
        "q1_2026_inv_median_c": q1_2026_inv_median_c,
        "q1_2025_inv": q1_2025_inv,
        "q1_2025_inv_count": q1_2025_inv_count,
        "q1_2025_inv_total_c": q1_2025_inv_total_c,
        "q1_2025_inv_mean_c": q1_2025_inv_mean_c,
        "q1_2025_inv_median_c": q1_2025_inv_median_c,
        "q4_2025_inv": q4_2025_inv,
        "q4_2025_inv_count": q4_2025_inv_count,
        "q4_2025_inv_total_c": q4_2025_inv_total_c,
        "q4_2025_inv_mean_c": q4_2025_inv_mean_c,
        "q4_2025_inv_median_c": q4_2025_inv_median_c,
        "ytd_2025_inv": ytd_2025_inv,
        "ytd_2025_inv_count": ytd_2025_inv_count,
        "ytd_2025_inv_total_c": ytd_2025_inv_total_c,
        "ytd_2025_inv_mean_c": ytd_2025_inv_mean_c,
        "ytd_2025_inv_median_c": ytd_2025_inv_median_c,
    }


def build_metrics(base_path="data"):
    accounts = pd.read_csv(os.path.join(base_path, "Accounts.csv"), low_memory=False)
    new_contacts = pd.read_csv(os.path.join(base_path, "js_contacts.csv"))
    contacts = pd.read_csv(os.path.join(base_path, "Contact Export.csv"))

    quarterly_data = create_quarterly_reports(contacts, new_contacts)

    new_contacts["Full Name"] = (
        new_contacts["First name"].astype(str).str.strip() + " " + new_contacts["Last name"].astype(str).str.strip()
    )
    new_contacts["Committed amount"] = pd.to_numeric(new_contacts["Committed amount"], errors="coerce")
    new_contacts["Investment count"] = pd.to_numeric(new_contacts["Investment count"], errors="coerce")

    investors_json, investors_df = build_investor_outputs(accounts)

    accounts.columns = accounts.columns.str.strip()
    acct_type = accounts["Account Type"].astype(str).str.strip()

    hnw_accounts = accounts.loc[
        accounts["Account Type"].astype(str).str.strip().str.lower() == "hnw"
    ].copy()

    acc_for_tiers = clean_accounts(hnw_accounts)
    account_metrics = compute_account_metrics(hnw_accounts)

    contact_type = new_contacts["Contact Type"].astype(str).str.strip().str.lower()
    
    hnw_contacts = new_contacts.loc[
        (~contact_type.isin(["accumulator", "institutional"]))
        & (new_contacts["Investor Group"].isna())
        & (new_contacts["Investment count"] > 0)
    ].copy()
    hnw_contacts_nonzero_commitment = hnw_contacts.loc[
        hnw_contacts["Committed amount"].fillna(0) > 0
    ].copy()


    hnw_name_set = set(
        hnw_contacts["Full Name"]
        .dropna()
        .astype(str)
        .str.strip()
        .str.lower()
    )


    investors_df_hnw = investors_df.loc[
        investors_df["name"].astype(str).str.strip().str.lower().isin(hnw_name_set)
    ].copy()
    

    tot_hnw_investors = hnw_contacts["Contact ID"].nunique()
    mean_hnw_commitment = hnw_contacts_nonzero_commitment["Committed amount"].mean()
    median_hnw_commitment = hnw_contacts_nonzero_commitment["Committed amount"].median()

    tier_1_investors = investors_df_hnw.loc[
        investors_df_hnw["total_commitment"] >= 1_000_001,
        ["name", "total_positions", "total_commitment"],
    ].rename(
        columns={
            "name": "Full Contact Name",
            "total_positions": "Count of Positions",
            "total_commitment": "Total Commitment",
        }
    )

    tier_1_investors_count = tier_1_investors["Full Contact Name"].nunique()
    tier_1_investors = tier_1_investors.sort_values("Total Commitment", ascending=False).head(20)

    tier_1_acc = acc_for_tiers.loc[
        acc_for_tiers["Total commitment"] >= 1_000_001,
        ["Account ID", "Legal name", "Contacts", "Total commitment"],
    ].rename(
        columns={
            "Total commitment": "Total Commitment",
        }
    )
    tier_1_acc_count = tier_1_acc['Account ID'].nunique()
    tier_1_acc = tier_1_acc.sort_values("Total Commitment", ascending=False).head(20)


    tier_2_investors = investors_df_hnw.loc[
        (investors_df_hnw["total_commitment"] >= 400_001)
        & (investors_df_hnw["total_commitment"] <= 1_000_000)
        & (investors_df_hnw["total_positions"] > 2),
        ["name", "total_positions", "total_commitment"],
    ].rename(
        columns={
            "name": "Full Contact Name",
            "total_positions": "Count of Positions",
            "total_commitment": "Total Commitment",
        }
    )
    tier_2_investors_count = tier_2_investors["Full Contact Name"].nunique()
    tier_2_investors = tier_2_investors.sort_values("Total Commitment", ascending=False).head(20)


    tier_2_acc = acc_for_tiers.loc[
        (acc_for_tiers["Total commitment"] >= 400_001)
        & (acc_for_tiers["Total commitment"] <= 1_000_000)
        & (acc_for_tiers["# of Positions"] > 2),
        ["Account ID", "Legal name", "Contacts", "Total commitment"],
    ].rename(
        columns={
            "Total commitment": "Total Commitment",
        }
    )
    tier_2_acc_count = tier_2_acc['Account ID'].nunique()
    tier_2_acc = tier_2_acc.sort_values("Total Commitment", ascending=False).head(20)

    tier_3_investors = investors_df_hnw.loc[
        (investors_df_hnw["total_commitment"] <= 400_000)
        & (investors_df_hnw["total_positions"] > 2),
        ["name", "total_positions", "total_commitment"],
    ].rename(
        columns={
            "name": "Full Contact Name",
            "total_positions": "Count of Positions",
            "total_commitment": "Total Commitment",
        }
    )
    tier_3_investors_count = tier_3_investors["Full Contact Name"].nunique()
    tier_3_investors = tier_3_investors.sort_values("Total Commitment", ascending=False).head(20)

    tier_3_acc = acc_for_tiers.loc[
        (acc_for_tiers["Total commitment"] <= 400_000)
        & (acc_for_tiers["# of Positions"] > 2),
        ["Account ID", "Legal name", "Contacts", "Total commitment"],
    ].rename(
        columns={
            "Total commitment": "Total Commitment",
        }
    )
    tier_3_acc_count = tier_3_acc['Account ID'].nunique()
    tier_3_acc = tier_3_acc.sort_values("Total Commitment", ascending=False).head(20)

    return {
        "tot_hnw_investors": tot_hnw_investors,
        "mean_hnw_commitment": mean_hnw_commitment,
        "median_hnw_commitment": median_hnw_commitment,
        "total_accounts": account_metrics["total_accounts"],
        #"total_active_accounts": account_metrics["total_active_accounts"],
        #"total_dormant_accounts": account_metrics["total_dormant_accounts"],
        "total_active_commitment": account_metrics["total_active_commitment"],
        "total_total_commitment": account_metrics["total_total_commitment"],
        "average_active_commitment": account_metrics["average_active_commitment"],
        "median_active_commitment": account_metrics["median_active_commitment"],
        "average_total_commitment": account_metrics["average_total_commitment"],
        "median_total_commitment": account_metrics["median_total_commitment"],
        "average_active_commitment_per_position": account_metrics["average_active_commitment_per_position"],
        "average_total_commitment_per_position": account_metrics["average_total_commitment_per_position"],
        "tier_1_investors": tier_1_investors,
        "tier_1_investors_count": tier_1_investors_count,
        "tier_2_investors": tier_2_investors,
        "tier_2_investors_count": tier_2_investors_count,
        "tier_3_investors": tier_3_investors,
        "tier_3_investors_count": tier_3_investors_count,
        "tier_1_acc": tier_1_acc,
        "tier_1_acc_count": tier_1_acc_count,
        "tier_2_acc": tier_2_acc,
        "tier_2_acc_count": tier_2_acc_count,
        "tier_3_acc": tier_3_acc,
        "tier_3_acc_count": tier_3_acc_count,
        "investors_json": investors_json,
        "q1_2026_inv": quarterly_data['q1_2026_inv'],
        "q1_2026_inv_count" : quarterly_data['q1_2026_inv_count'],   
        "q1_2026_inv_total_c" : quarterly_data['q1_2026_inv_total_c'],
        "q1_2026_inv_mean_c" : quarterly_data['q1_2026_inv_mean_c'],
        "q1_2026_inv_median_c": quarterly_data['q1_2026_inv_median_c'],
        "q1_2025_inv": quarterly_data['q1_2025_inv'],
        "q1_2025_inv_count": quarterly_data['q1_2025_inv_count'],
        "q1_2025_inv_total_c": quarterly_data['q1_2025_inv_total_c'],
        "q1_2025_inv_mean_c": quarterly_data['q1_2025_inv_mean_c'],
        "q1_2025_inv_median_c": quarterly_data['q1_2025_inv_median_c'],
        "q4_2025_inv": quarterly_data['q4_2025_inv'],
        "q4_2025_inv_count": quarterly_data['q4_2025_inv_count'],
        "q4_2025_inv_total_c": quarterly_data['q4_2025_inv_total_c'],
        "q4_2025_inv_mean_c": quarterly_data['q4_2025_inv_mean_c'],
        "q4_2025_inv_median_c": quarterly_data['q4_2025_inv_median_c'],
        "ytd_2025_inv": quarterly_data['ytd_2025_inv'],
        "ytd_2025_inv_count": quarterly_data['ytd_2025_inv_count'],
        "ytd_2025_inv_total_c": quarterly_data['ytd_2025_inv_total_c'],
        "ytd_2025_inv_mean_c": quarterly_data['ytd_2025_inv_mean_c'],
        "ytd_2025_inv_median_c": quarterly_data['ytd_2025_inv_median_c'],
    }


def create_pdf(output_path="outputs/HNW_Report.pdf", base_path="data"):
    data = build_metrics(base_path=base_path)
    pdf = HNWReport(orientation="P", unit="mm", format="A4")
    pdf.add_page()

    add_section_header(pdf, "HNW SUMMARY")
    add_key_value_table(
        pdf,
        [
            ("Total HNW Investors (with Position)", f"{data['tot_hnw_investors']:,}"),
            ("Total Active Accounts (Entities)", f"{data['total_accounts']:,}"),
            #("Total Active Accounts", f"{data['total_active_accounts']:,}"),
            #("Total Dormant Accounts", f"{data['total_dormant_accounts']:,}"),
            ("Active Commitment to-date (Accounts)", format_currency(data['total_active_commitment'])),
            ("Total Commitment to-date (Accounts)", format_currency(data['total_total_commitment'])),
            ("Average Active Commitment (From Accounts)", format_currency(data["average_active_commitment"])),
            ("Average Active Commitment Per Position (From Accounts)", format_currency(data["average_active_commitment_per_position"])),
            #("Median Active Commitment (From Accounts)", format_currency(data["median_active_commitment"])),
            ("Average Total Commitment (From Accounts)", format_currency(data["average_total_commitment"])),
            ("Average Total Commitment Per Position (From Accounts)", format_currency(data["average_total_commitment_per_position"])),
            #("Median Total Commitment (From Accounts)", format_currency(data["median_total_commitment"])),
            ("Average Commitment (From Contacts)", format_currency(data["mean_hnw_commitment"])),
            #("Median Commitment", format_currency(data["median_hnw_commitment"])),
            ("Tier 1 Investors ($1M+)", f"{data['tier_1_investors_count']:,}"),
            ("Tier 2 Investors ($400K-$1M)", f"{data['tier_2_investors_count']:,}"),
            ("Tier 3 Investors (<$400K)", f"{data['tier_3_investors_count']:,}"),
        ],
        extra_spaces = {1, 3, 5, 7, 8}
    )

    pdf.add_page()
    add_section_header(pdf, "QUARTERLY PERFORMANCE")
    add_side_by_side_sections(
        pdf,
        "Q1 2026 (YTD)",
        [
            ("New Investor Count", f"{data['q1_2026_inv_count']:,}"),
            ("Total Commitment", format_currency(data["q1_2026_inv_total_c"])),
            ("Average Commitment", format_currency(data["q1_2026_inv_mean_c"])),
            #("Median Commitment", format_currency(data["q1_2026_inv_median_c"])),
        ],
        "Q1 2025",
        [
            ("New Investor Count", f"{data['q1_2025_inv_count']:,}"),
            ("Total Commitment", format_currency(data["q1_2025_inv_total_c"])),
            ("Average Commitment", format_currency(data["q1_2025_inv_mean_c"])),
            #("Median Commitment", format_currency(data["q1_2025_inv_median_c"])),
        ],
    )
    add_side_by_side_sections(
        pdf,
        "Q4 2025",
        [
            ("New Investor Count", f"{data['q4_2025_inv_count']:,}"),
            ("Total Commitment", format_currency(data["q4_2025_inv_total_c"])),
            ("Average Commitment", format_currency(data["q4_2025_inv_mean_c"])),
            #("Median Commitment", format_currency(data["q4_2025_inv_median_c"])),
        ],
        "2025 FULL YEAR",
        [
            ("New Investor Count", f"{data['ytd_2025_inv_count']:,}"),
            ("Total Commitment", format_currency(data["ytd_2025_inv_total_c"])),
            ("Average Commitment", format_currency(data["ytd_2025_inv_mean_c"])),
            #("Median Commitment", format_currency(data["ytd_2025_inv_median_c"])),
        ],
    )
    pdf.ln(3)
    add_comparison_charts(
        pdf,
        "Q1 2026 vs Q1 2025 Comparison",
        [
            ("New Investors", data["q1_2026_inv_count"], data["q1_2025_inv_count"], "Q1 2026", "Q1 2025"),
            ("Total Commitment", data["q1_2026_inv_total_c"], data["q1_2025_inv_total_c"], "Q1 2026", "Q1 2025"),
            ("Average Commitment", data["q1_2026_inv_mean_c"], data["q1_2025_inv_mean_c"], "Q1 2026", "Q1 2025"),
            #("Median Commitment", data["q1_2026_inv_median_c"], data["q1_2025_inv_median_c"], "Q1 2026", "Q1 2025"),
        ]
    )

    pdf.add_page()
    add_section_header(pdf, "TIER 1 (TOP 20)")
    tier1_rows = [
        [str(r["Full Contact Name"]), f"{int(r['Count of Positions']):,}", format_currency(r["Total Commitment"])]
        for _, r in data["tier_1_investors"].iterrows()
    ]
    add_table(pdf, ["Investor", "Positions", "Total Commitment"], tier1_rows, [95, 30, 65], wrap_cols=[0])

    pdf.add_page()
    add_section_header(pdf, "TIER 2 (TOP 20)")
    tier2_rows = [
        [str(r["Full Contact Name"]), f"{int(r['Count of Positions']):,}", format_currency(r["Total Commitment"])]
        for _, r in data["tier_2_investors"].iterrows()
    ]
    add_table(pdf, ["Investor", "Positions", "Total Commitment"], tier2_rows, [95, 30, 65], wrap_cols=[0])

    pdf.add_page()
    add_section_header(pdf, "TIER 3 (TOP 20)")
    tier3_rows = [
        [str(r["Full Contact Name"]), f"{int(r['Count of Positions']):,}", format_currency(r["Total Commitment"])]
        for _, r in data["tier_3_investors"].iterrows()
    ]
    add_table(pdf, ["Investor", "Positions", "Total Commitment"], tier3_rows, [95, 30, 65], wrap_cols=[0])


    pdf.add_page()
    add_section_header(pdf, "HNW ACCOUNT TIERS SUMMARY")
    add_key_value_table(
        pdf,
        [
            ("Tier 1 Accounts ($1M+)", f"{data['tier_1_acc_count']:,}"),
            ("Tier 2 Accounts ($400K-$1M)", f"{data['tier_2_acc_count']:,}"),
            ("Tier 3 Accounts (<$400K)", f"{data['tier_3_acc_count']:,}"),
        ],
        extra_spaces= {}
    )

    pdf.ln(4)
    add_section_header(pdf, "TIER 1 ACCOUNTS (TOP 20)")
    acc_tier1_rows = [
        [str(r["Legal name"]), str(r["Contacts"]), format_currency(r["Total Commitment"])]
        for _, r in data["tier_1_acc"].iterrows()
    ]
    add_table(pdf, ["Account", "Contacts", "Total Commitment"], acc_tier1_rows, [62, 88, 40], wrap_cols=[0, 1], color_count=2)

    pdf.add_page()
    add_section_header(pdf, "TIER 2 ACCOUNTS (TOP 20)")
    acc_tier2_rows = [
        [str(r["Legal name"]), str(r["Contacts"]), format_currency(r["Total Commitment"])]
        for _, r in data["tier_2_acc"].iterrows()
    ]
    add_table(pdf, ["Account", "Contacts", "Total Commitment"], acc_tier2_rows, [62, 88, 40], wrap_cols=[0, 1], color_count=2)

    pdf.add_page()
    add_section_header(pdf, "TIER 3 ACCOUNTS (TOP 20)")
    acc_tier3_rows = [
        [str(r["Legal name"]), str(r["Contacts"]), format_currency(r["Total Commitment"])]
        for _, r in data["tier_3_acc"].iterrows()
    ]
    add_table(pdf, ["Account", "Contacts", "Total Commitment"], acc_tier3_rows, [62, 88, 40], wrap_cols=[0, 1], color_count=2)

    pdf.ln(8)
    add_section_header(pdf, "METRIC DEFINITIONS")
    pdf.set_font("helvetica", "", 8)
    pdf.multi_cell(0, 5, "- Main investor is the first listed contact on each account; account values are rolled up to that person.")
    pdf.multi_cell(0, 5, "- Institutional names are excluded from HNW metrics and tier tables.")
    pdf.multi_cell(0, 5, "- Total Accounts includes accounts with active and/or closed investment balances.")
    pdf.multi_cell(0, 5, "- Total Dormant Accounts includes closed accounts with no active balance and close date older than 36 months.")
    pdf.multi_cell(0, 5, "- Average Active Commitment is the average 'Active commitment' across active accounts.")
    pdf.multi_cell(0, 5, "- Tier 1: commitment >= $1,000,001.")
    pdf.multi_cell(0, 5, "- Tier 2: commitment $400,001 to $1,000,000 and positions > 2.")
    pdf.multi_cell(0, 5, "- Tier 3: commitment <= $400,000 and positions > 2.")

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    pdf.output(output_path)


def _parse_args():
    parser = argparse.ArgumentParser(description="Generate the HNW report.")
    parser.add_argument("--base-path", default="data")
    parser.add_argument("--output-path", default="outputs/HNW_Report.pdf")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    create_pdf(output_path=args.output_path, base_path=args.base_path)
