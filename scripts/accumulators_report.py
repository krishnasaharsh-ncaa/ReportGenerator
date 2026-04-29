import argparse
import os
import re
from datetime import datetime

import pandas as pd
from fpdf import FPDF
from pdf_utils import add_bullet_notes, add_key_value_table, add_section_header, add_subsection_header, add_side_by_side_metrics, add_side_by_side_sections, add_three_sections, add_table


class AccumulatorReport(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 16)
        self.set_text_color(30, 30, 30)
        self.cell(0, 10, "ACCUMULATOR INVESTOR REPORT", ln=True, align="L")
        self.set_font("helvetica", "", 10)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, datetime.now().strftime("%B %d, %Y"), ln=True, align="L")
        self.ln(5)

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
    cleaned = [strip_parens(x).strip() for x in raw]
    seen = set()
    out = []
    for name in cleaned:
        if name and name not in seen:
            out.append(name)
            seen.add(name)
    return out


def build_company_account_breakdown(acc_by_company_agg, accounts_df):
    """Classify accounts as individual vs combination per company."""
    accts = accounts_df.copy()
    accts.columns = accts.columns.str.strip()
    accts = accts[~accts["Legal name"].astype(str).str.startswith("Transfer")]
    accts["Total commitment"] = pd.to_numeric(
        accts["Total commitment"].astype(str).str.replace(r"[\$,]", "", regex=True),
        errors="coerce",
    ).fillna(0)
    accts["# of Positions"] = pd.to_numeric(
        accts["# of Positions"], errors="coerce"
    ).fillna(0).astype(int)
    accts["contacts_set"] = accts["Contacts"].apply(
        lambda cell: {name.lower() for name in parse_contacts(cell)}
    )

    rows = []
    for _, row in acc_by_company_agg.iterrows():
        company = str(row["Company"]).strip()
        investors_str = str(row["Investors"]) if pd.notna(row["Investors"]) else ""
        investor_names = {n.strip().lower() for n in investors_str.split(",") if n.strip()}

        ind_rows = []
        combo_rows = []
        for idx, acct in accts.iterrows():
            overlap = investor_names & acct["contacts_set"]
            if len(overlap) == 1:
                ind_rows.append(idx)
            elif len(overlap) >= 2:
                combo_rows.append(idx)

        ind_df = accts.loc[ind_rows]
        combo_df = accts.loc[combo_rows]

        rows.append({
            "Company": company,
            "Investors": investors_str,
            "IP": int(ind_df["# of Positions"].sum()),
            "CP": int(combo_df["# of Positions"].sum()),
            "ITC": ind_df["Total commitment"].sum(),
            "CTC": combo_df["Total commitment"].sum(),
        })

    return pd.DataFrame(rows)


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
    total_accounts_with_no_commitment = scoped.loc[(scoped['Active commitment'].isna()) | (scoped['Active commitment'] == 0), 'Account ID'].nunique()
    #total_active_accounts = account_series[active_mask].nunique()
    #total_dormant_accounts = account_series[dormant_mask].nunique()
    average_active_commitment = scoped.loc[active_mask, "Active commitment"].mean()
    average_total_commitment = scoped.loc[total_mask, "Total commitment"].mean()
    total_commitment_per_position = scoped.loc[total_mask, "Total commitment"].sum() / pd.to_numeric(scoped.loc[total_mask, "# of Positions"], errors="coerce").sum()
    sum_active_commitment = scoped.loc[active_mask, "Active commitment"].sum()
    sum_total_commitment = scoped.loc[total_mask, "Total commitment"].sum()

    return {
        "total_accounts": total_accounts,
        "total_accounts_with_no_commitment": total_accounts_with_no_commitment,
        #"total_active_accounts": total_active_accounts,
        #"total_dormant_accounts": total_dormant_accounts,
        "average_active_commitment": average_active_commitment,
        "average_total_commitment": average_total_commitment,
        "total_commitment_per_position": total_commitment_per_position,
        "sum_active_commitment": sum_active_commitment,
        "sum_total_commitment": sum_total_commitment,
    }


def build_metrics(base_path="data"):
    new_contacts = pd.read_csv(os.path.join(base_path, "js_contacts.csv"))
    accounts = pd.read_csv(os.path.join(base_path, "Accounts.csv"), low_memory=False)
    new_contacts.columns = new_contacts.columns.str.strip()
    accounts.columns = accounts.columns.str.strip()
    new_contacts["Full Name"] = (
        new_contacts["First name"].astype(str).str.strip() + " " + new_contacts["Last name"].astype(str).str.strip()
    )
    new_contacts["Committed amount"] = pd.to_numeric(new_contacts["Committed amount"], errors="coerce")
    new_contacts["Investment count"] = pd.to_numeric(new_contacts.get("Investment count"), errors="coerce")


    # inst_mask = (
    #     new_contacts["Institutional Investor"].astype(str).str.strip().str.lower() == "yes"
    # )

    acc_mask = (
        new_contacts['Contact Type'].astype(str).str.strip().str.lower() == 'accumulator'
    )

    acc_contacts = new_contacts.loc[acc_mask].copy()
    acc_contacts = acc_contacts.dropna(subset= ['First name'])

    #ACCUMULATORS BY COMPANY
    acc_comp = acc_contacts.loc[new_contacts['Company'].notna()]
    cols = ['Contact ID','First name', 'Full Name', 'Company', 'Investment count', 'Committed amount']
    not_req_cols = [c for c in acc_comp.columns if c not in cols]

    acc_comp = acc_comp.drop(columns= not_req_cols)
    #acc_comp = acc_comp.dropna(subset= ['First name'])
    acc_by_company = acc_comp.sort_values('Company')
    companies = acc_contacts['Company'].unique().tolist()
    acc_by_company_agg = (
        acc_comp.groupby("Company", as_index=False)
        .agg(
            Contact_Count=("Contact ID", "nunique"),
            Investors=("Full Name", lambda s: ", ".join(
                s.dropna().astype(str).str.strip().loc[lambda x: x.ne("")].unique()
            )),
            Investment_Count=("Investment count", "sum"),
            Total_Commitment=("Committed amount", "sum"),
        )
        .sort_values("Total_Commitment", ascending=False)
        .head(20)
    )


    accumulator_investor_count = acc_contacts["Contact ID"].nunique()
    acc_inv_no_commitment = acc_contacts.loc[
        (acc_contacts["Committed amount"].isna()) | (acc_contacts["Committed amount"] == 0),
        "Contact ID",
    ].nunique()
    avg_investment_acc = acc_contacts["Committed amount"].mean()
    median_investment_acc = acc_contacts["Committed amount"].median()
    total_commitment_acc = acc_contacts["Committed amount"].sum()

    top_accumulator = (
        acc_contacts.groupby("Full Name", as_index=False)
        .agg(
            Contact_Count=("Contact ID", "nunique"),
            Investment_Count=("Investment count", "sum"),
            Total_Commitment=("Committed amount", "sum"),
        )
        .sort_values("Total_Commitment", ascending=False)
        .head(20)
    )

    acc_mask_acc = (
    accounts['Account Type'].astype(str).str.strip().str.lower() == 'accumulator'
)
    inst_accounts = accounts.loc[acc_mask_acc].copy()

    top_accumulator_acc = (
    inst_accounts.groupby("Legal name", as_index=False)
    .agg(
        Contacts = ("Contacts", "first"),
        Total_Commitment=("Active commitment", "sum"),
    )
    .sort_values("Total_Commitment", ascending=False)
    .head(20)
)

    # inst_accounts = accounts.loc[
    #     accounts["Institutional Investor"].astype(str).str.strip().str.lower() == "yes"
    # ].copy()

    inst_accounts = accounts.loc[
        accounts["Account Type"].astype(str).str.strip().str.lower() == "accumulator"
    ].copy()

    account_metrics = compute_account_metrics(inst_accounts)

    company_breakdown_all = build_company_account_breakdown(acc_by_company_agg, accounts)

    return {
        "accumulator_investor_count": accumulator_investor_count,
        "acc_inv_no_commitment": acc_inv_no_commitment,
        "avg_investment_acc": avg_investment_acc,
        "median_investment_acc": median_investment_acc,
        "total_commitment_acc": total_commitment_acc,
        "total_accounts": account_metrics["total_accounts"],
        "total_accounts_with_no_commitment": account_metrics["total_accounts_with_no_commitment"],
        #"total_active_accounts": account_metrics["total_active_accounts"],
        #"total_dormant_accounts": account_metrics["total_dormant_accounts"],
        "average_active_commitment": account_metrics["average_active_commitment"],
        "average_total_commitment": account_metrics["average_total_commitment"],
        "total_commitment_per_position": account_metrics["total_commitment_per_position"],
        "sum_active_commitment": account_metrics["sum_active_commitment"],
        "sum_total_commitment": account_metrics["sum_total_commitment"],
        "top_accumulator": top_accumulator,
        "top_accumulator_acc": top_accumulator_acc,
        "companies": companies,
        "acc_by_company": acc_by_company,
        "acc_by_company_agg": acc_by_company_agg,
        "company_breakdown_all": company_breakdown_all,
    }


def create_pdf(output_path="outputs/Accumulator_Report.pdf", base_path="data"):
    data = build_metrics(base_path=base_path)
    pdf = AccumulatorReport(orientation="P", unit="mm", format="A4")
    pdf.add_page()

    add_section_header(pdf, "ACCUMULATOR SUMMARY")
    add_subsection_header(pdf, "INVESTOR OVERVIEW")

    add_side_by_side_metrics(pdf, 
        [
            ("Total Accumulator Investors", f"{data['accumulator_investor_count']:,}"),
            ("Total Investors with no Commitment", f"{data['acc_inv_no_commitment']:,}"),
        ],
        [
            ("Total Accounts", f"{data['total_accounts']:,}"),
            ("Total Accounts with no Active Commitment", f"{data['total_accounts_with_no_commitment']:,}"),
        ]
    )
    add_subsection_header(pdf, "INVESTMENT PORTFOLIO")
    add_side_by_side_metrics(pdf,
        [
            ("Average Active Commitment (Accounts)", format_currency(data["average_active_commitment"])),
            ("Average Total Commitment (Accounts)", format_currency(data["average_total_commitment"])),
            ("Sum Active Commitment (Accounts)", format_currency(data["sum_active_commitment"])),
            ("Sum Total Commitment (Accounts)", format_currency(data["sum_total_commitment"])),
        ],
        [
            ("Average Commitment per Position (Accounts)", format_currency(data["total_commitment_per_position"])),
            ("Total Accumulator Commitment (Contacts)", format_currency(data["total_commitment_acc"])),
            ("Average Investment (Accumulator)", format_currency(data["avg_investment_acc"])),
            ("Median Investment (Accumulator)", format_currency(data["median_investment_acc"])),
        ]
    )
    company_overview = (
        data["acc_by_company_agg"]
        .head(3)
        .merge(data["company_breakdown_all"][["Company", "IP", "CP", "ITC", "CTC"]], on="Company", how="left")
        .to_dict("records")
    )

    pdf.ln(10)
    add_section_header(pdf, "ACCUMULATORS BY COMPANY SUMMARY")
    rows = [
        [
            str(row['Company']),
            int(row['Contact_Count']),
            str(row['Investors']),
            f"{int((row.get('IP') or 0) + (row.get('CP') or 0)):,}",
            format_currency((row.get('ITC') or 0) + (row.get('CTC') or 0)),
        ]
        for row in company_overview
    ]
    add_table(pdf, ["Company", "Contact Count", "Investors", "Investment Count", "Total Commitment"], rows, [30, 30, 60, 35, 35], wrap_cols=[2], color_count= 3)

    pdf.ln(15)
    add_section_header(pdf, "COMPANY ACCOUNT BREAKDOWN (ALL ACCOUNTS)")
    breakdown_all = data["company_breakdown_all"]
    breakdown_all_rows = [
        [
            str(row["Company"]),
            str(row["Investors"]),
            f"{row['IP']:,}",
            f"{row['CP']:,}",
            format_currency(row["ITC"]),
            format_currency(row["CTC"]),
        ]
        for _, row in breakdown_all.iterrows()
    ]
    add_table(
        pdf,
        ["Company", "Investors", "IP", "CP", "ITC", "CTC"],
        breakdown_all_rows,
        [30, 60, 18, 18, 30, 30],
        wrap_cols=[1],
        color_count=2,
    )
    pdf.set_font("helvetica", "I", 7)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 4, "IP = Individual Positions  |  CP = Combined Positions  |  ITC = Individual Total Commitment  |  CTC = Combined Total Commitment", ln=True)
    pdf.set_text_color(0, 0, 0)

    pdf.add_page()
    add_section_header(pdf, "TOP 20 ACCUMULATOR INVESTORS")
    rows = [
        [
            str(row["Full Name"]),
            f"{int(row['Investment_Count']) if pd.notna(row['Investment_Count']) else 0:,}",
            format_currency(row["Total_Commitment"]),
        ]
        for _, row in data["top_accumulator"].iterrows()
    ]
    add_table(pdf, ["Investor", "Investment Count", "Total Commitment"], rows, [90, 40, 60])

    
    pdf.ln(10)
    add_section_header(pdf, "TOP 20 ACCUMULATOR ACCOUNTS")
    rows = [
        [
            str(row["Legal name"]),
            str(row["Contacts"]),
            format_currency(row["Total_Commitment"]),
        ]
        for _, row in data["top_accumulator_acc"].iterrows()
    ]
    add_table(
        pdf,
        ["Account Name", "Investors", "Total Commitment"],
        rows,
        [70, 90, 30],
        wrap_cols=[1],
        color_count=2,
    )
    
    pdf.ln(8)
    add_section_header(pdf, "METRIC DEFINITIONS")
    add_bullet_notes(
        pdf,
        [
            "- Accumulator investors are contacts and accounts where 'Contact Type' and 'Account Type' are marked 'Accumulator' in js_contacts.csv and Accounts.csv respectively.",
            "- Mean and median investment are calculated from 'Committed amount' for those accumulator contacts.",
            "- Top 20 table ranks accumulators investors by summed committed amount.",
            "- Average Active Commitment is the average 'Active commitment' across active accounts.",
        ],
    )

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    pdf.output(output_path)


def _parse_args():
    parser = argparse.ArgumentParser(description="Generate the accumulators report.")
    parser.add_argument("--base-path", default="data")
    parser.add_argument("--output-path", default="outputs/Accumulator_Report.pdf")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    create_pdf(output_path=args.output_path, base_path=args.base_path)
