import argparse
import os
from datetime import datetime

import pandas as pd
from fpdf import FPDF
from pdf_utils import add_key_value_table, add_section_header, add_side_by_side_sections, add_table


class InstitutionalReport(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 16)
        self.set_text_color(30, 30, 30)
        self.cell(0, 10, "INSTITUTIONAL INVESTOR REPORT", ln=True, align="L")
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

def safe_pdf_text(value):
    """Ensure text is compatible with FPDF latin-1 encoding."""
    if pd.isna(value):
        return "N/A"
    text = str(value).replace("\u200b", "")
    cleaned = text.encode("latin-1", "ignore").decode("latin-1").strip()
    return cleaned if cleaned else "N/A"


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
    sum_active_commitment = scoped.loc[active_mask, "Active commitment"].sum()
    sum_total_commitment = scoped.loc[total_mask, "Total commitment"].sum()

    return {
        "total_accounts": total_accounts,
        "total_accounts_with_no_commitment": total_accounts_with_no_commitment,
        #"total_active_accounts": total_active_accounts,
        #"total_dormant_accounts": total_dormant_accounts,
        "average_active_commitment": average_active_commitment,
        "average_total_commitment": average_total_commitment,
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

    inst_mask = (
        new_contacts['Contact Type'].astype(str).str.strip().str.lower() == 'institutional'
    )

    inst_contacts = new_contacts.loc[inst_mask].copy()

    institutional_investor_count = inst_contacts["Contact ID"].nunique()
    inst_inv_no_commitment = inst_contacts.loc[
        (inst_contacts["Committed amount"].isna()) | (inst_contacts["Committed amount"] == 0),
        "Contact ID",
    ].nunique()
    avg_investment_institutional = inst_contacts["Committed amount"].mean()
    median_investment_institutional = inst_contacts["Committed amount"].median()
    total_commitment_institutional = inst_contacts["Committed amount"].sum()

    top_institutional = (
        inst_contacts.groupby("Full Name", as_index=False)
        .agg(
            Contact_Count=("Contact ID", "nunique"),
            Investment_Count=("Investment count", "sum"),
            Total_Commitment=("Committed amount", "sum"),
        )
        .sort_values("Total_Commitment", ascending=False)
        .head(20)
    )

    # inst_accounts = accounts.loc[
    #     accounts["Institutional Investor"].astype(str).str.strip().str.lower() == "yes"
    # ].copy()

    inst_accounts = accounts.loc[
        accounts["Account Type"].astype(str).str.strip().str.lower() == "institutional"
    ].copy()

    acc_mask = (
    accounts['Account Type'].astype(str).str.strip().str.lower() == 'institutional'
)
    inst_accounts = accounts.loc[acc_mask].copy()

    top_institutional_acc = (
    inst_accounts.groupby("Legal name", as_index=False)
    .agg(
        Contacts = ("Contacts", "first"),
        Total_Commitment=("Total commitment", "sum"),
    )
    .sort_values("Total_Commitment", ascending=False)
    .head(20)
)


    account_metrics = compute_account_metrics(inst_accounts)

    return {
        "institutional_investor_count": institutional_investor_count,
        "inst_inv_no_commitment": inst_inv_no_commitment,
        "avg_investment_institutional": avg_investment_institutional,
        "median_investment_institutional": median_investment_institutional,
        "total_commitment_institutional": total_commitment_institutional,
        "total_accounts": account_metrics["total_accounts"],
        "total_accounts_with_no_commitment": account_metrics["total_accounts_with_no_commitment"],
        #"total_active_accounts": account_metrics["total_active_accounts"],
        #"total_dormant_accounts": account_metrics["total_dormant_accounts"],
        "average_active_commitment": account_metrics["average_active_commitment"],
        "average_total_commitment": account_metrics["average_total_commitment"],
        "sum_active_commitment": account_metrics["sum_active_commitment"],
        "sum_total_commitment": account_metrics["sum_total_commitment"],
        "top_institutional": top_institutional_acc,
    }


def create_pdf(output_path="outputs/Institutional_Report.pdf", base_path="data"):
    data = build_metrics(base_path=base_path)
    pdf = InstitutionalReport(orientation="P", unit="mm", format="A4")
    pdf.add_page()

    add_section_header(pdf, "INSTITUTIONAL SUMMARY")
    '''
    add_key_value_table(
        pdf,
        [
            ("Total Institutional Investors", f"{data['institutional_investor_count']:,}"),
            ("Total Investors with no Commitment", f"{data['inst_inv_no_commitment']:,}"),
            ("Total Accounts", f"{data['total_accounts']:,}"),
            ("Total Accounts with no Active Commitment", f"{data['total_accounts_with_no_commitment']:,}"),
            #("Total Active Accounts", f"{data['total_active_accounts']:,}"),
            #("Total Dormant Accounts", f"{data['total_dormant_accounts']:,}"),
            ("Average Active Commitment (Accounts)", format_currency(data["average_active_commitment"])),
            ("Total Institutional Commitment", format_currency(data["total_commitment_institutional"])),
            ("Mean Investment (Institutional)", format_currency(data["avg_investment_institutional"])),
            ("Median Investment (Institutional)", format_currency(data["median_investment_institutional"])),
        ],
    )
    '''
    add_side_by_side_sections(pdf, 
        "INVESTOR OVERVIEW",
        [
            ("Total Institutional Investors", f"{data['institutional_investor_count']:,}"),
            ("Total Investors with no Commitment", f"{data['inst_inv_no_commitment']:,}"),
            ("Total Accounts", f"{data['total_accounts']:,}"),
            ("Total Accounts with no Active Commitment", f"{data['total_accounts_with_no_commitment']:,}"),
        ],
        "INVESTMENT PORTFOLIO",
        [
            ("Average Active Commitment (Accounts)", format_currency(data["average_active_commitment"])),
            ("Average Total Commitment (Accounts)", format_currency(data["average_total_commitment"])),
            ("Sum Active Commitment (Accounts)", format_currency(data["sum_active_commitment"])),
            ("Sum Total Commitment (Accounts)", format_currency(data["sum_total_commitment"])),
            ("Total Institutional Commitment (Contacts)", format_currency(data["total_commitment_institutional"])),
            ("Mean Investment (Institutional)", format_currency(data["avg_investment_institutional"])),
            ("Median Investment (Institutional)", format_currency(data["median_investment_institutional"])),
        ]
    )

    pdf.ln(8)
    add_section_header(pdf, "TOP 20 INSTITUTIONAL INVESTORS")
    rows = [
        [
            safe_pdf_text(row["Legal name"]),
            safe_pdf_text(row["Contacts"]),
            format_currency(row["Total_Commitment"]),
        ]
        for _, row in data["top_institutional"].iterrows()
    ]
    add_table(
        pdf,
        ["Account Name", "Investors", "Total Commitment"],
        rows,
        [70, 90, 30],
        wrap_cols=[0, 1, 2],
    )

    pdf.ln(10)
    add_section_header(pdf, "METRIC DEFINITIONS")
    pdf.set_font("helvetica", "", 8)
    pdf.multi_cell(0, 5, "- Institutional investors are contacts and accounts where 'Contact Type' and 'Account Type' are marked 'Institutional' in js_contacts.csv and Accounts.csv respectively.")
    pdf.multi_cell(0, 5, "- Mean and median investment are calculated from 'Committed amount' for those institutional contacts.")
    pdf.multi_cell(0, 5, "- Top 20 table ranks institutional investors by summed committed amount.")
    #pdf.multi_cell(0, 5, "- Total Accounts includes accounts with active and/or closed investment balances.")
    #pdf.multi_cell(0, 5, "- Total Dormant Accounts includes closed accounts with no active balance and close date older than 36 months.")
    pdf.multi_cell(0, 5, "- Average Active Commitment is the average 'Active commitment' across active accounts.")

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    pdf.output(output_path)


def _parse_args():
    parser = argparse.ArgumentParser(description="Generate the institutional report.")
    parser.add_argument("--base-path", default="data")
    parser.add_argument("--output-path", default="outputs/Institutional_Report.pdf")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    create_pdf(output_path=args.output_path, base_path=args.base_path)
