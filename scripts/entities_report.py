import argparse
import os
from datetime import datetime

import pandas as pd
from fpdf import FPDF

from pdf_utils import add_section_header, add_side_by_side_sections, add_table, add_key_value_table


class EntityMetricsReport(FPDF):
    def header(self):
        self.set_font("helvetica", "B", 16)
        self.set_text_color(30, 30, 30)
        self.cell(0, 10, "ENTITY METRICS REPORT", ln=True, align="L")
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


def _clean_numeric(series):
    if pd.api.types.is_numeric_dtype(series):
        return series
    return pd.to_numeric(
        series.astype(str).str.replace(r"[\$,]", "", regex=True).str.strip(),
        errors="coerce",
    )


def _status_metrics(df):
    status_counts = df["Entity status"].astype(str).str.strip().value_counts().to_dict()
    active_count = status_counts.get("Active", 0)
    completed_count = status_counts.get("Completed", 0)
    active_commitment = df.loc[df["Entity status"] == "Active", "Commitment"].sum()
    completed_commitment = df.loc[df["Entity status"] == "Completed", "Commitment"].sum()
    active_unfunded_count = df.loc[
        (df["Entity status"] == "Active") & (df["Unfunded commitment"] > 0)
    ].shape[0]
    completed_unfunded_count = df.loc[
        (df["Entity status"] == "Completed") & (df["Unfunded commitment"] > 0)
    ].shape[0]
    total_commitment = df["Commitment"].sum()
    total_equity_balance = df["Equity balance"].sum()
    return {
        "active_count": active_count,
        "completed_count": completed_count,
        "active_commitment": active_commitment,
        "completed_commitment": completed_commitment,
        "active_unfunded_count": active_unfunded_count,
        "completed_unfunded_count": completed_unfunded_count,
        "total_commitment": total_commitment,
        "total_equity_balance": total_equity_balance,
    }


def _build_top_rows(df, top_n=10):
    cols = ["Entity name", "Entity status", "Commitment", "Equity balance"]
    scoped = (
        df[cols]
        .sort_values("Commitment", ascending=False, na_position="last")
        .head(top_n)
        .copy()
    )
    return [
        [
            str(row["Entity name"]),
            str(row["Entity status"]),
            format_currency(row["Commitment"]),
            format_currency(row["Equity balance"]),
        ]
        for _, row in scoped.iterrows()
    ]


def build_metrics(entity_overview_path="data/Entity_overview.xlsx"):
    entities = pd.read_excel(entity_overview_path, header=2)
    entities.columns = entities.columns.str.strip()

    if "Entity name" not in entities.columns and "Investment entity name" in entities.columns:
        entities = entities.rename(columns={"Investment entity name": "Entity name"})

    required_cols = {"Entity name", "Entity status", "Commitment", "Equity balance"}
    missing = sorted(required_cols - set(entities.columns))
    if missing:
        raise ValueError(f"Missing required columns in {entity_overview_path}: {missing}")

    entities["Entity name"] = entities["Entity name"].fillna("").astype(str).str.strip()
    entities["Entity status"] = entities["Entity status"].fillna("").astype(str).str.strip()
    entities["Commitment"] = _clean_numeric(entities["Commitment"]).fillna(0)
    if "Unfunded commitment" not in entities.columns:
        entities["Unfunded commitment"] = 0
    entities["Unfunded commitment"] = _clean_numeric(entities["Unfunded commitment"]).fillna(0)
    entities["Equity balance"] = _clean_numeric(entities["Equity balance"]).fillna(0)

    master_entities = entities.loc[
        entities["Entity name"].str.contains(r"Master", case=False, na=False)
    ].copy()
    pl_entities = entities.loc[
        entities["Entity name"].str.contains(r"Partner Loan", case=False, na=False)
    ].copy()

    return {
        "combined_metrics": _status_metrics(entities),
        "master_metrics": _status_metrics(master_entities),
        "pl_metrics": _status_metrics(pl_entities),
        "master_total_entities": master_entities["Entity name"].nunique(),
        "pl_total_entities": pl_entities["Entity name"].nunique(),
        "combined_total_entities": entities["Entity name"].nunique(),
        "master_top_rows": _build_top_rows(master_entities, top_n=10),
        "pl_top_rows": _build_top_rows(pl_entities, top_n=10),
    }


def create_pdf(output_path="outputs/Entity_Metrics_Report.pdf", base_path="data"):
    entity_overview_path = os.path.join(base_path, "Entity_overview.xlsx")
    data = build_metrics(entity_overview_path=entity_overview_path)

    master = data["master_metrics"]
    pl = data["pl_metrics"]

    pdf = EntityMetricsReport(orientation="P", unit="mm", format="A4")
    pdf.add_page()

    add_section_header(pdf, "ENTITY METRIC COMPARISON")
    add_side_by_side_sections(
        pdf,
        "MASTER ENTITIES",
        [
            ("Total Entities", f"{data['master_total_entities']:,}"),
            ("Entities Active", f"{master['active_count']:,}"),
            ("Entities Completed", f"{master['completed_count']:,}"),
            ("Commitment- Active Entities", format_currency(master["active_commitment"])),
            ("Commitment- Completed Entities", format_currency(master["completed_commitment"])),
            (
                "Total Entities Unfunded (A+C)",
                f"{master['active_unfunded_count']}+{master['completed_unfunded_count']}",
            ),
            ("Total Commitment", format_currency(master["total_commitment"])),
            ("Total Equity Balance", format_currency(master["total_equity_balance"])),
        ],
        "PARTNER LOAN ENTITIES",
        [
            ("Total Entities", f"{data['pl_total_entities']:,}"),
            ("Entities Active", f"{pl['active_count']:,}"),
            ("Entities Completed", f"{pl['completed_count']:,}"),
            ("Commitment- Active Entities", format_currency(pl["active_commitment"])),
            ("Commitment- Completed Entities", format_currency(pl["completed_commitment"])),
            (
                "Total Entities Unfunded (A+C)",
                f"{pl['active_unfunded_count']}+{pl['completed_unfunded_count']}",
            ),
            ("Total Commitment", format_currency(pl["total_commitment"])),
            ("Total Equity Balance", format_currency(pl["total_equity_balance"])),
        ],
    )

    add_section_header(pdf, "ALL ENTITIES COMBINED")
    add_key_value_table(
        pdf,
        [
            ("Total Entities", f"{data['combined_total_entities']:,}"),
            ("Entities Active", f"{data['combined_metrics']['active_count']:,}"),
            ("Entities Completed", f"{data['combined_metrics']['completed_count']:,}"),
            (
                "Commitment- Active Entities",
                format_currency(data["combined_metrics"]["active_commitment"]),
            ),
            (
                "Commitment- Completed Entities",
                format_currency(data["combined_metrics"]["completed_commitment"]),
            ),
            (
                "Total Entities Unfunded (A+C)",
                f"{data['combined_metrics']['active_unfunded_count']}+{data['combined_metrics']['completed_unfunded_count']}",
            ),
            ("Total Commitment", format_currency(data["combined_metrics"]["total_commitment"])),
            ("Total Equity Balance", format_currency(data["combined_metrics"]["total_equity_balance"])),
        ],
        extra_spaces= {2, 4}
    )

    pdf.ln(4)
    add_section_header(pdf, "TOP 10 MASTER ENTITIES")
    add_table(
        pdf,
        ["Entity name", "Entity status", "Commitment", "Equity balance"],
        data["master_top_rows"],
        [85, 30, 40, 40],
        wrap_cols=[0],
        color_count=2,
    )

    pdf.add_page()
    add_section_header(pdf, "TOP 10 PARTNER LOAN ENTITIES")
    add_table(
        pdf,
        ["Entity name", "Entity status", "Commitment", "Equity balance"],
        data["pl_top_rows"],
        [80, 30, 40, 40],
        wrap_cols=[0],
        color_count=2,
    )
    # pdf.ln(4)
    # add_section_header(pdf, "NOTE")
    # pdf.set_font("helvetica", "", 8)
    # pdf.multi_cell(0, 5, "- Main investor is the first listed contact on each account; account values are rolled up to that person.")
    

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    pdf.output(output_path)


def _parse_args():
    parser = argparse.ArgumentParser(description="Generate the entity metrics report.")
    parser.add_argument("--base-path", default="data")
    parser.add_argument("--output-path", default="outputs/Entity_Metrics_Report.pdf")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    create_pdf(output_path=args.output_path, base_path=args.base_path)
