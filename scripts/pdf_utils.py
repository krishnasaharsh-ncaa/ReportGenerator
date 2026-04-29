from typing import Iterable, Sequence
import os
import tempfile
import pandas as pd


def _ensure_matplotlib_config_dir() -> None:
    os.environ.setdefault("MPLBACKEND", "Agg")

    if os.environ.get("MPLCONFIGDIR"):
        os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)
        return

    cache_dir = os.path.join(tempfile.gettempdir(), "reportgenerator-matplotlib")
    os.makedirs(cache_dir, exist_ok=True)
    os.environ["MPLCONFIGDIR"] = cache_dir

def add_section_header(pdf, title: str) -> None:
    """Add a styled section header."""
    pdf.set_font("helvetica", "B", 11)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 7, title, ln=True)
    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

def add_subsection_header(pdf, title: str) -> None:
    """Add a styled sub section header."""
    pdf.set_font("helvetica", "B", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 7, title, ln=True)
    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)


def add_key_value_table(
    pdf,
    rows: Iterable[tuple[str, object]],
    *,
    extra_spaces: set[int] | None = None,
) -> None:
    """Render label/value rows."""
    extra_spaces = extra_spaces or {3, 4}
    pdf.set_font("helvetica", "", 9)
    for i, (label, value) in enumerate(rows):
        pdf.set_text_color(70, 70, 70)
        pdf.cell(100, 6, label)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("helvetica", "B", 9)
        pdf.cell(0, 6, str(value), ln=True, align="R")
        pdf.set_font("helvetica", "", 9)
        if i in extra_spaces:
            pdf.ln(3)
    pdf.ln(4)

def add_side_by_side_metrics(
    pdf,
    left_metrics: Sequence[tuple[str, object]],
    right_metrics: Sequence[tuple[str, object]],
) -> None:
    """Add two metric columns side by side without per-column headers."""
    x_start = pdf.get_x()
    y_start = pdf.get_y()

    pdf.set_font("helvetica", "", 8)
    for label, value in left_metrics:
        pdf.set_x(x_start)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(55, 5, label)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("helvetica", "B", 8)
        pdf.cell(35, 5, str(value), align="R")
        pdf.set_font("helvetica", "", 8)
        pdf.ln(5)

    left_y_end = pdf.get_y()

    pdf.set_xy(110, y_start)
    pdf.set_font("helvetica", "", 8)
    for label, value in right_metrics:
        pdf.set_x(110)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(55, 5, label)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("helvetica", "B", 8)
        pdf.cell(30, 5, str(value), align="R")
        pdf.set_font("helvetica", "", 8)
        pdf.ln(5)

    right_y_end = pdf.get_y()
    pdf.set_y(max(left_y_end, right_y_end))
    pdf.ln(10)


def add_side_by_side_sections(
    pdf,
    left_title: str,
    left_metrics: Sequence[tuple[str, object]],
    right_title: str,
    right_metrics: Sequence[tuple[str, object]],
) -> None:
    """Add two sections side by side with their own headers and metrics."""
    x_start = pdf.get_x()
    y_start = pdf.get_y()

    # Left section
    pdf.set_font("helvetica", "B", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(95, 7, left_title, ln=True)
    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y(), 105, pdf.get_y())
    pdf.ln(4)

    # Left metrics
    pdf.set_font("helvetica", "", 8)
    for label, value in left_metrics:
        pdf.set_x(x_start)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(55, 5, label)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("helvetica", "B", 8)
        pdf.cell(35, 5, str(value), align="R")
        pdf.set_font("helvetica", "", 8)
        pdf.ln(5)

    left_y_end = pdf.get_y()

    # Right section
    pdf.set_xy(110, y_start)
    pdf.set_font("helvetica", "B", 10)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(95, 7, right_title, ln=True)
    pdf.set_draw_color(200, 200, 200)
    pdf.line(110, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    # Right metrics
    pdf.set_font("helvetica", "", 8)
    for label, value in right_metrics:
        pdf.set_x(110)
        pdf.set_text_color(70, 70, 70)
        pdf.cell(55, 5, label)
        pdf.set_text_color(0, 0, 0)
        pdf.set_font("helvetica", "B", 8)
        pdf.cell(30, 5, str(value), align="R")
        pdf.set_font("helvetica", "", 8)
        pdf.ln(5)

    right_y_end = pdf.get_y()
    pdf.set_y(max(left_y_end, right_y_end))
    pdf.ln(10)


def add_three_sections(
    pdf,
    first_title: str,
    first_metrics: Sequence[tuple[str, object]],
    second_title: str,
    second_metrics: Sequence[tuple[str, object]],
    third_title: str,
    third_metrics: Sequence[tuple[str, object]],
) -> None:
    """Add three sections side by side with their own headers and metrics."""
    y_start = pdf.get_y()
    section_width = 60
    section_gap = 5
    label_width = 35
    value_width = 20
    x_positions = [10, 10 + section_width + section_gap, 10 + (section_width + section_gap) * 2]

    section_data = [
        (x_positions[0], first_title, first_metrics),
        (x_positions[1], second_title, second_metrics),
        (x_positions[2], third_title, third_metrics),
    ]

    section_ends = []
    for x_pos, title, metrics in section_data:
        pdf.set_xy(x_pos, y_start)
        pdf.set_font("helvetica", "B", 10)
        pdf.set_text_color(40, 40, 40)
        pdf.cell(section_width, 7, title, ln=True)
        pdf.set_draw_color(200, 200, 200)
        pdf.line(x_pos, pdf.get_y(), x_pos + section_width, pdf.get_y())
        pdf.ln(4)

        pdf.set_font("helvetica", "", 8)
        for label, value in metrics:
            pdf.set_x(x_pos)
            pdf.set_text_color(70, 70, 70)
            pdf.cell(label_width, 5, str(label))
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("helvetica", "B", 8)
            pdf.cell(value_width, 5, str(value), align="R")
            pdf.set_font("helvetica", "", 8)
            pdf.ln(5)

        section_ends.append(pdf.get_y())

    pdf.set_y(max(section_ends))
    pdf.ln(10)


def add_table(
    pdf,
    headers: Sequence[str],
    rows: Iterable[Sequence[object]],
    col_widths: Sequence[int | float],
    wrap_cols: Sequence[int] | None = None,
    color_count: int = 1,
) -> None:
    """Add a table with optional text wrapping columns."""
    pdf.set_font("helvetica", "B", 9)
    pdf.set_fill_color(245, 245, 245)
    pdf.set_text_color(50, 50, 50)
    for header, width in zip(headers, col_widths):
        pdf.cell(width, 8, header, border=0, fill=True, align="L")
    pdf.ln()

    pdf.set_font("helvetica", "", 8)
    wrap_cols = set(wrap_cols or [])

    for row in rows:
        max_lines = 1
        for i, (text, width) in enumerate(zip(row, col_widths)):
            if i in wrap_cols:
                num_lines = len(pdf.multi_cell(width, 4, str(text), split_only=True))
                max_lines = max(max_lines, num_lines)
        row_height = max_lines * 4 + 2 if wrap_cols else 6

        # Page break + repeat header
        if pdf.get_y() + row_height > pdf.h - 25:
            pdf.add_page()
            pdf.set_font("helvetica", "B", 9)
            pdf.set_fill_color(245, 245, 245)
            pdf.set_text_color(50, 50, 50)
            for header, width in zip(headers, col_widths):
                pdf.cell(width, 8, header, border=0, fill=True, align="L")
            pdf.ln()
            pdf.set_font("helvetica", "", 8)

        if wrap_cols:
            x_start = pdf.get_x()
            y_start = pdf.get_y()
            for i, (text, width) in enumerate(zip(row, col_widths)):
                pdf.set_xy(x_start + sum(col_widths[:i]), y_start)
                if i < color_count:
                    pdf.set_text_color(70, 70, 70)
                    pdf.set_font("helvetica", "", 8)
                else:
                    pdf.set_text_color(0, 0, 0)
                    pdf.set_font("helvetica", "B", 8)
                if i in wrap_cols:
                    pdf.multi_cell(width, 4, str(text), border=0, align="L")
                else:
                    pdf.cell(width, row_height, str(text), border=0, align="L")
            pdf.set_y(y_start + row_height)
        else:
            for i, (value, width) in enumerate(zip(row, col_widths)):
                if i == 0:
                    pdf.set_text_color(70, 70, 70)
                    pdf.set_font("helvetica", "", 8)
                else:
                    pdf.set_text_color(0, 0, 0)
                    pdf.set_font("helvetica", "B", 8)
                pdf.cell(width, row_height, str(value), border=0, align="L")
            pdf.ln()

    pdf.set_font("helvetica", "", 8)
    pdf.set_text_color(0, 0, 0)

def add_comparison_charts(pdf, title, comparisons):
    """
    Create 2x2 grid of comparison charts.
    
    Args:
        pdf: FPDF object
        title: Main title for the chart grid
        comparisons: List of tuples, each containing:
            (metric_name, value1, value2, label1, label2)
            Example: ("New Investors", 8, 26, "Q1 2026", "Q1 2025")
    """
    _ensure_matplotlib_config_dir()
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    
    # Create 2x2 subplot with smaller figure size
    fig, axes = plt.subplots(2, 2, figsize=(8, 6))
    fig.suptitle(title, fontsize=12, fontweight='bold')
    
    # Flatten axes for easier iteration
    axes = axes.flatten()
    
    # If fewer than 4 comparisons, hide unused subplots
    for idx in range(len(comparisons), 4):
        axes[idx].axis('off')
    
    for idx, comparison in enumerate(comparisons):
        if idx >= 4:  # Only handle up to 4 charts
            break
            
        metric_name, val1, val2, label1, label2 = comparison
        ax = axes[idx]
        
        # Convert to float
        val1 = float(val1) if not pd.isna(val1) else 0
        val2 = float(val2) if not pd.isna(val2) else 0
        
        # Create bar chart
        bars = ax.bar([label1, label2], [val1, val2], 
                     color=['#3478C8', '#C86450'], width=0.4)
        
        # Format values for display
        def format_val(v):
            if v >= 1_000_000:
                return f'${v/1_000_000:.1f}M'
            elif v >= 1_000:
                return f'${v/1_000:.1f}K'
            else:
                return f'{v:.0f}'
        
        # Add value labels INSIDE bars
        for bar, val in zip(bars, [val1, val2]):
            height = bar.get_height()
            # Place text inside the bar, near the top
            ax.text(bar.get_x() + bar.get_width()/2., height * 0.85,
                   format_val(val), ha='center', va='top', 
                   fontsize=8, fontweight='bold', color='white')
        
        # Styling
        ax.set_title(metric_name, fontsize=9, fontweight='bold', pad=8)
        ax.set_ylabel('Value', fontsize=7)
        ax.grid(axis='y', alpha=0.3, linestyle='--', linewidth=0.5)
        ax.tick_params(axis='x', labelsize=7)
        ax.tick_params(axis='y', labelsize=7)
        
        # Format y-axis
        max_val = max(val1, val2)
        if max_val >= 1_000_000:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1_000_000:.0f}M'))
        elif max_val >= 1_000:
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${x/1_000:.0f}K'))
        
        # Add some space at the top for better visibility
        ax.set_ylim(0, max_val * 1.1)
    
    plt.tight_layout()
    
    # Save to a unique temporary file so generate-all runs cannot collide.
    with tempfile.NamedTemporaryFile(prefix="reportgenerator-chart-", suffix=".png", delete=False) as tmp:
        chart_path = tmp.name

    try:
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
    finally:
        plt.close()

    try:
        # Add to PDF with smaller width
        pdf.ln(3)
        current_y = pdf.get_y()
        pdf.image(chart_path, x=15, y=current_y, w=180)  # Reduced from 190 to 180
        pdf.ln(95)  # Reduced from 120 to 95
    finally:
        if os.path.exists(chart_path):
            os.remove(chart_path)
