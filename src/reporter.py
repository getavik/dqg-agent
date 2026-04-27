import pandas as pd
from fpdf import FPDF
import os
import tempfile
import matplotlib.pyplot as plt
from datetime import datetime

def generate_profiling_pdf_from_html(html_string: str) -> bytes:
    """
    Generates a simple PDF indicating that the full report is an HTML file.
    fpdf2's HTML support is limited, so we guide the user to the better format.
    """
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", 'B', 16)
    pdf.cell(0, 10, "Data Profiling Report", 0, 1, 'C')
    pdf.ln(10)
    pdf.set_font("helvetica", '', 12)
    pdf.multi_cell(0, 10, 
        "This is a simplified PDF summary. For the full interactive experience, please open the "
        "HTML version of the report, which is displayed in the 'Profiling' tab of the application."
    )
    return pdf.output()

def format_profiling_summary_for_excel(summary: dict) -> pd.DataFrame:
    """
    Converts the ydata-profiling summary JSON into a readable DataFrame for Excel.
    """
    if not summary or 'columns' not in summary:
        return pd.DataFrame({"Info": ["Profiling data is not available."]})

    rows = []
    for col, details in summary['columns'].items():
        row = {
            "Column": col,
            "Type": details.get('type', 'N/A'),
            "Mean": details.get('mean', 'N/A'),
            "StdDev": details.get('std', 'N/A'),
            "Min": details.get('min', 'N/A'),
            "Max": details.get('max', 'N/A'),
            "Zeros (%)": details.get('p_zeros', 'N/A'),
            "Missing (%)": details.get('p_missing', 'N/A'),
            "Distinct Count": details.get('n_distinct', 'N/A'),
        }
        rows.append(row)
        
    return pd.DataFrame(rows)

class PDFReport(FPDF):
    def header(self):
        self.set_font('helvetica', 'B', 12)
        self.cell(0, 10, 'Data Quality Audit Report', 0, 1, 'C')
        self.ln(5)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.cell(0, 10, f'Page {self.page_no()}', 0, 0, 'C')


def generate_charts(rules, failures, df_fixed):
    paths = []
    
    # 1. Dimension Performance Bar Chart
    # Map rules to dimensions
    dim_total = {}
    dim_failed = {}
    
    # Calculate totals (Mock: 1 rule = 100 checks or just rule count? User asked for % of records. 
    # We have 'unexpected_count' in failures. We need 'total_count'. 
    # In 'validate_data', we get aggregate success %. 
    # For this view, let's use Rule Pass Rate (easier) or Record Pass Rate (requires parsing results deep).
    # Let's use Record Pass Rate if possible, else Rule Pass Rate.
    # Sticking to Rule Pass Rate (Success %) per dimension is safer with current data structure.
    # Actually, User asked "% completeness rule passed". 
    # Let's aggregate: For each dimension, Total Rules vs Failed Rules? 
    # Or Records Scanned vs Records Failed. 
    # We'll try Record-level heuristic: Total Rows * Rules = Total Checks. Sum(unexpected) = Failures.
    
    total_rows = len(df_fixed)
    
    # Initialize
    for r in rules:
        d = r.get("dimension", "Unclassified")
        dim_total[d] = dim_total.get(d, 0) + total_rows
        dim_failed[d] = 0
        
    for f in failures:
        # We need to look up dimension for this failure. 
        # Failure dict has 'expectation', 'column'. Match with rules.
        # This is O(N*M) but N is small.
        # Find matching rule
        match_dim = "Unclassified"
        for r in rules:
            if r["column"] == f["column"] and r["expectation"] == f["expectation"]:
                match_dim = r.get("dimension", "Unclassified")
                break
        
        dim_failed[match_dim] = dim_failed.get(match_dim, 0) + f.get("unexpected_count", 0)
        
    # Prepare Data for Plot
    dims = list(dim_total.keys())
    pass_rates = []
    for d in dims:
        total = dim_total[d]
        fails = dim_failed.get(d, 0)
        p = 100 * (total - fails) / total if total > 0 else 0
        pass_rates.append(p)
        
    fig1, ax1 = plt.subplots(figsize=(6, 4))
    colors = ['#4CAF50' if x > 90 else '#FFC107' if x > 80 else '#FF5722' for x in pass_rates]
    ax1.bar(dims, pass_rates, color=colors)
    ax1.set_ylim(0, 105)
    ax1.set_ylabel("Record Pass %")
    ax1.set_title("DQ Accuracy by Dimension")
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    p1 = os.path.join(tempfile.gettempdir(), "dq_chart_dim.png")
    plt.savefig(p1)
    plt.close()
    paths.append(p1)

    # 2. Validation Status Pie Chart (Keep existing conceptual pie)
    total_checks_all = sum(dim_total.values())
    total_fails_all = sum(dim_failed.values())
    passed_all = max(0, total_checks_all - total_fails_all)
    
    fig2, ax2 = plt.subplots(figsize=(6, 4))
    ax2.pie([passed_all, total_fails_all], labels=['Passed', 'Failed'], colors=['#4CAF50', '#FF5722'], autopct='%1.1f%%', startangle=140)
    ax2.set_title("Overall Data Reliability")
    
    p2 = os.path.join(tempfile.gettempdir(), "dq_chart_status.png")
    plt.savefig(p2)
    plt.close()
    paths.append(p2)
    
    # 3. Financial/Trend Forecast
    # Heuristic: Find Date and Amount/Revenue columns
    date_col = next((c for c in df_fixed.columns if "date" in c.lower() or "time" in c.lower()), None)
    val_col = next((c for c in df_fixed.columns if "amount" in c.lower() or "revenue" in c.lower() or "sales" in c.lower()), None)
    
    fig3, ax3 = plt.subplots(figsize=(10, 4))
    if date_col and val_col:
        try:
            # Prepare data
            df_plot = df_fixed.copy()
            df_plot[date_col] = pd.to_datetime(df_plot[date_col], errors='coerce')
            df_plot.dropna(subset=[date_col, val_col], inplace=True)
            df_plot.sort_values(date_col, inplace=True)
            
            # Aggregate Monthly
            df_plot.set_index(date_col, inplace=True)
            # Try monthly resampling if enough data, else rolling
            if (df_plot.index.max() - df_plot.index.min()).days > 60:
                trend = df_plot[val_col].resample('ME').sum()
            else:
                trend = df_plot[val_col] # Raw if short duration
                
            ax3.plot(trend.index, trend.values, marker='o', linestyle='-', color='#2196F3', label='Actual Cleaned')
            
            # Simple Linear Forecast (Mock visual extension)
            # Just extend the line slightly to show "Projection" visually
            if len(trend) > 1:
                x_vals = range(len(trend))
                z = pd.Series(trend.values)
                # Simple last value naive projection for visual demo
                last_val = trend.iloc[-1]
                avg_growth = (trend.iloc[-1] - trend.iloc[0]) / len(trend) if len(trend) > 0 else 0
                
                # Mock forecast points
                future_dates = [trend.index[-1] + pd.Timedelta(days=30*i) for i in range(1, 4)]
                future_vals = [last_val + (avg_growth * i) for i in range(1, 4)]
                ax3.plot(future_dates, future_vals, marker='x', linestyle='--', color='#9C27B0', label='Forecast')
            
            ax3.legend()
            ax3.set_title(f"Financial Trend & Forecast ({val_col})")
            ax3.set_ylabel(val_col.title())
            ax3.grid(True, linestyle='--', alpha=0.5)
        except Exception as e:
            ax3.text(0.5, 0.5, f"Could not generating forecast: Data format issue", ha='center')
            print(f"Chart Error: {e}")
    else:
        # Fallback chart if no financial data
        ax3.text(0.5, 0.5, "No Date/Revenue columns found for forecasting.", ha='center')
        ax3.axis('off')
        
    plt.tight_layout()
    p3 = os.path.join(tempfile.gettempdir(), "dq_chart_fin.png")
    plt.savefig(p3)
    plt.close()
    paths.append(p3)
    
    return paths

def generate_bi_charts(df):
    """
    Generates advanced BI charts: Top Generators, Yearly Trend, Regional Split.
    """
    paths = []
    
    # 1. Column Detection Heuristics
    cols = df.columns
    c_amt = next((c for c in cols if any(x in c.lower() for x in ['amount', 'revenue', 'sales', 'cost', 'price'])), None)
    c_date = next((c for c in cols if any(x in c.lower() for x in ['date', 'time', 'created', 'year'])), None)
    c_region = next((c for c in cols if any(x in c.lower() for x in ['region', 'city', 'country', 'state', 'location'])), None)
    c_name = next((c for c in cols if any(x in c.lower() for x in ['name', 'customer', 'client', 'vendor', 'agent'])), None)
    
    if not (c_amt and c_date):
        return [] # Minimum requirement for revenue analysis
        
    df_bi = df.copy()
    df_bi[c_amt] = pd.to_numeric(df_bi[c_amt], errors='coerce').fillna(0)
    df_bi[c_date] = pd.to_datetime(df_bi[c_date], errors='coerce')
    df_bi = df_bi.dropna(subset=[c_date])
    df_bi['Year'] = df_bi[c_date].dt.year
    
    # Chart 1: Top Revenue Generators (Who)
    if c_name:
        top_gen = df_bi.groupby(c_name)[c_amt].sum().nlargest(7).sort_values()
        
        fig1, ax1 = plt.subplots(figsize=(8, 4))
        ax1.barh(top_gen.index.astype(str), top_gen.values, color='#673AB7')
        ax1.set_title(f"Top {len(top_gen)} Revenue Generators")
        ax1.set_xlabel("Total Revenue")
        plt.tight_layout()
        
        p1 = os.path.join(tempfile.gettempdir(), "bi_chart_who.png")
        plt.savefig(p1)
        plt.close()
        paths.append(p1)
        
    # Chart 2: Peak Revenue Year (When)
    yearly = df_bi.groupby('Year')[c_amt].sum()
    
    fig2, ax2 = plt.subplots(figsize=(8, 4))
    ax2.plot(yearly.index.astype(str), yearly.values, marker='o', linestyle='-', color='#009688', linewidth=2)
    # Highlight Peak
    peak_yr = yearly.idxmax()
    peak_val = yearly.max()
    ax2.annotate(f"Peak: {peak_yr}", xy=(str(peak_yr), peak_val), xytext=(0, 10), 
                 textcoords='offset points', arrowprops=dict(arrowstyle="->"), ha='center')
                 
    ax2.set_title("Revenue Growth by Year")
    ax2.set_ylabel("Revenue")
    ax2.grid(True, linestyle='--', alpha=0.3)
    plt.tight_layout()
    
    p2 = os.path.join(tempfile.gettempdir(), "bi_chart_when.png")
    plt.savefig(p2)
    plt.close()
    paths.append(p2)
    
    # Chart 3: Region per Year (Where & When) - Stacked or Heatmap? 
    # User asked "Region generated how much revenue per year".
    if c_region:
        pivot = df_bi.pivot_table(index='Year', columns=c_region, values=c_amt, aggfunc='sum', fill_value=0)
        
        # Limit to top 5 regions to avoid clutter
        top_regions = df_bi.groupby(c_region)[c_amt].sum().nlargest(5).index
        pivot = pivot[top_regions]
        
        fig3, ax3 = plt.subplots(figsize=(8, 4))
        pivot.plot(kind='bar', stacked=True, ax=ax3, colormap='viridis')
        ax3.set_title("Revenue by Region per Year")
        ax3.set_ylabel("Revenue")
        plt.legend(title="Top Regions", bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        
        p3 = os.path.join(tempfile.gettempdir(), "bi_chart_where.png")
        plt.savefig(p3)
        plt.close()
        paths.append(p3)
        
    return paths

def safe_text(text):
    """Encodes text to latin-1, replacing unsupported chars with '?'."""
    if not isinstance(text, str):
        text = str(text)
    return text.encode('latin-1', 'replace').decode('latin-1')

def generate_pdf_report(df_original, df_fixed, rules, validation_results, intent, impact_text):
    pdf = PDFReport()
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.set_font('helvetica', '', 11)
    
    # Helper to clean text
    def clean(t): return safe_text(str(t))

    # Helper: Render flexible row with wrapping
    def render_table_row(pdf, widths, data, align='L', border=1):
        """
        Renders a row where cells wrap text using multi_cell.
        Ensures all cells in the row have the same height.
        """
        # Calculate max height needed
        max_lines = 1
        font_size = pdf.font_size_pt / 72 * 96 # Approx conversion or use pdf.font_size
        # Actually pdf.font_size is in user unit (mm). Line height is usually font_size * k
        line_height = 4 # Fixed for small fonts
        
        # Save current position
        x_start = pdf.get_x()
        y_start = pdf.get_y()
        
        # Determine height based on content wrapping
        # FPDF doesn't expose "get_string_width" perfectly for wrapping calc without writing
        # So we simulate or just use multi_cell behavior.
        # Hard to do perfectly in vanilla FPDF without 'GetMultiCellHeight'.
        # We will use value length heuristic for simplicity in this constrained agent env
        # OR better: Render invisibly? No.
        # Let's trust FPDF 'multi_cell' advances Y. We need to reset Y for next cell.
        
        # Strategy:
        # 1. Store X positions.
        # 2. Loop cells. Call multi_cell.
        # 3. Track max Y reached.
        # 4. Draw borders rectangles around largest Y.
        
        # This is complex in basic FPDF.
        # Simpler "Fit to Page" strategy requested by user:
        # Just ensure widths sum to page width and use single line but smaller font?
        # User said "without truncating any column values". Wrapping is mandatory.
        
        # OK, simplified wrapping implementation:
        row_heights = []
        for i, datum in enumerate(data):
            w = widths[i]
            # Calculate lines roughly: len(datum) * char_width / width
            # Char width approx 2mm for size 6-7
            # Rough & dirty but works for many cases
            text_len = pdf.get_string_width(datum)
            lines = max(1, int(text_len / w) + 1)
            row_heights.append(lines * line_height)
            
        height = max(row_heights)
        
        # Check page break
        if y_start + height > pdf.h - 15:
            pdf.add_page(orientation=pdf.cur_orientation)
            y_start = pdf.get_y()
            
        for i, datum in enumerate(data):
            w = widths[i]
            x = pdf.get_x()
            pdf.multi_cell(w, line_height, datum, border=border, align=align)
            # Move cursor back to top-right of this cell for next cell
            pdf.set_xy(x + w, y_start)
            
        # Move to next line
        pdf.set_xy(x_start, y_start + height)

    # 1. Executive Summary
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, '1. Executive Summary', 0, 1)
    pdf.set_font('helvetica', '', 11)
    
    pdf.cell(0, 8, f"Report Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", 0, 1)
    pdf.cell(0, 8, f"Total Rows: {len(df_original)}", 0, 1)
    pdf.cell(0, 8, f"Total Columns: {len(df_original.columns)}", 0, 1)
    
    # If the statistics dictionary is None or the key is missing, default to 0.0
    stats = validation_results.get("statistics")
    if stats:
        success_rate = stats.get("success_percent", 0.0)
    else:
        success_rate = 0.0
    
    # Ensure success_rate is not None before formatting
    if success_rate is None:
        success_rate = 0.0
        
    pdf.cell(0, 8, f"Data Health Score: {success_rate:.1f}%", 0, 1)
    pdf.ln(3)

    # 1b. Executive Analysis (AI or Heuristic)
    pdf.set_font('helvetica', 'B', 12)
    pdf.cell(0, 8, 'Executive Analysis:', 0, 1)
    pdf.set_font('helvetica', '', 10)
    # Strip potential markdown formatting if any remains
    clean_impact = safe_text(impact_text).replace('**', '').replace('##', '')
    pdf.multi_cell(0, 6, clean_impact)
    pdf.ln(5)
    
    # 2. Visual Insights (Consolidated)
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, '2. Visual Insights', 0, 1)
    
    # Generate all charts
    c_dim, c_status, c_fin = generate_charts(rules, validation_results["failures"], df_fixed)
    bi_paths = generate_bi_charts(df_fixed)
    
    # Collect all valid paths
    all_charts = [c_status, c_dim] # Priority: Health -> Dimensions
    if c_fin: all_charts.append(c_fin) # Financial from dq charts
    if bi_paths: all_charts.extend(bi_paths) # BI charts
    
    # Render Grid (2 cols roughly)
    # Page width ~190 available. Image w=90 is good for 2 cols.
    y_start = pdf.get_y()
    x_left = 10
    x_right = 105
    
    # Chart 1 & 2
    if len(all_charts) >= 1:
        pdf.image(all_charts[0], x=x_left, y=y_start, w=90)
    if len(all_charts) >= 2:
        pdf.image(all_charts[1], x=x_right, y=y_start, w=90)
    
    pdf.ln(70) # Height of row 1
    
    # Chart 3 & 4
    y_row2 = pdf.get_y()
    if len(all_charts) >= 3:
        # If we are near bottom, add page? 70mm is ~1/4 page. Should be ok.
        pdf.image(all_charts[2], x=x_left, y=y_row2, w=90)
    if len(all_charts) >= 4:
        pdf.image(all_charts[3], x=x_right, y=y_row2, w=90)
        
    if len(all_charts) >= 3:
        pdf.ln(70)
        
    # Chart 5 & 6
    y_row3 = pdf.get_y()
    if len(all_charts) >= 5:
        if y_row3 > 220: # Check overflow
            pdf.add_page()
            y_row3 = pdf.get_y()
        pdf.image(all_charts[4], x=x_left, y=y_row3, w=90)
    if len(all_charts) >= 6:
        pdf.image(all_charts[5], x=x_right, y=y_row3, w=90)
        
    if len(all_charts) >= 5:
        pdf.ln(70)
        
    pdf.ln(5) # Spacing
    
    # 3. Governance Rules
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, '3. Governance Rules Enforced', 0, 1)
    pdf.set_font('helvetica', '', 10)
    
    # Define Column Widths (Fixed for Portrait)
    # Total ~190mm
    c_widths = [35, 35, 25, 95] 
    headers = ["Column", "Dimension", "Severity", "Logic/Reason"]
    
    # Header
    pdf.set_fill_color(240, 240, 240)
    pdf.set_font('helvetica', 'B', 10)
    x_start = pdf.get_x()
    for i, h in enumerate(headers):
        pdf.cell(c_widths[i], 8, h, 1, 0, 'C', 1)
    pdf.ln()
    
    # Rows
    pdf.set_font('helvetica', '', 8)
    for r in rules:
        row_data = [
            clean(str(r.get("column", "-"))),
            clean(str(r.get("dimension", "General"))),
            clean(str(r.get("severity", "Medium"))),
            clean(str(r.get("reason", "")))
        ]
        render_table_row(pdf, c_widths, row_data)
        
    pdf.ln(5)

    # 4. Remediation Log
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, '4. Issues Remedied', 0, 1)
    pdf.set_font('helvetica', '', 11)
    
    if validation_results["failures"]:
        for f in validation_results["failures"]:
            col = safe_text(f.get("column"))
            exp = safe_text(f.get("expectation"))
            count = f.get("unexpected_count")
            
            msg = f"- Fixed {count} issues in '{col}' related to {exp}."
            if "regex" in str(f):
                msg += " (Standardization Applied)"
            
            pdf.multi_cell(0, 6, safe_text(msg)) # Tighter line height
    else:
        pdf.cell(0, 8, "No automated remediation was performed.", 0, 1)
        
    # 5. Data Highlight
    pdf.add_page("L") # Landscape for data table
    pdf.set_font('helvetica', 'B', 14)
    pdf.cell(0, 10, '5. Remediated Data Snapshot (First 20 Rows)', 0, 1)
    
    # Dynamic Width Calculation - "Fit to Page"
    pdf.set_font('helvetica', '', 8)
    
    # Use as many columns as possible, but ensure we fit page width (approx 277mm for A4 L)
    page_width = 275 
    
    cols = df_fixed.columns[:12] # Try 12 columns max
    
    # Initial Ideal Widths
    ideal_widths = []
    for c in cols:
        max_len = len(str(c))
        sample_vals = df_fixed[c].head(20).astype(str).tolist()
        for v in sample_vals:
            max_len = max(max_len, len(v))
        # Approx 1.8mm per char for size 7-8 + padding
        ideal_widths.append(max(15, min(70, max_len * 2)))
        
    total_ideal = sum(ideal_widths)
    
    # Scaling Factor
    scale = 1.0
    if total_ideal > page_width:
        scale = page_width / total_ideal
        
    final_widths = [w * scale for w in ideal_widths]
    
    # Table Header
    pdf.set_fill_color(240, 240, 240)
    pdf.set_font('helvetica', 'B', 8)
    for i, c in enumerate(cols):
        pdf.cell(final_widths[i], 8, clean(str(c)[:25]), 1, 0, 'C', 1)
    pdf.ln()
    
    # Rows
    pdf.set_font('helvetica', '', 7)
    for i in range(min(len(df_fixed), 30)):
        row_data = [clean(str(df_fixed.iloc[i][c])) for c in cols]
        render_table_row(pdf, final_widths, row_data)
    
    return pdf.output()
