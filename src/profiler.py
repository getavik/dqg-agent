import pandas as pd
from ydata_profiling import ProfileReport

def generate_profile(df: pd.DataFrame, title: str = "Data Profile") -> dict:
    """
    Generates a profile report using ydata-profiling.
    Returns a dictionary with summary statistics and the report object.
    """
    profile = ProfileReport(df, title=title, minimal=True)
    description = profile.get_description()
    
    # Extract key stats for the LLM
    # Extract key stats for the LLM
    summary = {
        "n_rows": description.table["n"],
        "n_var": description.table["n_var"],
        "columns": {}
    }
    
    for col, stats in description.variables.items():
        col_summary = {
            "type": str(stats["type"]),
            "n_distinct": stats["n_distinct"],
            "p_missing": stats["p_missing"],
        }
        # Add specific stats based on type if needed
        if "min" in stats:
            col_summary["min"] = stats["min"]
        if "max" in stats:
            col_summary["max"] = stats["max"]
            
        summary["columns"][col] = col_summary
        
    return {
        "summary": summary,
        "report_html": profile.to_html()
    }
