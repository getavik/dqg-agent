
import pandas as pd
import sys
import os
import traceback

sys.path.append(os.path.abspath("."))

from src.profiler import generate_profile
from src.llm_engine import analyze_intent
from src.validator import validate_data
from src.governance import detect_pii

with open("score_result.txt", "w", encoding="utf-8") as f:
    try:
        f.write("Loading sample data...\n")
        df = pd.read_csv("sample_data.csv")
        
        f.write("Stage 1: Profiling (Mocking description to save time if needed, but running real one first)\n")
        # To avoid heavy lifting if ydata is the issue, let's wrap it
        try:
           profile_results = generate_profile(df)
           profile_summary = profile_results["summary"]
           f.write("Profile generated.\n")
        except Exception as pe:
             f.write(f"Profile failed: {pe}\n")
             # Fallback mock summary for reproduction
             profile_summary = {
                 "columns": {
                     "email": {"p_missing": 0.1},
                     "revenue": {"p_missing": 0},
                     "phone": {"p_missing": 0.2},
                     "id": {"p_missing": 0},
                     "name": {"p_missing": 0},
                     "country": {"p_missing": 0}
                 }
             }

        f.write("Stage 2: Governance Mapping\n")
        pii_results = {} # detect_pii(df) - skipping to simplify debug if needed
        
        intent = "Financial Marketing"
        rules_output = analyze_intent(intent, profile_summary, pii_results)
        rules = rules_output["rules"]
        f.write(f"Generated {len(rules)} rules.\n")
        for r in rules:
            f.write(f" - {r['column']}: {r['expectation']}\n")

        f.write("Stage 3: Validation\n")
        validation_results = validate_data(df, rules)
        
        f.write("\nValidation Results:\n")
        f.write(f"Success: {validation_results['success']}\n")
        f.write(f"Statistics: {validation_results['statistics']}\n")
        f.write(f"Failures: {len(validation_results['failures'])}\n")
        
    except Exception as e:
        f.write(f"Error: {e}\n")
        traceback.print_exc(file=f)
