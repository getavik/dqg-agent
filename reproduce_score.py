
import pandas as pd
import sys
import os

# Ensure src is in path
sys.path.append(os.path.abspath("."))

from src.profiler import generate_profile
from src.llm_engine import analyze_intent
from src.validator import validate_data
from src.governance import detect_pii

try:
    print("Loading sample data...")
    df = pd.read_csv("sample_data.csv")
    
    print("Stage 1: Profiling")
    profile_results = generate_profile(df)
    profile_summary = profile_results["summary"]
    
    print("Stage 2: Governance Mapping")
    pii_results = detect_pii(df)
    
    # Mock intent that triggers failure
    intent = "Financial Marketing"
    rules_output = analyze_intent(intent, profile_summary, pii_results)
    rules = rules_output["rules"]
    print(f"Generated {len(rules)} rules.")
    for r in rules:
        print(f" - {r['column']}: {r['expectation']}")

    print("Stage 3: Validation")
    validation_results = validate_data(df, rules)
    
    print("\nValidation Results:")
    print(f"Success: {validation_results['success']}")
    print(f"Statistics: {validation_results['statistics']}")
    print(f"Failures: {len(validation_results['failures'])}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
