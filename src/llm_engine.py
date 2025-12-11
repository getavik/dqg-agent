import json

def analyze_intent(intent: str, profile_summary: dict, pii_results: dict) -> dict:
    """
    MOCK: Analyzes business intent and data profile to generate governance rules.
    """
    print(f"DEBUG: Analyzing intent '{intent}' with profile summary keys: {list(profile_summary.keys())}")
    
    # Mock logic based on keywords in intent
    rules = []
    
    # Deduplication helper
    existing_rules = set()

    def add_rule(rule):
        # Auto-map to dimension if not provided
        if "dimension" not in rule:
            exp = rule["expectation"]
            if "not_be_null" in exp:
                rule["dimension"] = "Completeness"
            elif "match_regex" in exp:
                rule["dimension"] = "Conformity"
            elif "between" in exp:
                rule["dimension"] = "Validity"
            elif "unique" in exp:
                rule["dimension"] = "Uniqueness"
            else:
                rule["dimension"] = "Accuracy"
                
        key = (rule["column"], rule["expectation"])
        if key not in existing_rules:
            rules.append(rule)
            existing_rules.add(key)

    # Example mock logic
    if "marketing" in intent.lower():
        add_rule({
            "column": "email",
            "expectation": "expect_column_values_to_not_be_null",
            "severity": "High",
            "reason": "Email is critical for marketing campaigns.",
            "dimension": "Completeness"
        })
    
    if "financial" in intent.lower():
        add_rule({
            "column": "revenue",
            "expectation": "expect_column_values_to_be_between",
            "min_value": 0,
            "severity": "High",
            "reason": "Revenue cannot be negative for reporting.",
            "dimension": "Validity"
        })
        
    # Generic checks based on profile
    for col, stats in profile_summary["columns"].items():
        if stats["p_missing"] > 0:
             add_rule({
                "column": col,
                "expectation": "expect_column_values_to_not_be_null",
                "severity": "Medium",
                "reason": f"Column {col} has missing values.",
                "dimension": "Completeness"
            })
            
        if "phone" in col.lower():
             add_rule({
                "column": col,
                "expectation": "expect_column_values_to_match_regex",
                # Regex to match the Target Standardization Format (e.g. +1-XXX-XXX-XXXX or +XX-...)
                # Anything NOT matching this will fail & be auto-fixed (standardized)
                "regex": r"^\+\d{1,3}-\d{1,4}-\d{3}-\d{4}((x|ext)\d{1,5})?$",
                "severity": "High",
                "reason": f"Column {col} contains invalid phone number formats.",
                "dimension": "Conformity"
            })

    # PII Checks
    for col, entities in pii_results.items():
        if entities:
             add_rule({
                "column": col,
                "expectation": "expect_column_values_to_match_regex", # Placeholder
                "severity": "Critical",
                "reason": f"PII Detected: {', '.join(entities)}. Ensure proper masking.",
                "dimension": "Confidentiality"
            })
            
    return {"rules": rules}

def generate_remediation(failed_validations: list) -> dict:
    """
    MOCK: Generates remediation plan for failed validations.
    """
    remediations = []
    
    for failure in failed_validations:
        col = failure["column"]
        expectation = failure["expectation"]
        
        if expectation == "expect_column_values_to_not_be_null":
            remediations.append({
                "issue": f"Missing values in {col}",
                "sql_fix": f"UPDATE table SET {col} = 'Unknown' WHERE {col} IS NULL;",
                "python_fix": f"df['{col}'].fillna('Unknown', inplace=True)"
            })
        elif expectation == "expect_column_values_to_be_between":
             remediations.append({
                "issue": f"Values out of range in {col}",
                "sql_fix": f"DELETE FROM table WHERE {col} < 0;", # Example
                "python_fix": f"df = df[df['{col}'] >= 0]"
            })
        elif expectation == "expect_column_values_to_match_regex":
            remediations.append({
                "issue": f"Invalid format in {col}",
                "sql_fix": f"UPDATE table SET {col} = 'Unknown' WHERE {col} NOT REGEXP '{failure.get('regex', '.*')}';",
                "python_fix": f"# Fixes invalid formats based on regex\ndf.loc[~df['{col}'].astype(str).str.match(r'{failure.get('regex', '.*')}'), '{col}'] = 'Unknown'"
            })
            
    return {"remediations": remediations}

import google.generativeai as genai

def generate_business_impact(intent: str, rules: list, failures: list, total_rows: int = 0, api_key: str = None, df_summary: dict = None) -> str:
    """
    Generates a business impact statement. Uses Gemini AI if API key is provided, else fallback to heuristics.
    """
    
    # ---------------------------------------------------------
    # PATH A: GEN_AI ENHANCED INSIGHTS (Dynamic & Rich)
    # ---------------------------------------------------------
    if api_key:
        try:
            genai.configure(api_key=api_key)
            
            # Dynamic Model Discovery to avoid 404s
            model_name = 'gemini-1.5-flash' # Default preference
            try:
                available_models = [m for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
                # Prefer flash, then pro, then any gemini
                model_map = {m.name.split('/')[-1]: m.name for m in available_models}
                
                # Priority list
                priorities = ['gemini-2.5-flash-live', 'gemini-2.0-flash-exp', 'gemini-1.5-flash', 'gemini-1.5-pro', 'gemini-1.0-pro', 'gemini-pro']
                found = False
                for p in priorities:
                    # Check partial match or exact
                    matches = [name for name in model_map.values() if p in name]
                    if matches:
                        model_name = matches[0]
                        found = True
                        break
                
                if not found and available_models:
                    model_name = available_models[0].name
                    
            except Exception as e:
                print(f"Model list failed, using default: {e}")

            model = genai.GenerativeModel(model_name)
            
            # Construct Prompt
            prompt_parts = [
                f"Act as a Chief Data Officer. Analyze the following data quality audit results for a dataset with Business Intent: '{intent}'.",
                f"\n--- Data Context ---",
                f"Total Rows: {total_rows}",
                f"Failed Records Impact: {sum(f.get('unexpected_count', 0) for f in failures)} issues detected.",
            ]
            
            if failures:
                prompt_parts.append("\n--- Key Deficiencies ---")
                for f in failures[:5]: # Top 5
                     prompt_parts.append(f"- Column '{f.get('column')}' failed check '{f.get('expectation')}'. Count: {f.get('unexpected_count')}")
                     
            if df_summary:
                prompt_parts.append(f"\n--- Financial/Operational Highligts ---")
                prompt_parts.append(df_summary) # Pass string summary of top generators etc.
                
            prompt_parts.append("\n--- goal ---")
            prompt_parts.append("Provide an 'Executive Data Brief' with exactly 3 bullet points. Do not use markdown bolding (**).")
            prompt_parts.append("• Strategic Risk: [One succinct sentence on business impact]")
            prompt_parts.append("• Operational Value: [One succinct sentence on remediation value]")
            prompt_parts.append("• Forecast: [One succinct sentence projecting a KPI]")
            prompt_parts.append("Keep it extremely concise, factual, and professional.")

            response = model.generate_content("\n".join(prompt_parts))
            return response.text
            
        except Exception as e:
            return f"AI Generation Failed: {str(e)}\n\n" + _generate_heuristic_impact(intent, rules, failures, total_rows)

    # ---------------------------------------------------------
    # PATH B: HEURISTIC / REGEX INSIGHTS (Fallback)
    # ---------------------------------------------------------
    return _generate_heuristic_impact(intent, rules, failures, total_rows)

def _generate_heuristic_impact(intent, rules, failures, total_rows):
    impact = []
    
    # 1. Intent Context
    impact.append(f"Business Objective: '{intent}'")
    
    if not failures:
        impact.append("Conclusion: Data Quality is 100% aligned with objectives.")
        return "\n\n".join(impact)
        
    # 2. Quantitative Impact
    total_issues = sum(f.get("unexpected_count", 0) for f in failures)
    affected_pct = min(100, (total_issues / total_rows * 100)) if total_rows > 0 else 0
    
    stats_section = [
        f"• Data Hygiene Improvement: {total_issues} data points cleansed.",
        f"• Dataset Reliability: Increased by approximately {affected_pct:.1f}% post-remediation."
    ]
    
    # 3. ROI / KPI Projections
    if "marketing" in intent.lower() and total_rows > 0:
        potential_conversion = total_issues * 0.05 
        est_value = potential_conversion * 100
        stats_section.append(f"• Campaign ROI: Potentially recovered ~${est_value:,.0f} in Lifecycle Value from {total_issues} invalid leads.")
        
    elif "financ" in intent.lower():
        stats_section.append(f"• Risk Mitigation: Reduced exposure to forecasting errors across {total_issues} financial records.")
        
    impact.append("KPI Improvements:\n" + "\n".join(stats_section))
    
    # 4. Strategic Analysis
    failed_cols = set(f["column"] for f in failures)
    impact.append("Strategic Impediments Removed:")
    
    if "marketing" in intent.lower():
        email_cols = [c for c in failed_cols if "email" in c.lower()]
        if email_cols:
             impact.append("- Restored reachability for email campaigns.")
        
        phone_cols = [c for c in failed_cols if "phone" in c.lower()]
        if phone_cols:
             impact.append("- Enabling SMS/Voice channels by standardizing numbers.")
            
    impact.append("\nFinal Verdict: The dataset is now structurally sound for downstream analytics.")
    
    return "\n\n".join(impact)
