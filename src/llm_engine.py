import json
import os
import google.generativeai as genai
import numpy as np
from src.llm_utils import _get_available_model
import pandas as pd

def _convert_numpy_types(obj):
    """Recursively converts numpy types to native Python types for JSON serialization."""
    if isinstance(obj, dict):
        return {k: _convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_convert_numpy_types(i) for i in obj]
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def analyze_intent(intent: str, profile_summary: dict, pii_results: dict, api_key: str = None) -> tuple:
    """
    Analyzes business intent and data profile to generate governance rules.
    Uses Gemini AI if API key is provided, else fallback to heuristics.
    Returns the rules and usage metadata.
    """
    if api_key:
        try:
            genai.configure(api_key=api_key)
            model, model_name = _get_available_model(api_key=api_key)
            if model is None:
                raise Exception("No suitable Gemini model found that supports generateContent.")
            
            sanitized_profile = _convert_numpy_types(profile_summary)

            prompt = f"""
            Act as a Senior Data Governance Architect. Analyze the provided business intent, technical profile, and PII findings to generate a set of governance and data quality rules in JSON format.
            
            Integrate the following OWASP AI/LLM Security & Governance principles and DAMA-DMBoK data quality dimensions into your analysis:
            
            1. PII DISCLOSURE (OWASP LLM02/LLM06): Flag any column containing high-risk Personally Identifiable Information (emails, SSNs, names, etc.). 
               - Expectation: Use 'expect_column_values_to_match_regex' with a masking/cleansing intent.
               - Severity: Critical.
               - Reason: Legal compliance (GDPR/HIPAA) and risk of Verbatim Memorization leakage.
               - SANITY CHECK: Before flagging, cross-reference the PII entity type with the column name. For example, if 'DATE_TIME' is detected in a column named 'refresh_rate' or 'frequency', it is likely a false positive and should NOT be flagged as a PII violation.
            
            2. SDPI (Sensitive Data Protection for AI): Flag any data that could leak proprietary logic, internal system paths, or trade secrets.
               - Expectation: Use 'expect_column_values_to_be_masked' or 'expect_column_values_to_match_regex' with a masking/cleansing intent.
               - Severity: High.
               - Reason: Prevention of Intellectual Property loss and Model Inversion attacks.

            3. DATA COMPLETENESS & BIAS (OWASP LLM03): Flag significant gaps in critical columns.
               - Expectation: Use 'expect_column_values_to_not_be_null'.
               - Severity: Medium/High.
               - Reason: Prevention of Model Bias and protection against Training Data Poisoning in logical gaps.

            Six Dimensions of Data Quality (DAMA-DMBoK):
            1. Accuracy: How well does a piece of information reflect the reality it represents?
            2. Completeness: Does the data include all the required information?
            3. Consistency: Is the data consistent within the same dataset and across different datasets?
            4. Timeliness: Is the information available when it is needed?
            5. Uniqueness: Is there only one record of each entity in the dataset?
            6. Validity: Does the data conform to a specific format or standard?
            
            Context Provided:
            - Business Intent: {intent}
            - Data Profile Summary: {json.dumps(sanitized_profile)}
            - PII Detection Results: {json.dumps(pii_results)}
            
            Requirements:
            1. Return ONLY a JSON object with a key "rules" containing a list of rule objects.
            2. Columns: "column", "expectation", "severity", "reason", "dimension".
            3. Dimensions: "Accuracy", "Completeness", "Consistency", "Timeliness", "Uniqueness", "Validity", "PII Disclosure (OWASP)", "SDPI Compliance", "Data Completeness & Bias (OWASP)".
            
            Explicitly flag verified PII violations as 'Critical' rules. Use your architectural judgement to dismiss obvious false positives where technical detection (e.g. Presidio) conflicts with business context (column names).
            """
            
            response = model.generate_content(prompt)
            clean_text = response.text.strip().replace("```json", "").replace("```", "")
            usage_metadata = response.usage_metadata
            usage_data = {
                "prompt_token_count": usage_metadata.prompt_token_count,
                "candidates_token_count": usage_metadata.candidates_token_count,
                "total_token_count": usage_metadata.total_token_count,
                "model_name": model_name
            }
            return json.loads(clean_text), usage_data
            
        except Exception as e:
            print(f"AI Rule Synthesis Failed: {e}. Falling back to heuristics.")

    rules = []
    existing_rules = set()

    def add_rule(rule):
        if "dimension" not in rule:
            exp = rule["expectation"]
            if "not_be_null" in exp:
                rule["dimension"] = "Completeness"
            elif "match_regex" in exp:
                rule["dimension"] = "Validity"
            elif "between" in exp:
                rule["dimension"] = "Accuracy"
            elif "unique" in exp:
                rule["dimension"] = "Uniqueness"
            else:
                rule["dimension"] = "Consistency"
        key = (rule["column"], rule["expectation"])
        if key not in existing_rules:
            rules.append(rule)
            existing_rules.add(key)

    # 1. Dimension: Completeness (Missing Values)
    for col, stats in profile_summary["columns"].items():
        if stats.get("p_missing", 0) > 0:
            severity = "High" if stats["p_missing"] > 0.5 else "Medium"
            add_rule({
                "column": col,
                "expectation": "expect_column_values_to_not_be_null",
                "severity": severity,
                "reason": f"Column {col} has {stats['p_missing']*100:.1f}% missing values.",
                "dimension": "Completeness"
            })

    # 2. Dimension: Validity & Conformity (Format & Types)
    for col, stats in profile_summary["columns"].items():
        col_lower = col.lower()
        if "phone" in col_lower:
            add_rule({
                "column": col,
                "expectation": "expect_column_values_to_match_regex",
                "regex": r"^\+\d{1,3}-\d{1,4}-\d{3}-\d{4}((x|ext)\d{1,5})?$",
                "severity": "High",
                "reason": f"Standardize {col} to international format.",
                "dimension": "Validity"
            })
        elif "email" in col_lower:
            add_rule({
                "column": col,
                "expectation": "expect_column_values_to_match_regex",
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "severity": "High",
                "reason": f"Ensure {col} follows valid email format.",
                "dimension": "Validity"
            })
        elif "ssn" in col_lower:
             add_rule({
                "column": col,
                "expectation": "expect_column_values_to_match_regex",
                "regex": r"^\d{3}-\d{2}-\d{4}$",
                "severity": "Critical",
                "reason": f"Standardize {col} to XXX-XX-XXXX format.",
                "dimension": "Validity"
            })

    # 3. Dimension: Accuracy (Range & Outliers)
    for col, stats in profile_summary["columns"].items():
        if "min" in stats and "max" in stats:
            if pd.api.types.is_numeric_dtype(pd.Series([stats["min"]])):
                if "revenue" in col.lower() or "amount" in col.lower() or "price" in col.lower():
                    add_rule({
                        "column": col,
                        "expectation": "expect_column_values_to_be_between",
                        "min_value": 0,
                        "severity": "High",
                        "reason": f"{col} should not be negative.",
                        "dimension": "Accuracy"
                    })

    # 4. Dimension: Uniqueness (Identifiers)
    for col, stats in profile_summary["columns"].items():
        col_lower = col.lower()
        if any(x in col_lower for x in ["id", "pk", "key", "code"]):
            n_rows = profile_summary.get("n_rows", 1)
            if stats.get("n_distinct") == n_rows:
                add_rule({
                    "column": col,
                    "expectation": "expect_column_values_to_be_unique",
                    "severity": "High",
                    "reason": f"{col} appears to be a unique identifier.",
                    "dimension": "Uniqueness"
                })

    # 5. Dimension: Timeliness (Dates)
    for col, stats in profile_summary["columns"].items():
        if stats.get("type") == "DateTime" or "date" in col.lower():
            add_rule({
                "column": col,
                "expectation": "expect_column_values_to_be_in_past", # Conceptual
                "severity": "Medium",
                "reason": f"Ensure {col} contains historical/current dates only.",
                "dimension": "Timeliness"
            })

    # 6. Dimension: Consistency (Pattern matching for categories)
    for col, stats in profile_summary["columns"].items():
        if stats.get("n_distinct", 0) < 10 and stats.get("type") == "Categorical":
            add_rule({
                "column": col,
                "expectation": "expect_column_values_to_be_in_set", # Conceptual
                "severity": "Medium",
                "reason": f"Standardize {col} values to a consistent set.",
                "dimension": "Consistency"
            })
    
    if "marketing" in intent.lower():
        if "email" in profile_summary["columns"]:
            add_rule({
                "column": "email",
                "expectation": "expect_column_values_to_not_be_null",
                "severity": "High",
                "reason": "Critical for cross-channel consistency.",
                "dimension": "Consistency"
            })

    # PII Checks - FIX BUG: ignore "error" key in pii_results
    if isinstance(pii_results, dict) and "error" not in pii_results:
        for col, entities in pii_results.items():
            if entities:
                 add_rule({
                    "column": col,
                    "expectation": "expect_column_values_to_match_regex",
                    "severity": "Critical",
                    "reason": f"PII Detected ({', '.join(entities)}). Requires masking.",
                    "dimension": "Confidentiality"
                })

    return {"rules": rules}, None

def generate_remediation(failed_validations: list) -> dict:
    remediations = []
    for failure in failed_validations:
        col = failure["column"]
        expectation = failure["expectation"]
        if expectation == "expect_column_values_to_not_be_null":
            remediations.append({"issue": f"Missing values in {col}", "sql_fix": f"UPDATE table SET {col} = 'Unknown' WHERE {col} IS NULL;", "python_fix": f"df['{col}'].fillna('Unknown', inplace=True)"})
        elif expectation == "expect_column_values_to_be_between":
             remediations.append({"issue": f"Values out of range in {col}", "sql_fix": f"DELETE FROM table WHERE {col} < 0;", "python_fix": f"df = df[df['{col}'] >= 0]"})
        elif expectation == "expect_column_values_to_match_regex":
            remediations.append({"issue": f"Invalid format in {col}", "sql_fix": f"UPDATE table SET {col} = 'Unknown' WHERE {col} NOT REGEXP '{failure.get('regex', '.*')}';", "python_fix": f"# Fixes invalid formats based on regex\ndf.loc[~df['{col}'].astype(str).str.match(r'{failure.get('regex', '.*')}'), '{col}'] = 'Unknown'"})
    return {"remediations": remediations}

def llm_remediate_column(series: pd.Series, column_name: str, expectation: dict, failed_values: list, api_key: str) -> tuple:
    if not api_key:
        return series, None
    try:
        genai.configure(api_key=api_key)
        model, model_name = _get_available_model(api_key=api_key)
        if model is None:
            raise Exception("No suitable Gemini model found that supports generateContent.")
        batch_size = 50
        cleansed_series = series.copy()
        total_usage = {"prompt_token_count": 0, "candidates_token_count": 0, "total_token_count": 0, "model_name": model_name}
        for i in range(0, len(failed_values), batch_size):
            batch = failed_values[i:i + batch_size]
            prompt = f"""
            Act as a Data Quality Steward, adhering to DAMA-DMBoK principles. Your task is to cleanse a data column based on a failed quality rule.
            **Data Quality Context:**
            - **Column:** {column_name}
            - **Failed Rule:** {expectation.get('expectation', 'N/A')}
            - **Description:** {expectation.get('reason', 'N/A')}
            **Data for Cleansing (Batch):**
            {json.dumps(batch)}
            **Instructions:**
            1.  **Analyze** each value and correct it to meet the data quality rule.
            2.  **Accuracy is Paramount:** Do NOT fabricate data. If a value cannot be corrected with high confidence, return the token "[UNABLE_TO_CLEANSE]".
            3.  **Transparency:** For each correction, provide a brief "reason" for the change.
            4.  **Format:** Return a JSON object where keys are the original values and values are objects containing the "corrected_value" and "reason".
            **Example Response:**
            {{
                "original_value_1": {{"corrected_value": "corrected_value_1", "reason": "Reason for correction."}},
                "original_value_2": {{"corrected_value": "[UNABLE_TO_CLEANSE]", "reason": "Could not determine the correct value."}}
            }}
            """
            response = model.generate_content(prompt)
            clean_text = response.text.strip().replace("```json", "").replace("```", "")
            corrections = json.loads(clean_text)
            usage = response.usage_metadata
            total_usage["prompt_token_count"] += usage.prompt_token_count
            total_usage["candidates_token_count"] += usage.candidates_token_count
            total_usage["total_token_count"] += usage.total_token_count
            for original_value, correction in corrections.items():
                if correction["corrected_value"] != "[UNABLE_TO_CLEANSE]":
                    cleansed_series.replace(original_value, correction["corrected_value"], inplace=True)
        return cleansed_series, total_usage
    except Exception as e:
        print(f"LLM Remediation Failed for column {column_name}: {e}")
        return series, None

def generate_business_impact(intent: str, rules: list, failures: list, total_rows: int = 0, api_key: str = None, df_summary: dict = None) -> tuple:
    if api_key:
        try:
            genai.configure(api_key=api_key)
            model, model_name = _get_available_model(api_key=api_key)
            if model is None:
                raise Exception("No suitable Gemini model found that supports generateContent.")
            prompt_parts = [f"Act as a Chief Data Officer. Analyze the following data quality audit results for a dataset with Business Intent: '{intent}'.", f"--- Data Context ---", f"Total Rows: {total_rows}", f"Failed Records Impact: {sum(f.get('unexpected_count', 0) for f in failures)} issues detected."]
            if failures:
                prompt_parts.append("--- Key Deficiencies ---")
                for f in failures[:5]:
                     prompt_parts.append(f"- Column '{f.get('column')}' failed check '{f.get('expectation')}'. Count: {f.get('unexpected_count')}")
            if df_summary:
                prompt_parts.append(f"--- Financial/Operational Highligts ---")
                prompt_parts.append(df_summary)
            prompt_parts.append("--- GOAL ---")
            prompt_parts.append("Provide an 'Executive Data Brief' as a JSON object with the following keys: 'title', 'summary', and 'insights'.")
            prompt_parts.append("The 'insights' value should be a list of 3-5 bullet points, each with a 'title' and 'text'.")
            prompt_parts.append("Example: {'title': '...', 'summary': '...', 'insights': [{'title': 'Strategic Risk', 'text': '...'}, ... ]}")
            response = model.generate_content("\n".join(prompt_parts))
            clean_text = response.text.strip().replace("```json", "").replace("```", "")
            usage_metadata = response.usage_metadata
            usage_data = {
                "prompt_token_count": usage_metadata.prompt_token_count,
                "candidates_token_count": usage_metadata.candidates_token_count,
                "total_token_count": usage_metadata.total_token_count,
                "model_name": model_name
            }
            return json.loads(clean_text), usage_data
        except Exception as e:
            return {"error": f"AI Generation Failed: {str(e)}"}, None
    return _generate_heuristic_impact(intent, rules, failures, total_rows), None

def _generate_heuristic_impact(intent, rules, failures, total_rows):
    impact = {"title": "Data Quality Audit & Remediation Report", "summary": f"The data quality audit for the business objective '{intent}' is complete. The following insights are based on the remediation of data quality issues.", "insights": []}
    total_issues = sum(f.get("unexpected_count", 0) for f in failures)
    affected_pct = min(100, (total_issues / total_rows * 100)) if total_rows > 0 else 0
    impact["insights"].append({"title": "Data Hygiene Improvement", "text": f"{total_issues} data points were cleansed, increasing dataset reliability by approximately {affected_pct:.1f}%."})
    if "marketing" in intent.lower() and total_rows > 0:
        potential_conversion = total_issues * 0.05 
        est_value = potential_conversion * 100
        impact["insights"].append({"title": "Campaign ROI", "text": f"Potentially recovered ~${est_value:,.0f} in Lifecycle Value from {total_issues} invalid leads."})
    elif "financ" in intent.lower():
        impact["insights"].append({"title": "Risk Mitigation", "text": f"Reduced exposure to forecasting errors across {total_issues} financial records."})
    failed_cols = set(f["column"] for f in failures)
    if "marketing" in intent.lower():
        email_cols = [c for c in failed_cols if "email" in c.lower()]
        if email_cols:
             impact["insights"].append({"title": "Restored Reachability", "text": "Restored reachability for email campaigns by fixing invalid email formats."})
        phone_cols = [c for c in failed_cols if "phone" in c.lower()]
        if phone_cols:
             impact["insights"].append({"title": "Enabled SMS/Voice Channels", "text": "Enabled SMS/Voice channels by standardizing phone numbers."})
    return impact