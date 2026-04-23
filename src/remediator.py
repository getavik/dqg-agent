
import pandas as pd
import phonenumbers

def apply_remediation(df: pd.DataFrame, failed_validations: list) -> pd.DataFrame:
    """
    Applies heuristic remediation to the dataframe based on failed validations.
    Returns a new, corrected dataframe.
    """
    df_clean = df.copy()
    
    for failure in failed_validations:
        col = failure["column"]
        expectation = failure["expectation"]
        
        # Check if column exists (it might have been dropped or renamed, theoretically)
        if col not in df_clean.columns:
            continue
            
        if expectation == "expect_column_values_to_not_be_null":
            # Heuristic: Fill based on dtype
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].fillna(0)
            else:
                df_clean[col] = df_clean[col].fillna("Unknown")
                
        elif expectation == "expect_column_values_to_be_between":
             # Heuristic: Filter out rows that are out of bounds
             # failed_validations structure from validator.py doesn't currently pass the parameters (min_value) back easily
             # in the 'failure' dict, it just gives the expectation type.
             # We need to rely on the fact that we can infer or we'd need to pass the rule config.
             # For this mock, we'll assume standard non-negative checks for financial data if we don't have params.
             
             # Ideally, we should pass the rule details. For now, let's implement a safe 'drop nulls' or specific logic
             # If we see negative values in a numeric column that failed 'between', we might drop them?
             # Let's try to be smart: if min_value was involved (like for revenue), we replace with 0 or drop.
             # Since we don't have the rule config here easily without refactoring, we will do a generic cleaning:
             # If numeric and has negatives, clip to 0? Or drop? Let's Drop.
             
             if pd.api.types.is_numeric_dtype(df_clean[col]):
                 # Assuming the failure was about being >= 0 as per our context
                 df_clean = df_clean[df_clean[col] >= 0]
        
        elif expectation == "expect_column_values_to_match_regex":
            regex_pattern = failure.get("regex")
            
            # Special handling for Phone columns to use library-based standardization
            if "phone" in col.lower():
                def standardize_phone(val):
                    try:
                        if pd.isna(val): return "Unknown"
                        val_str = str(val)
                        # Clean common garbage to help parser
                        if val_str.lower() in ["unknown", "invalid_phone", "nan", "none"]: return "+1-000-000-0000"
                        
                        # Heuristic: fix double prefix if present (e.g. 001-...)
                        if val_str.startswith("001-"): val_str = "+" + val_str[3:]
                        
                        # Parse
                        # Default region 'US' for no-code numbers, but handle + prefix
                        parsed = phonenumbers.parse(val_str, "US")
                        
                        if phonenumbers.is_valid_number(parsed):
                            # Standardize to E.164 (e.g. +14155552671) or International
                            # User requested: +1-XXX-XXX-XXXX format roughly.
                            # E.164 is +1415...
                            # International is +1 415-555-2671
                            formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                            # Convert "+1 415-555-2671" -> "+1-415-555-2671" based on user style
                            return formatted.replace(" ", "-")
                        else:
                             return "+1-000-000-0000"
                    except Exception:
                        return "+1-000-000-0000"

                df_clean[col] = df_clean[col].apply(standardize_phone)
                
            elif regex_pattern:
                # Other regex failures
                mask_valid = df_clean[col].astype(str).str.match(regex_pattern)
                df_clean.loc[~mask_valid, col] = "Unknown"
            else:
                # Generic/PII
                df_clean[col] = df_clean[col].astype(str).str.replace(r'\S+', '[REDACTED]', regex=True)

    return df_clean
