import pandas as pd
import phonenumbers
from src.llm_engine import llm_remediate_column

def apply_remediation(df: pd.DataFrame, failed_validations: list, api_key: str = None) -> tuple:
    """
    Applies heuristic and LLM-based remediation to the dataframe based on failed validations.
    Returns a new, corrected dataframe and the usage metadata from the LLM.
    """
    df_clean = df.copy()
    total_usage = None
    
    for failure in failed_validations:
        col = failure["column"]
        expectation = failure["expectation"]
        
        if col not in df_clean.columns:
            continue
            
        if expectation == "expect_column_values_to_not_be_null":
            if pd.api.types.is_numeric_dtype(df_clean[col]):
                df_clean[col] = df_clean[col].fillna(0)
            else:
                df_clean[col] = df_clean[col].fillna("Unknown")
                
        elif expectation == "expect_column_values_to_be_between":
             if pd.api.types.is_numeric_dtype(df_clean[col]):
                 df_clean.loc[df_clean[col] < 0, col] = 0
        
        elif expectation == "expect_column_values_to_match_regex":
            regex_pattern = failure.get("regex")
            
            if "phone" in col.lower():
                def standardize_phone(val):
                    try:
                        if pd.isna(val): return "Unknown"
                        val_str = str(val)
                        if val_str.lower() in ["unknown", "invalid_phone", "nan", "none"]: return "+1-000-000-0000"
                        if val_str.startswith("001-"): val_str = "+" + val_str[3:]
                        parsed = phonenumbers.parse(val_str, "US")
                        if phonenumbers.is_valid_number(parsed):
                            formatted = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
                            return formatted.replace(" ", "-")
                        else:
                             return "+1-000-000-0000"
                    except Exception:
                        return "+1-000-000-0000"
                df_clean[col] = df_clean[col].apply(standardize_phone)

            elif api_key and regex_pattern:
                mask_valid = df_clean[col].astype(str).str.match(regex_pattern)
                failed_values = df_clean.loc[~mask_valid, col].unique().tolist()
                
                if failed_values:
                    df_clean[col], usage = llm_remediate_column(df_clean[col], col, failure, failed_values, api_key)
                    if usage:
                        if total_usage is None:
                            total_usage = usage
                        else:
                            total_usage["prompt_token_count"] += usage["prompt_token_count"]
                            total_usage["candidates_token_count"] += usage["candidates_token_count"]
            
            elif regex_pattern:
                mask_valid = df_clean[col].astype(str).str.match(regex_pattern)
                df_clean.loc[~mask_valid, col] = "Unknown"
            else:
                df_clean[col] = df_clean[col].astype(str).str.replace(r'\S+', '[REDACTED]', regex=True)

    return df_clean, total_usage
