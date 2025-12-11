import great_expectations as gx
import pandas as pd

def validate_data(df: pd.DataFrame, rules: list) -> dict:
    """
    Runs Great Expectations validations based on generated rules.
    """
    context = gx.get_context()
    datasource_name = "my_pandas_datasource"
    asset_name = "my_df_asset"
    
    # Setup datasource (simplified for in-memory df)
    # In newer GX versions, we can just use validate directly on df or setup ephemeral datasource
    # For simplicity and compatibility, we'll use the validator object
    
    batch_definition = context.data_sources.add_pandas(datasource_name).add_dataframe_asset(name=asset_name).add_batch_definition(name="my_batch_def")
    
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    
    expectation_suite_name = "dqg_suite"
    suite = context.suites.add(gx.ExpectationSuite(name=expectation_suite_name))
    
    # Add expectations to suite
    for rule in rules:
        col = rule["column"]
        exp_type = rule["expectation"]
        
        # Map simple string expectations to GX objects
        # This is a simplified mapping
        if exp_type == "expect_column_values_to_not_be_null":
            suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column=col))
        elif exp_type == "expect_column_values_to_be_between":
            # Assuming min_value is provided in rule, defaulting to 0 if not for this demo
            min_val = rule.get("min_value", 0)
            suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column=col, min_value=min_val))
        elif exp_type == "expect_column_values_to_match_regex":
            regex = rule.get("regex", ".*")
            suite.add_expectation(gx.expectations.ExpectColumnValuesToMatchRegex(column=col, regex=regex))
            
    # Run validation
    validation_result = batch.validate(suite)
    
    # Process results
    results_summary = {
        "success": validation_result.success,
        "statistics": validation_result.statistics,
        "failures": []
    }
    
    for res in validation_result.results:
        if not res.success:
            results_summary["failures"].append({
                "column": res.expectation_config.kwargs.get("column"),
                "regex": res.expectation_config.kwargs.get("regex"),
                "expectation": res.expectation_config.type,
                "unexpected_count": res.result.get("unexpected_count"),
                "unexpected_percent": res.result.get("unexpected_percent")
            })
            
    return results_summary
