import great_expectations as gx
# Run this file anytime against your data

context = gx.get_context()

# Create an expectation suite
expectation_suite_name = "CSV_HOSPITALS_EXPECTATIONS_SUITE"
context.add_or_update_expectation_suite(expectation_suite_name=expectation_suite_name)

datasource = context.sources.add_or_update_pandas(name="csv_hospitals")#any name
#datasource = context.sources.add_sql(name="my_datasource", connection_string=connection_string)

# Add one or more assets
asset = datasource.add_csv_asset(name="Hospitals", 
                                 filepath_or_buffer="Hospitals.csv",
                                 encoding="ISO-8859-1")

#Test everythign in that asset or set of data in batches?Prefer batches for large sets
batch_request = datasource.get_asset("Hospitals").build_batch_request()

#connects test cases to data set  -Validator
validator = context.get_validator(batch_request=batch_request,expectation_suite_name=expectation_suite_name,)

validator.expect_column_values_to_not_be_null(column = "ID")

#Create Expectations or Tests
validator.save_expectation_suite(discard_failed_expectations=False)

#Save expectatin suite
# Create a checkpoint to run expectations and store the results
checkpoint = context.add_or_update_checkpoint(
    name="CSV_HOSPITALS_CHECKPOINT",
    run_name_template="%Y%m%d-%H%M%S-csv-hosptials-run",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": expectation_suite_name,
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                { 
                    "name": "update_data_docs", 
                    "action": {"class_name": "UpdateDataDocsAction"}
                },
            ]
        }
    ]
)

# Run the checkpoint
checkpoint_result = checkpoint.run()

# Open the data docs
context.open_data_docs()