# cdc-code lambda test event

{
  "bucket_name": "my-dms-bucket",
  "table_name": "customers",
  "load_file_prefix": "dms/prod/customers/",
  "cdc_prefix": "dms/prod/customers/cdc/"
}

## lambda layer

AWSSDKPandas-Python313: arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python313:4
