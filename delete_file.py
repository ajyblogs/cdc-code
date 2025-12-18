import boto3

s3 = boto3.client("s3")

PROD_BUCKET = "my-prod-bucket"
FOLDERS = [
    "folder1/",
    "folder2/",
    "folder3/",
    "folder4/",
    "folder5/"
]

def lambda_handler(event, context):
    bucket = event.get("bucket")

    if bucket == '':

        for prefix in FOLDERS:
            s3.delete_objects(
                Bucket=bucket,
                Delete={
                    "Objects": [{"Key": prefix}],
                    "Quiet": True
                }
            )

        print("Delete executed for 5 folders in production bucket")
