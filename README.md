# CKAN Extension to upload resources to AWS S3

This extension can be used to upload your CKAN resources to AWS S3 instead of the CKAN server. You can blacklist certain resource types (e.g. APIs) from being uploaded to S3. A paster command is provided to migrate resources to S3.

## Requirements

* `boto3` - for connecting to S3

## Setup/Configuration

For the extension to work, you need to ensure that the following configuration options have been set in your configuration file `*.ini`:

* `ckan.s3_resources.s3_aws_access_key_id` - AWS access key ID. Obtained from AWS.
* `ckan.s3_resources.s3_aws_secret_access_key` - AWS secret access key ID. Obtained from AWS.
* `ckan.s3_resources.s3_bucket_name` - The name of the bucket on S3 to upload the resources to.
* `ckan.s3_resources.s3_url` - Base URL
    * Resource URLs will be in the form of `<base_url><package_name>/<resource_filename>`
    * Package zip URLs will be in the form of `<base_url><package_name>/<package_name>.zip`
    * e.g. `ckan.s3_resources.s3_url = https://bucket-name.s3.amazonaws.com/`
* `ckan.s3_resources.archive_old_resources` - True/False. If set to True, whenever any resource/package gets updated, a timestamped copy is uploaded to an archive directory in S3
* `ckan.s3_resources.upload_filetype_blacklist` - A space separated list of file formats to ignore.
    * e.g. `ckan.s3_resources.upload_filetype_blacklist = csv pdf xls`

## Migration

The extension includes a paster command to help migrate the existing resources to S3. The command can be run by doing:

`paster --plugin=plugin_name migrate_s3`