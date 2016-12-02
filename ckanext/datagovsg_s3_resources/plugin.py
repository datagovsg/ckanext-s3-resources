import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import upload

class Datagovsg_S3_ResourcesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IResourceController, inherit=True)

    # IResourceController

    def before_create(self, context, resource):
        # Uploads resource to S3
        # WARNING: destructively modifies resource
        upload.upload_resource_to_s3(context, resource)


    def after_create(self, context, resource):
        # Uploads package and resource zip files to S3
        upload.upload_zipfiles_to_s3(context, resource)


    def before_update(self, context, current, resource):
        # Uploads resource to S3
        # WARNING: destructively modifies resource
        upload.upload_resource_to_s3(context, resource)


    def after_update(self, context, resource):
        # Check if we need to upload the files to S3\
        upload.upload_zipfiles_to_s3(context, resource)


