import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import upload

class Datagovsg_S3_ResourcesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IResourceController, inherit=True)

    def after_create(self, context, resource):
        # Call resource_update to upload files to S3 and update the CKAN db
        toolkit.get_action('resource_update')(context, resource)

    def after_update(self, context, resource):
        # Check if we need to upload the files to S3
        if resource['url_type'] == 'upload':
            upload.upload_resource_to_s3(context, resource)
            toolkit.get_action('resource_update')(context, resource)
        else:
            upload.upload_zipfiles_to_s3(context, resource)