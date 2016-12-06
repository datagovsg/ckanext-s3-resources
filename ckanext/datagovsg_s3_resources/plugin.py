import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import upload
from routes.mapper import SubMapper
import datetime

class Datagovsg_S3_ResourcesPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IRoutes

    def before_map(self, map):
        m = SubMapper(
            map,
            controller='ckanext.datagovsg_s3_resources.controllers.package:S3ResourcesPackageController')
        # import routes
        # print routes.url_for()
        m.connect('package_download',
                  '/dataset/{id}/download', action="package_download")
        m.connect(
            'resource_download',
            '/dataset/{id}/resource/{resource_id}/download',
            action="resource_download")
        return map

    # IResourceController

    def before_create(self, context, resource):
        # Set timestamp for archiving
        utc_datetime_now = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H:%M:%SZ")
        context['s3_upload_timestamp'] = utc_datetime_now
        # Uploads resource to S3
        # WARNING: destructively modifies resource
        upload.upload_resource_to_s3(context, resource)
        # Uploads package and resource zip files to S3
        upload.upload_zipfiles_to_s3(context, resource)


    def before_update(self, context, current, resource):
        # Set timestamp for archiving 
        utc_datetime_now = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H:%M:%SZ")
        context['s3_upload_timestamp'] = utc_datetime_now
        # Uploads resource to S3
        # WARNING: destructively modifies resource
        upload.upload_resource_to_s3(context, resource)
        # Uploads package and resource zip files to S3
        upload.upload_zipfiles_to_s3(context, resource)