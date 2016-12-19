'''plugin.py

DatagovsgS3ResourcesPlugin
Extends plugins.SingletonPlugin
'''

import mimetypes
import datetime
import ckan.plugins as plugins
import ckan.logic as logic
from routes.mapper import SubMapper
from pylons import config
import upload


class DatagovsgS3ResourcesPlugin(plugins.SingletonPlugin):
    '''DatagovsgS3ResourcesPlugin
    Extends plugins.SingletonPlugin

    1. Connects package and resource download routes
    2. Hooks into before_create, before_update to upload resource to S3
    3. Hooks into after_create, after_update to upload zipfiles to S3
    '''

    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)

    # IRoutes

    def before_map(self, map):
        '''Connect our package controller to package and resource download actions'''
        m = SubMapper(
            map,
            controller='ckanext.datagovsg_s3_resources.controllers.package:\
                S3ResourcesPackageController')
        # Connect routes for package and resource download
        m.connect('package_download',
                  '/dataset/{id}/download', action="package_download")
        m.connect(
            'resource_download',
            '/dataset/{id}/resource/{resource_id}/download',
            action="resource_download")
        return map

    # IResourceController

    def before_create(self, context, resource):
        '''Runs before resource_create. Modifies resource destructively to put in the S3 URL'''

        # Check if required config options exist
        if not upload.config_exists():
            raise Exception('Config options for S3 resources extension missing.')

        # Set timestamp for archiving
        utc_datetime_now = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H:%M:%SZ")
        context['s3_upload_timestamp'] = utc_datetime_now

        # If filetype of resource is blacklist, skip the upload to S3
        content_type, content_enc = mimetypes.guess_type(resource.get('url', ''))
        if content_type is not None:
            extension = mimetypes.guess_extension(content_type)
            blacklist = config.get('ckan.s3_resources.upload_filetype_blacklist').split()
            blacklist = [t.lower() for t in blacklist]
            # ignore leading dot in extension
            if extension.lower()[1:] not in blacklist:
                # Uploads resource to S3
                # WARNING: destructively modifies resource
                upload.upload_resource_to_s3(context, resource)

    def after_create(self, context, resource):
        '''Uploads package and resource zip files to S3
        Done after create instead of before to ensure metadata is generated correctly'''
        upload.upload_zipfiles_to_s3(context, resource)

    def before_update(self, context, current, resource):
        '''Runs before resource_update. Modifies resource destructively to put in the S3 URL'''

        # Check if required config options exist
        if not upload.config_exists():
            raise Exception('Config options for S3 resources extension missing.')

        # Set timestamp for archiving
        utc_datetime_now = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H:%M:%SZ")
        context['s3_upload_timestamp'] = utc_datetime_now

        # If filetype of resource is blacklist, skip the upload to S3
        content_type, content_enc = mimetypes.guess_type(resource.get('url', ''))
        if content_type is not None:
            extension = mimetypes.guess_extension(content_type)
            blacklist = config.get('ckan.s3_resources.upload_filetype_blacklist').split()
            blacklist = [t.lower() for t in blacklist]
            # ignore leading dot in extension
            if extension.lower()[1:] not in blacklist:
                # Uploads resource to S3
                # WARNING: destructively modifies resource
                upload.upload_resource_to_s3(context, resource)


    def after_update(self, context, resource):
        '''Uploads package and resource zip files to S3
        Done after create instead of before to ensure metadata is generated correctly'''
        upload.upload_zipfiles_to_s3(context, resource)

        # Push data to datastore
        # Unfortunately we have to do this here because datapusher currently runs on the
        # IResourceUrlChange.notify hook which is getting passed as input the OLD resource
        if plugins.plugin_loaded('datastore'):
            plugins.toolkit.c.pkg_dict = plugins.toolkit.get_action('datapusher_submit')(
                None, {'resource_id': resource['id']}
            )
