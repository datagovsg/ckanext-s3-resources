'''plugin.py

DatagovsgS3ResourcesPlugin
Extends plugins.SingletonPlugin
'''

import logging
import datetime
import ckan.plugins as plugins
from routes.mapper import SubMapper
import ckanext.datagovsg_s3_resources.upload as upload


class DatagovsgS3ResourcesPlugin(plugins.SingletonPlugin):
    '''
    DatagovsgS3ResourcesPlugin
    Extends plugins.SingletonPlugin

    1. Connects package and resource download routes
    2. Hooks into before_create, before_update to upload resource to S3
    3. Hooks into after_create, after_update to upload resource zipfile to S3
    '''

    plugins.implements(plugins.IResourceController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)

    ##############################################################
    # IRoutes ####################################################
    ##############################################################

    def before_map(self, map):
        '''Connect our package controller to resource download action'''
        m = SubMapper(
            map,
            controller='ckanext.datagovsg_s3_resources.controllers.package:\
                S3ResourcesPackageController')
        # Connect routes for resource download
        m.connect(
            'resource_download',
            '/dataset/{id}/resource/{resource_id}/download',
            action="resource_download")
        return map


    ##############################################################
    # IResourceController ########################################
    ##############################################################

    def before_create(self, context, resource):
        '''Runs before resource_create. Modifies resource destructively to put in the S3 URL'''

        # Check if required config options exist
        if not upload.config_exists():
            # Log an error
            logger = logging.getLogger(__name__)
            logger.error("Required S3 config options missing. Please check if required config options exist.")
            raise Exception('Required S3 config options missing')
        else:
            # Set timestamp for archiving
            context['s3_upload_timestamp'] = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H-%M-%SZ")

            # Only upload to S3 if not blacklisted
            if not upload.is_blacklisted(resource):
                upload.upload_resource_to_s3(context, resource)
            else:
                # Log a warning
                logger = logging.getLogger(__name__)
                logger.info("Resource %s from package %s is blacklisted and not uploaded to S3." % (resource['name'], resource['package_id']))

    def after_create(self, context, resource):
        '''Uploads resource zip file to S3
        Done after create instead of before to ensure metadata is generated correctly'''
        upload.upload_resource_zipfile_to_s3(context, resource)

    def before_update(self, context, _, resource):
        '''Runs before resource_update. Modifies resource destructively to put in the S3 URL'''

        # Check if required config options exist
        if not upload.config_exists():
            # Log an error
            logger = logging.getLogger(__name__)
            logger.info("Required S3 config options missing. Please check if required config options exist.")
            raise Exception('Required S3 config options missing')
        else:
            # Set timestamp for archiving
            context['s3_upload_timestamp'] = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H-%M-%SZ")

            # Only upload to S3 if not blacklisted
            if not upload.is_blacklisted(resource):
                upload.upload_resource_to_s3(context, resource)
            else:
                # Log a warning
                logger = logging.getLogger(__name__)
                logger.info("Resource %s from package %s is blacklisted and not uploaded to S3." % (resource['name'], resource['package_id']))

    def after_update(self, context, resource):
        '''Uploads resource zip file to S3
        Done after update instead of before to ensure metadata is generated correctly'''
        upload.upload_resource_zipfile_to_s3(context, resource)

        # Push data to datastore
        # Unfortunately we have to do this here because datapusher currently runs on the
        # IResourceUrlChange.notify hook which is getting passed as input the OLD resource
        # When we update a resource, the datapusher trigger is receiving the old URL, and so
        # we manually trigger the datapusher service after the resource has been updated.
        if plugins.plugin_loaded('datastore'):
            plugins.toolkit.c.pkg_dict = plugins.toolkit.get_action('datapusher_submit')(
                None, {'resource_id': resource['id']}
            )
