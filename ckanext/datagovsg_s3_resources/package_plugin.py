'''package_plugin.py

DatagovsgS3ResourcesPackagePlugin
Extends plugins.SingletonPlugin
'''
import datetime
import logging

from routes.mapper import SubMapper
import ckan.plugins as plugins
import ckanext.datagovsg_s3_resources.upload as upload


class DatagovsgS3ResourcesPackagePlugin(plugins.SingletonPlugin):
    '''
    DatagovsgS3ResourcesPackagePlugin
    Extends plugins.SingletonPlugin

    1. Connects package download route
    2. Hooks into after_update to upload package zipfile to S3
    '''

    plugins.implements(plugins.IPackageController, inherit=True)
    plugins.implements(plugins.IRoutes, inherit=True)


    ##############################################################
    # IRoutes ####################################################
    ##############################################################

    def before_map(self, map):
        '''Connect our package controller to package download action'''
        m = SubMapper(
            map,
            controller='ckanext.datagovsg_s3_resources.controllers.package:\
                S3ResourcesPackageController')
        # Connect routes for package download
        m.connect('package_download',
                  '/dataset/{id}/download', action="package_download")
        return map


    ##############################################################
    # IPackageController #########################################
    ##############################################################

    def after_update(self, context, pkg_dict):
        '''after_update - uploads package zipfile to s3'''

        # Obtain logger
        logger = logging.getLogger(__name__)

        # Check context object
        # If originating from resource create or update, skip package zipfile
        # upload for now
        # For more information, read documentation in 'before_create_or_update'
        if 'resource_create_or_update' not in context:
            logger.info("Package after_update without originating from resource create/update")
            # Check if required config options exist
            if not upload.config_exists():
                # Log an error
                logger.error("Required S3 config options missing. Please check if required config options exist.")
                raise Exception('Required S3 config options missing')
            else:
                upload.upload_package_zipfile_to_s3(context, pkg_dict)
        else:
            # Skip package_zipfile upload
            logger.info("Package after_update originating from resource create/update... Skipping package zipfile upload")
 