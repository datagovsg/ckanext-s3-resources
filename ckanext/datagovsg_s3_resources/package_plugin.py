'''package_plugin.py

DatagovsgS3ResourcesPackagePlugin
Extends plugins.SingletonPlugin
'''

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
        upload.upload_package_zipfile_to_s3(context, pkg_dict)
 