'''Adds paster command to migrate existing CKAN resources to S3'''
import copy
import datetime
import logging

import ckan.model as model
import ckan.lib.cli as cli
import ckan.plugins.toolkit as toolkit
import ckan.logic as logic
from pylons import config

import ckanext.datagovsg_s3_resources.upload as upload


class MigrateToS3(cli.CkanCommand):
    '''Migrate existing resources to S3

      Usage:
          migrate_s3 - uploads all resources that are currently not on S3
            to S3 and updates the URL on CKAN

          migrate_s3 force_s3 - uploads ALL resources to S3

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 1
    min_args = 0

    def command(self):
        '''Runs on the migrate_s3 command'''
        self._load_config()

        self.skip_existing_s3_upload = True

        if len(self.args) > 0:
            if self.args[0] == 'force_s3':
                skip_existing_s3_upload = False

        user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        context = {
            'ignore_auth': True
        }

        # package_names (list) - list of dataset names
        # pkg_crashes_w_error (list) - list of dicts with two fields: 'pkg_name' and 'error'
        # logger - logger object used to log messages
        package_names = toolkit.get_action('package_list')(context, {})
        self.pkg_crashes_w_error = []
        logger = logging.getLogger(__name__)

        for package_name in package_names:
            self.migrate_package_to_s3(context, package_name)

        logger.info("Package Crashes (1st round) = \n%s", self.pkg_crashes_w_error)
        logger.info("Attempting to reupload the failed packages")

        pkg_crashes_w_error_first_round = copy.copy(self.pkg_crashes_w_error)
        self.pkg_crashes_w_error = []
        for package_name_and_error in pkg_crashes_w_error_first_round:
            self.migrate_package_to_s3(context, package_name_and_error['pkg_name'])

        logger.info("Package Crashes = \n%s", self.pkg_crashes_w_error)

    def change_to_s3(self, context, resource):
        '''change_to_s3 - performs resource_update. The before and after update hooks
        upload the resource and the resource/package zipfiles to S3
        '''
        toolkit.get_action('resource_update')(context, resource)

    def migrate_package_to_s3(self, context, package_name):
        '''migrate_package_to_s3 - Migrates package to S3 by calling resource_update on each resource.
        '''
        # Obtain logger
        logger = logging.getLogger(__name__)
        logger.info("Starting package migration to S3 for package %s", package_name)
        try:
            pkg = toolkit.get_action('package_show')(context, {'id': package_name})
            if pkg.get('num_resources') > 0:
                for resource in pkg.get('resources'):
                    # If the resource is already uploaded to S3, don't reupload
                    if self.skip_existing_s3_upload and resource['url_type'] == 's3':
                        logger.info("Resource %s is already on S3, skipping to next resource.", resource.get('name', ''))
                        continue
                    # If filetype of resource is blacklisted, skip the upload to S3
                    if not upload.is_blacklisted(resource):
                        try:
                            logger.info("Attempting to migrate resource %s to S3...", resource.get('name', ''))
                            self.change_to_s3(context, resource)
                            logger.info("Successfully migrated resource %s to S3.", resource.get('name', ''))
                        except Exception as error:
                            logger.error("Error when migrating resource %s - %s", resource.get('name', ''), error)
                            raise error
                    else:
                        logger.info("Resource %s is blacklisted, skipping to next resource.", resource.get('name', ''))

                        # Upload resource and package zipfile to S3
                        # If not blacklisted, will be done automatically as part of resource_update.
                        # If blacklisted, we still want to upload the package zipfile, so we do it here.
                        upload.upload_resource_zipfile_to_s3(context, resource)
                        upload.upload_package_zipfile_to_s3(context, pkg)

        except Exception as error:
            logger.error("Error when migrating package %s with error %s", package_name, error)
            self.pkg_crashes_w_error.append({'pkg_name': package_name, 'error': error})
        finally:
            # Cleanup sqlalchemy session
            # Required to prevent errors when uploading remaining packages
            model.Session.remove()
