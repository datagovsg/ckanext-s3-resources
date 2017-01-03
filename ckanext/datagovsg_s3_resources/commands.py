'''Adds paster command to migrate existing CKAN resources to S3'''
import datetime

import ckan.model as model
import ckan.lib.cli as cli
import ckan.plugins.toolkit as toolkit
import ckan.logic as logic
from pylons import config

import ckanext.datagovsg_s3_resources.upload as upload


class MigrateToS3(cli.CkanCommand):
    '''Migrate existing resources to S3

      Usage:
          migrate_s3 - uploads all resources to S3 and updates the URL on CKAN

    '''
    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 0
    min_args = 0

    def command(self):
        '''Runs on the migrate_s3 command'''
        self._load_config()

        user = toolkit.get_action('get_site_user')({'model': model, 'ignore_auth': True}, {})
        context = {
            'model': model,
            'session': model.Session,
            'user': user['name'],
            'ignore_auth': True
        }
        # Set timestamp for archiving
        context['s3_upload_timestamp'] = datetime.datetime.utcnow().strftime("-%Y-%m-%dT%H:%M:%SZ")

        # dataset_names (list) - list of dataset names
        # key_errors (int) - count of key errors encountered during migration
        # validation_errors (int) - count of validation errors encountered during migration
        # other_errors_list (list) - list of (non-key and non-validation) errors encountered during migration
        # pkg_crashes (set) - set of package IDs of packages that encountered errors during migration
        dataset_names = toolkit.get_action('package_list')(context, {})
        key_errors = 0
        validation_errors = 0
        other_errors_list = []
        pkg_crashes = set()

        # blacklist (list) - list of filetypes that we want to avoid uploading
        # Obtain the space separated string from config, then split to obtain a list
        # and convert elements to lowercase
        blacklist = config.get('ckan.datagovsg_s3_resources.upload_filetype_blacklist', '').split()
        blacklist = [t.lower() for t in blacklist]

        # blacklisted (list) - Resources that have blacklisted filetypes. 
        #                      List of dicts with two fields: 'resource_id' and 'extension'
        # not_blacklist (int) - count of resources that have blacklisted filetypes
        # extensions_seen (set) - set of all filetypes that exist in our database
        blacklisted = []
        not_blacklisted = 0
        extensions_seen = set()

        for dataset_name in dataset_names:
            pkg = toolkit.get_action('package_show')(context, {'id': dataset_name})
            if pkg.get('num_resources') > 0:
                for resource in pkg.get('resources'):
                    # If the resource is already uploaded to S3, don't reupload
                    if resource['url_type'] == 's3':
                        continue
                    # If filetype of resource is blacklisted, skip the upload to S3
                    extension = resource['format'].lower()
                    extensions_seen.add(extension)
                    if extension not in blacklist:
                        not_blacklisted += 1
                        try:
                            self.change_to_s3(context, resource)
                        except logic.ValidationError:
                            validation_errors += 1
                            pkg_crashes.add(pkg['id'])
                        except KeyError:
                            key_errors += 1
                            pkg_crashes.add(pkg['id'])
                        except Exception as error:
                            other_errors_list.append({'id': pkg['id'], 'error': error})
                            pkg_crashes.add(pkg['id'])
                    else:
                        blacklisted.append({'resource_id': resource['id'], 'id': extension})

        print "NUMBER OF KEY ERROR CRASHES =", key_errors
        print "NUMBER OF VALIDATION ERROR CRASHES =", validation_errors
        print "NUMBER OF OTHER ERROR CRASHES =", len(other_errors_list)
        print "NUMBER OF PACKAGE CRASHES =", len(pkg_crashes)
        print "PACKAGE_IDs =", pkg_crashes
        print "OTHER ERRORS =", other_errors_list
        print "NOT BLACKLISTED =", not_blacklisted
        print "BLACKLISTED =", blacklisted
        print "EXTENSIONS SEEN =", extensions_seen

    def change_to_s3(self, context, resource):
        '''
        1. Uploads resource to S3
        2. Peforms resource_update
        3. Uploads the updated zipfiles to S3
        '''
        upload.migrate_to_s3_upload(context, resource)
        toolkit.get_action('resource_update')(context, resource)
        upload.upload_zipfiles_to_s3(context, resource)
