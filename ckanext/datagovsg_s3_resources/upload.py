'''
upload.py

Contains functions that upload the resources/zipfiles to S3.

Also contains the MetadataYAMLDumper class to generate the metadata for zipfiles.
'''
import cgi
import os
import StringIO
import zipfile
import mimetypes
import collections
import logging
import datetime
from dateutil import parser

from slugify import slugify
from pylons import config
import boto3
import yaml
import requests

import paste.fileapp
import ckan.plugins.toolkit as toolkit
import ckan.lib.uploader as uploader
from ckan.common import request


def setup_s3_bucket():
    '''
    setup_s3_bucket - Grabs the required info from config file and initializes S3 connection
    '''
    aws_access_key_id = config.get('ckan.datagovsg_s3_resources.s3_aws_access_key_id')
    aws_secret_access_key = config.get('ckan.datagovsg_s3_resources.s3_aws_secret_access_key')
    aws_region_name = config.get('ckan.datagovsg_s3_resources.s3_aws_region_name')
    if not aws_access_key_id or not aws_secret_access_key:
        s3 = boto3.resource('s3')
    elif aws_region_name:
        s3 = boto3.resource('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region_name)
    else:
        s3 = boto3.resource('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)

    bucket_name = config.get('ckan.datagovsg_s3_resources.s3_bucket_name')
    bucket = s3.Bucket(bucket_name)

    return bucket


def upload_resource_to_s3(context, resource):
    '''
    upload_resource_to_s3

    Uploads resource to S3 and modifies the following resource fields:
    - 'upload'
    - 'url_type'
    - 'url'
    '''

    # Init logger
    logger = logging.getLogger(__name__)
    logger.info("Starting upload_resource_to_s3 for resource %s" % resource.get('name', ''))

    # Init connection to S3
    bucket = setup_s3_bucket()

    # Get content type and extension
    content_type, _ = mimetypes.guess_type(
        resource.get('url', ''))
    extension = mimetypes.guess_extension(content_type)

    # Upload to S3
    pkg = toolkit.get_action('package_show')(context, {'id': resource['package_id']})
    timestamp = datetime.datetime.utcnow() # should match the assignment in the ResourceUpload class
    s3_filepath = (pkg.get('name')
                   + '/'
                   + 'resources'
                   + '/'
                   + slugify(resource.get('name'), to_lower=True)
                   + '-'
                   + timestamp.strftime("%Y-%m-%dT%H-%M-%SZ")
                   + extension)

    # If file is currently being uploaded, the file is in resource['upload']
    if isinstance(resource.get('upload', None), cgi.FieldStorage):
        logger.info("File is being uploaded")
        resource['upload'].file.seek(0)
        body = resource['upload'].file
    # If resource.get('url_type') == 'upload' then the resource is in CKAN file system
    elif resource.get('url_type') == 'upload':
        logger.info("File is on CKAN file store")
        upload = uploader.ResourceUpload(resource)
        filepath = upload.get_path(resource['id'])
        try:
            body = open(filepath, 'r')
        except OSError:
            abort(404, _('Resource data not found'))
    else:
        logger.info("File is downloadable from URL")
        try:
            # Start session to download files
            session = requests.Session()
            logger.info("Attempting to obtain resource %s from url %s" % (resource.get('name',''), resource.get('url', '')))
            response = session.get(
                resource.get('url', ''), timeout=30)
            # If the response status code is not 200 (i.e. success), raise Exception
            if response.status_code != 200:
                logger.error("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
                raise Exception("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
            body = response.content
            logger.info("Successfully obtained resource %s from url %s" % (resource.get('name',''), resource.get('url', '')))

        except requests.exceptions.RequestException:
            toolkit.abort(404, toolkit._(
                'Resource data not found'))

    try:
        logger.info("Uploading resource %s to S3" % resource.get('name', ''))
        bucket.Object(s3_filepath).delete()
        obj = bucket.put_object(Key=s3_filepath,
                                Body=body,
                                ContentType=content_type)
        obj.Acl().put(ACL='public-read')
        logger.info("Successfully uploaded resource %s to S3" % resource.get('name', ''))

    except Exception as exception:
        # Log the error and reraise the exception
        logger.error("Error uploading resource %s from package %s to S3" % (resource['name'], resource['package_id']))
        logger.error(exception)
        if resource.get('url_type') == 'upload':
            body.close()
        raise exception

    if resource.get('url_type') == 'upload':
        body.close()

    # Modify fields in resource
    resource['upload'] = ''
    resource['url_type'] = 's3'
    resource['url'] = config.get('ckan.datagovsg_s3_resources.s3_url_prefix') + s3_filepath
    update_timestamp(resource, timestamp)


def upload_resource_zipfile_to_s3(context, resource):
    '''
    upload_resource_zipfile_to_s3 - Uploads the resource zip file to S3
    '''

    # Init logger
    logger = logging.getLogger(__name__)
    logger.info("Starting upload_resource_zipfile_to_s3 for resource %s" % resource.get('name', ''))
    
    # If resource is an API, skip upload
    if resource.get('format', '') == 'API':
        return

    # Get resource's package
    pkg = toolkit.get_action('package_show')(context, {'id': resource['package_id']})

    # Initialize resource zip file
    resource_buff = StringIO.StringIO()
    resource_zip_archive = zipfile.ZipFile(resource_buff, mode='w')

    # Initialize metadata
    metadata = toolkit.get_action(
        'package_metadata_show')(data_dict={'id': pkg['id']})
    metadata_yaml_buff = StringIO.StringIO()
    metadata_yaml_buff.write(unicode("# Metadata for %s\r\n" % pkg[
                             "title"]).encode('ascii', 'ignore'))
    yaml.dump(prettify_json(metadata),
              metadata_yaml_buff, Dumper=MetadataYAMLDumper)

    # Write metadata to package and updated resource zip
    resource_zip_archive.writestr(
        'metadata-' + pkg.get('name') + '.txt', metadata_yaml_buff.getvalue())

    # Obtain extension type of the resource
    resource_extension = os.path.splitext(resource['url'])[1]
    filename = (slugify(resource['name'], to_lower=True)
                + resource_extension)

    # Case 1: Resource is not on s3 yet, need to download from CKAN
    if resource.get('url_type') == 'upload':
        logger.info("Obtaining resource file from CKAN for resource %s" % resource.get('name', ''))
        upload = uploader.ResourceUpload(resource)
        filepath = upload.get_path(resource['id'])

        resource_zip_archive.write(filepath, filename)

    # Case 2: Resource exists outside of CKAN, we should have a URL to download it
    else:
        # Try to download the resource from the provided URL
        try:
            logger.info("Obtaining file from URL %s" % resource.get('url', ''))
            session = requests.Session()
            response = session.get(resource.get('url', ''), timeout=30)
            # If the response status code is not 200 (i.e. success), raise Exception
            if response.status_code != 200:
                logger.error("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
                raise Exception("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
            logger.info("Successfully obtained file from URL %s" % resource.get('url', ''))
        except requests.exceptions.RequestException:
            toolkit.abort(404, toolkit._('Resource data not found'))

        resource_zip_archive.writestr(filename, response.content)

    # Initialize connection to S3
    bucket = setup_s3_bucket()

    # Upload the resource zip to S3
    resource_zip_archive.close()
    resource_filename = (pkg.get('name')
                         + '/'
                         + 'resources'
                         + '/'
                         + slugify(resource.get('name'), to_lower=True)
                         + '.zip')
    try:
        logger.info("Uploading resource zipfile to S3 for resource %s" % resource.get('name', ''))
        obj = bucket.put_object(
            Key=resource_filename,
            Body=resource_buff.getvalue(),
            ContentType='application/zip'
        )
        # Set permissions of the S3 object to be readable by public
        obj.Acl().put(ACL='public-read')
        logger.info("Successfully uploaded resource zipfile to S3 for resource %s" % resource.get('name', ''))
    except Exception as exception:
        # Log the error and reraise the exception
        logger.error("Error uploading resource %s zipfile to S3" % (resource['name']))
        logger.error(exception)
        raise exception

def upload_package_zipfile_to_s3(context, pkg_dict):
    '''
    upload_zipfiles_to_s3

    Uploads package zipfile to S3
    '''

    # Obtain package
    pkg = toolkit.get_action('package_show')(data_dict={'id': pkg_dict['id']})

    # Init logger
    logger = logging.getLogger(__name__)
    logger.info("Starting upload_package_zipfile_to_S3 for package %s" % pkg.get('name', ''))

    # If all resources are APIs, don't upload the zipfile
    if resources_all_api(pkg.get('resources')):
        logger.info("All resources are APIs, skipping package zipfile upload")
        return

    # Obtain package and package metadata
    metadata = toolkit.get_action(
        'package_metadata_show')(data_dict={'id': pkg['id']})

    # Initialize package zip file
    package_buff = StringIO.StringIO()
    package_zip_archive = zipfile.ZipFile(package_buff, mode='w')

    # Initialize metadata
    metadata_yaml_buff = StringIO.StringIO()
    metadata_yaml_buff.write(unicode("# Metadata for %s\r\n" % pkg[
                             "title"]).encode('ascii', 'ignore'))
    yaml.dump(prettify_json(metadata),
              metadata_yaml_buff, Dumper=MetadataYAMLDumper)

    # Write metadata to package and updated resource zip
    package_zip_archive.writestr(
        'metadata-' + pkg.get('name') + '.txt', metadata_yaml_buff.getvalue())

    # Start session to make requests: for downloading files from S3
    session = requests.Session()

    # Iterate over resources, downloading and storing them in the package zip file
    for resource in pkg.get('resources'):
        resource_extension = os.path.splitext(resource['url'])[1]
        filename = (slugify(resource['name'], to_lower=True)
                    + resource_extension)

        # Case 1: Resource is API, skip it
        if resource.get('format') == 'API':
            continue
        # Case 2: Resource is uploaded to CKAN server
        elif resource.get('url_type') == 'upload':
            logger.info("Obtaining resource file from CKAN for resource %s" % resource.get('name', ''))
            upload = uploader.ResourceUpload(resource)
            filepath = upload.get_path(resource['id'])
            package_zip_archive.write(filepath, filename)

        # Case 3: Resource is not on CKAN, should have a URL to download it from
        else:
            # Try to download the resource from the resource URL
            try:
                logger.info("Obtaining file from URL %s" % resource.get('url', ''))
                response = session.get(resource.get('url', ''), timeout=30)
                # If the response status code is not 200 (i.e. success), raise Exception
                if response.status_code != 200:
                    logger.error("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
                    raise Exception("Error obtaining resource from the given URL. Response status code is %d" % response.status_code)
                logger.info("Successfully obtained file from URL %s" % resource.get('url', ''))
            except requests.exceptions.RequestException:
                toolkit.abort(404, toolkit._('Resource data not found'))

            package_zip_archive.writestr(filename, response.content)


    # Initialize connection to S3
    bucket = setup_s3_bucket()

    # Upload package zip to S3
    package_zip_archive.close()
    package_file_name = (pkg.get('name')
                         + '/'
                         + pkg.get('name')
                         + '.zip')
    try:
        logger.info("Uploading package zipfile to S3 for package %s" % pkg.get('name', ''))
        obj = bucket.put_object(
            Key=package_file_name,
            Body=package_buff.getvalue(),
            ContentType='application/zip'
        )
        # Set object permissions to public readable
        obj.Acl().put(ACL='public-read')
        logger.info("Successfully uploaded package zipfile to S3 for package %s" % pkg.get('name', ''))
    except Exception as exception:
        # Log the error and reraise the exception
        logger.error("Error uploading package %s zip to S3" % (pkg['id']))
        logger.error(exception)
        raise exception

def resources_all_api(resources):
    for resource in resources:
        if resource.get('format', '') != 'API':
            return False
    return True

def is_blacklisted(resource):
    '''is_blacklisted - Check if the resource type is blacklisted'''
    blacklist = config.get('ckan.datagovsg_s3_resources.upload_filetype_blacklist', '').split()
    blacklist = [t.lower() for t in blacklist]
    resource_format = resource.get('format', '').lower()
    # If resource is being created, format will still be empty. Use file extension instead
    if resource_format == '':
        _, file_ext = os.path.splitext(resource.get('url'))
        resource_format = file_ext[1:].lower()
    return resource_format in blacklist

def update_timestamp(resource, timestamp):
    '''use the last modified time if it exists, otherwise use the created time.

    destructively modifies resource'''
    if resource.get('last_modified') is None and resource.get('created') is None:
        resource['created'] = timestamp
    else:
        resource['last_modified'] = timestamp


class MetadataYAMLDumper(yaml.SafeDumper):
    '''
    class MetadataYAMLDumper

    Used to generate metadata for the CKAN resources/packages
    '''
    def __init__(self, *args, **kws):
        kws['default_flow_style'] = False
        kws['explicit_start'] = True
        kws['line_break'] = '\r\n'

        super(MetadataYAMLDumper, self).__init__(*args, **kws)

    def expect_block_sequence(self):
        '''expect_block_sequence - add the first indentation for list'''
        self.increase_indent(flow=False, indentless=False)
        self.state = self.expect_first_block_sequence_item

    def expect_block_sequence_item(self, first=False):
        '''expect_block_sequence_item - modify this to add extra line breaks'''
        if not first and isinstance(self.event, yaml.SequenceEndEvent):
            self.indent = self.indents.pop()
            self.state = self.states.pop()
        else:
            self.write_indent()
            self.write_indicator(u'-', True, indention=True)
            # add a line break for sequence items which have mapping type
            if isinstance(self.event, yaml.MappingStartEvent):
                self.write_line_break()
            self.states.append(self.expect_block_sequence_item)
            self.expect_node(sequence=True)

    def represent_odict(self, data):
        '''represent_odict - represent OrderedDict'''
        value = list()
        node = yaml.nodes.MappingNode(
            'tag:yaml.org,2002:map', value, flow_style=None)
        if self.alias_key is not None:
            self.represented_objects[self.alias_key] = node
        for item_key, item_value in data.items():
            node_key = self.represent_data(item_key)
            node_value = self.represent_data(item_value)
            value.append((node_key, node_value))
        node.flow_style = False
        return node

    def choose_scalar_style(self):
        '''choose_scalar_style - single quotes'''
        is_dict_key = self.states[-1] == self.expect_block_mapping_simple_value
        if is_dict_key:
            return None
        return "'"

MetadataYAMLDumper.add_representer(
    collections.OrderedDict, MetadataYAMLDumper.represent_odict)


# Helper functions

def prettify_json(json):
    '''prettify_json - removes leading and trailing whitespace'''
    if isinstance(json, dict):
        for key in json.keys():
            prettified_name = key.replace('_', ' ').title()
            json[prettified_name] = prettify_json(json.pop(key))
    elif isinstance(json, list):
        return [prettify_json(obj) for obj in json]
    elif isinstance(json, basestring):
        # remove leading and trailing white spaces, new lines, tabs
        json = json.strip(' \t\n\r')
    return json


def is_downloadable_url(url):
    '''is_downloadable_url - check if url is downloadable'''
    content_type, _ = mimetypes.guess_type(url)
    if content_type and content_type != 'text/html':
        return True
    return False


def config_exists():
    '''config_exists - checks for the required s3 config options'''
    access_key = config.get('ckan.datagovsg_s3_resources.s3_aws_access_key_id')
    secret_key = config.get('ckan.datagovsg_s3_resources.s3_aws_secret_access_key')
    bucket_name = config.get('ckan.datagovsg_s3_resources.s3_bucket_name')
    url = config.get('ckan.datagovsg_s3_resources.s3_url_prefix')

    return not (access_key is None or
                secret_key is None or
                bucket_name is None or
                url is None)
