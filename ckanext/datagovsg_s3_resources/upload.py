from slugify import slugify
from pylons import config
import ckan.plugins.toolkit as toolkit
import ckan.lib.uploader as uploader
import boto3
import yaml
import paste.fileapp
import StringIO
import zipfile
import requests
import mimetypes
import cgi
import collections

def upload_resource_to_s3(context, rsc):
    # Init connection to s3
    s3 = boto3.resource('s3')
    bucket_name = config.get('ckan.s3_resources.s3_bucket_name')
    bucket = s3.Bucket(bucket_name)

    # Get content type and extension
    content_type, content_enc = mimetypes.guess_type(
        rsc.get('url', ''))
    extension = mimetypes.guess_extension(content_type)

    # Get location of file on CKAN server
    upload = uploader.ResourceUpload(rsc)
    filepath = upload.get_path(rsc['id'])

    # Upload to S3
    pkg = toolkit.get_action('package_show')(context, {'id': rsc['package_id']})
    s3_filepath = pkg.get('name') + '/' + slugify(rsc['name'], to_lower=True) + extension
    obj = bucket.put_object(Key=s3_filepath, Body=open(filepath, 'r'), ContentType=content_type)
    obj.Acl().put(ACL='public-read')

    # Modify fields in resource
    rsc['url_type'] = ''
    rsc['url'] = config.get('ckan.s3_resources.s3_url') + s3_filepath
    rsc['upload'] = ''
    rsc['s3'] = True


def upload_zipfiles_to_s3(context, new_rsc):
    # Get resource's package
    pkg = toolkit.get_action('package_show')(context, {'id': new_rsc['package_id']})

    # Obtain package and package metadata
    metadata = toolkit.get_action(
        'package_metadata_show')(data_dict={'id': pkg['id']})

    # Initialize package zip file
    package_buff = StringIO.StringIO()
    package_zip_archive = zipfile.ZipFile(package_buff, mode='w')

    # Initialize resource zip file
    new_rsc_buff = StringIO.StringIO()
    new_rsc_zip_archive = zipfile.ZipFile(new_rsc_buff, mode='w')

    # Initialize metadata
    # Package and resources have the same metadata file
    metadata_yaml_buff = StringIO.StringIO()
    metadata_yaml_buff.write(unicode("# Metadata for %s\r\n" % pkg[
                             "title"]).encode('ascii', 'ignore'))
    yaml.dump(prettify_json(metadata),
              metadata_yaml_buff, Dumper=MetadataYAMLDumper)

    # Write metadata to package and updated resource zip
    package_zip_archive.writestr(
        'metadata-' + pkg['name'] + '.txt', metadata_yaml_buff.getvalue())
    new_rsc_zip_archive.writestr(
        'metadata-' + pkg['name'] + '.txt', metadata_yaml_buff.getvalue())

    # Start session to make requests: for downloading files from S3
    session = requests.Session()

    # Find extension and content-type of the resource
    content_type, content_enc = mimetypes.guess_type(
        new_rsc.get('url', ''))
    extension = mimetypes.guess_extension(content_type)

    if pkg.get('resources'):
        # Iterate over resources, downloading and storing them in the package zip file
        for rsc in pkg['resources']:
            # Case 1: Resource is not on s3 yet, need to download from CKAN
            if rsc.get('url_type') == 'upload':
                upload = uploader.ResourceUpload(rsc)
                filepath = upload.get_path(rsc['id'])
                package_zip_archive.write(filepath, slugify(
                    rsc['name'], to_lower=True) + extension)
            # Case 2: Resource exists on S3, download into package zip file
            elif is_downloadable_url(rsc.get('url', '')):
                # Try to download the resource from the provided URL
                try:
                    response = session.get(rsc.get('url', ''), timeout=10)
                except requests.exceptions.RequestException as e:
                    toolkit.abort(404, toolkit._('Resource data not found'))

                package_zip_archive.writestr(
                    slugify(rsc['name'], to_lower=True) + extension,
                    response.content)

    # Initialize connection to s3
    s3 = boto3.resource('s3')
    bucket_name = config.get('ckan.s3_resources.s3_bucket_name')
    bucket = s3.Bucket(bucket_name)

    # Note that when dealing with the zipfiles we should already have all the resources uploaded to S3
    # Attempt to download the resource from url
    try:
        response = session.get(
            new_rsc.get('url', ''), timeout=10)

    except requests.exceptions.RequestException as e:
        toolkit.abort(404, toolkit._(
            'Resource data not found'))

    # Write new_rsc file into package zip
    package_zip_archive.writestr(
        slugify(new_rsc['name'], to_lower=True) + extension,
        response.content)

    # Write new_rsc file into the updated resource zip
    new_rsc_zip_archive.writestr(slugify(new_rsc['name'], to_lower=True) + extension, 
        response.content)

    # Upload updated resource zip
    new_rsc_zip_archive.close()
    file_name = str(pkg['name']) + '/' + slugify(new_rsc['name'], to_lower=True) + '.zip'
    obj = bucket.put_object(Key=file_name, Body=new_rsc_buff.getvalue(), ContentType='application/zip')
    obj.Acl().put(ACL='public-read')

    # Upload package zip
    package_zip_archive.close()
    package_file_name = str(pkg['name']) + '/' + str(pkg['name']) + '.zip'
    obj = bucket.put_object(Key=package_file_name, Body=package_buff.getvalue(), ContentType='application/zip')
    obj.Acl().put(ACL='public-read')


class MetadataYAMLDumper(yaml.SafeDumper):

    def __init__(self, *args, **kws):
        kws['default_flow_style'] = False
        kws['explicit_start'] = True
        kws['line_break'] = '\r\n'

        return super(MetadataYAMLDumper, self).__init__(*args, **kws)

    # add the first indentation for list
    def expect_block_sequence(self):
        self.increase_indent(flow=False, indentless=False)

        self.state = self.expect_first_block_sequence_item

    # modify this to add extra line breaks
    def expect_block_sequence_item(self, first=False):
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

    # represent OrderedDict
    def represent_odict(dumper, data):
        value = list()
        node = yaml.nodes.MappingNode(
            'tag:yaml.org,2002:map', value, flow_style=None)
        if dumper.alias_key is not None:
            dumper.represented_objects[dumper.alias_key] = node
        for item_key, item_value in data.items():
            node_key = dumper.represent_data(item_key)
            node_value = dumper.represent_data(item_value)
            value.append((node_key, node_value))
        node.flow_style = False
        return node

    # single quotes
    def choose_scalar_style(self):
        is_dict_key = self.states[-1] == self.expect_block_mapping_simple_value
        if is_dict_key:
            return None
        return "'"

MetadataYAMLDumper.add_representer(
    collections.OrderedDict, MetadataYAMLDumper.represent_odict)

# Helper functions

def prettify_json(json):
    if isinstance(json, dict):
        for key, value in json.items():
            prettified_name = key.replace('_', ' ').title()
            json[prettified_name] = prettify_json(json.pop(key))
    elif isinstance(json, list):
        return [prettify_json(obj) for obj in json]
    elif isinstance(json, basestring):
        # remove leading and trailing white spaces, new lines, tabs
        json = json.strip(' \t\n\r')
    return json

def is_downloadable_url(url):
    content_type, content_enc = mimetypes.guess_type(url)
    if content_type and content_type != 'text/html':
        return True
    return False

