import base64
from botocore.exceptions import ClientError
import boto3
import click
import docker
# from docker_image.reference import Reference
import fnmatch
import functools
#import jsonschema
import re
import os
import yaml

try:
    from yaml import CSafeLoader
except ImportError:
    from yaml import SafeLoader as CSafeLoader
    

DEFAULT_REPO = 'registry.hub.docker.com'
IMAGE_RE = re.compile("^(?:([^\/]+)\/)?(?:([^\/]+)\/)?([^@:\/]+)(?:[@:](.+))?$")


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx):
    """Kubernetes Image Transformer"""
    #if ctx.invoked_subcommand is None:
    #    ctx.invoke(remap)


@cli.command()
@click.option('-r', '--repo', multiple=True)
@click.option('-f', '--filename', type=click.Path())
@click.option('-v', '--verbose', is_flag=True)
def republish(repo, filename, verbose):
    """ Find docker images in a given set of config files and download/tag/republish them.
    """
    client = docker.from_env()
    repo_mapping = parse_repo_map(repo)
    ecr = None

    for old_image, new_image in mapped_images(
            iter_images(iter_files(filename)), repo_mapping):
        print old_image, new_image
        progress = client.images.pull(old_image, stream=verbose)
        if verbose:
            for l in progress:
                click.echo(l)
                click.clear()
        if 'dkr.ecr' in new_image:
            if ecr is None:
                ecr = ECR(client)
                ecr.login()
            ecr.ensure_repo(new_image)
        img = client.images.get(old_image)
        client.push(new_image, parse_named(new_image).get('tag'))


@cli.command()
@click.option('-r', '--repo', multiple=True)
@click.option('-f', '--filename', type=click.Path())
def images(repo, filename):
    """Find images in kube config files and resolve them to new repos/namespaces
    """
    repo_mapping = parse_repo_map(repo)
    for old_image, new_image in mapped_images(
            iter_images(iter_files(filename)), repo_mapping):
        print("%s -> %s" % (old_image, new_image))
        

@cli.command()
@click.option('-r', '--repo')
@click.option('-f', '--filename', type=click.Path())
def remap(repo, filename):
    repo_mapping = parse_repo_map(repo)
    with open(filename) as fh:
        data = list(yaml.load_all(fh, Loader=CSafeLoader))
    replaced = transform(data, repo_mapping)
    print replaced



class ECR(object):

    def __init__(self, docker):
        self.docker = docker
        self.client = boto3.Session().client('ecr')

    def login(self):
        result = self.client.get_authorization_token().get('authorizationData')[0]
        auth = base64.b64decode(result['authorizationToken'])[4:]
        login = self.docker.login(
            username='AWS',
            password=auth,
            email='none',
            reauth=True,
            registry=result['proxyEndpoint'])

    def ensure_repo(self, image):
        info = parse_named(image)
        try:
            self.client.create_repository(
                repositoryName="%s/%s" % (info['namespace'], info['name']))
        except ClientError as e:
            if e.response['Error']['Code'] != 'RepositoryAlreadyExistsException':
                raise


def parse_repo_map(repo):
    repo_map = {}
    for r in repo:
        if ':' in r:
            k, v = r.split(':', 1)
            repo_map[k] = v.rstrip('/')
        else:
            repo_map[DEFAULT_REPO] = r.rstrip('/')
    return repo_map


def iter_files(path):
    files = []
    if os.path.isdir(path):
        files.extend(
            map(functools.partial(os.path.join, path),
                fnmatch.filter(os.listdir(path), "*.yaml")))
    else:
        files = [path]
    return files


def iter_images(files):
    images = []
    for f in files:
        with open(f) as fh:
            data = list(yaml.load_all(fh, Loader=CSafeLoader))
            images.extend(parse_images(data))
    return set(images)


def resolve_image(image, repo_mapping):
    info = parse_named(image)
    registry = info.get('registry')
    if registry is None:
        registry = DEFAULT_REPO
    if registry in repo_mapping:
        nregistry = repo_mapping[registry]
        if registry == DEFAULT_REPO and registry not in image:
            nimage = "%s/%s" % (repo_mapping[registry], image)
        else:
            nimage = image.replace(registry, nregistry)
        if "/" in nregistry:
            nimage = nimage.replace(info['namespace'] + '/', '')
        return (image, nimage)


def parse_named(image):
    parts = dict(zip(
        ('registry', 'namespace', 'name', 'tag'),
        IMAGE_RE.match(image).groups()))
    if parts['namespace'] is None and '.' not in parts['registry']:
        parts['namespace'] = parts['registry']
        parts['registry'] = None
    return parts
    

def parse_images(data):
    images = []
    if isinstance(data, dict):
        for k, v in list(data.items()):
            if k == 'image':
                images.append(v)
            if isinstance(v, dict):
                images.extend(parse_images(v))
            if isinstance(v, list):
                images.extend(parse_images(v))
    if isinstance(data, list):
        for v in data:
            if isinstance(v, dict):
                images.extend(parse_images(v))
            if isinstance(v, list):
                images.extend(parse_images(v))
    return images


def mapped_images(images, repo_mapping):
    replaced = set()
    for i in images:
        r = resolve_image(i, repo_mapping)
        if r:
            replaced.add(r)
    return replaced


def transform(data, repo_mapping):
    replaced = []
    if isinstance(data, dict):
        for k, v in list(data.items()):
            if k == 'image':
                remapped = resolve_image(v, repo_mapping)
                if remapped:
                    replaced.append(remapped)
                    data[k] = replaced[-1][1]
        if isinstance(v, dict):
            replaced.extend(transform(v, repo_mapping))
        if isinstance(v, list):
            replaced.extend(transform(v, repo_mapping))
    if isinstance(data, list):
        for v in data:
            if isinstance(v, dict):
                replaced.extend(transform(v, repo_mapping))
            if isinstance(v, list):
                replaced.extend(transform(v, repo_mapping))
    return replaced
                

if __name__ == '__main__':
    cli()
