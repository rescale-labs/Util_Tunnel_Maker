#!/usr/bin/env python3

import configparser
import os
import sys
import requests
from fabric import Connection
from paramiko import RSAKey
import tempfile
from pathlib import Path
import logging
import argparse
import textwrap


logger = logging.getLogger('tunnel_maker')


def parse_command_line_arguments():
    """
    Parse command line arguments.
    """
    description = textwrap.dedent("""\
    This program facilitates the creation of an SSH tunnel between Rescale jobs or Workstations.  
    It creates a temporary SSH keypair, uploads the temporary private key to the Rescale job 1 
    (defined using --job1) and adds the temporary public key to ~/.ssh/authorized_keys on Rescale job 2 
    (defined using --job2). 
    It then creates a shell script called ~/create_ssh_tunnel.sh on job 1, that can be used to 
    create an SSH tunnel from job 1 to job 2.
    """)

    parser = argparse.ArgumentParser(
        description=description,
        epilog="Please report bugs to rbitsche@rescale.com"
        )

    parser.add_argument(
        "--job1",
        help="Rescale JobID of a job or workstation from which the SSH tunnel should be created. "
             "e.g. 'StGaQb'",
        metavar='JobID',
        required=True
    )

    parser.add_argument(
        "--job2",
        help="Rescale JobID of a job or workstation to which the SSH tunnel should be created. "
             "e.g. 'uaHMJc'",
        metavar='JobID',
        required=True
    )

    parser.add_argument(
        "--local_port_forwarding",
        help="Local port forwarding configuration for the SSH tunnel. Default is: '47827:localhost:47827'",
        metavar="port:host:hostport",
        required=False,
        default="47827:localhost:47827"
    )

    parser.add_argument(
        "--rescale_ssh_private_key",
        help="The SSH private key file used on Rescale. The corresponding public key must be configured in "
             "Rescale's User Profile settings (User Profile -> Job Settings).",
        metavar='FILE',
        required=False
    )

    parser.add_argument(
        "--api_config_file",
        metavar="FILE",
        help="Path to the API profiles configuration file. Default is '~/.config/rescale/apiconfig'. Note that "
             "the same configuration file is used by the Rescale CLI."
    )

    parser.add_argument(
        "--api_profile",
        metavar="PROFILE_NAME",
        help="Name of the API profile to read from the API profiles configuration file. Default: 'default'",
        default="default"
    )

    parser.add_argument(
        "--api_base_url",
        metavar="URL",
        help="Base URL for API access. Default: https://platform.rescale.com",
        default="https://platform.rescale.com"
    )

    cl_args = parser.parse_args()

    if cl_args.api_config_file is None:
        cl_args.api_config_file = Path.home().joinpath('.config', 'rescale', 'apiconfig')
    else:
        cl_args.api_config_file = Path(cl_args.api_config_file)

    return cl_args


def get_api_profile(cl_args):
    """
    Try to get the API key from the environment variable RESCALE_API_KEY
    and the api_base_url from the corresponding command line argument.
    If the environment variable RESCALE_API_KEY is not found,
    get the API key and api_base_url from the configuration file.
    :param cl_args: command line arguments
    """
    api_key, api_base_url = get_api_key_from_envvar(cl_args)
    if api_key is None:
        api_key, api_base_url = get_api_key_from_config_file(cl_args)
    logger.info(f'api_base_url is {api_base_url}.')
    return api_key, api_base_url


def get_api_key_from_envvar(cl_args):
    """
    Get the API key from the environment variable RESCALE_API_US_PROD
    or RESCALE_API_KEY, and the api_base_url from the respective
    command line argument.
    If it does not exist, return None.
    :param cl_args: command line arguments
    """
    for envvar in ('RESCALE_API_US_PROD', 'RESCALE_API_KEY'):
        api_key = os.environ.get(envvar)
        if api_key is not None:
            logger.info(f'Read API key from environment variable {envvar}.')
            return api_key, cl_args.api_base_url

    return None, cl_args.api_base_url


def get_api_key_from_config_file(cl_args):
    """
    Get the api_key and api_base_url from the API configuration file.
    :param cl_args: command line arguments
    """
    logger.info(f'Reading API configuration file {cl_args.api_config_file}, '
                f'profile: {cl_args.api_profile!r}')

    if not cl_args.api_config_file.is_file():
        raise FileNotFoundError(f'File {cl_args.api_config_file} not found!')

    config = configparser.ConfigParser()
    config.read(cl_args.api_config_file)
    api_profile = config[cl_args.api_profile]
    try:
        api_key = api_profile['apikey']
        api_base_url = api_profile['apibaseurl']
    except KeyError:
        logger.error(f"Keys 'apikey' and 'apibaseurl' must be defined for profile {cl_args.api_profile!r} "
                     f"in file {cl_args.api_config_file}.")
        sys.exit(1)

    logger.info(f'api_base_url is {api_base_url}.')

    return api_key, api_base_url


def log_and_raise_for_status(response):
    """
    If request was unsuccessful, log resonse.text and raise HTTPError.
    :param response: Response Object
    """
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        logger.error(err.response.text)
        raise


class Api:
    """
    Class for access to the Rescale API.
    """

    def __init__(self, api_key, base_url='https://platform.rescale.com'):
        """
        :param api_key: Rescale API key
        :param base_url: Rescale API base URL
        """
        self.api_key = api_key
        self.authorization = f'Token {self.api_key}'
        self.base_url = base_url

    def get_all_result_pages(self, url, params=None):
        """
        Get all results of a paginated GET request.
        :param url: URL for the GET request.
        :param params: parameters for the GET request.
        """
        results = []
        response = requests.get(
            url,
            headers={'Authorization': self.authorization},
            params=params
        )
        log_and_raise_for_status(response)
        results.extend(response.json()["results"])

        while response.json()["next"] is not None:
            response = requests.get(response.json()["next"], headers={'Authorization': self.authorization})
            log_and_raise_for_status(response)
            results.extend(response.json()["results"])
        return results

    def get_instances(self, job_id):
        """
        Get the list of instances for a job.
        :param job_id: ID of the job
        """
        instances = self.get_all_result_pages(
            f"{self.base_url}/api/v2/jobs/{job_id}/instances/"
        )
        return instances

    def get_head_node(self, job_id):
        """
        Retrun information about the head node of a job.
        Exit if no instances can be found.
        :param job_id: ID of the job
        """
        instances = self.get_instances(job_id)

        if len(instances) == 0:
            logger.error(f'No instances found for JobID {job_id!r}. Is the cluster running?')
            sys.exit(1)

        if len(instances) == 1:
            return instances[0]

        for instance in instances:
            if instance.get('role') == 'MPI_MASTER':
                return instance

        return None


def setup_logging():
    """
    Set basic configuration for the logging system.
    """
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(name)s %(levelname)s: %(message)s')


def create_temp_ssh_keypair(tempdir, size=2048):
    """
    Create an RSA keypair and save the private key and public key to a file in tempdir.
    :param tempdir: Path object, path where the private key and public key are saved as files.
    :param size: Size of the SSH key in bits.
    """
    logger.info('Creating temporary SSH keypair.')
    key = RSAKey.generate(bits=size)

    private_key_path = tempdir.joinpath('tunnel_maker_private_key.pem')
    public_key_path = tempdir.joinpath('tunnel_maker_public_key.pub')

    key.write_private_key_file(private_key_path)
    with open(public_key_path, 'w') as f:
        f.write(f'{key.get_name()} {key.get_base64()}')

    logger.info(f"Private key written to: {private_key_path}")
    logger.info(f"Public key written to: {public_key_path}")

    return key, private_key_path, public_key_path


def connect_to_instance(instance, private_key_filename=None, timeout=20):
    """
    Return a connection object for the instance.
    :param instance: instance to connect to.
    :param private_key_filename, private key file for authentication
    :param timeout: timeout limit for the SSH connection
    """
    connect_kwargs = {'key_filename': private_key_filename} if private_key_filename is not None else None

    return Connection(
        host=instance['publicIp'],
        user=instance['username'],
        port=instance['sshPort'],
        connect_timeout=timeout,
        connect_kwargs=connect_kwargs
    )


def test_ssh_connection(connection):
    """
    Test SSH connection and exit program if connection is unsuccessful.
    :param connection: Connection to test.
    """
    logger.info(f'Testing SSH connection to {connection.host}')
    result = connection.run('echo "SSH connection successful"', hide=True)
    if result.ok:
        logger.info("SSH connection successful")
    else:
        logger.error(f'Could not connect to {connection.host}.')
        sys.exit(1)


def setup_tunnel(con_job1, con_job2, cl_args):
    """
    Creates a temporary SSH keypair, upload the temporary private key to the job 1
    and add  the temporary public key to ~/.ssh/authorized_keys on job 2.
    Create a shell script on job 1, that can be used to create an SSH tunnel from job 1 to job 2.
    :param con_job1: Connection object for job1
    :param con_job2: Connection object for job2
    :param cl_args: command line arguments
    """
    with tempfile.TemporaryDirectory() as td:
        tempdir = Path(td)
        logger.info(f'Created temporary directory: {tempdir}')

        key, private_key_path, public_key_path = create_temp_ssh_keypair(tempdir)

        logger.info(f'Uploading private key to {con_job1.host}')
        result = con_job1.put(private_key_path)
        logger.info(f'Uploaded {result.local} to {result.remote} on {con_job1.host}')

    authorized_keys_line = f'{key.get_name()} {key.get_base64()}'
    logger.info(f'Appending public key to ~/.ssh/authorized_keys on {con_job2.host}')
    con_job2.run(f'echo "{authorized_keys_line}" >> ~/.ssh/authorized_keys', hide=True)

    tunnel_script_name = 'create_ssh_tunnel.sh'
    tunnel_script_content = f"#!/bin/bash\n" \
                            f"ssh -p {con_job2.port} -i ~/{private_key_path.name} " \
                            f"{con_job2.user}@{con_job2.host} -L {cl_args.local_port_forwarding} -N -v"
    logger.info(f'Creating script {tunnel_script_name} on {con_job1.host}')
    con_job1.run(f'echo "{tunnel_script_content}" >> ~/{tunnel_script_name}', hide=True)
    con_job1.run(f'chmod 775 ~/{tunnel_script_name}', hide=True)


def main():
    """
    main function
    """

    setup_logging()

    cl_args = parse_command_line_arguments()

    api_key, api_base_url = get_api_profile(cl_args)
    api = Api(api_key=api_key, base_url=api_base_url)

    job1_head_node = api.get_head_node(cl_args.job1)
    job2_head_node = api.get_head_node(cl_args.job2)

    con_job1 = connect_to_instance(job1_head_node, private_key_filename=cl_args.rescale_ssh_private_key)
    con_job2 = connect_to_instance(job2_head_node, private_key_filename=cl_args.rescale_ssh_private_key)

    test_ssh_connection(con_job1)
    test_ssh_connection(con_job2)

    setup_tunnel(con_job1, con_job2, cl_args)

    con_job1.close()
    con_job2.close()

    logger.info('DONE')


if __name__ == '__main__':
    main()
