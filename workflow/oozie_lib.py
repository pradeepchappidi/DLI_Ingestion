"""
Common functions for access to the Oozie API
See: https://oozie.apache.org/docs/4.1.0/WebServicesAPI.html
"""


from __future__ import print_function
from sys import stderr
import requests
from requests_kerberos import HTTPKerberosAuth, REQUIRED


# Server specific constants
OOZIE_URL = 'https://poldcdhen001.dev.intranet:11443/oozie'  # Replace by build process
BASE_URL = OOZIE_URL + '/v1/'
SSL_CERT = '/opt/cloudera/security/pki/rootca.cert.pem'

# Set up Kerberos Auth for API requests
kerberos_auth = HTTPKerberosAuth(mutual_authentication=REQUIRED, force_preemptive=True)

# Disable a warning about the SSL cert
requests.packages.urllib3.disable_warnings()


def get_json(endpoint, options=[]):
    """
    Execute a single GET request against the Oozie API and check the response code.
    Return the response as a JSON object.

    endpoint: The API endpoint to use.
    options: A list of request options.

    Example: /oozie/v1/job/job-id?show=info&timezone=GMT
             ^--------^^--------^ ^-------^ ^----------^
             Base      Endpoint   Option    Option
    """
    # Format the full request string
    request_str = '{base}{endpoint}{options}'.format(
        base=BASE_URL,
        endpoint=endpoint,
        options=('?'+'&'.join(options)) if options else ''
    )

    # Make the API request
    response = requests.get(request_str, auth=kerberos_auth, verify=SSL_CERT)

    # Make sure the response is okay
    if response.status_code != requests.codes.ok:
        print("Error Accessing Oozie API. Response Status: {}".format(response.status_code), file=stderr)
        response.raise_for_status()

    return response.json()


def get_yield_json(endpoint, options=[], request_len=10000):
    """
    Execute a series of paginated GET requests against the Oozie API, checking the response codes.
    Each response is yielded as a JSON object, so that this method behaves like an iterator.

    endpoint: The API endpoint to use.
    options: A list of request options.
    request_len: Determines how big each page is (default 10,000).

    Example: /oozie/v1/job/job-id?show=info&timezone=GMT
             ^--------^^--------^ ^-------^ ^----------^
             Base      Endpoint   Option    Option

    Note: The offset and len options are used to control pagination. Do NOT specify these in the call.
    The total key in the response is used to determine with all pages have been retrieved.
    """
    offset = 1  # Oozie page offsets start at 1
    while True:
        # Add pagination options to base options list. (Concatenate to get new list, not append to existing.)
        page_opts = options + ['len={}'.format(request_len), 'offset={}'.format(offset)]

        # Query Oozie with the additional paging constraints
        response = get_json(endpoint, options=page_opts)

        # Extract the total field from the response, and yield the response to the caller
        total = int(response['total'])
        yield response

        # Increase the offset to get the next page, check if there should be another page
        offset += request_len
        if offset > total:
            break  # No more pages, we're done!


def put(endpoint, options=[]):
    """
    Execute a single PUT request against the Oozie API and check the response code.

    endpoint: The API endpoint to use.
    options: A list of request options.

    Example: /oozie/v1/job/job-id?action=rerun
             ^--------^^--------^ ^----------^
             Base      Endpoint   Option
    """
    # Format the full request string
    request_str = '{base}{endpoint}{options}'.format(
        base=BASE_URL,
        endpoint=endpoint,
        options=('?'+'&'.join(options)) if options else ''
    )

    # Make the API request
    response = requests.put(request_str, auth=kerberos_auth, verify=SSL_CERT)

    # Make sure the response is okay
    if response.status_code != requests.codes.ok:
        print("Error Accessing Oozie API. Response Status: {}".format(response.status_code), file=stderr)
        response.raise_for_status()
