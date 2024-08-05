from ..server_config import load_config
from .construct_url import construct_url
from retry import retry
import requests
import json




class Quotas():

    _timeout = 10.0

    def __init__(self):
        self.config = load_config()
        self.name = "jasmin_authenticator"
        self.auth_name = "authentication"

    @retry(requests.ConnectTimeout, tries=5, delay=1, backoff=2)
    def get_projects_services(self, oauth_token: str, service_name):
        """Make a call to the JASMIN Projects Portal to get the service information."""
        config = self.config[self.auth_name][self.name]
        token_headers = {
            "Content-Type": "application/x-ww-form-urlencoded",
            "cache-control": "no-cache",
            "Authorization": f"Bearer {oauth_token}",
        }
        # Contact the user_services_url to get the information about the services
        url = construct_url([config["user_services_url"]], {"name":{service_name}})
        try:
            response = requests.get(
                url,
                headers=token_headers,
                timeout=Quotas._timeout,
            )
        except requests.exceptions.ConnectionError:
            raise RuntimeError(f"User services url {url} could not be reached.")
        except KeyError:
            raise RuntimeError(f"Could not find 'user_services_url' key in the {self.name} section of the .server_config file.")
        if response.status_code == requests.codes.ok: # status code 200
            try:
                response_json = json.loads(response.text)
                return response_json
            except json.JSONDecodeError:
                raise RuntimeError(f"Invalid JSON returned from the user services url: {url}")
        else:
            raise RuntimeError(f"Error getting data for {service_name}")
        
    
    def extract_tape_quota(self, oauth_token: str, service_name):
        """Get the service information then process it to extract the quota for the service."""
        # Try to get the service information and throw an exception if an error is encountered
        try:
            result = self.get_projects_services(self, oauth_token, service_name)
        except(RuntimeError, ValueError) as e:
            raise type(e)(f"Error getting information for {service_name}: {e}")
        
        # Process the result to get the requirements
        for attr in result:
            # Check that the category is Group Workspace
            if attr["category"] == 1:
                # If there are no requirements, throw an error
                if attr["requirements"]:
                    requirements = attr["requirements"]
                else:
                    raise ValueError(f"Cannot find any requirements for {service_name}.")
            else:
                raise ValueError(f"Cannot find a Group Workspace with the name {service_name}. Check the category.")
                
        # Go through the requirements to find the tape resource requirement
        for requirement in requirements:
            # Only return provisioned requirements
            if requirement["status"] == 50:
                # Find the tape resource and get its quota
                if requirement["resource"]["short_name"] == "tape":
                    if requirement["amount"]:
                        tape_quota = requirement["amount"]
                        return tape_quota
                    else:
                        raise ValueError(f"Issue getting tape quota for {service_name}. Either quota is zero or couldn't be found.")
                else:
                    raise ValueError(f"No tape resources could be found for {service_name}")
            else:
                raise ValueError(f"No provisioned requirements found for {service_name}. Check the status of your requested resources.")