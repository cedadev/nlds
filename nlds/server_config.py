import json
import os.path
from .nlds_setup import CONFIG_FILE_LOCATION

def validate_config_file(json_config):
    """Validate the JSON config file to match the schema in load_config_file."""
    # Server section
    try:
        auth_section = json_config["authentication"]
    except KeyError:
        raise RuntimeError(
            f"The config file at {CONFIG_FILE_LOCATION} does not contain an "
            "['authentication'] section."
        )

    for key in ["authenticator_backend"]:
        try:
            value = auth_section[key]
        except KeyError:
            raise KeyError(
                f"The config file at {CONFIG_FILE_LOCATION} does not "
                f"contain {key} in the ['authentication'] section."
            )


def load_config():
    """Config file for the server contains:
        authentication : {
            authenticator_backend : <authenticator backend>,
        }
        ... optional settings for authenticator backend ...
    """
    # Location of config file is ./.serverconfig.  Open it, checking that it
    # exists as well.
    try:
        fh = open(os.path.abspath(f"{CONFIG_FILE_LOCATION}"))
    except FileNotFoundError:
        raise FileNotFoundError(
            f"{CONFIG_FILE_LOCATION}",
            "The config file cannot be found."
        )

    # Load the JSON file, ensuring it is correctly formatted
    try:
        json_config = json.load(fh)
    except json.JSONDecodeError as je:
        raise RuntimeError(
            f"The config file at {CONFIG_FILE_LOCATION} has an error at "
            f"character {je.pos}: {je.msg}."
        )

    # Check that the JSON file contains the correct keywords / is in the correct
    # format
    validate_config_file(json_config)

    return json_config
