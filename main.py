"__author__ = 'Samuel Kozuch'"
"__credits__ = 'Keboola *YYYY*'"
"__project__ = 'processor-split-by-value'"

"""
Python 3 environment 
"""

#import pip
#pip.main(['install', '--disable-pip-version-check', '--no-cache-dir', 'logging_gelf'])

import sys
import os
import logging
import csv
import json
import pandas as pd
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker


### Environment setup
abspath = os.path.abspath(__file__)
script_path = os.path.dirname(abspath)
os.chdir(script_path)

### Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")
"""
logger = logging.getLogger()
logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
    host=os.getenv('KBC_LOGGER_ADDR'),
    port=int(os.getenv('KBC_LOGGER_PORT'))
    )
logging_gelf_handler.setFormatter(logging_gelf.formatters.GELFFormatter(null_character=True))
logger.addHandler(logging_gelf_handler)

# removes the initial stdout logging
logger.removeHandler(logger.handlers[0])
"""

### Access the supplied rules
cfg = docker.Config('/data/')
params = cfg.get_parameters()
by_column = params['by_column']

### Get proper list of tables
cfg = docker.Config('/data/')
in_tables = cfg.get_input_tables()
out_tables = cfg.get_expected_output_tables()
logging.info("IN tables mapped: "+str(in_tables))
logging.info("OUT tables mapped: "+str(out_tables))

### destination to fetch and output files
DEFAULT_FILE_INPUT = "/data/in/tables/"
DEFAULT_FILE_DESTINATION = "/data/out/tables/"


def create_manifest(file_name, destination):
    """
    Function for manifest creation.
    """

    file = '/data/out/tables/' + str(file_name) + '.manifest'

    manifest_template = {
                         "destination": str(destination)
                        }

    manifest = manifest_template

    try:
        with open(file, 'w') as file_out:
            json.dump(manifest, file_out)
            logging.info("Output %s manifest file produced." % file_name)
    except Exception as e:
        logging.warn("Could not produce %s output file manifest." % file_name)
        logging.warn(e)

def main():
    """
    Main execution script.
    """
    for table in in_tables:
        data = pd.read_csv(table['full_path'], dtype=str)

        for value in data[by_column].unique():
            logging.debug("Current value for column %s is: %s" % (by_column, value))
            sub = data[data[by_column].isin([value])]

            filename = str(by_column) + '_' + value
            destination = "in.c-processor-split.%s" % filename
            
            sub.to_csv('/data/out/tables/%s.csv' % filename, index=False)
            create_manifest(filename, destination)


if __name__ == "__main__":

    main()

    logging.info("Done.")