# source_factory.py

import os
import yaml
from configparser import ConfigParser
from importlib import import_module
from typing import Optional
from loguru import logger
import time

class Source:
    cfg = ConfigParser()
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    cfg.read(os.path.join(parent_dir, 'config.ini'))

    def __init__(self, keyname: Optional[str] = None, **kwargs):
        self.keyname = keyname
        self.path = f"{Source.cfg.get('DATALAKE', 'path')}\\sources\\{keyname}"
        self.metadata = {}
        self.load_metadata()
        self.additional_attributes = kwargs

    def load_metadata(self):
        try:
            with open("sources.yaml", "r", encoding="utf-8") as yaml_file:
                sources_metadata = yaml.safe_load(yaml_file)

            if self.keyname in sources_metadata:
                self.metadata = sources_metadata[self.keyname]
            else:
                logger.warning(f"Metadata for '{self.keyname}' not found in sources.yaml.")
        except FileNotFoundError:
            logger.warning("sources.yaml file not found.")

    def run(self, level: str, **kwargs) -> None:
        s = import_module(f"sources.{self.keyname}", ['gather', 'level0', 'level1', 'level2', 'persist'])
        assert level in ['gather', 'level0', 'level1', 'level2', 'persist'], "Specify a correct level to process"
        source_method = getattr(s, level)

        # Log when a source is called and the action being performed
        logger.info(f"Calling source: {self.keyname}, Action: {level}")

        # Combine the attributes from sources.yaml and the method call's kwargs
        attributes = {**self.metadata, **kwargs, **self.additional_attributes, 'path': self.path}
        # Remove 'provider' and 'table' attributes from the attributes dictionary
        attributes.pop('provider', None)
        attributes.pop('table', None)
        attributes.pop('description', None)

        # Log the start time of processing
        logger.info(f"{self.keyname} - Processing started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

        source_method(self, **attributes)  # Pass the self instance here

        # Log the finish time of processing
        logger.info(f"{self.keyname} - Processing finished at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
