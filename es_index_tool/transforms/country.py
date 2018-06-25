import json
import os

from ..transforms import Transform

DATA_PATH = os.path.join(os.path.dirname(__file__), '..', 'data')


class CountryTransform(Transform):
    """Convert two-letter country codes to country names."""

    def __init__(self, config):
        super().__init__(config)
        # Get two-letter country code/country name pairings.
        countries_path = os.path.join(DATA_PATH, 'countries.json')
        with open(countries_path, 'r') as fi_:
            countries_dict = json.load(fi_)
        self.countries_dict = countries_dict

    def transform(self, value):
        return self.countries_dict.get(value)
