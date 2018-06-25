from datetime import datetime, timedelta

import dateutil.parser

from ..transforms import Transform


class AgeCategoryTransform(Transform):
    """Convert birth date to age categories for search filters."""

    def transform(self, value):
        # TODO: make categories configurable
        if value:
            if isinstance(value, str):
                value = dateutil.parser.parse(value, ignoretz=True)
            # FIXME: get timezones/UTC working
            now = datetime.now()
            diff_21 = timedelta(days=365*21)
            diff_18 = timedelta(days=365*18)
            if value > now - diff_18:
                categories = ['under 18']
            else:
                if value < now - diff_21:
                    categories = ['over 18', 'over 21']
                else:
                    categories = ['over 18']
        else:
            categories = ['unspecified']
        return categories
