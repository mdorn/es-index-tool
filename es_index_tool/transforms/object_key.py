from ..transforms import Transform


class ObjectKeyTransform(Transform):
    """Extract relevant values from embedded objects in the source document."""

    def transform(self, value):
        return [obj[self.config['key']] for obj in value]
