"""EndpointSlice and Pod-to-Service mapping errors and helpers."""


class AmbiguousPodServiceMappingError(ValueError):
    """A Pod belongs to zero or multiple Services without an explicit label."""


class EndpointMappingError(ValueError):
    """EndpointSlice identity cannot be resolved uniquely."""
