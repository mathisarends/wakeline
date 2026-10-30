class ConflictError(RuntimeError):
    """Too many concurrent writes to one instance; retry the management operation."""
