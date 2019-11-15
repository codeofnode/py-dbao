class Error(Exception):
    """Base class for other exceptions"""
    pass


class UserNotFoundError(Error):
    """when current user is not found"""
    pass


class NoPermissionFoundError(Error):
    """when current user is does not have any permission"""
    pass


class UnAuthorizedActionError(Error):
    """when current user is does not have any permission"""
    pass


class RecordNotFoundError(Error):
    """when current user is not found"""
    pass


class CollectionNotFoundError(Error):
    """when current user is not found"""
    pass
