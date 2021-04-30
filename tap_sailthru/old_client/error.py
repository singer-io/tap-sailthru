# -*- coding: utf-8 -*-
"""
Defines exceptions for different status codes.
"""

# pylint: disable=missing-class-docstring
class SailthruClientError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruClient429Error(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruServer5xxError(Exception):
    pass

# pylint: disable=missing-class-docstring
class SailthruJobTimeout(Exception):
    pass
