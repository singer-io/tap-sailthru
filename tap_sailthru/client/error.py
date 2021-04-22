# -*- coding: utf-8 -*-

class SailthruClientError(Exception):
    pass

class SailthruClient429Error(Exception):
    pass

class SailthruServer5xxError(Exception):
    pass
