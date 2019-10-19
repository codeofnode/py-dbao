from errors import *
from datetime import datetime
from copy import deepcopy
from returntypes import Result

EPOCH = datetime.datetime.utcfromtimestamp(0)
ANONYMOUS_USER = '$ANONYMOUS_USER'
PREV_REC_KEY = 'fetchPrevRecord'
PERMISSION_REQUIRED_KEY = '_permissionRequired'

def unix_time_millis(dt):
    return (dt - epoch).total_seconds() * 1000.0

"""
    The BaseStore class
    @class
"""

class Store:
    anonymous_user = ANONYMOUS_USER

    def __init__(self, url, **kargs):
        self.url = url
        self.defaultPageSize = kargs.get('pageSize', 10);
        self.schemaValidationEnabled = kargs.get('schemaValidation', False)
        self.authorizationEnabled = kargs.get('authorization', False)
        self.prevRecordEnabled = kargs.get(PREV_REC_KEY, False)
        self.pubSubEnabled = kargs.get('pubSubEnabled', False)
        self.collectionPrefix = kargs.get('collectionPrefix', '')
        self.schemaForCollections = {}
        self.permissionForCollections = {}

    def genId():
        return unix_time_millis(datetime.now())

    @staticmethod
    def checkUser(user):
        if not user:
            raise UserNotFoundError
        if not user.permissions or not isinstance(user.permissions, list):
            raise NoPermissionFoundError
        return 1

    @staticmethod
    def getUserId(user):
        if user and hasattr(user, '_id'):
            return user._id
        return Store.anonymous_user

    @staticmethod
    def transformOutput(rec):
        # implement if required
        return rec

    def connectDb(self):
        # to extend
        pass

    def shouldFindPrevRecord(self, **kargs):
        return kargs[PREV_REC_KEY] if \
                self.shouldValidate(**kargs) or self.shouldAuthorize(**kargs) or PREV_REC_KEY in kargs else \
                self.prevRecordEnabled

    def shouldAuthorize(self, **kargs):
        return kargs.get('authorization', self.authorizationEnabled)

    def shouldValidate(self, **kargs):
        return kargs.get('schemaValidation', self.schemaValidationEnabled)

    def authorize(self, user, coll, op, data, options = {}):
        if self.shouldAuthorize(options):
            Store.checkUser(user)
            if (not data or PERMISSION_REQUIRED_KEY not in data or \
                    not isinstance(data[PERMISSION_REQUIRED_KEY], list) or op not in data[PERMISSION_REQUIRED_KEY]):
                self.authorizeCollection(user, coll, op, options)
            elif (data[PERMISSION_REQUIRED_KEY][op] not in user.roles):
                raise UnAuthorizedActionError
        return { 'updatedBy': Store.getUserId(), 'updatedAt': datetime.now() }

    def authorizeCollection(self, user, coll, op, options = {}):
        if self.shouldAuthorize(options) and coll in self.permissionForCollections:
            Store.checkUser(user)
            if op not in self.permissionForCollections[coll] or self.permissionForCollections[coll][op] not in user.roles:
                raise UnAuthorizedActionError
        return { 'updatedBy': Store.getUserId(), 'updatedAt': datetime.now() }

    def validateSchema(self, coll, data, options = {}):
        if self.shouldValidate(options)
            # to extend
            pass
        return 1

    def emitDbEvent(self, event, *args, **kargs):
        if self.pubSubEnabled:
            # implement your own
            pass

    def getDbCollectionName(self, name):
        return self.collectionPrefix + name

    def getPrevDoc(self, user, coll, _id, options):
        if self.shouldFindPrevRecord(options):
            prevRec = self.read(self, coll, _id, options)
            if not prevRec:
                raise RecordNotFoundError
            return prevRec

    def list(self, user, coll, filters, options):
        # extend it
        pass

    def listAll(self, user, coll, filters, options):
        # warning : this will fill your memory
        result = Result();
        listOptions = deepcopy(options)
        listOptions['count'] = True
        firstResult = self.list(user, coll, filters, listOptions)
        result.records = firstResult.records
        result.total = firstResult.total
        listOptions['count'] = False
        for point in range(len(result.records), result.total):
            listOptions['skip'] = point
            restOfResults = self.list(user, coll, filters, listOptions)
            for rec in restOfResults:
                result.records.append(rec)
            point += len(restOfResults)
        if (len(result.records) != result.total):
          result.isRecordChanged = True
        return result;

    def findOne(self, user, coll, filters, options = {}):
        options['limit'] = 1
        return this.list(user, coll, filters, options).records[0]

    def listCollections(self):
        # extend it
        pass

    def _read(self, coll, _id, options):
        # extend it
        pass

    def _write(self, coll, _id, data, options):
        # extend it
        pass

    def _del(self, coll, _id, options):
        # extend it
        pass

    def read(self, user, coll, _id, options):
        # extend it
        pass

    def write(self, user, coll, _id, data, options):
        # extend it
        pass

    def del(self, user, coll, _id, options):
        # extend it
        pass

    def setupIndexes(self, coll, indexes):
        if isinstance(indexes, list):
            #extend it
            pass

    def mkcoll(self, coll, indexes, schema = None, permissionRequired = None, **kargs):
        self.setupIndexes(coll, indexes)
        if self.shouldValidate(**kargs) and schema:
          self.schemaForCollections[coll] = schema
        if self.shouldAuthorize(**kargs) and permissionRequired:
          this.permissionForCollections[coll] = permissionRequired

    def rmcoll(coll)
        pass