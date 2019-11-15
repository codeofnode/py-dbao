from errors import *
from datetime import datetime
from copy import deepcopy
from returntypes import Result

EPOCH = datetime.datetime.utcfromtimestamp(0)
PRE_AUTH = '$PRE_AUTHORIZED'
ANONYMOUS_USER = '$ANONYMOUS_USER'
PREV_REC_KEY = 'fetchPrevRecord'
PERMISSION_REQUIRED_KEY = '_permissionRequired'


def unix_time_millis(dt):
    """unix_time_millis

    :param dt:
    """
    return (dt - epoch).total_seconds() * 1000.0


class Store:
    """Store"""
    anonymous_user = ANONYMOUS_USER

    def __init__(self, url, **kargs):
        """__init__

        :param url:
        :param **kargs:
        """
        self.url = url
        self.defaultPageSize = kargs.get('pageSize', 10)
        self.schemaValidationEnabled = kargs.get('schemaValidation', False)
        self.authorizationEnabled = kargs.get('authorization', False)
        self.prevRecordEnabled = kargs.get(PREV_REC_KEY, False)
        self.pubSubEnabled = kargs.get('pubSubEnabled', False)
        self.collectionPrefix = kargs.get('collectionPrefix', '')
        self.schemaForCollections = {}
        self.permissionForCollections = {}

    def genId():
        """genId"""
        return unix_time_millis(datetime.now())

    @staticmethod
    def checkUser(user):
        """checkUser

        :param user:
        """
        if not user:
            raise UserNotFoundError
        if not user.permissions or not isinstance(user.permissions, list):
            raise NoPermissionFoundError
        return 1

    @staticmethod
    def getUserId(user):
        """getUserId

        :param user:
        """
        if user and hasattr(user, '_id'):
            return user._id
        return Store.anonymous_user

    @staticmethod
    def transformOutput(rec):
        """transformOutput

        :param rec:
        """
        # implement if required
        return rec

    @staticmethod
    def addRBACListFilter(user, filters):
        """addRBACListFilter

        :param user:
        :param filters:
        """
        # extend it
        pass

    def connectDb(self):
        """connectDb"""
        # to extend
        pass

    def shouldFindPrevRecord(self, kargs):
        """shouldFindPrevRecord

        :param kargs:
        """
        return kargs[PREV_REC_KEY] if \
            ((self.shouldValidate(kargs) or
              self.shouldAuthorize(kargs)
              ) and
             PREV_REC_KEY in kargs) \
            else \
            self.prevRecordEnabled

    def shouldAuthorize(self, kargs):
        """shouldAuthorize

        :param kargs:
        """
        return kargs.get('authorization', self.authorizationEnabled)

    def shouldValidate(self, kargs):
        """shouldValidate

        :param kargs:
        """
        return kargs.get('schemaValidation', self.schemaValidationEnabled)

    def authorize(self, user, coll, op, data, options={}):
        """authorize

        :param user:
        :param coll:
        :param op:
        :param data:
        :param options:
        """
        if self.shouldAuthorize(options):
            Store.checkUser(user)
            if (not data or PERMISSION_REQUIRED_KEY not in data or
                    not isinstance(data[PERMISSION_REQUIRED_KEY], list) or
                    op not in data[PERMISSION_REQUIRED_KEY]):
                self.authorizeCollection(user, coll, op, options)
            elif (data[PERMISSION_REQUIRED_KEY][op] not in user.roles):
                raise UnAuthorizedActionError
        return {'updatedBy': Store.getUserId(
            user), 'updatedAt': datetime.now()}

    def authorizeCollection(self, user, coll, op, options={}):
        """authorizeCollection

        :param user:
        :param coll:
        :param op:
        :param options:
        """
        if self.shouldAuthorize(
                options) and coll in self.permissionForCollections:
            Store.checkUser(user)
            if (op not in self.permissionForCollections[
                    coll] or
                    self.permissionForCollections[coll][op] not in user.roles):
                raise UnAuthorizedActionError
        return {'updatedBy': Store.getUserId(
            user), 'updatedAt': datetime.now()}

    def validateSchema(self, coll, data, patch={}, options={}):
        """validateSchema

        :param coll:
        :param data:
        :param patch:
        :param options:
        """
        if self.shouldValidate(options):
            # to extend
            pass
        return 1

    def transformCollectionName(self, rec):
        """transformCollectionName

        :param rec:
        """
        return rec[(len(self.collectionPrefix) + 1):]

    def emitDbEvent(self, event, *args, **kargs):
        """emitDbEvent

        :param event:
        :param *args:
        :param **kargs:
        """
        if self.pubSubEnabled:
            # implement your own
            pass

    def getDbCollection(self, name):
        """getDbCollection

        :param name:
        """
        # extend it
        pass

    def getDbCollectionName(self, name):
        """getDbCollectionName

        :param name:
        """
        return self.collectionPrefix + name

    def getPrevDoc(self, user, coll, _id, options={}):
        """getPrevDoc

        :param user:
        :param coll:
        :param _id:
        :param options:
        """
        if self.shouldFindPrevRecord(options):
            if 'projection' not in options:
                options['projection'] = {}
            if PERMISSION_REQUIRED_KEY not in options['projection']:
                options['projection'][PERMISSION_REQUIRED_KEY] = True
            prevRec = self.read(self, coll, _id, options)
            if not prevRec:
                raise RecordNotFoundError
            return prevRec

    def list(self, user, coll, filters, options):
        """list

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        # extend it
        pass

    def count(self, user, coll, filters, options):
        """count

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        # extend it
        pass

    def preAuthorizedColl(self, user, coll, filters, options):
        """preAuthorizedColl

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        listOptions = self.preAuthorizeCollQuery(user, coll, options)
        collection = options.get('dbCollection', self.getDbCollection(coll))
        return collection

    def preAuthorizeCollQuery(self, user, coll, options):
        """preAuthorizeCollQuery

        :param user:
        :param coll:
        :param options:
        """
        if PRE_AUTH not in options or not options[PRE_AUTH]:
            self.authorizeCollection(user, coll, 0, options)
            options = deepcopy(options)
            options[PRE_AUTH] = True
        return options

    def listAll(self, user, coll, filters, options):
        """listAll

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        # warning : this will fill your memory
        result = Result()
        listOptions = self.preAuthorizeCollQuery(user, coll, options)
        listOptions['count'] = True
        firstResult = self.list(user, coll, filters, listOptions)
        result.records = firstResult.records
        result.total = firstResult.total
        listOptions['count'] = False
        for point in range(len(result.records), result.total):
            listOptions['skip'] = point
            restOfResults = self.list(user, coll, filters, listOptions)
            for rec in restOfResults.records:
                result.records.append(rec)
            point += len(restOfResults.records)
        if (len(result.records) != result.total):
            result.isRecordChanged = True
        return result

    def findOne(self, user, coll, filters, options={}):
        """findOne

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        options['limit'] = 1
        return this.list(user, coll, filters, options).records[0]

    def listCollections(self):
        """listCollections"""
        # extend it
        pass

    def read(self, user, coll, _id, options):
        """read

        :param user:
        :param coll:
        :param _id:
        :param options:
        """
        # extend it
        pass

    def write(self, user, coll, _id, data, options):
        """write

        :param user:
        :param coll:
        :param _id:
        :param data:
        :param options:
        """
        # extend it
        pass

    def delete(self, user, coll, _id, options):
        """delete

        :param user:
        :param coll:
        :param _id:
        :param options:
        """
        # extend it
        pass

    def setupindexes(self, coll, indexes):
        """setupindexes

        :param coll:
        :param indexes:
        """
        if isinstance(indexes, list):
            # extend it
            pass

    def createCollection(self, coll):
        """createCollection

        :param coll:
        """
        # extend it
        pass

    def mkcoll(self, coll, indexes, schema=None,
               permissionRequired=None, **kargs):
        """mkcoll

        :param coll:
        :param indexes:
        :param schema:
        :param permissionRequired:
        :param **kargs:
        """
        self.setupIndexes(coll, indexes)
        if self.shouldValidate(**kargs) and schema:
            self.schemaForCollections[coll] = schema
        if self.shouldAuthorize(**kargs) and permissionRequired:
            this.permissionForCollections[coll] = permissionRequired

    def rmcoll(self, coll):
        """rmcoll

        :param coll:
        """
        pass
