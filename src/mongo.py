from pymongo import MongoClient, ObjectId, IndexModel, ASCENDING, DESCENDING
from copy import deepcopy

from base import Store, PRE_AUTH, PERMISSION_REQUIRED_KEY


class MongoStore(Store):
    """MongoStore"""

    @staticmethod
    def genId(inputId):
        """genId

        :param inputId:
        """
        return ObjectId(inputId) if (isinstance(inputId, str) and
                                     ObjectId.is_valid(inputId)) else inputId

    @staticmethod
    def transformOutput(rec):
        """transformOutput

        :param rec:
        """
        if rec and '_id' in rec and isinstance(rec['_id'], str):
            rec['_id'] = str(rec['_id'])
        return rec

    @staticmethod
    def addRBACListFilter(users, filters):
        """addRBACListFilter

        :param users:
        :param filters:
        """
        mainFilter = [{
            PERMISSION_REQUIRED_KEY: {'$exists': False}
        }, {
            PERMISSION_REQUIRED_KEY: {'$in': user.roles}
        }]
        if '$or' in filters:
            if '$and' in filters:
                filters['$and'] = filters['$and'] + mainFilter
            else:
                filters['$and'] = [{'$or': mainFilter}, filters]
        else:
            filters['$or'] = mainFilter
        return filters

    def getDbCollection(self, name):
        """getDbCollection

        :param name:
        """
        return self.db.collection(self.getDbCollectionName(name))

    def count(self, user, coll, filters, options):
        """count

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        collection = self.preAuthorizedColl(user, coll, filters, options)
        if collection:
            return collection.count(filters)
        raise CollectionNotFoundError

    def list(self, user, coll, filters, options={}):
        """list

        :param user:
        :param coll:
        :param filters:
        :param options:
        """
        result = Result()
        result.records = []
        result.total = 0
        listOptions = self.preAuthorizeCollQuery(user, coll, options)
        listFilters = MongoStore.addRBACListFilter(
            filters if isinstance(filters, dict) else {})
        skip = int(listOptions.get('skip', 0))
        limit = int(listOptions.get('limit', 10))
        count = listOptions.get('count', True)
        projection = listOptions.get('projection', None)
        collection = self.getDbCollection(coll)
        if collection:
            cursor = collection.find(listFilters, projection=projection)
            if 'sort' in listOptions:
                cursor = cursor.sort(sort)
            result.records = map(MongoStore.transformOutput,
                                 list(cursor.skip(skip).limit(limit)))
            if count:
                result.total = self.count(self, user, listFilters, listOptions)
        return result

    def listcolls(self):
        """listcolls"""
        return map(
                self.transformCollectionName, self.db.list_collection_names())

    def read(self, user, coll, _id, options):
        """read

        :param user:
        :param coll:
        :param _id:
        :param options:
        """
        cont = self.getDbCollection(coll).find_one({
            '_id': MongoStore.genId(_id)
        }, projection=options.get('projection'))
        self.authorize(user, coll, 0, cont, options)
        self.emitDbEvent('read:'+coll, str(_id), cont)
        return cont

    def write(self, user, coll, _id, data, options={}):
        """write

        :param user:
        :param coll:
        :param _id:
        :param data:
        :param options:
        """
        collection = self.getDbCollection(coll)
        if _id:
            prevInfo = self.getPrevDoc(user, coll, _id, options)
            authUpdate = self.authorize(user, coll, 1, prevInfo, options)
            self.validateSchema(self, coll, prevInfo, data, options)
            rawQuery = options.get('rawDbQuery', False)
            if rawQuery:
                if '$set' in data:
                    data['$set'].update(authUpdate)
                else:
                    data['$set'] = authUpdate
                collection.find_one_and_update({_id: Store.GenId(_id)}, data)
            else:
                data.update(authUpdate)
                collection.find_one_and_update({_id: Store.GenId(_id)},
                                               {'$set': data})
            self.emitDbEvent('update:'+coll, str(_id), cont)
            return 1
        self.validateSchema(self, coll, data, {}, options)
        data.update(self.authorizeCollection(self, user, coll, 1, options))
        newId = collection.insert_one(data).insertedId
        self.emitDbEvent('create:'+coll, str(newId), data)
        return newId

    def delete(self, user, coll, _id, options):
        """delete

        :param user:
        :param coll:
        :param _id:
        :param options:
        """
        prevInfo = self.getPrevDoc(user, coll, _id, options)
        authUpdate = self.authorize(user, coll, 1, prevInfo, options)
        self.getDbCollection(coll).delete_one({_id: Store.GenId(_id)})
        self.emitDbEvent('delete:'+coll, str(_id), prevInfo)
        return 1

    def setupIndexes(self, coll, indexes):
        """setupIndexes

        :param coll:
        :param indexes:
        """
        allIndexes = []
        if isinstance(indexes, list):
            collection = self.getDbCollection(coll)
            for ind in indexes:
                allKeys = ind.get('keyTuples', None)
                if not allKeys:
                    continue
                indexArgs = ind.get('indexArgs', {})
                allIndexes.append(IndexModel(allKeys, **indexArgs))
        return allIndexes

    def createCollection(self, coll):
        """createCollection

        :param coll:
        """
        self.db.create_collection(self.getDbCollectionName(coll))

    def rmcoll(self, coll, user):
        """rmcoll

        :param coll:
        :param user:
        """
        self.authorizeCollection(coll, 1, user)
        self.getDbCollection(coll).drop()
        return 1
