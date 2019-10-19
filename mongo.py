from pymongo import MongoClient, ObjectId

from base import Store

class MongoStore(Store)

    @staticmethod
    def genId(inputId):
        return ObjectId(inputId) if (isinstance(inputId, str) and ObjectId.is_valid(inputId)) else inputId;

    @staticmethod
    def transformOutput(rec):
        if rec and '_id' in rec and isinstance(rec['_id'], str):
            rec['_id'] = str(rec['_id'])
        return rec

    def getDbCollection(name):
        return self.db.collection(self.getDbCollectionName(name))

    def list(self, user, coll, filters, options):
        self.authorizeCollection(coll, 0, options.user, options);
        const { sort, skip = 0, limit = 10, count, fields } = options || {};
        let total = 0;
        const collection = this.getCollection(coll);
        if (collection) {
          let cursor = collection.find(filter, { fields });
          if (sort) cursor = cursor.sort(sort);
          let records = (await cursor.skip(skip).limit(limit).toArray());
          if (!fields) {
            records = records.map(doc => doc._id);
          }
          if (count) {
            total = await collection.count(filter);
            return { records, total };
          }
          return { records };
        }
        return { records: [] };
  }

  /**
   * list all the collections.
   * @return {Promise} promise - return a promise
   */
  async listcolls() {
    return (await this.db.listCollections().toArray())
      .map(cl => cl.name.substring(this.dbprefix.length));
  }

  /**
   * read a document.
   * @param {string} coll - the collection, that should be read
   * @param {string} _id - the document id, that should be read
   * @param {object} fields - the fields to retrieve
   * @param {object} options - the options to read a document
   * @return {Promise} promise - return a promise */
  async read(coll, _id, fields, options = {}) {
    const cont = await this.getCollection(coll).findOne({ _id: Store.GenId(_id) }, { fields });
    this.authorize(coll, 0, options.user, cont, options);
    this.emitDbEvent(`read:${coll}:${_id}`, cont);
    return cont;
  }

  /**
   * create or update a document.
   * @param {string} coll - the collection, that should be read
   * @param {string} _id - the doc id at which, that should be created/updated
   * @param {object} data - the content in to write
   * @param {object} options - the options to update the collection
   * @return {Promise} promise - return a promise
   */
  async write(coll, _id, data, options = {}) {
    const collection = this.getCollection(coll);
    if (_id) {
      const prevInfo = await this.getPrevDoc(coll, Store.GenId(_id), options);
      const authUpdate = this.authorize(coll, 1, options.user, prevInfo, options);
      // TODO validating schema
      if (options.dbOp && options.dbOp !== '$set') {
        await collection.findOneAndUpdate({ _id: Store.GenId(_id) },
         { [options.dbOp]: data, $set: authUpdate });
      } else {
        await collection.findOneAndUpdate({ _id: Store.GenId(_id) },
         { $set: Object.assign(data, authUpdate) });
      }
      this.emitDbEvent(`update:${coll}:${_id}`, data, prevInfo);
      return 1;
    }
    Object.assign(data, this.authorizeCollection(coll, 1, options.user, options));
    this.validateSchema(coll, data, options);
    const newId = (await collection.insertOne(data)).insertedId;
    this.emitDbEvent(`create:${coll}:${newId}`, data);
    return newId;
  }

  /**
   * delete a document.
   * @param {string} coll - the collection, that should be read
   * @param {string} _id - the doc id at which, that should be deleted
   * @param {object} options - the options to delete the collection
   * @return {Promise} promise - return a promise
   */
  async del(coll, _id, options = {}) {
    const prevInfo = await this.getPrevDoc(coll, Store.GenId(_id), options);
    this.authorize(coll, 1, options.user, prevInfo, options);
    await this.getCollection(coll).deleteOne({ _id: Store.GenId(_id) });
    this.emitDbEvent(`delete:${coll}:${_id}`, prevInfo);
    return 1;
  }

  /**
   * create a collection.
   * @param {string} coll - the new collection, that should be created
   * @param {Boolean} loadFixture=true - to ask to create index just register the collection
   * @param {Object} auth - the auth for collection
   * @param {Object[]} indexes - the indexes to apply
   * @param {Object} schema - the scheme of collection
   */
  async mkcoll(coll, loadFixture = true, auth, indexes = [], schema) {
    Base.prototype.mkcoll.call(this, coll, auth, schema);
    if (loadFixture) {
      await this.db.createCollection(this.dbprefix + coll);
      await (this.getCollection(coll).createIndexes(indexes.map(ind => Object.assign(ind,
        { name: ind.name || Object.keys(ind).join('_') }))));
    }
    return 1;
  }

  /**
   * remove a collection.
   * @param {string} coll - the collection name that should be deleted
   * @param {Object} user - the user who is firing the query
   * @return {Promise} promise - return a promise
   */
  rmcoll(coll, user) {
    this.authorizeCollection(coll, 1, user);
    this.getCollection(coll).drop();
    return 1;
  }
