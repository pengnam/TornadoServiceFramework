import tornado.gen
import motor

class DB(object):

    def __init__(self, db):
        self.db = db

    #################### Single document #####################
    @tornado.gen.coroutine
    def get_document(self, collection_name, id):
        data = yield self.query_one(collection_name, {"_id" : id})
        return data

    @tornado.gen.coroutine
    def has_document(self, collection_name, id):
        count = yield self.db[collection_name].find({"_id" : id}).count()
        return count > 0

    @tornado.gen.coroutine
    def insert_document(self, collection_name, data):
        result = yield self.db[collection_name].save(data)
        return result

    @tornado.gen.coroutine
    def save_document(self, collection_name, data):
        result = yield self.db[collection_name].save(data)
        return result

    @tornado.gen.coroutine
    def update_document(self, collection_name, id, changes):
        result = yield self.db[collection_name].update({"_id":id}, {"$set" : changes })
        return result

    @tornado.gen.coroutine
    def query_one(self, collection_name, query):
        data = yield self.db[collection_name].find_one(query)
        return data

    @tornado.gen.coroutine
    def remove_by_query(self, collection_name, query):
        result = yield self.db[collection_name].remove(query)
        return result

    #################### Multiple Document ####################
    @tornado.gen.coroutine
    def query_ids(self, collection_name, query, sort=None, pagination=None):
        ids_cursor = self.db[collection_name].find(query, {"_id" : 1})
        if sort is not None and sort.get("by") is not None:
            if sort["by"] == "updated":
                ids_cursor.sort([("updated_at", sort["order"])])
            elif sort["by"] == "created":
                ids_cursor.sort([("created_at", sort["order"])])
            if sort["by"] == "price":
                ids_cursor.sort([("attributes.price", sort["order"])])
            elif sort["by"] == "bedrooms":
                ids_cursor.sort([("attributes.bedrooms", sort["order"])])
            if sort["by"] == "floor_area":
                ids_cursor.sort([("attributes.floor_area", sort["order"])])
            elif sort["by"] == "is_verified":
                ids_cursor.sort([("attributes.is_verified", sort["order"])])
            elif sort["by"] == "posted_at":
                ids_cursor.sort([("posted_at", sort["order"])])

        ids = []

        if pagination:
            ids_cursor.skip(pagination["skip"])
            ids_cursor.limit(pagination["limit"])
        while (yield ids_cursor.fetch_next):
            ids.append(ids_cursor.next_object()["_id"])
        return ids

    @tornado.gen.coroutine
    def has_documents(self, collection_name, ids):
        count = yield self.db[collection_name].find({"_id" : { "$in" : ids }}).count()
        return count == len(ids)

    @tornado.gen.coroutine
    def get_documents(self, collection_name, ids, field=None):
        cursor = self.db[collection_name].find({"_id" : { "$in" : ids } }, field)
        documents = {}
        while (yield cursor.fetch_next):
            obj = cursor.next_object()
            documents[obj.get("_id")] = obj
        return documents

    @tornado.gen.coroutine
    def count_documents(self, collection_name, query):
        cursor = self.db[collection_name].find(query)
        count = yield cursor.count()
        return count

    @tornado.gen.coroutine
    def update_documents(self, collection_name, query, changes):
        result = yield self.db[collection_name].update(query, {"$set" : changes}, multi=True)
        return result

    @tornado.gen.coroutine
    def delete_documents(self, collection_name, query):
        result = yield self.db[collection_name].remove(query)
        return result

    @tornado.gen.coroutine
    def query_via_cursor(self, collection_name, query, sort=None, pagination=None, return_count=False):
        pagination = pagination or {}

        cursor = self.db[collection_name].find(query)
        if return_count:
            count = yield cursor.count()
        if sort:
            cursor.sort(sort)

        if "page" in pagination or "page_size" in pagination:
            page = pagination.get("page", 1)
            page_size = pagination.get("page_size", 20)
            skip = (page-1) * page_size
            limit = page_size
        elif "limit" in pagination and "skip" in pagination:
            skip = pagination.get("skip", 0)
            limit = pagination.get("limit", 20)
        else:
            skip = 0
            limit = 20

        result = []
        if pagination:
            cursor.skip(skip)
            cursor.limit(limit)
            result = yield cursor.to_list(length=limit)
        else:
            while (yield cursor.fetch_next):
                result.append(cursor.next_object())

        if not return_count:
            raise tornado.gen.Return(result)
        else:
            raise tornado.gen.Return({"data":result, "count":count})

    @tornado.gen.coroutine
    def aggregate_ids_by_one_field(self, collection_name, query, aggregate_field, count_only=False):
        """Aggregate all the object in this collection that match the query, grouping them by aggregate_field

        Return a list of aggregation
        [
            { "_id" : <value of the aggregate_field>, "data" : <list of object id> }
        """
        if not count_only:
            aggregation = [
                { "$match" : query },
                { "$project" : { "_id" : 1, aggregate_field : 1 } },
                { "$group" : { "_id" : "${0}".format(aggregate_field), "data" : { "$push" : "$_id" } } }
            ]
        else:
            aggregation = [
                { "$match" : query },
                { "$project" : { "_id" : 1, aggregate_field : 1 } },
                { "$group" : { "_id" : "${0}".format(aggregate_field), "data" : { "$sum" : 1 } } }
            ]
        aggregation_result = yield self.db[collection_name].aggregate(aggregation, cursor={"batchSize": 100})
        result = []
        while (yield aggregation_result.fetch_next):
            item = aggregation_result.next_object()
            result.append(item)
        return result

    @tornado.gen.coroutine
    def aggregate(self, collection_name, aggregation):
        aggregation_result = yield self.db[collection_name].aggregate(aggregation, cursor={})
        result = []
        while (yield aggregation_result.fetch_next):
            item = aggregation_result.next_object()
            result.append(item)
        return result
