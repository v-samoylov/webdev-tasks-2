var MongoClient = require('mongodb').MongoClient;

function sendMongoRequest(request, callback) {
    MongoClient.connect(request.url, function (err, db) {
        if (err) {
            return callback(err);
        }
        var onResponseCallback = (err, data) => {
            callback(err, data);
            db.close();
        };
        var collection = db.collection(request.collection);
        switch (request.method) {
            case 'insert':
                var response = collection.insert(
                    request.document,
                    request.options,
                    onResponseCallback
                );
                break;
            case 'find':
                var response = collection.find(
                    request.filter,
                    request.projection
                );
                response.toArray(onResponseCallback);
                break;
            case 'remove':
                var response = collection.remove(
                    request.filter,
                    onResponseCallback
                );
                break;
            case 'updateMany':
                var response = collection.update(
                    request.filter,
                    request.operations,
                    request.options,
                    onResponseCallback
                );
                break;
        }
    });
}

var MongoRequest = function (url) {
    var _collectionName = null;
    var method = null;
    var filter = {};
    var operations = {};
    var options = {};
    var document = null;
    var projection = {};
    function send(request, callback) {
        request.url = url;
        request.method = method;
        request.collection = _collectionName;
        sendMongoRequest(request, callback);
    };
    this.collection = (collectionName) => {
        _collectionName = collectionName;
        return this;
    };
    this.where = (propName) => {
        var isNegated = false;
        this.equal = (value) => {
            isNegated ?
                filter[propName] = {$ne: value} :
                filter[propName] = value;
            return this;
        };
        this.lessThan = (value) => {
            isNegated ?
                filter[propName] = {$gte: value} :
                filter[propName] = {$lt: value};
            return this;
        };
        this.greatThan = (value) => {
            isNegated ?
                filter[propName] = {$lte: value} :
                filter[propName] = {$gt: value};
            return this;
        };
        this.include = (valueArray) => {
            isNegated ?
                filter[propName] = {$nin: valueArray} :
                filter[propName] = {$in: valueArray};
            return this;
        };
        this.not = () => {
            isNegated = true;
            return this;
        };
        return this;
    };
    this.find = (callback) => {
        method = 'find';
        return send({
            filter,
            projection
        }, callback);
    };
    this.remove = (callback) => {
        method = 'remove';
        return send({
            filter
        }, callback);
    };
    this.set = (propName, propValue) => {
        operations['$set'] = {};
        operations['$set'][propName] = propValue;
        this.update = (callback) => {
            method = 'updateMany';
            options.multi = true;
            return send({
                filter,
                operations,
                options
            }, callback);
        };
        return this;
    };
    this.insert = (doc, callback) => {
        document = doc;
        method = 'insert';
        return send({
            document,
            options
        }, callback);
    };
};

module.exports.server = function (url) {
    var request = new MongoRequest(url);
    return request;
};
