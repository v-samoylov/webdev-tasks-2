var MongoClient = require('mongodb').MongoClient;

function sendMongoRequest(request, callback) {
    MongoClient.connect(request.url, function (err, db) {
        var collection = db.collection(request.collectionName);
        var response = collection[request.method](
            request.filter,
            request.operations,
            request.options,
            function (err, data) {
                if (data.readable) {
                    data.toArray(function (err, result) {
                        callback(err, result);
                        db.close();
                    });
                } else {
                    callback(err, data);
                    db.close();
                }
            }
        );
    });
}

var MongoRequest = function (url) {
    this.url = url;
    this.collectionName = null;
    this.method = null;
    this.filter = {};
    this.operations = {};
    this.options = {};
    this.collection = (collectionName) => {
        this.collectionName = collectionName;
        return this;
    };
    this.where = (propName) => {
        var isNegated = false;
        this.equal = (value) => {
            isNegated ?
                this.filter[propName] = {$ne: value} :
                this.filter[propName] = value;
            return this;
        };
        this.lessThan = (value) => {
            isNegated ?
                this.filter[propName] = {$gte: value} :
                this.filter[propName] = {$lt: value};
            return this;
        };
        this.greatThan = (value) => {
            isNegated ?
                this.filter[propName] = {$lte: value} :
                this.filter[propName] = {$gt: value};
            return this;
        };
        this.include = (valueArray) => {
            isNegated ?
                this.filter[propName] = {$nin: valueArray} :
                this.filter[propName] = {$all: valueArray};
            return this;
        };
        this.not = () => {
            isNegated = true;
            return this;
        };
        return this;
    };
    this.find = (callback) => {
        this.method = 'find';
        return sendMongoRequest(this, callback);
    };
    this.remove = (callback) => {
        this.method = 'remove';
        return sendMongoRequest(this, callback);
    };
    this.set = (propName, propValue) => {
        this.operations['$set'] = {};
        this.operations['$set'][propName] = propValue;
        this.update = (callback) => {
            this.method = 'updateMany';
            this.options.multi = true;
            return sendMongoRequest(this, callback);
        };
        return this;
    };
};

module.exports.server = function (url) {
    var request = new MongoRequest(url);
    return request;
};
