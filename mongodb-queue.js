/**
 *
 * mongodb-queue.js - Use your existing MongoDB as a local queue.
 *
 * Copyright (c) 2014 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2014/
 *
 **/

var crypto = require('crypto')
var semver = require('semver');
var _      = require('lodash');

// some helper functions
function id() {
    return crypto.randomBytes(16).toString('hex')
}

function now() {
    return (new Date()).toISOString()
}

function nowPlusSecs(secs) {
    return (new Date(Date.now() + secs * 1000)).toISOString()
}

module.exports = function(db, name, opts) {
    return new Queue(db, name, opts)
}

// the Queue object itself
function Queue(db, name, opts) {
    if ( !db ) {
        throw new Error("mongodb-queue: provide a mongodb.MongoClient.db")
    }
    if ( !name ) {
        throw new Error("mongodb-queue: provide a queue name")
    }
    opts = opts || {}

    this.db = db
    this.driverVersion = _.get(db, "client.s.options.metadata.driver.version");
    this.usePromises = false;
    if(_.isString(this.driverVersion) && semver.satisfies(this.driverVersion, '>=5.0.0')) {
        this.usePromises = true;
    }
    this.name = name
    this.col = db.collection(name)
    this.visibility = opts.visibility || 30
    this.delay = opts.delay || 0

    if ( opts.deadQueue ) {
        this.deadQueue = opts.deadQueue
        this.maxRetries = opts.maxRetries || 5
    }

    if ( opts.usePromises ) {
        this.usePromises = true;
    }
}

Queue.prototype.createIndexes = function(callback) {
    var self = this

    if(self.usePromises) {
        self.col.createIndex({ deleted: 1, visible: 1 }).then((indexname) => {
            self.col.createIndex({ack: 1}, {unique: true, sparse: true}).then(() => {
                callback(null, indexname);
            }).catch((err) => {
                callback(err);
            });
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.createIndex({ deleted : 1, visible : 1 }, function(err, indexname) {
            if (err) return callback(err)
            self.col.createIndex({ ack : 1 }, { unique : true, sparse : true }, function(err) {
                if (err) return callback(err)
                callback(null, indexname)
            })
        })
    }
}

Queue.prototype.add = function(payload, opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }
    var delay = opts.delay || self.delay
    var visible = delay ? (delay instanceof Date ? delay.toISOString() : nowPlusSecs(delay)) : now()

    var msgs = []
    if (payload instanceof Array) {
        if (payload.length === 0) {
            var errMsg = 'Queue.add(): Array payload length must be greater than 0'
            return callback(new Error(errMsg))
        }
        payload.forEach(function(payload) {
            msgs.push({
                visible  : visible,
                payload  : payload,
            })
        })
    } else {
        msgs.push({
            visible  : visible,
            payload  : payload,
        })
    }

    if(self.usePromises) {
        self.col.insertMany(msgs).then((results) => {
            if (payload instanceof Array) return callback(null, '' + results.insertedIds);
            callback(null, '' + results.insertedIds["0"]);
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.insertMany(msgs, function(err, results) {
            if (err) return callback(err);
            if (payload instanceof Array) return callback(null, '' + results.insertedIds);
            callback(null, '' + results.insertedIds["0"]);
        })
    }
}

Queue.prototype.get = function(opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }

    var visibility = opts.visibility || self.visibility
    var query = {
        deleted : null,
        visible : { $lte : now() },
    }
    var sort = {
        _id : 1
    }
    var update = {
        $inc : { tries : 1 },
        $set : {
            ack     : id(),
            visible : nowPlusSecs(visibility),
        }
    }

    var wrapper = (result) => {
        var msg = result.value
        if (!msg) return callback()

        // convert to an external representation
        msg = {
            // convert '_id' to an 'id' string
            id      : '' + msg._id,
            ack     : msg.ack,
            payload : msg.payload,
            tries   : msg.tries,
        }
        // if we have a deadQueue, then check the tries, else don't
        if ( self.deadQueue ) {
            // check the tries
            if ( msg.tries > self.maxRetries ) {
                // So:
                // 1) add this message to the deadQueue
                // 2) ack this message from the regular queue
                // 3) call ourself to return a new message (if exists)
                self.deadQueue.add(msg, function(err) {
                    if (err) return callback(err)
                    self.ack(msg.ack, function(err) {
                        if (err) return callback(err)
                        self.get(callback)
                    })
                })
                return
            }
        }
        callback(null, msg)
    };

    if(self.usePromises) {
        self.col.findOneAndUpdate(query, update, { sort: sort, returnDocument: 'after' }).then(wrapper).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.findOneAndUpdate(query, update, { sort: sort, returnDocument : 'after' }, function(err, result) {
            if(err) {
                callback(err);
            }
            else {
                wrapper(result);
            }
        })
    }
}

Queue.prototype.peek = function(callback) {
    var self = this;
    var query = {
        deleted : null,
        visible : { $lte : now() },
    }
    var sort = {
        _id : 1
    }

    var wrapper = (msg) => {
        if (!msg) return callback()
        msg.id = ''+msg._id
        delete msg._id;
        callback(null, msg)
    };

    if(self.usePromises) {
        self.col.findOne(query, { sort: sort, returnOriginal : false , returnDocument: "after" }).then(wrapper).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.findOne(query, { sort: sort, returnOriginal : false , returnDocument: "after" }, function(err, msg) {
            if(err) {
                callback(err);
            }
            else {
                wrapper(msg);
            }
        })
    }
}

Queue.prototype.ping = function(ack, opts, callback) {
    var self = this
    if ( !callback ) {
        callback = opts
        opts = {}
    }

    var visibility = opts.visibility || self.visibility
    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }
    var update = {
        $set : {
            visible : nowPlusSecs(visibility)
        }
    }
    var wrapper = (msg) => {
        if ( !msg.value ) {
            return callback(new Error("Queue.ping(): Unidentified ack  : " + ack))
        }
        callback(null, '' + msg.value._id)
    };
    if(self.usePromises) {
        self.col.findOneAndUpdate(query, update, { returnDocument : 'after' }).then(wrapper).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.findOneAndUpdate(query, update, { returnDocument : 'after' }, function(err, msg) {
            if (err) {
                callback(err);
            }
            else {
                wrapper(msg);
            }
        })
    }
}

Queue.prototype.ack = function(ack, callback) {
    var self = this

    var query = {
        ack     : ack,
        visible : { $gt : now() },
        deleted : null,
    }
    var update = {
        $set : {
            deleted : now(),
        }
    }
    var wrapper = (msg) => {
        if ( !msg.value ) {
            return callback(new Error("Queue.ack(): Unidentified ack : " + ack))
        }
        callback(null, '' + msg.value._id)
    };
    if(self.usePromises) {
        self.col.findOneAndUpdate(query, update, { returnDocument : 'after' }).then(wrapper).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.findOneAndUpdate(query, update, { returnDocument : 'after' }, function(err, msg) {
            if (err) {
                callback(err)
            }
            else {
                wrapper(msg);
            }
        })
    }
}

Queue.prototype.clean = function(callback) {
    var self = this

    var query = {
        deleted : { $exists : true },
    }

    if(self.usePromises) {
        self.col.deleteMany(query).then(() => {
            callback();
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.deleteMany(query, callback)
    }
}

Queue.prototype.total = function(callback) {
    var self = this

    if(self.usePromises) {
        self.col.countDocuments().then((count) => {
            callback(null, count);
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.countDocuments(function(err, count) {
            if (err) return callback(err)
            callback(null, count)
        })
    }
}

Queue.prototype.size = function(callback) {
    var self = this

    var query = {
        deleted : null,
        visible : { $lte : now() },
    }

    if(self.usePromises) {
        self.col.countDocuments(query).then((count) => {
            callback(null, count);
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.countDocuments(query, function(err, count) {
            if (err) return callback(err)
            callback(null, count)
        })
    }
}

Queue.prototype.inFlight = function(callback) {
    var self = this

    var query = {
        ack     : { $exists : true },
        visible : { $gt : now() },
        deleted : null,
    }

    if(self.usePromises) {
        self.col.countDocuments(query).then((count) => {
            callback(null, count);
        }).catch((err) => {
            callback(err);
        });
    }
    else {
        self.col.countDocuments(query, function(err, count) {
            if (err) return callback(err)
            callback(null, count)
        })
    }
}

Queue.prototype.done = function(callback) {
    var self = this

    var query = {
        deleted : { $exists : true },
    }

    if(self.usePromises) {
        self.col.countDocuments(query).then((count) => {
            callback(null, count);
        }).catch(callback);
    }
    else {
        self.col.countDocuments(query, function(err, count) {
            if (err) return callback(err)
            callback(null, count)
        })
    }
}
