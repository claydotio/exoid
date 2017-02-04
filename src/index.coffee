_map = require 'lodash/map'
_isArray = require 'lodash/isArray'
_filter = require 'lodash/filter'
_isEmpty = require 'lodash/isEmpty'
_isUndefined = require 'lodash/isUndefined'
_find = require 'lodash/find'
_transform = require 'lodash/transform'
_zip = require 'lodash/zip'
_defaults = require 'lodash/defaults'
_pickBy = require 'lodash/pickBy'
_mapValues = require 'lodash/mapValues'
_clone = require 'lodash/clone'
_forEach = require 'lodash/forEach'
_findIndex = require 'lodash/findIndex'
Rx = require 'rx-lite'
log = require 'loga'
stringify = require 'json-stable-stringify'
uuid = require 'node-uuid'

module.exports = class Exoid
  constructor: ({@api, cache, @ioEmit, @io}) ->
    cache ?= {}

    @_cache = {}
    @_batchQueue = []
    @_listeners = {}
    @_consumeTimeout = null

    @cacheStreams = new Rx.ReplaySubject 1
    @cacheStreams.onNext Rx.Observable.just @_cache
    @cacheStream = @cacheStreams.switch()

    @io.on 'disconnect', @invalidateAll

    _map cache, (result, key) =>
      req = JSON.parse key

      @_cacheSet key, Rx.Observable.just result

  _deferredRequestStream: (req, options = {}) =>

    {isErrorable, streamId, clientChangesStream, initialSortFn} = options

    batchStream = @_batchCacheRequest req, {isErrorable, streamId}
    requestStream = if streamId \
                    then @_replaySubjectFromIo @io, streamId
                    else Rx.Observable.just null
    clientChangesStream ?= Rx.Observable.just null
    changesStream = Rx.Observable.merge requestStream, clientChangesStream

    Rx.Observable.defer =>
      Rx.Observable.concat(
        batchStream, changesStream
      )
      .scan (items, update) =>
        @_combineChanges {
          items
          initial: if update?.changes then null else update
          changes: update?.changes
        }, {initialSortFn}
      , null
      .shareReplay 1

  _replaySubjectFromIo: (io, eventName) =>
    unless @_listeners[eventName]
      replaySubject = new Rx.ReplaySubject 0
      ioListener = io.on eventName, (data) ->
        replaySubject.onNext data
      @_listeners[eventName] = {replaySubject, ioListener}
    @_listeners[eventName].replaySubject

  _batchCacheRequest: (req, {isErrorable, streamId}) =>
    streamId ?= uuid.v4()
    req.streamId = streamId

    unless @_consumeTimeout
      @_consumeTimeout = setTimeout @_consumeBatchQueue

    res = new Rx.AsyncSubject()

    @_batchQueue.push {req, res, isErrorable, streamId}

    res

  _updateCacheStream: =>
    stream = Rx.Observable.combineLatest _map @_cache, ({stream}, key) ->
      stream.map (value) -> [key, value]
    .map (pairs) ->
      _transform pairs, (cache, [key, val]) ->
        cache[key] = val
      , {}

    @cacheStreams.onNext stream

  getCacheStream: => @cacheStream

  _cacheSet: (key, stream, options) =>
    unless @_cache[key]?
      requestStreams = new Rx.ReplaySubject 1
      @_cache[key] = {stream: requestStreams.switch(), requestStreams, options}
      @_updateCacheStream()

    @_cache[key].requestStreams.onNext stream

  _consumeBatchQueue: =>
    queue = @_batchQueue
    @_batchQueue = []
    @_consumeTimeout = null

    start = Date.now()
    onBatch = (responses) =>
      _forEach responses, ({result, error}, streamId) =>
        queueIndex = _findIndex queue, {streamId}
        {req, res, isErrorable} = queue[queueIndex]
        # console.log '-----------'
        # console.log req.path, req.body, req.query, Date.now() - start
        # console.log '-----------'
        queue.splice queueIndex, 1
        if _isEmpty queue
          @io.off batchId, onBatch

        if isErrorable and error?
          properError = new Error "#{JSON.stringify error}"
          res.onError _defaults properError, error
        else if not error?
          res.onNext result
          res.onCompleted()
        else
          log.error error

    batchId = uuid.v4()
    @io.on batchId, onBatch, (error) ->
      _map queue, ({res, isErrorable}) ->
        if isErrorable
          res.onError error
        else
          log.error error

    @ioEmit 'exoid', {
      batchId: batchId
      isClient: window?
      requests: _map queue, 'req'
    }

  _combineChanges: ({items, initial, changes}, {initialSortFn}) ->
    if initial
      items = _clone initial
      if _isArray(items) and initialSortFn
        items = initialSortFn items
    else if changes
      items ?= []
      _forEach changes, (change) ->
        existingIndex = change.oldId and
                        _findIndex(items, {id: change.oldId}) or
                        _findIndex(items, {clientId: change.newVal?.clientId})
        if existingIndex? and existingIndex isnt -1 and change.newVal
          items.splice existingIndex, 1, change.newVal
        else if existingIndex? and existingIndex isnt -1
          items.splice existingIndex, 1
        else
          items = items.concat [change.newVal]
    return items

  getCached: (path, body) =>
    req = {path, body}
    key = stringify req

    if @_cache[key]?
      @_cache[key].stream.take(1).toPromise()
    else
      Promise.resolve null

  stream: (path, body, options = {}) =>
    {ignoreCache} = options

    req = {path, body}
    key = stringify req
    streamId = uuid.v4()

    if not @_cache[key]? or ignoreCache
      options = _defaults options, {
        streamId
        isErrorable: false
      }
      clientChangesStream = options.clientChangesStream
      clientChangesStream ?= new Rx.ReplaySubject 0
      clientChangesStream = clientChangesStream.map (change) ->
        {initial: null, changes: [{newVal: change}], isClient: true}
      options.clientChangesStream = clientChangesStream
      stream = @_deferredRequestStream req, options

      if ignoreCache
        return stream

      @_cacheSet key, stream, options

    return @_cache[key].stream

  call: (path, body) =>
    req = {path, body}

    stream = @_batchCacheRequest req, {isErrorable: true}
    return stream.take(1).toPromise().then (result) ->
      return result

  invalidateAll: =>
    _map @_listeners, (listener, streamId) =>
      @io.off streamId, listener?.ioListener

    @_cache = _pickBy _mapValues(@_cache, (cache, key) =>
      {requestStreams, options} = cache
      unless requestStreams.hasObservers()
        return false
      req = JSON.parse key
      set = @_deferredRequestStream req, options
      requestStreams.onNext set
      cache
    ), (val) -> val

    @_listeners = {}
    return null

  invalidate: (path, body) =>
    req = {path, body}
    key = stringify req

    _map @_cache, ({requestStreams}, cacheKey) =>
      req = JSON.parse cacheKey
      if req.path is path and _isUndefined body or cacheKey is key
        requestStreams.onNext @_deferredRequestStream req
    return null
