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

    @dataCacheStreams = new Rx.ReplaySubject 1
    @dataCacheStreams.onNext Rx.Observable.just cache
    @dataCacheStream = @dataCacheStreams.switch()

    @io.on 'disconnect', @invalidateAll

    _map cache, (result, key) =>
      req = JSON.parse key

      @_cacheSet key, {dataStream: Rx.Observable.just result}

  _updateDataCacheStream: =>
    stream = Rx.Observable.combineLatest _map @_cache, ({dataStream}, key) ->
      dataStream.map (value) -> [key, value]
    .map (pairs) ->
      _transform pairs, (cache, [key, val]) ->
        cache[key] = val
      , {}

    @dataCacheStreams.onNext stream

  getCacheStream: => @dataCacheStream

  _cacheSet: (key, {combinedStream, dataStream, options}) =>
    if dataStream and not @_cache[key]?.dataStream
      dataStreams = new Rx.ReplaySubject 1
      @_cache[key] ?= {}
      @_cache[key].dataStreams = dataStreams
      @_cache[key].dataStream = dataStreams.switch()

    if combinedStream and not @_cache[key]?.combinedStream
      combinedStreams = new Rx.ReplaySubject 1
      @_cache[key] ?= {}
      @_cache[key].options = options
      @_cache[key].combinedStreams = combinedStreams
      @_cache[key].combinedStream = combinedStreams.switch()

    if dataStream
      @_cache[key].dataStreams.onNext dataStream
      @_updateDataCacheStream()

    if combinedStream
      @_cache[key].combinedStreams.onNext combinedStream

  _batchRequest: (req, {isErrorable, streamId} = {}) =>
    streamId ?= uuid.v4()
    req.streamId = streamId

    unless @_consumeTimeout
      @_consumeTimeout = setTimeout @_consumeBatchQueue

    res = new Rx.AsyncSubject()
    @_batchQueue.push {req, res, isErrorable, streamId}
    res

  _consumeBatchQueue: =>
    queue = @_batchQueue
    @_batchQueue = []
    @_consumeTimeout = null

    start = Date.now()
    onBatch = (responses) =>
      _forEach responses, ({result, error}, streamId) =>
        queueIndex = _findIndex queue, {streamId}
        if queueIndex is -1
          console.log 'stream ignored', streamId
          return
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

  _combinedRequestStream: (req, options = {}) =>
    {isErrorable, streamId, clientChangesStream,
      initialSortFn, ignoreCache} = options

    initialDataStream = @_initialDataRequest req, {
      isErrorable, streamId, ignoreCache
    }
    additionalDataStream = if streamId \
                           then @_replaySubjectFromIo @io, streamId
                           else Rx.Observable.just null
    clientChangesStream ?= Rx.Observable.just null
    changesStream = Rx.Observable.merge(
      additionalDataStream, clientChangesStream
    )
    .shareReplay 1

    # ideally we'd use concat here instead, but initialDataStream is
    # a switch observable because of cache
    Rx.Observable.merge(
      initialDataStream, changesStream
    )
    .scan (items, update) =>
      @_combineChanges {
        items
        initial: if update?.changes then null else update
        changes: update?.changes
      }, {initialSortFn}
    , null

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

  _replaySubjectFromIo: (io, eventName) =>
    unless @_listeners[eventName]
      replaySubject = new Rx.ReplaySubject 0
      # TODO: does this have bad side-effects?
      # if stream gets to 0 subscribers, the next subscriber starts over
      # from scratch and we lose all the progress of the .scan.
      # This is because shareReplay (and any subject) will disconnect when it
      # hits 0 and reconnect. The supposed solution is "autoconnect", I think,
      # but it's not in rxjs at the moment: http://stackoverflow.com/a/36118469
      disposable = replaySubject.subscribe ->
        null
      ioListener = io.on eventName, (data) ->
        replaySubject.onNext data
      @_listeners[eventName] = {replaySubject, ioListener, disposable}
    @_listeners[eventName].replaySubject

  _initialDataRequest: (req, {isErrorable, streamId, ignoreCache}) =>
    key = stringify req
    if not @_cache[key]?.dataStream or ignoreCache
      # should only be caching the actual async result and nothing more, since
      # that's all we can really get from server -> client rendering with
      # json.stringify
      @_cacheSet key, {dataStream: @_batchRequest(req, {isErrorable, streamId})}

    @_cache[key].dataStream

  getCached: (path, body) =>
    req = {path, body}
    key = stringify req

    if @_cache[key]?
      @_cache[key].dataStream.take(1).toPromise()
    else
      Promise.resolve null

  stream: (path, body, options = {}) =>
    req = {path, body}
    key = stringify req

    unless @_cache[key]?.combinedStream
      streamId = uuid.v4()
      options = _defaults options, {
        streamId
        isErrorable: false
      }
      clientChangesStream = options.clientChangesStream
      clientChangesStream ?= new Rx.ReplaySubject 0
      clientChangesStream = clientChangesStream.map (change) ->
        {initial: null, changes: [{newVal: change}], isClient: true}
      options.clientChangesStream = clientChangesStream

      @_cacheSet key, {
        options
        combinedStream: @_combinedRequestStream req, options
      }

    @_cache[key]?.combinedStream

  call: (path, body) =>
    req = {path, body}

    stream = @_batchRequest req, {isErrorable: true}
    return stream.take(1).toPromise().then (result) ->
      return result

  invalidateAll: =>
    _map @_listeners, (listener, streamId) =>
      @io.off streamId, listener?.ioListener
      listener?.disposable?.dispose()
    @_listeners = {}

    @_cache = _pickBy _mapValues(@_cache, (cache, key) =>
      {dataStreams, combinedStreams, options} = cache
      unless combinedStreams.hasObservers()
        return false
      req = JSON.parse key
      dataStreams.onNext @_batchRequest req
      combinedStreams.onNext @_combinedRequestStream req, options
      cache
    ), (val) -> val
    return null

  invalidate: (path, body) =>
    req = {path, body}
    key = stringify req

    _map @_cache, ({dataStreams, combinedStreams, options}, cacheKey) =>
      req = JSON.parse cacheKey
      if req.path is path and _isUndefined body or cacheKey is key
        dataStreams.onNext @_batchRequest req
        combinedStreams.onNext @_combinedRequestStream req, options
    return null
