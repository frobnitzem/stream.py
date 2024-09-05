# ChangeLog

## [unreleased]


## [1.0.0] - 2024-09-04

This is a major release of the `stream` package, which
makes many changes to the API.  Most notably, the `Stream`
class has been broken up into separate `Source`, `Stream`, and `Sink`
classes to denote the status of a stream (whether a source
or sink is connected).


### Added

- `@source`, `@stream`, and `@sink` decorators make it easy to
  create these objects from ordinary functions.

- a `last` Sink object was added to access the last item(s) from the stream

- a `dup` operator which replaces `tee`.

- a `tap` operator which passively calls a callback for each item
  pulled through the stream

### Changed

- Stream-s are not mutable by default.  Adding/removing items
  from a stream only works for Source objects.

- item[] returns a stream.  It does not work for negative indices.
  Use last() as a Sink instead, which is explicit.

- divided up functionality into core, ops, and parallel
  modules `__init__.py` imports them all.

- replaced tee function from stream with a stateful function caller
  Added added dup, tap for other patterns.

- removed Executor. It's largely redundant with python's
  ThreadPoolExecutor and ProcessPoolExecutor.

- several parallel routines were renamed.  Their parallelization
  strategies may change still more in the future

    * rename ThreadedFeeder -> QueueSink

    * rename ForkedFeeder   -> PipeSink

    * rename QCollector     -> QueueSource

    * rename PCollector     -> PipeSource

    * rename ThreadPool     -> ThreadStream

    * rename ProcessPool    -> ProcessStream


### Fixed

- Stream and Sink objects are now referentially transparent.
  Because they are closures which call a function once a source
  iterable is available, only explicit global values in these
  functions will change their state.

