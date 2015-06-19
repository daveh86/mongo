// wiredtiger_session_cache.cpp

/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/db/storage/wiredtiger/wiredtiger_session_cache.h"

#include "mongo/db/storage/wiredtiger/wiredtiger_kv_engine.h"
#include "mongo/db/storage/wiredtiger/wiredtiger_util.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/log.h"

namespace mongo {

template <typename T> void TaggedAtomicWrapper<T>::set(T *p, uint64_t t) {
  ptr = p;
  tag = t;
}
template <typename T>
bool TaggedAtomicWrapper<T>::
operator==(volatile TaggedAtomicWrapper const &p) const {
  return (ptr == p.ptr) && (tag == p.tag);
}
template <typename T>
bool TaggedAtomicWrapper<T>::
operator!=(volatile TaggedAtomicWrapper const &p) const {
  return !operator==(p);
}
template <typename T> T *TaggedAtomicWrapper<T>::get_ptr(void) const {
  return ptr;
}
template <typename T> T &TaggedAtomicWrapper<T>::operator*() const {
  return *ptr;
}
template <typename T> T *TaggedAtomicWrapper<T>::operator->() const {
  return ptr;
}
template <typename T> TaggedAtomicWrapper<T>::operator bool(void) const {
  return ptr != 0;
}

WiredTigerSession::WiredTigerSession(WT_CONNECTION *conn, int epoch)
    : _epoch(epoch), _session(NULL), _cursorGen(0), _cursorsCached(0),
      _cursorsOut(0), _next(0) {
  invariantWTOK(
      conn->open_session(conn, NULL, "isolation=snapshot", &_session));
}

WiredTigerSession::~WiredTigerSession() {
  if (_session) {
    invariantWTOK(_session->close(_session, NULL));
  }
}

WT_CURSOR *WiredTigerSession::getCursor(const std::string &uri, uint64_t id,
                                        bool forRecordStore) {
  // Find the most recently used cursor
  for (CursorCache::iterator i = _cursors.begin(); i != _cursors.end(); ++i) {
    if (i->_id == id) {
      WT_CURSOR *c = i->_cursor;
      _cursors.erase(i);
      _cursorsOut++;
      _cursorsCached--;
      return c;
    }
  }

  WT_CURSOR *c = NULL;
  int ret = _session->open_cursor(_session, uri.c_str(), NULL,
                                  forRecordStore ? "" : "overwrite=false", &c);
  if (ret != ENOENT)
    invariantWTOK(ret);
  if (c)
    _cursorsOut++;
  return c;
}

void WiredTigerSession::releaseCursor(uint64_t id, WT_CURSOR *cursor) {
  invariant(_session);
  invariant(cursor);
  _cursorsOut--;

  invariantWTOK(cursor->reset(cursor));

  // Cursors are pushed to the front of the list and removed from the back
  _cursors.push_front(WiredTigerCachedCursor(id, _cursorGen++, cursor));
  _cursorsCached++;

  // "Old" is defined as not used in the last N**2 operations, if we have N
  // cursors cached.
  // The reasoning here is to imagine a workload with N tables performing
  // operations randomly
  // across all of them.  We would like to cache N cursors in that case.
  uint64_t cutoff = std::max(100, _cursorsCached * _cursorsCached);
  while (_cursorGen - _cursors.back()._gen > cutoff) {
    cursor = _cursors.back()._cursor;
    _cursors.pop_back();
    invariantWTOK(cursor->close(cursor));
  }
}

void WiredTigerSession::closeAllCursors() {
  invariant(_session);
  for (CursorCache::iterator i = _cursors.begin(); i != _cursors.end(); ++i) {
    WT_CURSOR *cursor = i->_cursor;
    if (cursor) {
      invariantWTOK(cursor->close(cursor));
    }
  }
  _cursors.clear();
}

namespace {
AtomicUInt64 nextCursorId(1);
AtomicUInt64 currSessionsInCache(0);
}
// static
uint64_t WiredTigerSession::genCursorId() {
  return nextCursorId.fetchAndAdd(1);
}

// -----------------------

WiredTigerSessionCache::WiredTigerSessionCache(WiredTigerKVEngine *engine)
    : _engine(engine), _conn(engine->getConnection()), _sessionsOut(0),
      _shuttingDown(0), _highWaterMark(1), _head(Tagger(0, 0)) {}

WiredTigerSessionCache::WiredTigerSessionCache(WT_CONNECTION *conn)
    : _engine(NULL), _conn(conn), _sessionsOut(0), _shuttingDown(0),
      _highWaterMark(1), _head(Tagger(0, 0)) {}

WiredTigerSessionCache::~WiredTigerSessionCache() { shuttingDown(); }

void WiredTigerSessionCache::shuttingDown() {
  if (_shuttingDown.load())
    return;
  _shuttingDown.store(1);

  {
    // This ensures that any calls, which are currently inside of
    // getSession/releaseSession
    // will be able to complete before we start cleaning up the pool. Any
    // others, which are
    // about to enter will return immediately because of _shuttingDown == true.
    boost::lock_guard<boost::shared_mutex> lk(_shutdownLock);
  }

  closeAll();
}

void WiredTigerSessionCache::closeAll() {
  // Increment the epoch as we are now closing all sessions with this epoch
  _epoch++;
  // Grab each session from the list and delete
  Tagger cachedSession;
  while ((cachedSession = _head.load()) != 0) {
    // Keep trying to remove the head until we succeed
    if (_head.compare_exchange_weak(
            cachedSession, Tagger(cachedSession->_next, cachedSession->_tag))) {
      currSessionsInCache.fetchAndSubtract(1);
      WiredTigerSession *session = cachedSession.get_ptr();
      session->_tag++;
      session->_next = 0;
      delete session;
    }
  }
}

WiredTigerSession *WiredTigerSessionCache::getSession() {
  boost::shared_lock<boost::shared_mutex> shutdownLock(_shutdownLock);

  // We should never be able to get here after _shuttingDown is set, because no
  // new
  // operations should be allowed to start.
  invariant(!_shuttingDown.loadRelaxed());

  // Set the high water mark if we need too
  if (++_sessionsOut > _highWaterMark) {
    _highWaterMark = _sessionsOut.load();
  }

  // We are popping here, compare_exchange will try and replace the
  // current head (our session) with the next session in the queue
  Tagger cachedSession = _head.load();
  while (
      cachedSession &&
      !_head.compare_exchange_weak(
          cachedSession, Tagger(cachedSession->_next, cachedSession->_tag))) {
  }
  if (cachedSession) {
    currSessionsInCache.fetchAndSubtract(1);
    WiredTigerSession *outbound_session = cachedSession.get_ptr();
    // Clear the next session for when we put it back
    outbound_session->_next = 0;
    // Increment the tag to avoid ABA when this element is put back
    outbound_session->_tag++;
    return outbound_session;
  }

  // Outside of the cache partition lock, but on release will be put back on the
  // cache
  return new WiredTigerSession(_conn, _epoch);
}

void WiredTigerSessionCache::releaseSession(WiredTigerSession *session) {
  invariant(session);
  invariant(session->cursorsOut() == 0);

  boost::shared_lock<boost::shared_mutex> shutdownLock(_shutdownLock);
  if (_shuttingDown.loadRelaxed()) {
    // Leak the session in order to avoid race condition with clean shutdown,
    // where the
    // storage engine is ripped from underneath transactions, which are not
    // "active"
    // (i.e., do not have any locks), but are just about to delete the recovery
    // unit.
    // See SERVER-16031 for more information.
    return;
  }

  // This checks that we are only caching idle sessions and not something which
  // might hold
  // locks or otherwise prevent truncation.
  {
    WT_SESSION *ss = session->getSession();
    uint64_t range;
    invariantWTOK(ss->transaction_pinned_range(ss, &range));
    invariant(range == 0);
  }

  bool returnedToCache = false;

  invariant(session->_getEpoch() <= _epoch);

  /**
   * In this case we only want to return sessions until we hit the maximum
   * number of
   * sessions we have ever seen demand for concurrently. We also want to
   * immediately
   * delete any session that is from a non-current epoch.
   */
  if (session->_getEpoch() == _epoch) {
    // When returning, we replace the atomic head with the session pointer and
    // its
    // tag value. This lets other threads work out that this has been popped and
    // replaced, if they have been waiting the whole time this session has been
    // out (ABA Problem).
    Tagger returning_session(session, session->_tag);
    Tagger old_head = _head.load();
    while (currSessionsInCache.load() < _highWaterMark.load()) {
      returning_session->_next = old_head.get_ptr();
      if (_head.compare_exchange_weak(old_head, returning_session)) {
        returnedToCache = true;
        currSessionsInCache.fetchAndAdd(1);
        break;
      }
    }
  }
  _sessionsOut--;
  // Do all cleanup outside of the cache partition spinlock.
  if (!returnedToCache) {
    delete session;
  }

  if (_engine && _engine->haveDropsQueued()) {
    _engine->dropAllQueued();
  }
}
}
