// wiredtiger_session_cache.h

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

#pragma once

#include <string>
#include <deque>
#include <atomic>

#include <boost/thread/shared_mutex.hpp>

#include <wiredtiger.h>

#include "mongo/platform/atomic_word.h"
#include "mongo/util/concurrency/spin_lock.h"

namespace mongo {


    /**
     * A class for working around the ABA problem when creating atomic data stores
     * This contains an instance of a tag integer and a pointer to the data structure
     * that is to be stored. The tag value is incremented ever time an object is popped
     * this avoids the ABA problem by making this object different every time the
     * we pop from the stack
     */
    template <class T>
    class TaggedAtomicWrapper {
    public:
        TaggedAtomicWrapper(void): ptr(0), tag(0) {}
        TaggedAtomicWrapper(TaggedAtomicWrapper const & p) = default;
        TaggedAtomicWrapper & operator= (TaggedAtomicWrapper const & p) = default;
        explicit TaggedAtomicWrapper(T* p, uint64_t t = 0): ptr(p), tag(t) {}

        void set(T * p, uint64_t t);
        bool operator== (volatile TaggedAtomicWrapper const & p) const;
        bool operator!= (volatile TaggedAtomicWrapper const & p) const;
        T * get_ptr(void) const;
        T & operator*() const;
        T * operator->() const;
        operator bool(void) const;
    protected:
       T* ptr;
       uint64_t tag;
    };

    class WiredTigerKVEngine;

    /**
     * This is a structure that caches 1 cursor for each uri.
     * The idea is that there is a pool of these somewhere.
     * NOT THREADSAFE
     */
    class WiredTigerSession {
    public:

        /**
         * Creates a new WT session on the specified connection.
         *
         * @param conn WT connection
         * @param cachePartition If the session comes from the session cache, this indicates to
         *          which partition it should be returned. Value of -1 means it doesn't come from
         *          cache and that it should not be cached, but closed directly.
         * @param epoch In which session cache cleanup epoch was this session instantiated. Value
         *          of -1 means that this value is not necessary since the session will not be
         *          cached.
         */
        WiredTigerSession(WT_CONNECTION* conn, int epoch = -1, uint64_t id = 0);
        ~WiredTigerSession();

        WT_SESSION* getSession() const { return _session; }

        WT_CURSOR* getCursor(const std::string& uri,
                             uint64_t id,
                             bool forRecordStore);
        void releaseCursor(uint64_t id, WT_CURSOR *cursor);

        void closeAllCursors();

        int cursorsOut() const { return _cursorsOut; }

        static uint64_t genCursorId();

        //WiredTigerSession* next() const { return _next; };

        //WiredTigerCursor* setNext(WiredTigerCursor* next) { _next = next };

        /**
         * For "metadata:" cursors. Guaranteed never to collide with genCursorId() ids.
         */
        static const uint64_t kMetadataCursorId = 0;

        uint64_t sessionId;

        // The tag value is incremented every time the object is popped. This prevents ABA
        uint64_t _tag = 0;

    private:
        friend class WiredTigerSessionCache;

        // Vodou for ABA problem
        //typedef std::pair<uint64_t, WiredTigerSession*> TaggedSession;
        typedef TaggedAtomicWrapper<WiredTigerSession> Tagger;

        // The cursor cache is a deque of pairs that contain an ID and cursor
        typedef std::pair<uint64_t, WT_CURSOR*> CursorMap;
        typedef std::deque<CursorMap> CursorCache;

        // Used internally by WiredTigerSessionCache
        int _getEpoch() const { return _epoch; }

        const int _epoch;
        WT_SESSION* _session; // owned
        CursorCache _cursors; // owned
        int _cursorsOut;

        // Sessions are stored as a linked list stack. So each Session needs a pointer
        WiredTigerSession* _next;
    };

    class WiredTigerSessionCache {
    public:

        WiredTigerSessionCache( WiredTigerKVEngine* engine );
        WiredTigerSessionCache( WT_CONNECTION* conn );
        ~WiredTigerSessionCache();

        WiredTigerSession* getSession();
        void releaseSession( WiredTigerSession* session );

        void closeAll();

        void shuttingDown();

        WT_CONNECTION* conn() const { return _conn; }

    private:

        // Vodou for ABA problem
        typedef TaggedAtomicWrapper<WiredTigerSession> Tagger;

        WiredTigerKVEngine* _engine; // not owned, might be NULL
        WT_CONNECTION* _conn; // not owned
        int _epoch;

        // We want to track how many sessions we have out concurrently
        std::atomic_uint_fast64_t _sessionsOut;

        // Regular operations take it in shared mode. Shutdown sets the _shuttingDown flag and
        // then takes it in exclusive mode. This ensures that all threads, which would return
        // sessions to the cache would leak them.
        boost::shared_mutex _shutdownLock;
        AtomicUInt32 _shuttingDown; // Used as boolean - 0 = false, 1 = true

        // This is the most sessions we have ever concurrently used. Its our naive way
        // to know if we should toss a session or return it to cache
        std::atomic_uint_fast64_t _highWaterMark;

        // The sessions are stored as a linked list stack. So we need to track the head
        std::atomic<Tagger> _head;
    };

}
