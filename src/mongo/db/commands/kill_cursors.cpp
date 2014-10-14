/// kill_cursors.cpp

/**
*    Copyright (C) 2014 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,b
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/db/commands.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/clientcursor.h"
#include "mongo/util/log.h"

namespace mongo {

    class KillCursorsCmd : public Command {
    public:
        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual bool adminOnly() const { return false; }
        virtual bool slaveOk() const { return true; }
        virtual bool maintenanceMode() const { return true; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::killCursors);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }
        virtual void help( stringstream& help ) const {
            help << "kill cursors\n";
        }
        KillCursorsCmd() : Command("killCursors") { }

        virtual bool run(OperationContext* txn,
                         const string& dbname,
                         BSONObj& cmdObj,
                         int options,
                         string& errmsg,
                         BSONObjBuilder& result,
                         bool fromRepl) {

            // ensure that the cursors object is an array
            if ( cmdObj.firstElement().type() != mongo::Array) {
                    errmsg = "The argument to killCursors must be an Array of CursorId's";
                    return false;
            }

            int n = 0;
            int numDeleted = 0;

            BSONObj arr = cmdObj.firstElement().embeddedObject();
            n = arr.nFields();

            if ( n <= 0 ) {
                errmsg = "sent invalid number of cursors to kill";
                return false;
            }

            // Safety check carried over from OP_KILL_CURSORS
            if ( n >= 30000 ) {
                errmsg = "cannot kill more than 30000 cursors";
                return false;
            }

            BSONObjIterator it( arr );
            while( it.more() ) {
                CursorId next = it.next().numberLong();
                // next will be 0, if the CursorId value is not a long long
                if( next == 0 ) {
                    LOG(2) << "Received invalid CursorId in killCursors Command";
                    continue;
                }
                if ( CollectionCursorCache::eraseCursorGlobalIfAuthorized(txn, next) ) {
                    LOG(2) << "Killed cursor: " << next;
                    numDeleted++;
                }
                if ( inShutdown() ){
                    break;
                }
            }

            if ( logger::globalLogDomain()->shouldLog(logger::LogSeverity::Debug(1))
                    || numDeleted != n ) {
                LOG( numDeleted == n ? 1 : 0 ) << "killcursors: found "
                    << numDeleted << " of " << n << endl;
            }
            return true;
        }
    };
    static KillCursorsCmd killCursorsCmd;

}
