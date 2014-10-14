/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommands

#include "mongo/platform/basic.h"

#include "mongo/s/commands/cluster_list_cursors.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/s/cursors.h"

namespace mongo {

CmdClusterListCursors cmdClusterListCursors;

    void CmdClusterListCursors::addRequiredPrivileges(const std::string& dbname,
                                       const BSONObj& cmdObj,
                                       std::vector<Privilege>* out) {
        ActionSet actions;
        actions.addAction(ActionType::listCursors);
        out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
    }

    CmdClusterListCursors::CmdClusterListCursors() : Command( "listCursors" ) { }

    bool CmdClusterListCursors::run(OperationContext* txn,
             const string& dbname,
             BSONObj& cmdObj, int options,
             string& errmsg,
             BSONObjBuilder& result,
             bool fromRepl) {

        if (cmdObj.firstElement().type() != mongo::String ) {
            errmsg = "The argument provided to listCursors must be a collection name";
            return false;
        }
        string ns = dbname + "." + cmdObj.firstElement().valuestrsafe();

        BSONArrayBuilder data;
        uint64_t numObj;

        numObj = cursorCache.enumerateCursors(data, ns);
        if( numObj > 0 ) {
            result.append("cursors", data.arr() );
        } else {
            result.append("cursors", "no open cursors");
        }
        return true;
    }

}
