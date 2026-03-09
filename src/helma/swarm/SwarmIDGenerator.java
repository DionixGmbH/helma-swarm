/*
 * Helma License Notice
 *
 * The contents of this file are subject to the Helma License
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. A copy of the License is available at
 * http://adele.helma.org/download/helma/license.txt
 *
 * Copyright 1998-2003 Helma Software. All Rights Reserved.
 *
 * $RCSfile$
 * $Author$
 * $Revision$
 * $Date$
 */
package helma.swarm;

import helma.objectmodel.db.IDGenerator;
import helma.objectmodel.db.DbMapping;
import helma.objectmodel.db.NodeManager;
import helma.framework.core.Application;
import org.jgroups.*;
import org.apache.commons.logging.Log;

import java.util.List;

public class SwarmIDGenerator implements IDGenerator, MembershipListener,
                                         SwarmRequestHandler {

    Application app;
    NodeManager nmgr;
    SwarmChannelAdapter adapter;
    Address address;
    volatile View view;
    Log log;

    public void init(Application app) {
        this.app = app;
        nmgr = app.getNodeManager();
        String logName = new StringBuffer("helma.")
                                  .append(app.getName())
                                  .append(".swarm")
                                  .toString();
        log = app.getLogger(logName);
        try {
            adapter = ChannelUtils.getAdapter(app);
            adapter.registerListener(ChannelUtils.IDGEN, this);
            JChannel channel = adapter.getTransport();
            address = channel.getAddress();
            view = channel.getView();
            log.info("SwarmIDGenerator: Got initial view: " + view);
        } catch (Exception e) {
            log.error("SwarmIDGenerator: Error starting/joining channel", e);
            e.printStackTrace();
        }
    }

    public void shutdown() {
        if (adapter != null) {
            adapter.unregisterListener(ChannelUtils.IDGEN);
        }
        ChannelUtils.stopAdapter(app);
    }

    public String generateID(DbMapping dbmap) throws Exception {
        String typeName = dbmap == null ? null : dbmap.getTypeName();
        // try three times to get id from group coordinator
        for (int i = 0; i < 3; i++) {
            List<Address> members = view.getMembers();
            if (members == null || members.isEmpty()) {
                throw new NullPointerException("View has no members: " + view);
            }
            Address coordinator = members.get(0);
            if (address.equals(coordinator)) {
                // we are the group coordinator, use local id generator
                log.info("SwarmIDGenerator: Generating ID locally for " + dbmap);
                return nmgr.doGenerateID(dbmap);
            }
            Message msg = new Message(coordinator).setObject(typeName);
            Object response = null;
            try {
                response = adapter.sendRequest(ChannelUtils.IDGEN, msg, 20000);
                if (response != null) {
                    log.info("SwarmIDGenerator: Received ID " + response + " for " + dbmap);
                }
            } catch (Exception timeout) {
                log.info("SwarmIDGenerator: Message to " + coordinator + " timed out");
            }
            if (response != null) {
                return (String) response;
            }
        }
        throw new RuntimeException("SwarmIDGenerator: Unable to get ID from group coordinator");
    }

    public Object handle(Message msg) {
        List<Address> members = view.getMembers();
        if (members == null || members.isEmpty()) return null;
        if (!address.equals(members.get(0))) return null;
        try {
            Object obj = msg.getObject();
            DbMapping dbmap = obj == null ? null : nmgr.getDbMapping(obj.toString());
            log.debug("SwarmIDGenerator: Processing ID request for " + dbmap);
            return nmgr.doGenerateID(dbmap);
        } catch (Exception x) {
            log.error("SwarmIDGenerator: Error in central ID generator", x);
        }
        return null;
    }

    public void viewAccepted(View view) {
        log.info("SwarmIDGenerator: Got View: " + view);
        this.view = view;
    }

    public void suspect(Address addr) {
        log.info("SwarmIDGenerator: Got suspect: " + addr);
    }

    public void block() {
        log.info("SwarmIDGenerator: Got block");
    }

    public void unblock() {
        log.info("SwarmIDGenerator: Got unblock");
    }
}
