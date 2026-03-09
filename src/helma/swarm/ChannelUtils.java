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

import org.jgroups.*;
import org.w3c.dom.*;
import helma.framework.core.Application;
import helma.framework.repository.Repository;
import helma.framework.repository.Resource;
import helma.framework.repository.FileResource;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.WeakHashMap;
import java.util.Iterator;
import java.util.List;
import java.io.File;

public class ChannelUtils {

    // weak hashmap for channel adapters (JGroups 3.x)
    static WeakHashMap adapters = new WeakHashMap();

    // Ids for multiplexing.
    // SwarmSessionManager acts as main listener so it can use state exchange.
    final static Integer CACHE = Integer.valueOf(1);
    final static Integer IDGEN = Integer.valueOf(2);

    static SwarmChannelAdapter getAdapter(Application app)
            throws Exception {
        SwarmChannelAdapter adapter = (SwarmChannelAdapter) adapters.get(app);

        if (adapter == null) {
            SwarmConfig config = new SwarmConfig(app);
            JChannel channel = new JChannel(config.getJGroupsProps());
            String groupName = app.getProperty("swarm.name", app.getName());
            channel.connect(groupName + "_swarm");
            adapter = new SwarmChannelAdapter(channel, app);
            adapter.start();
            adapters.put(app, adapter);
        }

        return adapter;
    }

    static void stopAdapter(Application app) {
        SwarmChannelAdapter adapter = (SwarmChannelAdapter) adapters.remove(app);
        if (adapter != null) {
            JChannel channel = adapter.getTransport();
            if (channel.isConnected())
                channel.disconnect();
            if (channel.isOpen())
                channel.close();
            adapter.stop();
        }
    }

    public static boolean isMaster(Application app) {
        SwarmChannelAdapter adapter = (SwarmChannelAdapter) adapters.get(app);
        if (adapter != null) {
            try {
                JChannel channel = adapter.getTransport();
                View view = channel.getView();
                Address address = channel.getAddress();
                List<Address> members = view.getMembers();
                return !members.isEmpty() && address.equals(members.get(0));
            } catch (Exception x) {
                app.logError("ChannelUtils.isMaster()", x);
            }
        }
        return false;
    }
}

class SwarmConfig {

    // Default stack for JGroups 3.x (used when no swarm.conf or no matching stack).
    // Aligns with the udp stack in swarm.conf (FRAG2, FD, VIEW_SYNC).
    String jGroupsProps =
            "UDP(mcast_addr=224.0.0.132;mcast_port=22024;ip_ttl=32;" +
                "bind_port=48848;port_range=1000;" +
                "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "FD(timeout=10000;max_tries=5;shun=true):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=5000):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "VIEW_SYNC(avg_send_interval=60000):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true):" +
            "FRAG2(frag_size=8192):" +
            "pbcast.STATE_TRANSFER";

    public SwarmConfig (Application app) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

        Resource res = null;

        String conf = app.getProperty("swarm.conf");

        if (conf != null) {
            res = new FileResource(new File(conf));
        } else {
            Iterator reps = app.getRepositories().iterator();
            while (reps.hasNext()) {
                Repository rep = (Repository) reps.next();
                res = rep.getResource("swarm.conf");
                if (res != null)
                    break;
            }
        }

        if (res == null || !res.exists()) {
            app.logEvent("Resource \"" + conf + "\" not found, using defaults");
            return;
        }

        app.logEvent("HelmaSwarm: Reading config from " + res);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(res.getInputStream());
            Element root = document.getDocumentElement();
            NodeList nodes = root.getElementsByTagName("jgroups-stack");

            if (nodes.getLength() == 0) {
                app.logEvent("No JGroups stack found in swarm.conf, using defaults");
            } else {
                NodeList jgroups = null;

                String stackName = app.getProperty("swarm.jgroups.stack", "udp");
                for (int i = 0; i < nodes.getLength(); i++) {
                    Element elem = (Element) nodes.item(i);
                    if (stackName.equalsIgnoreCase(elem.getAttribute("name"))) {
                        jgroups = elem.getChildNodes();
                        break;
                    }
                }
                if (jgroups == null) {
                    app.logEvent("JGroups stack \"" + stackName +
                            "\" not found in swarm.conf, using first element");
                    jgroups = nodes.item(0).getChildNodes();
                }

                StringBuffer buffer = new StringBuffer();
                for (int i = 0; i < jgroups.getLength(); i++) {
                    Node node = jgroups.item(i);
                    if (! (node instanceof Text)) {
                        continue;
                    }
                    String str = ((Text) node).getData();
                    for (int j = 0; j < str.length(); j++) {
                        char c = str.charAt(j);
                        if (!Character.isWhitespace(c)) {
                            buffer.append(c);
                        }
                    }
                }
                if (buffer.length() > 0) {
                    jGroupsProps = buffer.toString();
                }
            }

        } catch (Exception e) {
            app.logError("HelmaSwarm: Error reading config from " + res, e);
        }
    }

    public String getJGroupsProps() {
        return jGroupsProps;
    }

}
