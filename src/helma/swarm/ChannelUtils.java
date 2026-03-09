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
import helma.framework.core.Application;
import helma.framework.repository.Repository;
import helma.framework.repository.Resource;
import helma.framework.repository.FileResource;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import java.util.WeakHashMap;
import java.util.List;
import java.util.Iterator;
import java.io.File;

public class ChannelUtils {

    // weak hashmap for channel adapters (JGroups 3.x+)
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
            // For JGroups 5.x we must use an XML configuration file name
            // rather than the old protocol stack string syntax.
            JChannel channel = new JChannel(config.getJGroupsConfig());
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

    // Name of the JGroups XML configuration to use with JGroups 5.x.
    // This is ultimately passed to new JChannel(jGroupsConfig).
    private final String jGroupsConfig;

    public SwarmConfig(Application app) {
        String stackName = app.getProperty("swarm.jgroups.stack", "udp");

        // 1) Try to resolve from swarm.conf (app-specific or repository default)
        String fromSwarmConf = resolveFromSwarmConf(app, stackName);

        if (fromSwarmConf != null && fromSwarmConf.length() > 0) {
            jGroupsConfig = fromSwarmConf;
        } else {
            // 2) Fallback to built-in JGroups configs
            if (stackName != null) {
                String s = stackName.trim();
                String lower = s.toLowerCase();
                if ("udp".equals(lower)) {
                    jGroupsConfig = "udp.xml";
                } else if ("tcp".equals(lower)) {
                    jGroupsConfig = "tcp.xml";
                } else {
                    // Assume the property directly names a JGroups XML config
                    // (either on the classpath or as a filesystem path)
                    jGroupsConfig = s;
                }
            } else {
                jGroupsConfig = "udp.xml";
            }
        }
    }

    public String getJGroupsConfig() {
        return jGroupsConfig;
    }

    /**
     * Resolve the JGroups configuration name/path from swarm.conf.
     * This keeps compatibility with per-app swarm.conf while being JGroups 5–friendly:
     * each &lt;jgroups-stack&gt; should supply either a 'config' attribute
     * or textual content naming a JGroups XML config (e.g. "udp.xml", "tcp.xml",
     * or "my-swarm-udp.xml").
     */
    private String resolveFromSwarmConf(Application app, String stackName) {
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
                if (res != null) {
                    break;
                }
            }
        }

        if (res == null || !res.exists()) {
            // No swarm.conf – caller will fall back to defaults.
            return null;
        }

        app.logEvent("HelmaSwarm: Reading JGroups stack config from " + res);

        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(res.getInputStream());
            Element root = document.getDocumentElement();
            NodeList nodes = root.getElementsByTagName("jgroups-stack");

            if (nodes.getLength() == 0) {
                app.logEvent("No <jgroups-stack> elements found in swarm.conf");
                return null;
            }

            String wanted = (stackName != null) ? stackName : "udp";
            String selectedConfig = null;

            for (int i = 0; i < nodes.getLength(); i++) {
                Element elem = (Element) nodes.item(i);
                String nameAttr = elem.getAttribute("name");
                if (nameAttr == null) {
                    continue;
                }
                if (!wanted.equalsIgnoreCase(nameAttr)) {
                    continue;
                }

                // Prefer explicit config="..." attribute.
                String configAttr = elem.getAttribute("config");
                if (configAttr != null && configAttr.trim().length() > 0) {
                    selectedConfig = configAttr.trim();
                    break;
                }

                // Fallback: use trimmed textual content as the config name/path.
                StringBuilder buf = new StringBuilder();
                NodeList children = elem.getChildNodes();
                for (int j = 0; j < children.getLength(); j++) {
                    Node node = children.item(j);
                    if (node instanceof Text) {
                        buf.append(((Text) node).getData());
                    }
                }
                String text = buf.toString().trim();
                if (text.length() > 0) {
                    selectedConfig = text;
                }
                break;
            }

            if (selectedConfig == null || selectedConfig.length() == 0) {
                app.logEvent("JGroups stack \"" + wanted +
                             "\" not found or has no config in swarm.conf");
            }
            return selectedConfig;

        } catch (Exception e) {
            app.logError("HelmaSwarm: Error reading JGroups stack config from " + res, e);
            return null;
        }
    }

}
