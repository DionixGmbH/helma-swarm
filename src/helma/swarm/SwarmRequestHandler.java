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
 * JGroups 3.x: local interface for request-response handlers (replaces org.jgroups.blocks.RequestHandler).
 */
package helma.swarm;

import org.jgroups.Message;

public interface SwarmRequestHandler {
    Object handle(Message msg);
}
