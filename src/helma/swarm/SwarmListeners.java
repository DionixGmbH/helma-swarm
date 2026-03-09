/*
 * Local listener interfaces for JGroups 5.x, decoupling HelmaSwarm from
 * removed org.jgroups.* listener types.
 */
package helma.swarm;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.View;

import java.io.InputStream;
import java.io.OutputStream;

interface SwarmMessageListener {
    void receive(Message msg);

    default void getState(OutputStream output) throws Exception {
    }

    default void setState(InputStream input) throws Exception {
    }
}

interface SwarmMembershipListener {
    default void viewAccepted(View view) {
    }

    default void suspect(Address addr) {
    }

    default void block() {
    }

    default void unblock() {
    }
}

