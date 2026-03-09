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
 * JGroups 3.x compatibility: replaces PullPushAdapter with a channel adapter
 * that multiplexes by key and supports request-response for ID generation.
 */
package helma.swarm;

import org.jgroups.*;
import helma.framework.core.Application;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Adapter that replaces JGroups 2.x PullPushAdapter for 3.x.
 * Multiplexes messages by key and supports request-response for IDGEN.
 */
public class SwarmChannelAdapter {

    private final JChannel channel;
    private final Application app;
    private MessageListener mainListener;
    private final Map<Object, Object> listeners = new HashMap<>(); // key -> MessageListener or RequestHandler
    private final Map<Object, BlockingQueue<Object>> pendingRequests = new HashMap<>();
    private final Object pendingLock = new Object();

    public SwarmChannelAdapter(JChannel channel, Application app) {
        this.channel = channel;
        this.app = app;
    }

    public void start() throws Exception {
        channel.setReceiver(new Receiver() {
            @Override
            public void receive(Message msg) {
                Object o = msg.getObject();
                if (!(o instanceof KeyedPayload)) {
                    if (mainListener != null) {
                        mainListener.receive(msg);
                    }
                    return;
                }
                KeyedPayload kp = (KeyedPayload) o;
                Object listener = listeners.get(kp.key);
                if (listener == null) {
                    if (mainListener != null) {
                        mainListener.receive(wrapPayload(msg, kp.payload));
                    }
                    return;
                }
                if (kp.payload instanceof RequestPayload) {
                    RequestPayload req = (RequestPayload) kp.payload;
                    if (listener instanceof SwarmRequestHandler) {
                        Message reqMsg = wrapPayload(msg, req.payload);
                        Object response = ((SwarmRequestHandler) listener).handle(reqMsg);
                        try {
                            channel.send(new Message(msg.getSrc()).setObject(new KeyedPayload(kp.key, new ResponsePayload(req.requestId, response))));
                        } catch (Exception e) {
                            app.logError("SwarmChannelAdapter: send response", e);
                        }
                    }
                    return;
                }
                if (kp.payload instanceof ResponsePayload) {
                    ResponsePayload resp = (ResponsePayload) kp.payload;
                    synchronized (pendingLock) {
                        BlockingQueue<Object> q = pendingRequests.get(resp.requestId);
                        if (q != null) {
                            q.offer(resp.result);
                            pendingRequests.remove(resp.requestId);
                        }
                    }
                    return;
                }
                if (listener instanceof MessageListener) {
                    ((MessageListener) listener).receive(wrapPayload(msg, kp.payload));
                }
            }

            @Override
            public void getState(OutputStream output) throws Exception {
                if (mainListener != null) {
                    mainListener.getState(output);
                }
            }

            @Override
            public void setState(InputStream input) throws Exception {
                if (mainListener != null) {
                    mainListener.setState(input);
                }
            }

            @Override
            public void viewAccepted(View view) {
                if (mainListener instanceof MembershipListener) {
                    ((MembershipListener) mainListener).viewAccepted(view);
                }
                for (Object l : listeners.values()) {
                    if (l instanceof MembershipListener) {
                        ((MembershipListener) l).viewAccepted(view);
                    }
                }
            }

            @Override
            public void suspect(Address addr) {
            }

            @Override
            public void block() {
                if (mainListener instanceof MembershipListener) {
                    ((MembershipListener) mainListener).block();
                }
                for (Object l : listeners.values()) {
                    if (l instanceof MembershipListener) {
                        ((MembershipListener) l).block();
                    }
                }
            }

            @Override
            public void unblock() {
                if (mainListener instanceof MembershipListener) {
                    ((MembershipListener) mainListener).unblock();
                }
                for (Object l : listeners.values()) {
                    if (l instanceof MembershipListener) {
                        ((MembershipListener) l).unblock();
                    }
                }
            }
        });
    }

    public void stop() {
        // no-op; channel disconnect/close done by ChannelUtils
    }

    public void setListener(MessageListener listener) {
        this.mainListener = listener;
    }

    public void registerListener(Object key, Object listener) {
        listeners.put(key, listener);
    }

    public void unregisterListener(Object key) {
        listeners.remove(key);
    }

    public void send(Object key, Message msg) throws Exception {
        Object payload = msg.getObject();
        channel.send(new Message(msg.getDest()).setObject(new KeyedPayload(key, payload)));
    }

    /** Request-response: send request and block for response (for IDGEN). */
    public Object sendRequest(Object key, Message msg, long timeoutMs) throws Exception {
        Object requestId = UUID.randomUUID();
        BlockingQueue<Object> queue = new LinkedBlockingQueue<>(1);
        synchronized (pendingLock) {
            pendingRequests.put(requestId, queue);
        }
        try {
            channel.send(new Message(msg.getDest()).setObject(new KeyedPayload(key, new RequestPayload(requestId, msg.getObject()))));
            Object result = queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            synchronized (pendingLock) {
                pendingRequests.remove(requestId);
            }
        }
    }

    public JChannel getTransport() {
        return channel;
    }

    private static Message wrapPayload(Message orig, Object payload) {
        Message m = new Message(orig.getDest()).setObject(payload);
        m.setSrc(orig.getSrc());
        return m;
    }

    /** Wrapper so receivers can dispatch by key. */
    public static class KeyedPayload implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Object key;
        public final Object payload;

        public KeyedPayload(Object key, Object payload) {
            this.key = key;
            this.payload = payload;
        }
    }

    public static class RequestPayload implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Object requestId;
        public final Object payload;

        public RequestPayload(Object requestId, Object payload) {
            this.requestId = requestId;
            this.payload = payload;
        }
    }

    public static class ResponsePayload implements Serializable {
        private static final long serialVersionUID = 1L;
        public final Object requestId;
        public final Object result;

        public ResponsePayload(Object requestId, Object result) {
            this.requestId = requestId;
            this.result = result;
        }
    }
}
