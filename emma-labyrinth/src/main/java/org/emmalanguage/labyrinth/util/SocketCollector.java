/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.emmalanguage.labyrinth.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class SocketCollector<T> {

    /** Server socket to listen at. */
    private final ServerSocket socket;

    /** Set by the same thread that reads it. */
    private DataInputViewStreamWrapper inStream;

    /** The socket for the specific stream. */
    private Socket connectedSocket;

    private final ArrayList<T> elements = new ArrayList<>();

    public CountDownLatch finishLatch = new CountDownLatch(1);

    SocketCollector() throws IOException {
        //System.out.println("SocketCollector created");
        try {
            socket = new ServerSocket(0, 1);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not open socket to receive back stream results");
        }

        new Thread() {
            @Override
            public void run(){
                try {
                    connectedSocket = socket.accept();
                    inStream = new DataInputViewStreamWrapper(connectedSocket.getInputStream());
                    //System.out.println("SocketCollector accepted");

                    ObjectInputStream ois = new ObjectInputStream(inStream);
                    TypeSerializer<T> serializer = (TypeSerializer<T>) ois.readObject();

                    while (true) {
                        elements.add(serializer.deserialize(inStream));
                    }
                }
                catch (EOFException e) {
                    try {
                        connectedSocket.close();
                    } catch (Throwable ignored) {}

                    try {
                        socket.close();
                    } catch (Throwable ignored) {}

                    finishLatch.countDown();
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }.start();
    }


    public ArrayList<T> getElements() {
        return elements;
    }

    // ------------------------------------------------------------------------
    //  properties
    // ------------------------------------------------------------------------

    /**
     * Returns the port on which the iterator is getting the data. (Used internally.)
     * @return The port
     */
    int getPort() {
        return socket.getLocalPort();
    }
}
