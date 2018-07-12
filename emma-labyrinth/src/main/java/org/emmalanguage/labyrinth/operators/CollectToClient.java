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

package org.emmalanguage.labyrinth.operators;

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.emmalanguage.labyrinth.util.Nothing;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public class CollectToClient<IN> extends BagOperator<IN, Nothing> {

    private final InetAddress hostIp;
    private final int port;

    private transient Socket client;
    private transient OutputStream outputStream;
    private transient DataOutputViewStreamWrapper streamWriter;

    private boolean opened = false;

    public CollectToClient(InetAddress hostIp, int port) {
        //System.out.println("CollectToClient created");
        this.hostIp = hostIp;
        this.port = port;
    }

    @Override
    public void openOutBag() {
        super.openOutBag();

        assert !opened; // Can be opened only once. It should be in the terminal basic block.
        opened = true;

        //System.out.println("CollectToClient openOutBag");

        try {
            client = new Socket(hostIp, port);
            outputStream = client.getOutputStream();

            streamWriter = new DataOutputViewStreamWrapper(outputStream);
            //System.out.println("CollectToClient Connected");

            ObjectOutputStream oos = new ObjectOutputStream(outputStream);
            oos.writeObject(inSer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeInBag(int inputId) {
        super.closeInBag(inputId);
        //System.out.println("CollectToClient.closeInBag");
        try {
            outputStream.flush();
            outputStream.close();
            client.close();
            //System.out.println("CollectToClient.closeInBag flushed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        out.closeBag();
    }

    @Override
    public void pushInElement(IN e, int logicalInputId) {
        super.pushInElement(e, logicalInputId);
        //System.out.println("CollectToClient.pushInElement(" + e + ")");
        try {
            inSer.serialize(e, streamWriter);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
