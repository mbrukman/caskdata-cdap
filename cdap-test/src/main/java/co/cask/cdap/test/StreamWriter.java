/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * This interface defines ways to send data to a stream.
 */
public interface StreamWriter {

  /**
   * Create the stream.
   * @throws java.io.IOException If there is an error creating the stream.
   */
  void createStream() throws IOException;

  /**
   * Sends a UTF-8 encoded string to the stream.
   * @param content Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(String content) throws IOException;

  /**
   * Sends a byte array to the stream. Same as calling {@link #send(byte[], int, int) send(content, 0, content.length)}.
   * @param content Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(byte[] content) throws IOException;

  /**
   * Sends a byte array to the stream.
   * @param content Data to be sent.
   * @param off Offset in the array to start with
   * @param len Number of bytes to sent starting from {@code off}.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(byte[] content, int off, int len) throws IOException;

  /**
   * Sends the content of a {@link java.nio.ByteBuffer} to the stream.
   * @param buffer Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(ByteBuffer buffer) throws IOException;

  /**
   * Sends a UTF-8 encoded string to the stream.
   * @param headers Key-value pairs to be sent as
   *                headers of {@link co.cask.cdap.api.flow.flowlet.StreamEvent StreamEvent}.
   * @param content Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(Map<String, String> headers, String content) throws IOException;

  /**
   * Sends a byte array to the stream. Same as calling {@link #send(byte[], int, int) send(content, 0, content.length)}.
   * @param headers Key-value pairs to be sent as
   *                headers of {@link co.cask.cdap.api.flow.flowlet.StreamEvent StreamEvent}.
   * @param content Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(Map<String, String> headers, byte[] content) throws IOException;

  /**
   * Sends a byte array to the stream.
   * @param headers Key-value pairs to be sent as
   *                headers of {@link co.cask.cdap.api.flow.flowlet.StreamEvent StreamEvent}.
   * @param content Data to be sent.
   * @param off Offset in the array to start with
   * @param len Number of bytes to sent starting from {@code off}.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(Map<String, String> headers, byte[] content, int off, int len) throws IOException;

  /**
   * Sends the content of a {@link java.nio.ByteBuffer} to the stream.
   * @param headers Key-value pairs to be sent as
   *                headers of {@link co.cask.cdap.api.flow.flowlet.StreamEvent StreamEvent}.
   * @param buffer Data to be sent.
   * @throws java.io.IOException If there is error writing to the stream.
   */
  void send(Map<String, String> headers, ByteBuffer buffer) throws IOException;
}
