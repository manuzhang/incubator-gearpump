/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gearpump.external.hbase

import java.io.File

import org.apache.gearpump.Message
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.external.hbase.HBaseSink.HBaseWriterFactory
import org.apache.gearpump.streaming.sink.DataSink
import org.apache.gearpump.streaming.task.TaskContext
import org.apache.gearpump.util.{Constants, FileUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.security.UserProvider
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.security.UserGroupInformation

class HBaseSink private[hbase](
    userConfig: UserConfig, tableName: String, factory: HBaseWriterFactory) extends DataSink {

  private lazy val hbaseWriter = factory.getHBaseWriter(userConfig, tableName)

  def this(userConfig: UserConfig, tableName: String) = {
    this(userConfig, tableName, new HBaseWriterFactory)
  }

  override def open(context: TaskContext): Unit = {}


  override def write(message: Message): Unit = {
    hbaseWriter.put(message.value)
  }

  override def close(): Unit = {
    hbaseWriter.close()
  }

}

object HBaseSink {

  val HBASESINK = "hbasesink"
  val TABLE_NAME = "hbase.table.name"
  val COLUMN_FAMILY = "hbase.table.column.family"
  val COLUMN_NAME = "hbase.table.column.name"
  val HBASE_USER = "hbase.user"

  def apply[T](userConfig: UserConfig, tableName: String): HBaseSink = {
    new HBaseSink(userConfig, tableName)
  }

  private def getConnection(userConfig: UserConfig, configuration: Configuration): Connection = {
    if (UserGroupInformation.isSecurityEnabled) {
      val principal = userConfig.getString(Constants.GEARPUMP_KERBEROS_PRINCIPAL)
      val keytabContent = userConfig.getBytes(Constants.GEARPUMP_KEYTAB_FILE)
      if (principal.isEmpty || keytabContent.isEmpty) {
        val errorMsg = s"HBase is security enabled, user should provide kerberos principal in " +
          s"${Constants.GEARPUMP_KERBEROS_PRINCIPAL} and keytab file " +
          s"in ${Constants.GEARPUMP_KEYTAB_FILE}"
        throw new Exception(errorMsg)
      }
      val keytabFile = File.createTempFile("login", ".keytab")
      FileUtils.writeByteArrayToFile(keytabFile, keytabContent.get)
      keytabFile.setExecutable(false)
      keytabFile.setWritable(false)
      keytabFile.setReadable(true, true)

      UserGroupInformation.setConfiguration(configuration)
      UserGroupInformation.loginUserFromKeytab(principal.get, keytabFile.getAbsolutePath)
      keytabFile.delete()
    }

    val userName = userConfig.getString(HBASE_USER)
    if (userName.isEmpty) {
      ConnectionFactory.createConnection(configuration)
    } else {
      val user = UserProvider.instantiate(configuration)
        .create(UserGroupInformation.createRemoteUser(userName.get))
      ConnectionFactory.createConnection(configuration, user)
    }
  }

  class HBaseWriterFactory extends java.io.Serializable {

    def getHBaseWriter(userConfig: UserConfig, tableName: String): HBaseWriter = {
      new HBaseWriter(userConfig, tableName)
    }
  }

  class HBaseWriter(connection: Connection, tableName: String) {

    private val table: Table = connection.getTable(TableName.valueOf(tableName))

    def this(userConfig: UserConfig, tableName: String) = {
      this(getConnection(userConfig, HBaseConfiguration.create()), tableName)
    }

    def insert(rowKey: String, columnGroup: String, columnName: String, value: String): Unit = {
      insert(Bytes.toBytes(rowKey), Bytes.toBytes(columnGroup),
        Bytes.toBytes(columnName), Bytes.toBytes(value))
    }

    def insert(
        rowKey: Array[Byte], columnGroup: Array[Byte], columnName: Array[Byte], value: Array[Byte])
    : Unit = {
      val put = new Put(rowKey)
      put.addColumn(columnGroup, columnName, value)
      table.put(put)
    }

    def put(msg: Any): Unit = {
      msg match {
        case seq: Seq[Any] =>
          seq.foreach(put)
        case tuple: (_, _, _, _) =>
          tuple._1 match {
            case _: String =>
              insert(tuple._1.asInstanceOf[String], tuple._2.asInstanceOf[String],
                tuple._3.asInstanceOf[String], tuple._4.asInstanceOf[String])
            case _: Array[Byte@unchecked] =>
              insert(tuple._1.asInstanceOf[Array[Byte]], tuple._2.asInstanceOf[Array[Byte]],
                tuple._3.asInstanceOf[Array[Byte]], tuple._4.asInstanceOf[Array[Byte]])
            case _ =>
            // Skip
          }
      }
    }

    def close(): Unit = {
      table.close()
      connection.close()
    }


  }
}
