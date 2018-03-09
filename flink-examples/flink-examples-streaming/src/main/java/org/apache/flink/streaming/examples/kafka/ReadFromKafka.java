/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.kafka;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;


/**
 * Read Strings from Kafka and print them to standard out.
 * Note: On a cluster, DataStream.print() will print to the TaskManager's .out file!
 *
 * Please pass the following arguments to run the example:
 * 	--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 *
 */
public class ReadFromKafka {

	public static void main(String[] args) throws Exception {
		// parse input arguments
		// final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		/*if(parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: Kafka --topic <topic> " +
					"--bootstrap.servers <kafka brokers> --zookeeper.connect <zk quorum> --group.id <some id>");
			return;
		}*/

		Properties consumerConfig = new Properties();
		consumerConfig.put(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		consumerConfig.put(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "fake");
		consumerConfig.put(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "fake");
		consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "TRIM_HORIZON");
		consumerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setMaxParallelism(1);
		// env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds

		// env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		DataStream<String> messageStream = env
				.addSource(new FlinkKinesisConsumer<>(
						"abc",
						new SimpleStringSchema(), consumerConfig
						));

		// write kafka stream to standard out.
		messageStream.print();

		env.execute("Read from Kinesalite example");
	}
}
